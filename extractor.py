"""
Stage 1: Extraction Engine with Automatic PDF Renaming
Extracts climate risk data and renames PDFs based on extracted metadata
"""
import json
import time
import uuid
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from openai import AzureOpenAI
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient

from config import Config
from indexing import DocumentIndexer
from document_id_generator import generate_document_id, update_document_id_after_extraction
from question_batcher import QUESTION_BATCHES, get_questions_for_batch, should_skip_remaining_batches
from questions_loader import load_questions_from_file

class ClimateRiskExtractor:
    """Main extraction engine with automatic PDF renaming"""

    def __init__(self, rename_pdfs: bool = True, backup_originals: bool = True):
        """
        Initialize extractor

        Args:
            rename_pdfs: Automatically rename PDFs after extraction
            backup_originals: Keep backup of original PDFs
        """
        Config.validate()
        Config.create_folders()

        self.rename_pdfs = rename_pdfs
        self.backup_originals = backup_originals

        # Create backup folder if needed
        if self.backup_originals:
            self.backup_folder = Path("documents_original_backup")
            self.backup_folder.mkdir(exist_ok=True)

        self.indexer = DocumentIndexer()

        self.openai_client = AzureOpenAI(
            api_key=Config.AZURE_OPENAI_KEY,
            api_version=Config.OPENAI_API_VERSION,
            azure_endpoint=Config.AZURE_OPENAI_ENDPOINT
        )

        search_credential = AzureKeyCredential(Config.AZURE_SEARCH_KEY)
        self.search_client = SearchClient(
            endpoint=Config.AZURE_SEARCH_ENDPOINT,
            index_name=Config.AZURE_SEARCH_TEMP_INDEX,
            credential=search_credential
        )

        self.questions = {}
        self.documents_log_path = Path(Config.MASTER_FOLDER) / "documents.csv"
        self.processed_docs = self._load_processed_docs()

        # Track renamed files
        self.rename_log_path = Path(Config.LOGS_FOLDER) / "renamed_files.csv"
        self._initialize_rename_log()

    def load_questions(self, questions_file: str):
        """Load questions from file"""
        self.questions = load_questions_from_file(questions_file)
        print(f"✓ Loaded {len(self.questions)} questions")

    def _initialize_rename_log(self):
        """Create rename log CSV"""
        if not self.rename_log_path.exists():
            self.rename_log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.rename_log_path, 'w', encoding='utf-8') as f:
                f.write("timestamp,original_filename,new_filename,document_id,company,year,type\n")

    def _generate_new_filename(self, q1_data: Dict) -> str:
        """
        Generate descriptive filename from extracted metadata

        Args:
            q1_data: Q1 extraction results

        Returns:
            New filename (e.g., "Nestle_Annual_Report_2024.pdf")
        """
        company = q1_data.get("company_name", "Unknown_Company")
        year = q1_data.get("publication_year", "0000")
        report_type = q1_data.get("report_type", "Report")

        # Clean strings for filename
        import re
        company_clean = re.sub(r'[^a-zA-Z0-9]', '_', str(company))
        report_clean = re.sub(r'[^a-zA-Z0-9]', '_', str(report_type))

        # Shorten if too long
        company_clean = company_clean[:30]
        report_clean = report_clean[:20]

        return f"{company_clean}_{report_clean}_{year}.pdf"

    def _rename_pdf(self, original_path: Path, q1_data: Dict, doc_id: str) -> Optional[Path]:
        """
        Rename PDF file based on extracted metadata

        Args:
            original_path: Original PDF path
            q1_data: Extracted Q1 data (company, title, year)
            doc_id: Document ID

        Returns:
            New path if renamed, None if failed
        """
        if not self.rename_pdfs:
            return None

        try:
            # Generate new filename
            new_filename = self._generate_new_filename(q1_data)
            new_path = original_path.parent / new_filename

            # Check if target already exists
            if new_path.exists() and new_path != original_path:
                # Add counter to avoid collision
                counter = 1
                stem = new_path.stem
                while new_path.exists():
                    new_filename = f"{stem}_{counter}.pdf"
                    new_path = original_path.parent / new_filename
                    counter += 1

            # Backup original if requested
            if self.backup_originals and original_path.exists():
                backup_path = self.backup_folder / original_path.name
                shutil.copy2(original_path, backup_path)
                print(f"  📦 Backed up to: {backup_path}")

            # Rename the file
            if original_path != new_path:
                original_path.rename(new_path)
                print(f"  ✏️  Renamed to: {new_filename}")

                # Log the rename
                self._log_rename(
                    original_path.name,
                    new_filename,
                    doc_id,
                    q1_data.get("company_name", ""),
                    q1_data.get("publication_year", ""),
                    q1_data.get("report_type", "")
                )

                return new_path

            return original_path

        except Exception as e:
            print(f"  ⚠️ Could not rename file: {e}")
            return None

    def _log_rename(self, original: str, new: str, doc_id: str, company: str, year: str, doc_type: str):
        """Log file rename to CSV"""
        ts = datetime.now().isoformat()
        with open(self.rename_log_path, 'a', encoding='utf-8') as f:
            f.write(f'{ts},"{original}","{new}","{doc_id}","{company}",{year},"{doc_type}"\n')

    def _load_processed_docs(self) -> set:
        """Load list of already processed documents"""
        if not self.documents_log_path.exists():
            self._initialize_documents_csv()
            return set()

        processed = set()
        with open(self.documents_log_path, 'r', encoding='utf-8') as f:
            for line in f.readlines()[1:]:
                if line.strip():
                    doc_id = line.split(',')[0].strip('"')
                    processed.add(doc_id)
        return processed

    def _initialize_documents_csv(self):
        """Create documents.csv with header"""
        header = "document_id,company_name,report_title,publication_year,reporting_period,report_type,is_relevant,document_type,skip_further_extraction,processing_status,processing_timestamp,file_name,confidence\n"
        self.documents_log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.documents_log_path, 'w', encoding='utf-8') as f:
            f.write(header)

    def _search_context(self, session_id: str, query: str, top_k: int = 15) -> List[Dict]:
        """Search for relevant chunks in ephemeral index"""
        try:
            response = self.openai_client.embeddings.create(
                input=query[:8191],
                model=Config.AZURE_EMBEDDING_DEPLOYMENT
            )
            query_embedding = response.data[0].embedding

            results = self.search_client.search(
                search_text=None,
                vector_queries=[{
                    "kind": "vector",
                    "vector": query_embedding,
                    "fields": "embedding",
                    "k": top_k
                }],
                filter=f"session_id eq '{session_id}'",
                select=["content", "chunk_index"],
                top=top_k
            )

            return [{"content": r["content"], "chunk_index": r["chunk_index"], 
                    "score": r.get("@search.score", 0.0)} for r in results]

        except Exception as e:
            print(f" ⚠️ Search error: {e}")
            return []

    def _call_llm_batch(self, context_chunks: List[Dict], questions: List[Dict]) -> Dict:
        """Call LLM with multiple questions at once"""
        context_text = "\n\n".join([
            f"[Chunk {c['chunk_index']}] {c['content']}"
            for c in context_chunks[:Config.MAX_SEARCH_RESULTS]
        ])

        questions_text = "\n\n".join([f"{q['id']}: {q['text']}" for q in questions])

        system_prompt = """You are a climate risk data extraction specialist.

CRITICAL: Return ONLY a valid JSON object. No markdown, no explanations.

Instructions:
1. Return ONE JSON object with keys Q0, Q1, Q2, etc.
2. Follow the exact output schema in each question
3. If not found, set found: false
4. Include source_page numbers
5. Extract verbatim text for source_text fields

Example: {"Q0": {"is_relevant": true, ...}, "Q1": {"company_name": "...", ...}}"""

        user_prompt = f"""Document excerpts:\n{context_text}\n\nQuestions:\n{questions_text}\n\nReturn JSON:"""

        try:
            response = self.openai_client.chat.completions.create(
                model=Config.AZURE_OPENAI_DEPLOYMENT,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,
                max_tokens=4000,
                timeout=120.0
            )

            response_text = response.choices[0].message.content.strip()

            # Clean markdown
            if "```" in response_text:
                for part in response_text.split("```"):
                    if part.strip().startswith("json"):
                        response_text = part[4:].strip()
                    elif part.strip().startswith("{"):
                        response_text = part.strip()

            return json.loads(response_text)
        except json.JSONDecodeError:
            print(f" ⚠️ JSON parse error")
            return {q['id']: {"error": "parse_failed", "found": False} for q in questions}
        except Exception as e:
            print(f" ⚠️ LLM error: {str(e)[:100]}")
            return {q['id']: {"error": str(e)[:200], "found": False} for q in questions}

    def extract_document(self, pdf_path: str, skip_if_processed: bool = True) -> Optional[str]:
        """Extract climate risk data from a single document"""
        pdf_path_obj = Path(pdf_path)
        filename = pdf_path_obj.name
        temp_doc_id = generate_document_id(pdf_path)

        if skip_if_processed and temp_doc_id in self.processed_docs:
            print(f"⏭️  {filename} (already processed)")
            return None

        print(f"\n{'='*70}")
        print(f"📄 {filename}")
        print(f"{'='*70}")

        session_id = f"ephemeral_{uuid.uuid4().hex}"
        start_time = time.time()

        try:
            # Index
            print(f"[1/5] Indexing...")
            if not self.indexer.index_document(pdf_path, session_id):
                self._log_failed(temp_doc_id, filename)
                return None

            # Get context
            print(f"[2/5] Retrieving context...")
            context = self._search_context(session_id, "climate risk adaptation vulnerability assessment")
            print(f"✓ {len(context)} chunks")

            # Extract
            print(f"[3/5] Extracting ({len(QUESTION_BATCHES)} batches)...")
            all_results = {}

            for batch_id, batch_config in QUESTION_BATCHES.items():
                batch_qs = get_questions_for_batch(batch_id, self.questions)
                if not batch_qs:
                    continue

                print(f"  → {batch_config['name']}")
                batch_results = self._call_llm_batch(context, batch_qs)
                all_results.update(batch_results)

                # Q0 gate
                if batch_id == "batch_1_metadata" and should_skip_remaining_batches(batch_results):
                    print(f"  ⚠️ Not relevant (Q0 gate)")
                    final_id = update_document_id_after_extraction(temp_doc_id, batch_results.get("Q1", {}))
                    self._save_results(final_id, all_results)
                    self._log_from_results(final_id, filename, all_results, "skipped")
                    self.indexer.delete_by_session(session_id)
                    return final_id

                time.sleep(0.5)

            # Finalize
            print(f"[4/5] Finalizing...")
            final_id = update_document_id_after_extraction(temp_doc_id, all_results.get("Q1", {}))
            self._save_results(final_id, all_results)
            self._log_from_results(final_id, filename, all_results, "completed")

            # RENAME PDF
            print(f"[5/5] Renaming PDF...")
            if all_results.get("Q1"):
                new_path = self._rename_pdf(pdf_path_obj, all_results["Q1"], final_id)

            # Cleanup
            self.indexer.delete_by_session(session_id)

            elapsed = time.time() - start_time
            print(f"✓ Complete ({elapsed:.1f}s) → {final_id}.json")

            return final_id

        except Exception as e:
            print(f"❌ Error: {e}")
            self._log_failed(temp_doc_id, filename)
            try:
                self.indexer.delete_by_session(session_id)
            except:
                pass
            return None

    def _save_results(self, doc_id: str, results: Dict):
        """Save to JSON"""
        output_path = Path(Config.RAW_FOLDER) / f"{doc_id}.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

    def _log_failed(self, doc_id: str, filename: str):
        """Log failed document"""
        ts = datetime.now().isoformat()
        row = f'"{doc_id}","unknown","unknown",0,"unknown","unknown",false,"unknown",true,failed,{ts},"{filename}",0.0\n'
        with open(self.documents_log_path, 'a', encoding='utf-8') as f:
            f.write(row)
        self.processed_docs.add(doc_id)

    def _log_from_results(self, doc_id: str, filename: str, results: Dict, status: str):
        """Log with metadata"""
        ts = datetime.now().isoformat()
        q0 = results.get("Q0", {})
        q1 = results.get("Q1", {})

        company = str(q1.get("company_name", "unknown")).replace(',', ';').replace('"', '')[:100]
        title = str(q1.get("report_title", "unknown")).replace(',', ';').replace('"', '')[:200]
        year = q1.get("publication_year", 0)
        is_rel = q0.get("is_relevant", False)
        doc_type = str(q0.get("document_type", "unknown")).replace(',', ';')[:100]

        row = f'"{doc_id}","{company}","{title}",{year},"unknown","unknown",{is_rel},"{doc_type}",false,{status},{ts},"{filename}",{q0.get("confidence", 0.0)}\n'
        with open(self.documents_log_path, 'a', encoding='utf-8') as f:
            f.write(row)
        self.processed_docs.add(doc_id)

    def extract_batch(self, documents_folder: str, incremental: bool = True):
        """Extract all documents"""
        folder = Path(documents_folder)
        pdfs = list(folder.glob("*.pdf"))

        print(f"\n{'='*70}")
        print("STAGE 1: EXTRACTION WITH AUTO-RENAME")
        print(f"{'='*70}")
        print(f"PDFs found: {len(pdfs)}")
        print(f"Auto-rename: {self.rename_pdfs}")
        print(f"Backup originals: {self.backup_originals}")
        print(f"Incremental: {incremental}")

        success, skipped, failed = 0, 0, 0

        for i, pdf in enumerate(pdfs, 1):
            print(f"\n[{i}/{len(pdfs)}]")
            result = self.extract_document(str(pdf), skip_if_processed=incremental)

            if result:
                success += 1
            elif incremental:
                skipped += 1
            else:
                failed += 1

        print(f"\n{'='*70}")
        print(f"✓ Success: {success} | ⏭️ Skipped: {skipped} | ❌ Failed: {failed}")

        if self.rename_pdfs:
            print(f"\n📝 Rename log: {self.rename_log_path}")
            if self.backup_originals:
                print(f"📦 Original backups: {self.backup_folder}")

        print(f"\nNext: python stage2_transform.py")
