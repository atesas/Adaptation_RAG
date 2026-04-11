# =============================================================================
# query.py
# Free-form RAG Q&A over PDF documents.
#
# Three main workflows:
#   1. Index:     python query.py --index              (one-time setup)
#   2. Query:    python query.py --run                (answer all questions)
#   3. Ask:      python query.py --ask "question?"   (single question)
#
# Also supports:
#   - Query + taxonomy suggestions: python query.py --run --suggest
#   - Check indexed status: python query.py --status
# =============================================================================
import argparse
import asyncio
import csv
import hashlib
import json
import sys
import textwrap
from datetime import datetime
from pathlib import Path
from typing import Optional

# ── Constants ──────────────────────────────────────────────────────────────────
INDEX_NAME = "pdf-qa-chunks"
CHUNK_SIZE = 1500
CHUNK_OVERLAP = 200
QUESTIONS_FILE = Path("questions.txt")
DOCUMENTS_DIR = Path("documents")
RESULTS_DIR = Path("results")
METADATA_FILE = RESULTS_DIR / "document_index.json"


# ── Logging setup ──────────────────────────────────────────────────────────────
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("query")

# Suppress verbose Azure SDK logs
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core").setLevel(logging.WARNING)
logging.getLogger("azure.search").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)

from azure.core.credentials import AzureKeyCredential
from azure.search.documents.aio import SearchClient as AsyncSearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    HnswAlgorithmConfiguration,
    SearchableField,
    SearchField,
    SearchFieldDataType,
    SearchIndex,
    SimpleField,
    VectorSearch,
    VectorSearchProfile,
)
from azure.search.documents.models import VectorizedQuery
from openai import AsyncAzureOpenAI

import config
from adapters.corporate_pdf import CorporatePDFAdapter

# ── Constants ──────────────────────────────────────────────────────────────────
INDEX_NAME = "pdf-qa-chunks"
CHUNK_SIZE = 1500
CHUNK_OVERLAP = 200
QUESTIONS_FILE = Path("questions.txt")
DOCUMENTS_DIR = Path("documents")
RESULTS_DIR = Path("results")
PROGRESS_FILE = RESULTS_DIR / ".progress.json"


# ── Document Metadata Tracking ────────────────────────────────────────────────
def load_metadata() -> dict:
    """Load document index metadata."""
    if METADATA_FILE.exists():
        return json.loads(METADATA_FILE.read_text(encoding="utf-8"))
    return {
        "query_index": {
            "index_name": INDEX_NAME,
            "updated_at": None,
            "documents": {},
        }
    }


def save_metadata(data: dict) -> None:
    """Save document index metadata."""
    data["query_index"]["updated_at"] = datetime.utcnow().isoformat()
    METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    METADATA_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def update_document_metadata(doc_name: str, info: dict) -> None:
    """Update metadata for a specific document."""
    data = load_metadata()
    if "query_index" not in data:
        data["query_index"] = {"index_name": INDEX_NAME, "documents": {}}
    if "documents" not in data["query_index"]:
        data["query_index"]["documents"] = {}
    data["query_index"]["documents"][doc_name] = {
        "indexed": True,
        "first_indexed": data["query_index"]["documents"].get(doc_name, {}).get("first_indexed") or datetime.utcnow().isoformat(),
        "last_indexed": datetime.utcnow().isoformat(),
        "chunk_count": info.get("chunks", 0),
        "char_count": info.get("chars", 0),
        "has_ocr": info.get("has_ocr", False),
    }
    save_metadata(data)


def print_status() -> None:
    """Print indexed documents status."""
    data = load_metadata()
    idx = data.get("query_index", {})
    docs = idx.get("documents", {})
    
    print(f"\n{'='*72}")
    print(f"Query Index: {idx.get('index_name', INDEX_NAME)}")
    print(f"Updated: {idx.get('updated_at', 'Never')}")
    print(f"{'='*72}")
    
    if not docs:
        print("\nNo documents indexed yet.")
        print("Run 'python query.py --index' to index documents.")
        return
    
    print(f"\nIndexed Documents ({len(docs)}):")
    for doc_name, info in sorted(docs.items()):
        print(f"  - {doc_name}")
        print(f"      Chunks: {info.get('chunk_count', 0)}")
        print(f"      Chars: {info.get('char_count', 0):,}")
        print(f"      OCR: {'Yes' if info.get('has_ocr') else 'No'}")
        print(f"      Last indexed: {info.get('last_indexed', 'N/A')}")


# ── Chunk index schema ────────────────────────────────────────────────────────
def _chunk_index_schema() -> SearchIndex:
    fields = [
        SimpleField(name="chunk_id", type=SearchFieldDataType.String, key=True, filterable=True),
        SimpleField(name="doc_id", type=SearchFieldDataType.String, filterable=True),
        SearchableField(name="text", type=SearchFieldDataType.String),
        SimpleField(name="title", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="source_path", type=SearchFieldDataType.String, filterable=True),
        SimpleField(name="chunk_index", type=SearchFieldDataType.Int32, filterable=False),
        SearchField(
            name="embedding",
            type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
            searchable=True,
            vector_search_dimensions=3072,
            vector_search_profile_name="default",
        ),
    ]
    vector_search = VectorSearch(
        algorithms=[HnswAlgorithmConfiguration(name="default")],
        profiles=[VectorSearchProfile(name="default", algorithm_configuration_name="default")],
    )
    return SearchIndex(name=INDEX_NAME, fields=fields, vector_search=vector_search)


# ── Helpers ────────────────────────────────────────────────────────────────────
def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    """Split text into overlapping chunks."""
    if len(text) <= chunk_size:
        return [text]
    
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        if end >= len(text):
            chunks.append(text[start:])
            break
        
        window = text[start:end]
        last_newline = window.rfind("\n\n")
        if last_newline > chunk_size * 0.5:
            actual_end = start + last_newline
        else:
            last_period = window.rfind(". ")
            if last_period > chunk_size * 0.5:
                actual_end = start + last_period + 1
            else:
                actual_end = end
        
        chunks.append(text[start:actual_end].strip())
        start = actual_end - overlap if actual_end - overlap > start else actual_end
    
    return [c for c in chunks if c]


def _make_chunk_id(doc_id: str, chunk_index: int) -> str:
    """Create URL-safe chunk ID from document name."""
    import re
    # Remove/replace invalid characters: keep only alphanumeric, underscore, dash
    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', doc_id)
    # Collapse multiple underscores
    safe_name = re.sub(r'_+', '_', safe_name)
    # Trim to reasonable length
    if len(safe_name) > 50:
        import hashlib
        safe_name = safe_name[:30] + "_" + hashlib.md5(doc_id.encode()).hexdigest()[:8]
    return f"{safe_name}_chunk_{chunk_index}"


def load_questions(questions_file: Path = QUESTIONS_FILE) -> list[str]:
    """Load questions from text file."""
    if not questions_file.exists():
        print(f"Questions file not found: {questions_file}")
        return []
    
    return [
        line.strip() for line in questions_file.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def get_pdf_files(documents_dir: Path = DOCUMENTS_DIR) -> list[Path]:
    """Get all PDF files from documents directory."""
    return sorted(
        p for p in documents_dir.iterdir()
        if p.suffix.lower() == ".pdf" and p.is_file()
    )


# ── Client helpers ─────────────────────────────────────────────────────────────
def _build_clients():
    from utils.clients import build_clients
    return build_clients(ensure_indexes=False)


def _build_index_client() -> SearchIndexClient:
    config.require_credentials()
    credential = AzureKeyCredential(config.AZURE_SEARCH_KEY)
    return SearchIndexClient(endpoint=config.AZURE_SEARCH_ENDPOINT, credential=credential)


def _build_search_client() -> AsyncSearchClient:
    config.require_credentials()
    credential = AzureKeyCredential(config.AZURE_SEARCH_KEY)
    return AsyncSearchClient(
        endpoint=config.AZURE_SEARCH_ENDPOINT,
        index_name=INDEX_NAME,
        credential=credential,
    )


async def _embed_text(client: AsyncAzureOpenAI, text: str) -> list[float]:
    """Generate embedding for a text string."""
    response = await client.embeddings.create(
        model=config.EMBEDDING_DEPLOYMENT,
        input=text,
    )
    return response.data[0].embedding


# ── Index management ───────────────────────────────────────────────────────────
def ensure_index_exists(index_client: SearchIndexClient) -> None:
    """Create the index if it doesn't exist."""
    existing = {idx.name for idx in index_client.list_indexes()}
    if INDEX_NAME not in existing:
        index_client.create_index(_chunk_index_schema())
        print(f"Created index: {INDEX_NAME}")


def clear_index(index_client: SearchIndexClient) -> None:
    """Delete all documents from the index (keeps the index structure)."""
    try:
        index_client.delete_index(INDEX_NAME)
        print(f"  Deleted index: {INDEX_NAME}")
    except Exception:
        pass
    index_client.create_index(_chunk_index_schema())
    print(f"  Recreated empty index: {INDEX_NAME}")


# ── Index a single document ───────────────────────────────────────────────────
async def index_document(
    pdf_path: Path,
    openai_client: AsyncAzureOpenAI,
    search_client: AsyncSearchClient,
    chunk_size: int = CHUNK_SIZE,
    chunk_overlap: int = CHUNK_OVERLAP,
    use_ocr: bool = True,
) -> dict:
    """
    Index a single PDF document.
    Returns summary with doc_id, title, chunk count.
    
    Uses OCR automatically if extracted text is too short (< 5000 chars).
    """
    adapter = CorporatePDFAdapter({"document_type": "corporate_report"})
    
    # First try PyPDF2
    docs = []
    async for doc in adapter.fetch(str(pdf_path)):
        docs.append(doc)
    
    if not docs:
        return {"error": f"No text extracted from {pdf_path.name}"}
    
    # Combine all text from this PDF
    full_text = "\n\n".join(doc.raw_text for doc in docs)
    title = docs[0].title if docs else pdf_path.stem
    
    # Check if text is too short - use OCR
    MIN_TEXT_LENGTH = 5000
    if use_ocr and len(full_text) < MIN_TEXT_LENGTH:
        log.info(f"Text too short ({len(full_text)} chars), trying OCR...")
        try:
            from utils.ocr import extract_text_with_ocr
            ocr_text = extract_text_with_ocr(pdf_path)
            if ocr_text and len(ocr_text) > len(full_text):
                log.info(f"OCR extracted {len(ocr_text):,} chars (vs {len(full_text)} with PyPDF2)")
                full_text = ocr_text
                title = pdf_path.stem  # Reset title after OCR
        except Exception as e:
            log.warning(f"OCR failed: {e}")
    
    if not full_text.strip():
        return {"error": f"No text extracted from {pdf_path.name} (tried PyPDF2 + OCR)"}
    
    doc_id = hashlib.sha256(pdf_path.name.encode()).hexdigest()[:16]
    
    # Chunk the text
    chunks = chunk_text(full_text, chunk_size, chunk_overlap)
    print(f"  Extracted {len(full_text):,} chars -> {len(chunks)} chunks")
    
    # Index each chunk
    for i, chunk_text_content in enumerate(chunks):
        chunk_id = _make_chunk_id(doc_id, i)
        embedding = await _embed_text(openai_client, chunk_text_content)
        
        doc = {
            "chunk_id": chunk_id,
            "doc_id": doc_id,
            "text": chunk_text_content,
            "title": title,
            "source_path": str(pdf_path),
            "chunk_index": i,
            "embedding": embedding,
        }
        
        await search_client.upload_documents([doc])
    
    return {
        "doc_id": doc_id,
        "title": title,
        "source_path": str(pdf_path),
        "chunks": len(chunks),
        "chars": len(full_text),
    }


# ── Answer a question ─────────────────────────────────────────────────────────
async def answer_question(
    question: str,
    openai_client: AsyncAzureOpenAI,
    search_client: AsyncSearchClient,
    top_k: int = 10,
) -> dict:
    """
    Answer a question using RAG over indexed PDF chunks.
    Returns dict with answer, sources, and metadata.
    """
    # Generate embedding for the question
    question_embedding = await _embed_text(openai_client, question)
    
    # Search for relevant chunks
    vector_query = VectorizedQuery(
        vector=question_embedding,
        k_nearest_neighbors=top_k,
        fields="embedding",
    )
    
    results = await search_client.search(
        search_text=question,
        vector_queries=[vector_query],
        top=top_k,
        select=["chunk_id", "doc_id", "text", "title", "source_path"],
    )
    
    chunks = []
    async for result in results:
        chunks.append({
            "chunk_id": result["chunk_id"],
            "doc_id": result["doc_id"],
            "text": result["text"],
            "title": result["title"],
            "source_path": result["source_path"],
            "score": result["@search.score"],
        })
    
    if not chunks:
        return {
            "answer": "No relevant information found in this document.",
            "sources": [],
            "chunks_retrieved": 0,
        }
    
    # Format chunks for the prompt
    passages_block = "\n\n".join(
        f"[{i+1}] SOURCE: {c['title']}\n"
        f"    {c['text'][:600]}..."
        if len(c['text']) > 600 else
        f"[{i+1}] SOURCE: {c['title']}\n"
        f"    {c['text']}"
        for i, c in enumerate(chunks)
    )
    
    prompt = f"""You are a climate-risk research analyst answering questions from internal analysts
at a food & beverage company. You have been given relevant text chunks extracted from
a single corporate sustainability, annual, or climate-risk report.

Your job:
1. Answer the analyst's question directly and concisely based ONLY on this document.
2. Ground every claim in the provided text chunks — do not invent facts.
3. After each claim, add an inline citation with the source chunk number: [Chunk: N]
4. If multiple chunks say the same thing, cite all relevant chunks.
5. If the document does not contain enough information to answer, say so clearly
   and explain what specific information is missing.

---
ANALYST QUESTION:
{question}

---
RETRIEVED CHUNKS ({len(chunks)} chunks from this document):
{passages_block}

---
Write your answer in plain prose. Use short paragraphs. Lead with the direct
answer, then supporting detail, then caveats. End with a one-line summary of
the evidence quality (e.g. "Strong evidence — 4 relevant passages" or "Weak evidence — only 1 passing mention").
"""
    
    response = await openai_client.chat.completions.create(
        model=config.OUTPUT_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=2000,
    )
    
    answer = (response.choices[0].message.content or "").strip()
    
    return {
        "answer": answer,
        "sources": [{"title": c["title"], "chunk_id": c["chunk_id"]} for c in chunks],
        "chunks_retrieved": len(chunks),
    }


# ── Save results ───────────────────────────────────────────────────────────────
def save_results_json(all_results: list[dict], output_path: Path) -> None:
    """Save all results to JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    output = {
        "generated_at": datetime.utcnow().isoformat(),
        "total_documents": len(all_results),
        "results": all_results,
    }
    
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Saved JSON results to: {output_path}")


def save_results_csv(all_results: list[dict], output_path: Path) -> None:
    """Save all results to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    rows = []
    for doc_result in all_results:
        document = doc_result["document"]
        for q_result in doc_result["answers"]:
            rows.append({
                "document": document["title"],
                "source_path": document["source_path"],
                "question": q_result["question"],
                "answer": q_result["answer"],
                "chunks_retrieved": q_result.get("chunks_retrieved", 0),
            })
    
    if not rows:
        print("No results to save to CSV")
        return
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["document", "source_path", "question", "answer", "chunks_retrieved"])
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Saved CSV results to: {output_path}")


# ── Progress tracking ──────────────────────────────────────────────────────────
def load_progress() -> dict:
    """Load progress from file."""
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
    return {"completed": [], "started_at": None}


def save_progress(progress: dict) -> None:
    """Save progress to file."""
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2), encoding="utf-8")


def mark_completed(progress: dict, document_name: str) -> None:
    """Mark a document as completed."""
    if document_name not in progress["completed"]:
        progress["completed"].append(document_name)
    save_progress(progress)


def clear_progress() -> None:
    """Delete progress file."""
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()


# ── Main run pipeline ──────────────────────────────────────────────────────────
async def run_pipeline(
    single_document: Optional[str] = None,
    chunk_size: int = CHUNK_SIZE,
    chunk_overlap: int = CHUNK_OVERLAP,
    top_k: int = 10,
    resume: bool = False,
    output_prefix: str = "results/qa_results",
    use_ocr: bool = True,
) -> list[dict]:
    """
    Process documents one at a time:
    1. Index the document
    2. Answer all questions
    3. Clear the index
    4. Move to next document
    
    Returns all results.
    """
    questions = load_questions()
    if not questions:
        log.error("No questions found. Exiting.")
        return []
    
    log.info(f"Loaded {len(questions)} questions")
    
    if single_document:
        pdf_files = [DOCUMENTS_DIR / single_document]
        if not pdf_files[0].exists():
            log.error(f"Document not found: {pdf_files[0]}")
            return []
    else:
        pdf_files = get_pdf_files()
    
    if not pdf_files:
        log.error(f"No PDF files found in {DOCUMENTS_DIR}")
        return []
    
    # Handle resume mode
    progress = load_progress()
    if resume:
        completed = progress.get("completed", [])
        pdf_files = [f for f in pdf_files if f.name not in completed]
        log.info(f"Resume mode: skipping {len(completed)} completed documents")
    
    if not pdf_files:
        log.info("All documents already processed!")
        return []
    
    log.info(f"Processing {len(pdf_files)} PDF file(s)")
    
    if not progress.get("started_at"):
        progress["started_at"] = datetime.utcnow().isoformat()
        save_progress(progress)
    
    store, openai_client = _build_clients()
    index_client = _build_index_client()
    ensure_index_exists(index_client)
    
    # Load existing results if resuming
    all_results = []
    existing_json = Path(output_prefix).with_suffix(".json")
    if resume and existing_json.exists():
        try:
            existing_data = json.loads(existing_json.read_text(encoding="utf-8"))
            all_results = existing_data.get("results", [])
            log.info(f"Loaded {len(all_results)} existing results")
        except Exception as e:
            log.warning(f"Could not load existing results: {e}")
    
    start_time = datetime.utcnow()
    completed_count = 0
    
    try:
        for doc_idx, pdf_path in enumerate(pdf_files, 1):
            doc_start = datetime.utcnow()
            log.info(f"{'='*72}")
            log.info(f"Document {doc_idx}/{len(pdf_files)}: {pdf_path.name}")
            log.info(f"{'='*72}")
            
            # Clear the index for this document
            clear_index(index_client)
            search_client = _build_search_client()
            
            try:
                # Step 1: Index this document
                log.info("  Indexing document...")
                doc_info = await index_document(
                    pdf_path=pdf_path,
                    openai_client=openai_client,
                    search_client=search_client,
                    chunk_size=chunk_size,
                    chunk_overlap=chunk_overlap,
                    use_ocr=use_ocr,
                )
                
                if "error" in doc_info:
                    log.error(f"  ERROR: {doc_info['error']}")
                    all_results.append({
                        "document": {"title": pdf_path.stem, "source_path": str(pdf_path)},
                        "error": doc_info["error"],
                        "answers": [],
                    })
                    mark_completed(progress, pdf_path.name)
                    continue
                
                log.info(f"  Indexed: {doc_info['title']} ({doc_info['chunks']} chunks, {doc_info['chars']:,} chars)")
                
                # Step 2: Answer all questions for this document
                log.info(f"  Answering {len(questions)} questions...")
                doc_answers = []
                
                for q_idx, question in enumerate(questions, 1):
                    q_start = datetime.utcnow()
                    log.info(f"    [{q_idx:2d}/{len(questions)}] {question[:60]}...")
                    
                    result = await answer_question(
                        question=question,
                        openai_client=openai_client,
                        search_client=search_client,
                        top_k=top_k,
                    )
                    
                    q_elapsed = (datetime.utcnow() - q_start).total_seconds()
                    log.info(f"            -> {result['chunks_retrieved']} chunks, {q_elapsed:.1f}s")
                    
                    doc_answers.append({
                        "question": question,
                        "answer": result["answer"],
                        "sources": result["sources"],
                        "chunks_retrieved": result["chunks_retrieved"],
                    })
                
                all_results.append({
                    "document": {
                        "title": doc_info["title"],
                        "source_path": doc_info["source_path"],
                        "chunks": doc_info["chunks"],
                        "chars": doc_info["chars"],
                    },
                    "answers": doc_answers,
                })
                
                doc_elapsed = (datetime.utcnow() - doc_start).total_seconds()
                log.info(f"  Completed: {len(doc_answers)} answers in {doc_elapsed:.1f}s")
                
                # Save progress after each document
                mark_completed(progress, pdf_path.name)
                completed_count += 1
                
                # Also save intermediate results (in case of interruption)
                temp_json = Path(output_prefix).with_suffix(".json")
                save_results_json(all_results, temp_json)
                
            finally:
                await search_client.close()
            
            # Estimate remaining time
            docs_remaining = len(pdf_files) - doc_idx
            if completed_count > 0:
                avg_time = (datetime.utcnow() - start_time).total_seconds() / completed_count
                eta_seconds = avg_time * docs_remaining
                eta_minutes = eta_seconds / 60
                log.info(f"  Progress: {doc_idx}/{len(pdf_files)} docs, ETA: {eta_minutes:.0f} min remaining")
            
            print()
    
    finally:
        await store.close()
        await openai_client.close()
    
    # Clear progress file on successful completion
    clear_progress()
    
    total_elapsed = (datetime.utcnow() - start_time).total_seconds()
    log.info(f"Pipeline finished: {completed_count} documents in {total_elapsed/60:.1f} minutes")
    
    return all_results


# ── Ask single question ────────────────────────────────────────────────────────
async def ask_single_question(question: str, top_k: int = 10) -> None:
    """Ask a single question against already-indexed content."""
    store, openai_client = _build_clients()
    search_client = _build_search_client()
    
    try:
        result = await answer_question(
            question=question,
            openai_client=openai_client,
            search_client=search_client,
            top_k=top_k,
        )
        
        print("\n" + "="*72)
        print(f"Question: {question}")
        print("="*72)
        print(textwrap.fill(result["answer"], width=72))
        print(f"\n({result['chunks_retrieved']} chunks retrieved)")
        
    finally:
        await search_client.close()
        await store.close()
        await openai_client.close()


# ── CLI ────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Free-form RAG Q&A over PDF documents",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Index all PDFs (one-time setup)
  python query.py --index

  # Run Q&A on all documents
  python query.py --run

  # Run Q&A on specific document
  python query.py --run --document "Danone_annual_report_2024.pdf"

  # Run Q&A + generate taxonomy suggestions
  python query.py --run --suggest

  # Ask single question across all docs
  python query.py --ask "What climate hazards have been identified?"

  # Ask question filtered to specific document
  python query.py --ask "What climate hazards?" --document "Danone_annual_report_2024.pdf"

  # Check indexed documents status
  python query.py --status

  # Skip OCR (faster but may miss scanned PDFs)
  python query.py --run --no-ocr

Output files:
  results/qa_results.json           - Full Q&A results in JSON
  results/qa_results.csv            - Q&A results in CSV format
  results/taxonomy_suggestions.json - Taxonomy suggestions (with --suggest)
  results/document_index.json       - Document metadata
""",
    )
    
    # Main commands
    parser.add_argument("--index", action="store_true",
                        help="Index all PDFs in documents/ (one-time setup)")
    parser.add_argument("--run", action="store_true",
                        help="Run Q&A on indexed documents")
    parser.add_argument("--ask", "-q", type=str, default=None,
                        help="Ask a single question against indexed content")
    parser.add_argument("--status", action="store_true",
                        help="Show indexed documents status")
    
    # Options
    parser.add_argument("--document", "-d", type=str, default=None,
                        help="Process a specific document (filename)")
    parser.add_argument("--suggest", action="store_true",
                        help="Generate taxonomy suggestions after Q&A")
    parser.add_argument("--top-k", "-k", type=int, default=10,
                        help="Number of chunks to retrieve (default: 10)")
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE,
                        help=f"Chunk size in characters (default: {CHUNK_SIZE})")
    parser.add_argument("--output", "-o", type=str, default=None,
                        help="Output filename prefix (default: results/qa_results)")
    parser.add_argument("--taxonomy", "-t", type=str, default="_design/taxonomy.yaml",
                        help="Path to taxonomy file for suggestions (default: _design/taxonomy.yaml)")
    parser.add_argument("--no-ocr", action="store_true",
                        help="Skip OCR for scanned PDFs")
    
    args = parser.parse_args()
    
    # Handle --status
    if args.status:
        print_status()
        return
    
    # Handle --index
    if args.index:
        log.info("Indexing all PDF documents...\n")
        
        async def do_index():
            from adapters.corporate_pdf import CorporatePDFAdapter
            from utils.clients import build_clients
            
            store, openai_client = build_clients(ensure_indexes=False)
            index_client = _build_index_client()
            
            # Ensure index exists
            existing = {idx.name for idx in index_client.list_indexes()}
            if INDEX_NAME not in existing:
                index_client.create_index(_chunk_index_schema())
                log.info(f"Created index: {INDEX_NAME}")
            
            search_client = _build_search_client()
            adapter = CorporatePDFAdapter({"document_type": "corporate_report"})
            
            pdf_files = sorted(
                p for p in DOCUMENTS_DIR.iterdir()
                if p.suffix.lower() == ".pdf" and p.is_file()
            )
            
            if not pdf_files:
                log.error(f"No PDF files found in {DOCUMENTS_DIR}")
                return
            
            log.info(f"Found {len(pdf_files)} PDF files")
            
            for pdf_path in pdf_files:
                log.info(f"Indexing: {pdf_path.name}")
                try:
                    docs = []
                    async for doc in adapter.fetch(str(pdf_path)):
                        docs.append(doc)
                    
                    if not docs:
                        log.warning(f"No text extracted from {pdf_path.name}")
                        continue
                    
                    full_text = "\n\n".join(doc.raw_text for doc in docs)
                    title = docs[0].title if docs else pdf_path.stem
                    doc_id = pdf_path.name  # Use filename as doc_id
                    
                    chunks = chunk_text(full_text, args.chunk_size, CHUNK_OVERLAP)
                    log.info(f"  Extracted {len(full_text):,} chars -> {len(chunks)} chunks")
                    
                    for i, chunk_text_content in enumerate(chunks):
                        chunk_id = _make_chunk_id(doc_id, i)
                        embedding = await _embed_text(openai_client, chunk_text_content)
                        
                        doc = {
                            "chunk_id": chunk_id,
                            "doc_id": doc_id,
                            "text": chunk_text_content,
                            "title": title,
                            "source_path": str(pdf_path),
                            "chunk_index": i,
                            "embedding": embedding,
                        }
                        await search_client.upload_documents([doc])
                    
                    # Update metadata
                    update_document_metadata(pdf_path.name, {
                        "chunks": len(chunks),
                        "chars": len(full_text),
                        "has_ocr": False,
                    })
                    
                    log.info(f"  Indexed: {len(chunks)} chunks")
                    
                except Exception as e:
                    log.error(f"Error indexing {pdf_path.name}: {e}")
            
            await search_client.close()
            await store.close()
            await openai_client.close()
        
        asyncio.run(do_index())
        log.info("\nIndexing complete!")
        print_status()
        return
    
    # Handle --run
    if args.run:
        log.info("Starting Q&A pipeline...\n")
        output_prefix = args.output or "results/qa_results"
        results = asyncio.run(run_pipeline(
            single_document=args.document,
            chunk_size=args.chunk_size,
            top_k=args.top_k,
            output_prefix=output_prefix,
            use_ocr=not args.no_ocr,
        ))
        
        if results:
            # Save results
            output_prefix = args.output or "results/qa_results"
            output_dir = Path(output_prefix).parent
            output_dir.mkdir(parents=True, exist_ok=True)
            
            json_path = Path(output_prefix).with_suffix(".json")
            csv_path = Path(output_prefix).with_suffix(".csv")
            
            save_results_json(results, json_path)
            save_results_csv(results, csv_path)
            
            print(f"\n{'='*72}")
            print("Q&A Pipeline complete!")
            print(f"  Documents processed: {len(results)}")
            print(f"  Questions per document: {len(results[0]['answers']) if results else 0}")
            print(f"  Output: {json_path}")
            print(f"  Output: {csv_path}")
            
            # Handle --suggest
            if args.suggest:
                print(f"\nGenerating taxonomy suggestions...")
                import subprocess
                result = subprocess.run([
                    sys.executable, "suggest_taxonomy.py",
                    "--input", str(json_path),
                    "--taxonomy", args.taxonomy,
                ])
                if result.returncode == 0:
                    print(f"  Taxonomy suggestions: results/taxonomy_suggestions.json")
    
    # Handle --ask
    elif args.ask:
        asyncio.run(ask_single_question(args.ask, top_k=args.top_k))
    
    # No command - show help
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
