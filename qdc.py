# =============================================================================
# qdc.py — Question-Driven Classification
#
# Alternative ingestion workflow. Instead of:
#   PDF → Stage A (extract everything) → Stage B (classify against taxonomy)
#
# QDC does:
#   Questions → PDF chunks → targeted extraction → classify extracted answers
#
# The output is a QDCResult: structured passages directly answering your
# questions, each classified in the taxonomy. You can compare these results
# to the standard ingest output to see which workflow gives better signal.
#
# Usage (single document, question file):
#   python qdc.py --path documents/Danone_annual_report_2024.pdf \
#                 --questions questions.txt
#
# Usage (questions inline):
#   python qdc.py --path documents/Danone_annual_report_2024.pdf \
#                 --question "What water efficiency targets has the company set?" \
#                 --question "What is the company doing about heat stress on workers?"
#
# Usage (save results to JSON):
#   python qdc.py --path documents/Danone_annual_report_2024.pdf \
#                 --questions questions.txt --output results.json
#
# Usage (also upsert into knowledge store):
#   python qdc.py --path documents/Danone_annual_report_2024.pdf \
#                 --questions questions.txt --upsert
#
# The --questions file is plain text: one question per line, blank lines ignored.
# =============================================================================

import argparse
import asyncio
import csv
import json
import re
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml
from openai import AsyncAzureOpenAI

import config
from knowledge_store import KnowledgeStore
from schemas.passage import (
    ClassifiedPassage,
    IRO_TYPES,
    VALUE_CHAIN_POSITIONS,
    EVIDENCE_QUALITY_LEVELS,
    TIME_HORIZONS,
)
from schemas.validation import ValidationStatus, ReviewPriority
from taxonomy import taxonomy

logger = logging.getLogger(__name__)

# ── Prompt templates ──────────────────────────────────────────────────────────

_EXTRACT_PROMPT = Path("prompts/qdc_extract_v1.txt").read_text(encoding="utf-8")
_CLASSIFY_PROMPT = Path("prompts/qdc_classify_v1.txt").read_text(encoding="utf-8")

# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class QDCPassage:
    """A passage extracted and classified by the QDC workflow."""
    passage_id:    str
    question_id:   str
    question:      str
    text:          str
    page_ref:      Optional[str]
    source_doc_id: str
    source_path:   str
    # Classification fields (filled after classify step)
    category:               Optional[str] = None
    subcategory:            Optional[str] = None
    iro_type:               Optional[str] = None
    value_chain_position:   Optional[str] = None
    evidence_quality:       Optional[str] = None
    time_horizon:           Optional[str] = None
    confidence:             Optional[float] = None
    confidence_rationale:   Optional[str] = None
    entities:               Optional[list] = None
    frameworks_referenced:  Optional[list] = None
    geographic_scope:       Optional[list] = None
    classification_note:    Optional[str] = None


@dataclass
class QDCResult:
    source_path:    str
    questions:      list[str]
    passages:       list[QDCPassage]
    run_at:         str


# ── Chunk text ────────────────────────────────────────────────────────────────

def _chunk_text(text: str, chunk_size: int = 3000, overlap: int = 200) -> list[tuple[int, str]]:
    """Split text into overlapping chunks. Returns list of (char_offset, chunk)."""
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        chunks.append((start, text[start:end]))
        start += chunk_size - overlap
    return chunks


def _parse_json_list(raw: str) -> list:
    """
    Robustly extract a JSON array from a model response.
    Handles: bare arrays, objects wrapping arrays, markdown code blocks.
    """
    # Strip markdown code fences
    raw = re.sub(r"```(?:json)?\s*", "", raw).strip().rstrip("`").strip()

    # Try direct parse first
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            # Look for any list value — try common keys first
            for key in ("passages", "results", "items", "data", "classifications", "extractions"):
                val = parsed.get(key)
                if isinstance(val, list):
                    return val
            # Fall back to first list value found
            for val in parsed.values():
                if isinstance(val, list):
                    return val
        return []
    except json.JSONDecodeError:
        pass

    # Try to find a JSON array anywhere in the text
    match = re.search(r"\[.*\]", raw, re.DOTALL)
    if match:
        try:
            parsed = json.loads(match.group())
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass

    return []


# ── Stage 1: Extract targeted passages from each chunk ───────────────────────

async def _extract_from_chunk(
    chunk_text: str,
    chunk_offset: int,
    source_doc_id: str,
    source_path: str,
    questions: list[dict],   # [{"id": "q1", "text": "..."}]
    openai_client: AsyncAzureOpenAI,
    sem: asyncio.Semaphore,
) -> list[QDCPassage]:
    questions_block = "\n".join(
        f"  [{q['id']}] {q['text']}" for q in questions
    )
    page_range = f"chars {chunk_offset}–{chunk_offset + len(chunk_text)}"
    prompt = (
        _EXTRACT_PROMPT
        .replace("{questions_block}", questions_block)
        .replace("{source_doc_id}", source_doc_id)
        .replace("{page_range}", page_range)
        .replace("{chunk_text}", chunk_text)
    )

    async with sem:
        response = await openai_client.chat.completions.create(
            model=config.STAGE_A_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=2000,
        )

    raw = response.choices[0].message.content.strip()
    items = _parse_json_list(raw)
    if not items:
        if raw and "[]" not in raw:
            logger.warning("QDC extract: no passages parsed for chunk at offset %d", chunk_offset)
        return []

    passages = []
    for item in items:
        if not isinstance(item, dict):
            continue
        text = (item.get("passage") or "").strip()
        if not text:
            continue
        passages.append(QDCPassage(
            passage_id=str(uuid.uuid4()),
            question_id=item.get("question_id", ""),
            question=item.get("question", ""),
            text=text,
            page_ref=item.get("page_ref"),
            source_doc_id=source_doc_id,
            source_path=source_path,
        ))
    return passages


# ── Stage 2: Classify extracted passages ─────────────────────────────────────

def _build_full_taxonomy_excerpt() -> str:
    """Return a YAML excerpt of all top-level taxonomy nodes."""
    import yaml as _yaml
    t = taxonomy._taxonomy   # access raw dict
    excerpt = {k: v for k, v in t.items()
               if isinstance(v, dict) and k not in ("taxonomy_version", "last_updated", "sector_focus")}
    return _yaml.dump(excerpt, allow_unicode=True, sort_keys=False)


async def _classify_batch(
    passages: list[QDCPassage],
    openai_client: AsyncAzureOpenAI,
    sem: asyncio.Semaphore,
    batch_size: int = 10,
) -> list[QDCPassage]:
    """Classify passages in batches. Returns passages with classification fields filled."""

    async def _classify_chunk(batch: list[QDCPassage]) -> list[QDCPassage]:
        passages_block = "\n\n".join(
            f"[passage_id={p.passage_id}] [question_id={p.question_id}]\n"
            f"Question: {p.question}\n"
            f"Text: {p.text}"
            for p in batch
        )
        prompt = (
            _CLASSIFY_PROMPT
            .replace("{iro_types}",             "\n".join(f"  - {v}" for v in IRO_TYPES))
            .replace("{value_chain_positions}",  "\n".join(f"  - {v}" for v in VALUE_CHAIN_POSITIONS))
            .replace("{evidence_quality_levels}", "\n".join(f"  - {v}" for v in EVIDENCE_QUALITY_LEVELS))
            .replace("{taxonomy_excerpt}",       _build_full_taxonomy_excerpt())
            .replace("{n_passages}",             str(len(batch)))
            .replace("{passages_block}",         passages_block)
        )

        async with sem:
            response = await openai_client.chat.completions.create(
                model=config.STAGE_B_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=3000,
            )

        raw = response.choices[0].message.content.strip()
        items = _parse_json_list(raw)
        if not items:
            logger.warning("QDC classify: no classifications parsed in batch")
            return batch

        # Map results back to passages by passage_id
        result_map = {r.get("passage_id"): r for r in items if isinstance(r, dict)}
        for p in batch:
            r = result_map.get(p.passage_id)
            if not r:
                continue
            p.category             = r.get("category")
            p.subcategory          = r.get("subcategory")
            p.iro_type             = r.get("iro_type", "not_specified")
            p.value_chain_position = r.get("value_chain_position", "not_specified")
            p.evidence_quality     = r.get("evidence_quality", "anecdotal")
            p.time_horizon         = r.get("time_horizon", "unspecified")
            p.confidence           = float(r.get("confidence", 0.5))
            p.confidence_rationale = r.get("confidence_rationale", "")
            p.entities             = r.get("entities", [])
            p.frameworks_referenced = r.get("frameworks_referenced", [])
            p.geographic_scope     = r.get("geographic_scope", [])
            p.classification_note  = r.get("classification_note")
        return batch

    # Split into batches and run concurrently
    batches = [passages[i:i+batch_size] for i in range(0, len(passages), batch_size)]
    results = await asyncio.gather(*[_classify_chunk(b) for b in batches])
    return [p for batch in results for p in batch]


# ── Main orchestrator ─────────────────────────────────────────────────────────

async def _process_one_document(
    doc_text: str,
    source_doc_id: str,
    source_path: str,
    q_list: list[dict],
    openai_client: AsyncAzureOpenAI,
    sem: asyncio.Semaphore,
) -> list[QDCPassage]:
    """Extract targeted passages from a single document."""
    chunks = _chunk_text(doc_text)
    tasks = [
        _extract_from_chunk(
            chunk_text=chunk,
            chunk_offset=offset,
            source_doc_id=source_doc_id,
            source_path=source_path,
            questions=q_list,
            openai_client=openai_client,
            sem=sem,
        )
        for offset, chunk in chunks
    ]
    batches = await asyncio.gather(*tasks)
    return [p for batch in batches for p in batch]


async def run_qdc(
    source_path: str,
    questions: list[str],
    openai_client: AsyncAzureOpenAI,
    store: Optional[KnowledgeStore] = None,
    concurrency: int = 5,
    upsert: bool = False,
) -> QDCResult:
    """
    Full QDC pipeline:
    1. Read source PDF/text file OR every PDF/txt in a folder
    2. For each document: chunk → extract targeted passages per question
    3. Deduplicate across all documents
    4. Classify all unique passages
    5. Optionally upsert into knowledge store
    """
    from adapters.corporate_pdf import CorporatePDFAdapter

    adapter = CorporatePDFAdapter(config={})
    q_list = [{"id": f"q{i+1}", "text": q} for i, q in enumerate(questions)]
    sem = asyncio.Semaphore(concurrency)

    # ── Collect raw documents from adapter (handles file OR folder) ───────────
    # Group adapter chunks by source file so each file is one logical document.
    # CorporatePDFAdapter may yield multiple Document objects per file when the
    # file is very large (split by MAX_DOCUMENT_CHARS).
    docs_by_file: dict[str, list] = {}
    async for raw_doc in adapter.fetch(source_path):
        key = raw_doc.source_url  # absolute path to the file
        docs_by_file.setdefault(key, []).append(raw_doc)

    if not docs_by_file:
        raise ValueError(f"No text extracted from {source_path}")

    logger.info("QDC: %d file(s) loaded, %d questions", len(docs_by_file), len(q_list))

    # ── Extract from each file concurrently ───────────────────────────────────
    async def _process_file(file_path: str, chunks: list) -> list[QDCPassage]:
        # Concatenate chunks for this file (they're ordered parts of the same doc)
        combined_text = "\n".join(c.raw_text for c in chunks)
        # Use the file stem as source_doc_id for readable citations
        source_doc_id = Path(file_path).stem[:60]
        logger.info("QDC: extracting from %s (%d chars)", source_doc_id, len(combined_text))
        return await _process_one_document(
            doc_text=combined_text,
            source_doc_id=source_doc_id,
            source_path=file_path,
            q_list=q_list,
            openai_client=openai_client,
            sem=sem,
        )

    file_results = []
    for fp, chunks in docs_by_file.items():
        result = await _process_file(fp, chunks)
        file_results.append(result)
    all_passages = [p for result in file_results for p in result]

    # ── Deduplicate by (source_doc_id + text) ─────────────────────────────────
    # Dedup per-document so identical text from different companies is kept.
    seen: set[str] = set()
    unique_passages: list[QDCPassage] = []
    for p in all_passages:
        key = f"{p.source_doc_id}::{p.text.strip().lower()}"
        if key not in seen:
            seen.add(key)
            unique_passages.append(p)

    logger.info(
        "QDC: %d unique passages from %d file(s) (%d before dedup)",
        len(unique_passages), len(docs_by_file), len(all_passages),
    )

    # ── Classify ──────────────────────────────────────────────────────────────
    if unique_passages:
        unique_passages = await _classify_batch(unique_passages, openai_client, sem)
        logger.info("QDC: classification complete")

    # ── Optional upsert ───────────────────────────────────────────────────────
    if upsert and store and unique_passages:
        logger.info("QDC: upserting %d passages into knowledge store", len(unique_passages))
        for p in unique_passages:
            cp = _qdc_to_classified_passage(p)
            await store.upsert_passage(cp)

    return QDCResult(
        source_path=source_path,
        questions=questions,
        passages=unique_passages,
        run_at=datetime.utcnow().isoformat(),
    )


def _qdc_to_classified_passage(p: QDCPassage) -> ClassifiedPassage:
    """Convert a QDCPassage to a ClassifiedPassage for storage."""
    import hashlib
    return ClassifiedPassage(
        passage_id=p.passage_id,
        content_hash=hashlib.sha256(p.text.encode()).hexdigest(),
        source_doc_id=p.source_doc_id,
        text=p.text,
        page_ref=p.page_ref,
        char_start=None,
        char_end=None,
        topic_hint="qdc",
        extraction_note=f"qdc:{p.question_id}",
        category=p.category or "unknown",
        subcategory=p.subcategory or "",
        seed_category=False,
        iro_type=p.iro_type or "not_specified",
        value_chain_position=p.value_chain_position or "not_specified",
        evidence_quality=p.evidence_quality or "anecdotal",
        time_horizon=p.time_horizon or "unspecified",
        geographic_scope=p.geographic_scope or [],
        entities=p.entities or [],
        sector_relevance=[],
        frameworks_referenced=p.frameworks_referenced or [],
        taxonomy_eligible=None,
        taxonomy_activity_code=None,
        esrs_hazard_ref=None,
        scenario_referenced=None,
        esrs_e2_relevant=False,
        confidence=p.confidence or 0.5,
        confidence_rationale=p.confidence_rationale or "",
        classification_note=p.classification_note,
        classification_model=config.STAGE_B_MODEL,
        classified_at=datetime.utcnow(),
        validation_status=ValidationStatus.PENDING_REVIEW,
        review_priority=ReviewPriority.NORMAL,
        reviewer_id=None,
        reviewed_at=None,
        review_notes=f"QDC passage for question: {p.question}",
    )


# ── Pretty print results ──────────────────────────────────────────────────────

def _print_results(result: QDCResult) -> None:
    docs = len({p.source_doc_id for p in result.passages})
    print(f"\n{'='*72}")
    print(f"QDC Results — {result.source_path}")
    print(f"Run at: {result.run_at}")
    print(f"Questions: {len(result.questions)}  |  Documents: {docs}  |  Passages: {len(result.passages)}")
    print(f"{'='*72}\n")

    # Group by question
    by_q: dict[str, list[QDCPassage]] = {}
    for p in result.passages:
        by_q.setdefault(p.question_id, []).append(p)

    for i, q in enumerate(result.questions, 1):
        qid = f"q{i}"
        passages = by_q.get(qid, [])
        print(f"[{qid}] {q}")
        print(f"      {len(passages)} passage(s) found")
        for p in passages:
            cat = f"{p.category}/{p.subcategory}" if p.category else "unclassified"
            conf = f"{p.confidence:.0%}" if p.confidence is not None else "n/a"
            print(f"  → [{cat}] conf={conf}  doc={p.source_doc_id}  page={p.page_ref or '?'}")
            print(f"    {p.text[:200]}{'...' if len(p.text) > 200 else ''}")
        print()


# ── CSV export ───────────────────────────────────────────────────────────────

def _save_csv(result: QDCResult, csv_path: str) -> None:
    """
    Save results as a pivot table CSV:
      - One row per document
      - One column per question (header = question text)
      - Each cell contains all matching passages for that doc+question,
        separated by ' || ', with [category | conf%] appended to each passage
    Also writes a companion detail CSV (*_detail.csv) with one row per passage
    for deeper analysis.
    """
    # ── Build index: {source_doc_id: {question_id: [passages]}} ──────────────
    index: dict[str, dict[str, list[QDCPassage]]] = {}
    for p in result.passages:
        index.setdefault(p.source_doc_id, {}).setdefault(p.question_id, []).append(p)

    all_docs = sorted(index.keys())
    q_ids    = [f"q{i+1}" for i in range(len(result.questions))]

    # ── Pivot CSV ─────────────────────────────────────────────────────────────
    pivot_path = csv_path
    with open(pivot_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        # Header: document + one column per question
        writer.writerow(["document"] + result.questions)
        for doc_id in all_docs:
            row = [doc_id]
            for qid in q_ids:
                passages = index[doc_id].get(qid, [])
                if not passages:
                    row.append("")
                else:
                    cells = []
                    for p in passages:
                        cat   = p.subcategory or p.category or "?"
                        conf  = f"{p.confidence:.0%}" if p.confidence is not None else "?"
                        cells.append(f"{p.text.strip()} [{cat} | {conf}]")
                    row.append(" || ".join(cells))
            writer.writerow(row)

    # ── Detail CSV (one row per passage) ─────────────────────────────────────
    detail_path = Path(csv_path).with_stem(Path(csv_path).stem + "_detail")
    with open(detail_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "document", "question_id", "question", "text",
            "category", "subcategory", "iro_type",
            "value_chain_position", "evidence_quality", "time_horizon",
            "confidence", "page_ref", "entities", "frameworks_referenced",
        ])
        for p in result.passages:
            writer.writerow([
                p.source_doc_id,
                p.question_id,
                p.question,
                p.text,
                p.category or "",
                p.subcategory or "",
                p.iro_type or "",
                p.value_chain_position or "",
                p.evidence_quality or "",
                p.time_horizon or "",
                f"{p.confidence:.2f}" if p.confidence is not None else "",
                p.page_ref or "",
                "; ".join(p.entities or []),
                "; ".join(p.frameworks_referenced or []),
            ])

    print(f"Pivot CSV saved to   {pivot_path}")
    print(f"Detail CSV saved to  {detail_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Question-Driven Classification — extract and classify targeted evidence."
    )
    parser.add_argument("--path",      "-p", required=True,
                        help="Path to a PDF/text file OR a folder of PDFs. "
                             "All .pdf and .txt files in the folder are processed.")
    parser.add_argument("--questions", "-q", default=None,
                        help="Path to plain-text questions file (one question per line)")
    parser.add_argument("--question",  "-Q", action="append", default=[],
                        help="Inline question (repeatable). Used if --questions not provided.")
    parser.add_argument("--output",    "-o", default=None,
                        help="Save results as JSON to this path")
    parser.add_argument("--csv",       "-C", default=None, metavar="PATH",
                        help="Save pivot CSV (documents × questions) to this path. "
                             "Also writes a *_detail.csv with one row per passage.")
    parser.add_argument("--upsert",    "-u", action="store_true",
                        help="Also upsert extracted passages into the knowledge store")
    parser.add_argument("--concurrency", "-c", type=int, default=3, metavar="N",
                        help="Max parallel LLM calls per document (default: 3). "
                             "Lower if you hit timeouts; raise if you have high TPM quota.")
    parser.add_argument("--taxonomy",    "-t", default=None, metavar="PATH",
                        help="Override taxonomy file (e.g. _design/taxonomy_tight.yaml). "
                             "Defaults to TAXONOMY_PATH env var or _design/taxonomy.yaml.")
    args = parser.parse_args()

    if args.taxonomy:
        import config as _cfg
        _cfg.TAXONOMY_PATH = Path(args.taxonomy)

    # Build question list
    questions: list[str] = []
    if args.questions:
        raw = Path(args.questions).read_text(encoding="utf-8")
        questions = [line.strip() for line in raw.splitlines() if line.strip()]
    questions += [q.strip() for q in args.question if q.strip()]
    if not questions:
        parser.error("Provide questions via --questions file or --question flags.")

    # Build clients
    config.require_credentials()
    openai_client = AsyncAzureOpenAI(
        azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
        api_key=config.AZURE_OPENAI_KEY,
        api_version="2024-08-01-preview",
        timeout=120.0,
        max_retries=3,
    )
    store = None
    if args.upsert:
        store = KnowledgeStore(
            search_endpoint=config.AZURE_SEARCH_ENDPOINT,
            search_key=config.AZURE_SEARCH_KEY,
            openai_client=openai_client,
        )

    async def _main():
        try:
            result = await run_qdc(
                source_path=args.path,
                questions=questions,
                openai_client=openai_client,
                store=store,
                concurrency=args.concurrency,
                upsert=args.upsert,
            )
            _print_results(result)
            if args.output:
                out = {
                    "source_path": result.source_path,
                    "run_at":      result.run_at,
                    "questions":   result.questions,
                    "passages": [
                        {k: v for k, v in asdict(p).items()}
                        for p in result.passages
                    ],
                }
                Path(args.output).write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
                print(f"JSON saved to {args.output}")
            if args.csv:
                _save_csv(result, args.csv)
        finally:
            await openai_client.close()
            if store:
                await store.close()

    asyncio.run(_main())


if __name__ == "__main__":
    main()
