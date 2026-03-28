# =============================================================================
# ingest.py
# THE ONLY entry point for adding documents to the system.
# Orchestrates: adapter → normalize → Stage A → Stage B → triage → upsert
#
# TWO-STEP WORKFLOW (download first, classify later):
#   Step 1:  python ingest.py --source google_cse_corporate --path "query" --download-only
#            → downloads to tmp/staged/, prints a manifest, stops before any LLM call
#   Step 2:  python ingest.py --source corporate_pdf_direct --path tmp/staged/file.pdf
#            → runs full pipeline on a single file you chose
# =============================================================================

import hashlib
import json
import logging
import re
import shutil
import unicodedata
import uuid
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml
from openai import AsyncAzureOpenAI

import config
from adapters.base import BaseAdapter
from extractor import build_classified_passage, run_stage_a, run_stage_b, triage
from knowledge_store import KnowledgeStore
from schemas.document import Document
from taxonomy import taxonomy

logger = logging.getLogger(__name__)

_ADAPTER_REGISTRY: dict[str, type[BaseAdapter]] = {}
_STAGED_DIR = config.TMP_DIR / "staged"
_MANIFEST_FILE = _STAGED_DIR / "manifest.json"


def _load_adapters() -> None:
    from adapters.corporate_pdf import CorporatePDFAdapter
    from adapters.google_cse import GoogleCSEAdapter
    from adapters.gcf_api import GCFAPIAdapter
    from adapters.oecd_api import OECDAPIAdapter
    _ADAPTER_REGISTRY["CorporatePDFAdapter"] = CorporatePDFAdapter
    _ADAPTER_REGISTRY["GoogleCSEAdapter"] = GoogleCSEAdapter
    _ADAPTER_REGISTRY["GCFAPIAdapter"] = GCFAPIAdapter
    _ADAPTER_REGISTRY["OECDAPIAdapter"] = OECDAPIAdapter


def _load_sources() -> dict:
    with open(config.SOURCES_PATH, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data.get("sources", {})


async def normalize(raw_doc: Document) -> Document:
    text = raw_doc.raw_text
    text = text.replace("\x00", "")
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = text.strip()

    if len(text) > config.MAX_DOCUMENT_CHARS:
        logger.warning(
            "Document %s truncated from %d to %d chars",
            raw_doc.source_url, len(text), config.MAX_DOCUMENT_CHARS,
        )
        text = text[: config.MAX_DOCUMENT_CHARS]

    content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()

    lang = raw_doc.language
    try:
        from langdetect import detect
        detected = detect(text[:2000])
        if detected != raw_doc.language:
            lang = detected
    except Exception:
        pass

    raw_doc.raw_text = text
    raw_doc.content_hash = content_hash
    raw_doc.doc_id = str(uuid.uuid4())
    raw_doc.language = lang
    raw_doc.extraction_status = "pending"
    return raw_doc


async def download_only(query_or_path: str, source_key: str) -> list[dict]:
    """
    Step 1 of the two-step workflow.

    Runs the adapter fetch, saves each document's text to tmp/staged/,
    and writes a manifest.json listing what was found.
    Does NOT call any LLM. Does NOT write to Azure AI Search.

    Returns a list of manifest entries so callers can print a summary.
    """
    if not _ADAPTER_REGISTRY:
        _load_adapters()

    sources = _load_sources()
    if source_key not in sources:
        raise ValueError(f"Unknown source_key: '{source_key}'")
    source_cfg = sources[source_key]

    if not source_cfg.get("enabled", False):
        raise ValueError(f"Source '{source_key}' is disabled in sources.yaml")

    adapter_name = source_cfg["adapter"]
    if adapter_name not in _ADAPTER_REGISTRY:
        raise ValueError(f"Adapter '{adapter_name}' not registered")

    _STAGED_DIR.mkdir(parents=True, exist_ok=True)
    adapter: BaseAdapter = _ADAPTER_REGISTRY[adapter_name](source_cfg)
    manifest: list[dict] = []

    async for raw_doc in adapter.fetch(query_or_path):
        doc = await normalize(raw_doc)
        slug = re.sub(r"[^\w\-]", "_", (doc.title or "document")[:60])
        staged_path = _STAGED_DIR / f"{slug}_{doc.content_hash[:8]}.txt"
        staged_path.write_text(doc.raw_text, encoding="utf-8")

        entry = {
            "staged_path": str(staged_path),
            "title": doc.title,
            "source_url": doc.source_url,
            "language": doc.language,
            "chars": len(doc.raw_text),
            "content_hash": doc.content_hash,
            "document_type": doc.document_type,
            "source_type": doc.source_type,
        }
        manifest.append(entry)
        print(
            f"  ✓ {doc.title or 'Untitled'}\n"
            f"    URL:  {doc.source_url}\n"
            f"    Size: {len(doc.raw_text):,} chars | lang: {doc.language}\n"
            f"    File: {staged_path}\n"
        )

    _MANIFEST_FILE.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


async def ingest(
    query_or_path: str,
    source_key: str,
    client_facing: bool = False,
    force: bool = False,
    inter_doc_delay: float = 0.0,   # kept for backward compat; concurrency flag is preferred
    concurrency: int = 5,
    store: Optional[KnowledgeStore] = None,
    openai_client: Optional[AsyncAzureOpenAI] = None,
) -> dict:
    if not _ADAPTER_REGISTRY:
        _load_adapters()

    sources = _load_sources()
    if source_key not in sources:
        raise ValueError(f"Unknown source_key: '{source_key}'")
    source_cfg = sources[source_key]

    if not source_cfg.get("enabled", False):
        raise ValueError(f"Source '{source_key}' is disabled (enabled: false in sources.yaml)")

    adapter_name = source_cfg["adapter"]
    if adapter_name not in _ADAPTER_REGISTRY:
        raise ValueError(f"Adapter '{adapter_name}' not registered")

    if store is None or openai_client is None:
        store, openai_client = _build_clients()

    adapter: BaseAdapter = _ADAPTER_REGISTRY[adapter_name](source_cfg)

    collect_prompt = config.PROMPTS_DIR / f"collect_{config.COLLECT_PROMPT_VERSION}.txt"
    classify_prompt = config.PROMPTS_DIR / f"classify_{config.CLASSIFY_PROMPT_VERSION}.txt"
    source_type = source_cfg.get("source_type", "")

    summary: dict = {
        "documents_processed": 0,
        "documents_skipped_duplicate": 0,
        "passages_extracted": 0,
        "passages_auto_approved": 0,
        "passages_pending_review": 0,
        "passages_auto_rejected": 0,
        "errors": [],
    }

    sem = asyncio.Semaphore(concurrency)

    # ── Step 1: collect and normalise all chunks ──────────────────────────────
    docs: list[Document] = []
    async for raw_doc in adapter.fetch(query_or_path):
        doc: Document = await normalize(raw_doc)
        if not force:
            is_dup = await store.deduplicate_document(doc.content_hash)
            if is_dup:
                summary["documents_skipped_duplicate"] += 1
                logger.info("Skipping duplicate document: %s", doc.source_url)
                continue
        await store.register_document(doc)
        docs.append(doc)

    summary["documents_processed"] = len(docs)
    if not docs:
        await store.close()
        await openai_client.close()
        return summary

    print(
        f"  {len(docs)} chunk(s) loaded.  Running Stage A "
        f"(concurrency={concurrency})...",
        flush=True,
    )

    # ── Step 2: Stage A — parallel across all chunks ──────────────────────────
    async def _stage_a(d: Document) -> tuple[Document, list[dict]]:
        async with sem:
            try:
                return d, await run_stage_a(d, collect_prompt, openai_client)
            except Exception as exc:
                err = f"Stage A failed for {d.doc_id}: {exc}"
                logger.error(err)
                summary["errors"].append(err)
                await store.update_document_status(d.doc_id, "failed", str(exc))
                return d, []

    a_results: list[tuple[Document, list[dict]]] = list(
        await asyncio.gather(*[_stage_a(d) for d in docs])
    )

    all_pairs: list[tuple[Document, dict]] = [
        (d, pd) for d, pds in a_results for pd in pds
    ]
    print(
        f"  Stage A done: {len(all_pairs)} passage(s) found.  "
        f"Running Stage B (concurrency={concurrency})...",
        flush=True,
    )

    # ── Step 3: Stage B + upsert — parallel across all passages ──────────────
    async def _stage_b_upsert(d: Document, pd: dict) -> Optional[ClassifiedPassage]:
        async with sem:
            try:
                hint = pd.get("topic_hint", "")
                tax_excerpt = taxonomy.get_taxonomy_excerpt_for_hint(hint)
                stage_b = await run_stage_b(
                    pd, d, tax_excerpt, classify_prompt, openai_client
                )
                if stage_b is None:
                    logger.warning("Stage B returned None for a passage in %s", d.doc_id)
                    return None
                passage = build_classified_passage(pd, stage_b, d)
                passage = triage(passage, source_type=source_type, client_facing=client_facing)
                await store.upsert_passage(passage)
                return passage
            except Exception as exc:
                err = f"Passage error in {d.doc_id}: {exc}"
                logger.error(err)
                summary["errors"].append(err)
                return None

    b_results: list[Optional[ClassifiedPassage]] = list(
        await asyncio.gather(*[_stage_b_upsert(d, pd) for d, pd in all_pairs])
    )

    for passage in b_results:
        if passage is None:
            continue
        summary["passages_extracted"] += 1
        status = passage.validation_status.value
        if status == "auto_approved":
            summary["passages_auto_approved"] += 1
        elif status == "auto_rejected":
            summary["passages_auto_rejected"] += 1
        else:
            summary["passages_pending_review"] += 1

    # ── Step 4: mark all chunks as extracted ──────────────────────────────────
    for d, _ in a_results:
        await store.update_document_status(d.doc_id, "extracted")

    await store.close()
    await openai_client.close()
    return summary


async def reclassify_rejected(
    store: KnowledgeStore,
    openai_client: AsyncAzureOpenAI,
) -> dict:
    """Re-run Stage B on every auto_rejected passage with classification_note='invalid_taxonomy_value'.

    Call this after updating taxonomy.yaml to recover passages that were rejected
    because the model returned an unmapped category.  Stage A is NOT re-run —
    the passage text is already stored in Azure Search.
    """
    import asyncio as _asyncio
    from datetime import datetime as _datetime
    from schemas.document import Document as _Document

    classify_prompt = config.PROMPTS_DIR / f"classify_{config.CLASSIFY_PROMPT_VERSION}.txt"
    passages = await store.get_passages_for_reclassify()
    summary = {
        "examined": len(passages),
        "reclassified": 0,
        "still_rejected": 0,
        "errors": [],
    }
    print(f"Found {len(passages)} passage(s) to reclassify...", flush=True)

    # Cache document metadata to avoid repeat lookups
    _doc_cache: dict[str, Optional[dict]] = {}

    for i, passage in enumerate(passages, start=1):
        try:
            if passage.source_doc_id not in _doc_cache:
                _doc_cache[passage.source_doc_id] = await store.get_document_by_id(
                    passage.source_doc_id
                )
            meta = _doc_cache[passage.source_doc_id] or {}

            doc_stub = _Document(
                doc_id=passage.source_doc_id,
                content_hash="",
                raw_text="",
                title=meta.get("title"),
                language="en",
                source_url=meta.get("source_url", "unknown"),
                source_type=meta.get("source_type", "corporate_pdf"),
                adapter="",
                publication_date=None,
                ingestion_date=_datetime.utcnow(),
                reporting_year=meta.get("reporting_year"),
                document_type=meta.get("document_type", "corporate_report"),
                company_name=meta.get("company_name"),
                company_id=None,
                csrd_wave=None,
                country=[],
                sector_hint=[],
                extraction_status="extracted",
                extraction_error=None,
            )

            passage_dict = {
                "text": passage.text,
                "topic_hint": passage.topic_hint,
                "extraction_note": passage.extraction_note,
                "page_ref": passage.page_ref,
                "char_start": passage.char_start,
                "char_end": passage.char_end,
            }

            hint = passage.topic_hint or ""
            tax_excerpt = taxonomy.get_taxonomy_excerpt_for_hint(hint)
            stage_b = await run_stage_b(
                passage_dict, doc_stub, tax_excerpt, classify_prompt, openai_client
            )
            if stage_b is None:
                summary["errors"].append(f"Stage B returned None for {passage.passage_id}")
                continue

            new_passage = build_classified_passage(passage_dict, stage_b, doc_stub)
            # Preserve the original passage identity so upsert updates in-place
            new_passage.passage_id = passage.passage_id
            new_passage.content_hash = passage.content_hash
            new_passage.source_doc_id = passage.source_doc_id

            source_type = meta.get("source_type", "corporate_pdf")
            new_passage = triage(new_passage, source_type=source_type)
            await store.upsert_passage(new_passage)

            status = new_passage.validation_status.value
            if status == "auto_rejected":
                summary["still_rejected"] += 1
            else:
                summary["reclassified"] += 1

            print(
                f"  [{i:>4}/{len(passages)}] {status:<20} "
                f"conf={new_passage.confidence:.2f}  {passage.passage_id[:8]}...",
                flush=True,
            )

        except Exception as exc:
            err = f"Reclassify error for {passage.passage_id}: {exc}"
            logger.error(err)
            summary["errors"].append(err)

    return summary


def _build_clients() -> tuple[KnowledgeStore, AsyncAzureOpenAI]:
    config.require_credentials()
    openai_client = AsyncAzureOpenAI(
        azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
        api_key=config.AZURE_OPENAI_KEY,
        api_version="2024-08-01-preview",
        max_retries=6,
    )
    store = KnowledgeStore(
        search_endpoint=config.AZURE_SEARCH_ENDPOINT,
        search_key=config.AZURE_SEARCH_KEY,
        openai_client=openai_client,
    )
    store.ensure_indexes()
    return store, openai_client


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import asyncio

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s %(message)s")
    # Keep our own application loggers at INFO, suppress noisy SDK loggers
    logging.getLogger("__main__").setLevel(logging.INFO)
    logging.getLogger("extractor").setLevel(logging.INFO)
    logging.getLogger("taxonomy").setLevel(logging.INFO)
    logging.getLogger("knowledge_store").setLevel(logging.INFO)
    logging.getLogger("adapters").setLevel(logging.INFO)

    parser = argparse.ArgumentParser(
        description="Adaptation Intelligence Platform — ingest documents",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
TWO-STEP WORKFLOW (search first, classify later):

  Step 1 — download only (no LLM, no Azure):
    python ingest.py --source google_cse_corporate \\
                     --path "Danone CSRD 2024" \\
                     --download-only

    → prints what was found, saves text files to tmp/staged/
    → review the files, then run Step 2 on the ones you want

  Step 2 — classify a single staged file:
    python ingest.py --source corporate_pdf_direct \\
                     --path tmp/staged/Danone_abc123.txt

  Or process all staged files at once:
    python ingest.py --source corporate_pdf_direct --path tmp/staged/ --all-staged
""",
    )
    parser.add_argument("--source", required=False, default=None, help="Source key from sources.yaml")
    parser.add_argument("--path", required=False, default=None, help="File path, URL, or search query")
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="Fetch and save to tmp/staged/ without running LLM extraction",
    )
    parser.add_argument(
        "--all-staged",
        action="store_true",
        help="Process every .txt file in tmp/staged/ (use after --download-only review)",
    )
    parser.add_argument(
        "--client-facing",
        action="store_true",
        help="Mark all passages as P1_CLIENT priority",
    )
    parser.add_argument(
        "--reset-indexes",
        action="store_true",
        help="DELETE and recreate all Azure Search indexes (wipes all data). Run once after a schema change.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess documents even if already in the store (skips dedup check)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        metavar="N",
        help="Max concurrent LLM calls (Stage A and Stage B run in parallel up to this limit). "
             "Default 5. Lower to 2-3 on S0 tier if you see 429 errors.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        metavar="SECONDS",
        help="(Legacy) Sleep between chunks. Prefer --concurrency to control throughput.",
    )
    parser.add_argument(
        "--reclassify",
        action="store_true",
        help="Re-run Stage B on all auto_rejected passages with invalid_taxonomy_value. "
             "Use after updating taxonomy.yaml to recover rejected passages.",
    )
    args = parser.parse_args()

    if not (args.reclassify or args.reset_indexes) and (args.source is None or args.path is None):
        parser.error("--source and --path are required unless using --reclassify or --reset-indexes")

    if args.reclassify:
        store, openai_client = _build_clients()

        async def _reclassify():
            try:
                return await reclassify_rejected(store, openai_client)
            finally:
                await store.close()
                await openai_client.close()

        result = asyncio.run(_reclassify())
        print(f"\nReclassify complete: {result}")
        import sys; sys.exit(0)

    if args.reset_indexes:
        config.require_credentials()
        from openai import AsyncAzureOpenAI
        from knowledge_store import KnowledgeStore
        _oa = AsyncAzureOpenAI(
            azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
            api_key=config.AZURE_OPENAI_KEY,
            api_version="2024-08-01-preview",
        )
        store = KnowledgeStore(
            search_endpoint=config.AZURE_SEARCH_ENDPOINT,
            search_key=config.AZURE_SEARCH_KEY,
            openai_client=_oa,
        )
        print("Deleting and recreating all Azure Search indexes...")
        store.reset_indexes()
        print("Done. All indexes recreated with current schema.")
        import sys; sys.exit(0)

    if args.download_only:
        print(f"\nSearching: {args.path!r}\nSource:    {args.source}\n")
        results = asyncio.run(download_only(args.path, args.source))
        print(f"\n{len(results)} document(s) staged in tmp/staged/")
        print(f"Manifest:  {_MANIFEST_FILE}")
        print("\nReview the files above, then run:")
        print("  python ingest.py --source corporate_pdf_direct --path tmp/staged/<file>.txt")
        print("  python ingest.py --source corporate_pdf_direct --path tmp/staged/ --all-staged")

    elif args.all_staged:
        # --path can be any directory; defaults to tmp/staged/
        target_dir = Path(args.path) if Path(args.path).is_dir() else _STAGED_DIR
        files = sorted(p for p in target_dir.iterdir()
                       if p.suffix.lower() in {".pdf", ".txt"} and p.is_file())
        if not files:
            print(f"No .pdf or .txt files found in {target_dir}. Run --download-only first.")
        else:
            print(f"Processing {len(files)} file(s) from {target_dir} ...")
            result = asyncio.run(
                ingest(str(target_dir), "corporate_pdf_direct",
                       client_facing=args.client_facing, force=args.force,
                       inter_doc_delay=args.delay, concurrency=args.concurrency)
            )
            print(f"\nDone. {result}")

    else:
        result = asyncio.run(
            ingest(
                query_or_path=args.path,
                source_key=args.source,
                client_facing=args.client_facing,
                force=args.force,
                inter_doc_delay=args.delay,
                concurrency=args.concurrency,
            )
        )
        print(result)
