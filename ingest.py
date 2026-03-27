# =============================================================================
# ingest.py
# THE ONLY entry point for adding documents to the system.
# Orchestrates: adapter → normalize → Stage A → Stage B → triage → upsert
# =============================================================================

import hashlib
import logging
import re
import unicodedata
import uuid
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


def _load_adapters() -> None:
    from adapters.corporate_pdf import CorporatePDFAdapter
    from adapters.google_cse import GoogleCSEAdapter
    _ADAPTER_REGISTRY["CorporatePDFAdapter"] = CorporatePDFAdapter
    _ADAPTER_REGISTRY["GoogleCSEAdapter"] = GoogleCSEAdapter


def _load_sources() -> dict:
    with open(config.SOURCES_PATH, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data.get("sources", {})


async def normalize(raw_doc: Document) -> Document:
    text = raw_doc.raw_text

    # Remove null bytes and normalise unicode
    text = text.replace("\x00", "")
    text = unicodedata.normalize("NFKC", text)

    # Normalise whitespace: collapse multiple blank lines, strip leading/trailing
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
            logger.info(
                "Language override for %s: adapter=%s detected=%s",
                raw_doc.source_url, raw_doc.language, detected,
            )
            lang = detected
    except Exception:
        pass  # langdetect optional — keep adapter value if unavailable

    raw_doc.raw_text = text
    raw_doc.content_hash = content_hash
    raw_doc.doc_id = str(uuid.uuid4())
    raw_doc.language = lang
    raw_doc.extraction_status = "pending"
    return raw_doc


async def ingest(
    query_or_path: str,
    source_key: str,
    client_facing: bool = False,
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

    summary = {
        "documents_processed": 0,
        "documents_skipped_duplicate": 0,
        "passages_extracted": 0,
        "passages_auto_approved": 0,
        "passages_pending_review": 0,
        "passages_auto_rejected": 0,
        "errors": [],
    }

    async for raw_doc in adapter.fetch(query_or_path):
        doc: Document = await normalize(raw_doc)

        is_dup = await store.deduplicate_document(doc.content_hash)
        if is_dup:
            summary["documents_skipped_duplicate"] += 1
            logger.info("Skipping duplicate document: %s", doc.source_url)
            continue

        await store.register_document(doc)
        summary["documents_processed"] += 1

        try:
            passage_dicts = await run_stage_a(doc, collect_prompt, openai_client)
        except Exception as exc:
            err = f"Stage A failed for {doc.doc_id}: {exc}"
            logger.error(err)
            summary["errors"].append(err)
            await store.update_document_status(doc.doc_id, "failed", str(exc))
            continue

        for passage_dict in passage_dicts:
            try:
                hint = passage_dict.get("topic_hint", "")
                tax_excerpt = taxonomy.get_taxonomy_excerpt_for_hint(hint)
                stage_b = await run_stage_b(
                    passage_dict, doc, tax_excerpt, classify_prompt, openai_client
                )
                if stage_b is None:
                    logger.warning("Stage B returned None for a passage in %s", doc.doc_id)
                    continue

                passage = build_classified_passage(passage_dict, stage_b, doc)
                passage = triage(passage, source_type=source_type, client_facing=client_facing)
                await store.upsert_passage(passage)

                summary["passages_extracted"] += 1
                status = passage.validation_status.value
                if status == "auto_approved":
                    summary["passages_auto_approved"] += 1
                elif status == "auto_rejected":
                    summary["passages_auto_rejected"] += 1
                else:
                    summary["passages_pending_review"] += 1

            except Exception as exc:
                err = f"Passage error in {doc.doc_id}: {exc}"
                logger.error(err)
                summary["errors"].append(err)

        await store.update_document_status(doc.doc_id, "extracted")

    return summary


def _build_clients() -> tuple[KnowledgeStore, AsyncAzureOpenAI]:
    openai_client = AsyncAzureOpenAI(
        azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
        api_key=config.AZURE_OPENAI_KEY,
        api_version="2024-08-01-preview",
    )
    store = KnowledgeStore(
        search_endpoint=config.AZURE_SEARCH_ENDPOINT,
        search_key=config.AZURE_SEARCH_KEY,
        openai_client=openai_client,
    )
    return store, openai_client


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import asyncio

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    parser = argparse.ArgumentParser(description="Adaptation Intelligence Platform — ingest a document")
    parser.add_argument("--source", required=True, help="Source key from sources.yaml")
    parser.add_argument("--path", required=True, help="File path, URL, or search query")
    parser.add_argument("--client-facing", action="store_true", help="Mark all passages as P1_CLIENT priority")
    args = parser.parse_args()

    result = asyncio.run(
        ingest(
            query_or_path=args.path,
            source_key=args.source,
            client_facing=args.client_facing,
        )
    )
    print(result)
