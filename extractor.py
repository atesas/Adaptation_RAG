# =============================================================================
# extractor.py
# Stage A (collect) and Stage B (classify) extraction logic.
# Called only from ingest.py — not a public API.
# =============================================================================

import hashlib
import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from openai import AsyncAzureOpenAI

import config
from schemas.document import Document
from schemas.passage import ClassifiedPassage
from schemas.validation import (
    ValidationStatus,
    ReviewPriority,
    AUTO_APPROVE_CONFIDENCE_THRESHOLD,
    AUTO_REJECT_CONFIDENCE_THRESHOLD,
)
from taxonomy import taxonomy

logger = logging.getLogger(__name__)

_AUTO_APPROVE_SOURCE_TYPES = {
    "gcf_api", "oecd_api", "world_bank_api", "unfccc_api", "gef_api",
}


async def run_stage_a(
    doc: Document, prompt_path: Path, openai_client: AsyncAzureOpenAI
) -> list[dict]:
    template = prompt_path.read_text(encoding="utf-8")
    prompt = _apply_template(template, {
        "source_name": doc.source_url,
        "document_type": doc.document_type,
        "reporting_year": str(doc.reporting_year or "unknown"),
        "language": doc.language,
        "company_name": doc.company_name or "N/A",
        "document_text": doc.raw_text,
    })
    system_msg, user_msg = _split_prompt(prompt)
    result = await _call_llm(openai_client, config.STAGE_A_MODEL, system_msg, user_msg, json_object=False)
    if result is None:
        logger.error("Stage A failed for doc %s after retries", doc.doc_id)
        return []
    passages = _parse_json_array(result)
    if passages is None:
        result2 = await _call_llm(openai_client, config.STAGE_A_MODEL, system_msg, user_msg, json_object=False)
        passages = _parse_json_array(result2) if result2 else None
    return passages or []


async def run_stage_b(
    passage_dict: dict,
    doc: Document,
    taxonomy_excerpt: str,
    prompt_path: Path,
    openai_client: AsyncAzureOpenAI,
) -> Optional[dict]:
    template = prompt_path.read_text(encoding="utf-8")
    prompt = _apply_template(template, {
        "taxonomy_excerpt": taxonomy_excerpt,
        "source_name": doc.source_url,
        "document_type": doc.document_type,
        "reporting_year": str(doc.reporting_year or "unknown"),
        "company_name": doc.company_name or "N/A",
        "topic_hint": passage_dict.get("topic_hint", ""),
        "extraction_note": passage_dict.get("extraction_note") or "none",
        "passage_text": passage_dict.get("text", ""),
    })
    system_msg, user_msg = _split_prompt(prompt)
    result = await _call_llm(openai_client, config.STAGE_B_MODEL, system_msg, user_msg)
    if result is None:
        return None
    stage_b = _parse_json_object(result)
    if stage_b is None:
        result2 = await _call_llm(openai_client, config.STAGE_B_MODEL, system_msg, user_msg)
        if result2 is None:
            return None
        stage_b = _parse_json_object(result2)
        if stage_b is None:
            return None

    is_valid, errors = taxonomy.validate_classification(stage_b)
    if not is_valid:
        logger.warning("Stage B invalid taxonomy values: %s", errors)
        stage_b["confidence"] = 0.0
        stage_b["classification_note"] = "invalid_taxonomy_value"
        for error in errors:
            if error.startswith(("invalid category:", "subcategory path not found:")):
                raw_value = error.split("'")[1]
                taxonomy.record_candidate_extension(
                    value=raw_value,
                    hint=passage_dict.get("topic_hint", ""),
                    source_doc_id=doc.doc_id,
                    frequency=1,
                )

    return stage_b


def build_classified_passage(
    passage_dict: dict,
    stage_b: dict,
    doc: Document,
) -> ClassifiedPassage:
    text = passage_dict.get("text", "")
    content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return ClassifiedPassage(
        passage_id=str(uuid.uuid4()),
        content_hash=content_hash,
        source_doc_id=doc.doc_id,
        text=text,
        page_ref=str(passage_dict["page_ref"]) if passage_dict.get("page_ref") is not None else None,
        char_start=passage_dict.get("char_start"),
        char_end=None,
        topic_hint=passage_dict.get("topic_hint", ""),
        extraction_note=passage_dict.get("extraction_note"),
        category=stage_b.get("category") or "",
        subcategory=stage_b.get("subcategory") or "",
        seed_category=bool(stage_b.get("seed_category", False)),
        iro_type=stage_b.get("iro_type") or "not_specified",
        value_chain_position=stage_b.get("value_chain_position") or "not_specified",
        evidence_quality=stage_b.get("evidence_quality") or "anecdotal",
        time_horizon=stage_b.get("time_horizon") or "unspecified",
        geographic_scope=stage_b.get("geographic_scope") or [],
        entities=stage_b.get("entities") or [],
        sector_relevance=stage_b.get("sector_relevance") or [],
        frameworks_referenced=stage_b.get("frameworks_referenced") or [],
        taxonomy_eligible=stage_b.get("taxonomy_eligible"),
        taxonomy_activity_code=stage_b.get("taxonomy_activity_code"),
        esrs_hazard_ref=stage_b.get("esrs_hazard_ref"),
        scenario_referenced=stage_b.get("scenario_referenced"),
        esrs_e2_relevant=bool(stage_b.get("esrs_e2_relevant", False)),
        confidence=float(stage_b.get("confidence", 0.0)),
        confidence_rationale=stage_b.get("confidence_rationale") or "",
        classification_note=stage_b.get("classification_note"),
        classification_model=config.STAGE_B_MODEL,
        classified_at=datetime.utcnow(),
        validation_status=ValidationStatus.RAW,
        review_priority=None,
        reviewer_id=None,
        reviewed_at=None,
        review_notes=None,
    )


def triage(
    passage: ClassifiedPassage,
    source_type: str,
    client_facing: bool = False,
) -> ClassifiedPassage:
    note = passage.classification_note or ""
    extraction_note = passage.extraction_note or ""

    if passage.confidence < AUTO_REJECT_CONFIDENCE_THRESHOLD:
        passage.validation_status = ValidationStatus.AUTO_REJECTED
        passage.review_priority = None
        return passage

    auto_approve = (
        passage.confidence >= AUTO_APPROVE_CONFIDENCE_THRESHOLD
        and passage.seed_category
        and source_type in _AUTO_APPROVE_SOURCE_TYPES
        and "quantitative_claim" not in note
        and "qualifying_language_detected" not in note
    )
    if auto_approve:
        passage.validation_status = ValidationStatus.AUTO_APPROVED
        passage.review_priority = None
        return passage

    passage.validation_status = ValidationStatus.PENDING_REVIEW

    if client_facing:
        passage.review_priority = ReviewPriority.P1_CLIENT
    elif "quantitative_claim" in note or "quantitative_claim" in extraction_note:
        passage.review_priority = ReviewPriority.P2_QUANT
    elif not passage.seed_category:
        passage.review_priority = ReviewPriority.P3_NEW_CAT
    else:
        passage.review_priority = ReviewPriority.P4_STANDARD

    return passage


# ── Private helpers ───────────────────────────────────────────────────────────

def _split_prompt(prompt: str) -> tuple[str, str]:
    if "---\n\nUSER:" in prompt:
        parts = prompt.split("---\n\nUSER:", 1)
        system_part = parts[0].replace("SYSTEM:\n", "").strip()
        user_part = parts[1].strip()
    else:
        system_part = ""
        user_part = prompt.strip()
    return system_part, user_part


async def _call_llm(
    client: AsyncAzureOpenAI,
    model: str,
    system_msg: str,
    user_msg: str,
    json_object: bool = True,
) -> Optional[str]:
    messages: list[dict] = []
    if system_msg:
        messages.append({"role": "system", "content": system_msg})
    messages.append({"role": "user", "content": user_msg})
    try:
        kwargs: dict = {
            "model": model,
            "messages": messages,
            "temperature": 0.0,
        }
        if json_object:
            kwargs["response_format"] = {"type": "json_object"}
        response = await client.chat.completions.create(**kwargs)
        return response.choices[0].message.content
    except Exception as exc:
        logger.error("LLM call failed (%s): %s", model, exc)
        return None


def _parse_json_array(text: str) -> Optional[list]:
    if not text:
        return None
    # Strip markdown code fences if present
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = stripped.split("\n", 1)[-1]
        stripped = stripped.rsplit("```", 1)[0].strip()
    # Try direct parse first
    try:
        data = json.loads(stripped)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for v in data.values():
                if isinstance(v, list):
                    return v
        return None
    except json.JSONDecodeError:
        pass
    # Fall back: find first '[' ... last ']' in the text
    start = stripped.find("[")
    end = stripped.rfind("]")
    if start != -1 and end > start:
        try:
            data = json.loads(stripped[start : end + 1])
            if isinstance(data, list):
                return data
        except json.JSONDecodeError:
            pass
    return None


def _parse_json_object(text: str) -> Optional[dict]:
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        return None


def _apply_template(template: str, variables: dict) -> str:
    """Replace {key} placeholders without interpreting other { } as format fields."""
    for key, value in variables.items():
        template = template.replace("{" + key + "}", str(value))
    return template
