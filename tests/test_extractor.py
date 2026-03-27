# =============================================================================
# tests/test_extractor.py
# Tests for extractor.py — Stage A, Stage B, triage, build_classified_passage
# =============================================================================

import json
import uuid
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from extractor import (
    _parse_json_array,
    _parse_json_object,
    _split_prompt,
    build_classified_passage,
    run_stage_a,
    run_stage_b,
    triage,
)
from schemas.document import Document
from schemas.passage import ClassifiedPassage
from schemas.validation import (
    AUTO_APPROVE_CONFIDENCE_THRESHOLD,
    AUTO_REJECT_CONFIDENCE_THRESHOLD,
    ReviewPriority,
    ValidationStatus,
)


def _make_doc(**kwargs) -> Document:
    defaults = dict(
        doc_id=str(uuid.uuid4()),
        content_hash="abc",
        raw_text="Some climate text about water stress in operations.",
        title="Test Report",
        language="en",
        source_url="/docs/report.pdf",
        source_type="corporate_pdf",
        adapter="CorporatePDFAdapter",
        publication_date=None,
        ingestion_date=datetime.utcnow(),
        reporting_year=2024,
        document_type="corporate_report",
        company_name="TestCo",
        company_id="test-co-001",
        csrd_wave=1,
        country=["FR"],
        sector_hint=["food_and_beverage"],
        extraction_status="pending",
        extraction_error=None,
    )
    defaults.update(kwargs)
    return Document(**defaults)


def _make_passage(**kwargs) -> ClassifiedPassage:
    defaults = dict(
        passage_id=str(uuid.uuid4()),
        content_hash="hash123",
        source_doc_id="doc-001",
        text="Water stress affects our operations in Southern Europe.",
        page_ref="12",
        char_start=None,
        char_end=None,
        topic_hint="hazard",
        extraction_note=None,
        category="hazards",
        subcategory="hazards.physical_chronic.water_stress",
        seed_category=True,
        iro_type="risk.physical_chronic_risk",
        value_chain_position="own_operations",
        evidence_quality="implemented",
        time_horizon="medium",
        geographic_scope=["ES", "FR"],
        entities=["TestCo"],
        sector_relevance=["food_and_beverage"],
        frameworks_referenced=["csrd_esrs"],
        taxonomy_eligible=None,
        taxonomy_activity_code=None,
        esrs_hazard_ref="water_stress",
        scenario_referenced=None,
        esrs_e2_relevant=False,
        confidence=0.9,
        confidence_rationale="Directly maps to water stress node.",
        classification_note=None,
        classification_model="gpt-4o-mini",
        classified_at=datetime.utcnow(),
        validation_status=ValidationStatus.RAW,
        review_priority=None,
        reviewer_id=None,
        reviewed_at=None,
        review_notes=None,
    )
    defaults.update(kwargs)
    return ClassifiedPassage(**defaults)


# ── _split_prompt ─────────────────────────────────────────────────────────────

class TestSplitPrompt:

    def test_splits_on_separator(self) -> None:
        prompt = "SYSTEM:\nYou are a bot.\n---\n\nUSER:\nHello?"
        system, user = _split_prompt(prompt)
        assert "You are a bot" in system
        assert "Hello?" in user

    def test_no_separator_returns_empty_system(self) -> None:
        prompt = "Just a plain prompt."
        system, user = _split_prompt(prompt)
        assert system == ""
        assert "plain prompt" in user


# ── _parse_json_array ─────────────────────────────────────────────────────────

class TestParseJsonArray:

    def test_parses_array(self) -> None:
        result = _parse_json_array('[{"text": "hello"}]')
        assert result == [{"text": "hello"}]

    def test_returns_none_for_invalid(self) -> None:
        assert _parse_json_array("not json") is None

    def test_extracts_array_from_wrapped_object(self) -> None:
        result = _parse_json_array('{"passages": [{"text": "hi"}]}')
        assert result == [{"text": "hi"}]

    def test_empty_array(self) -> None:
        assert _parse_json_array("[]") == []


# ── _parse_json_object ────────────────────────────────────────────────────────

class TestParseJsonObject:

    def test_parses_object(self) -> None:
        result = _parse_json_object('{"category": "hazards"}')
        assert result == {"category": "hazards"}

    def test_returns_none_for_array(self) -> None:
        assert _parse_json_object("[]") is None

    def test_returns_none_for_invalid(self) -> None:
        assert _parse_json_object("bad json") is None


# ── build_classified_passage ──────────────────────────────────────────────────

class TestBuildClassifiedPassage:

    def test_returns_classified_passage(self) -> None:
        doc = _make_doc()
        passage_dict = {
            "text": "Drought affected our Moroccan wheat suppliers in 2023.",
            "page_ref": "5",
            "char_start": 100,
            "topic_hint": "impact",
            "extraction_note": "quantitative_claim",
        }
        stage_b = {
            "category": "impacts",
            "subcategory": "impacts.agricultural.crop_yield_loss",
            "seed_category": True,
            "iro_type": "impact.actual_negative",
            "value_chain_position": "upstream.tier_1_supplier",
            "evidence_quality": "implemented",
            "time_horizon": "short",
            "geographic_scope": ["MA"],
            "entities": ["TestCo"],
            "sector_relevance": ["food_agriculture"],
            "frameworks_referenced": [],
            "taxonomy_eligible": None,
            "taxonomy_activity_code": None,
            "esrs_hazard_ref": None,
            "scenario_referenced": None,
            "esrs_e2_relevant": False,
            "confidence": 0.88,
            "confidence_rationale": "Directly describes crop yield impact.",
            "classification_note": None,
        }
        passage = build_classified_passage(passage_dict, stage_b, doc)
        assert isinstance(passage, ClassifiedPassage)
        assert passage.source_doc_id == doc.doc_id
        assert passage.confidence == 0.88
        assert passage.validation_status == ValidationStatus.RAW
        assert passage.review_priority is None

    def test_content_hash_is_sha256_of_text(self) -> None:
        import hashlib
        doc = _make_doc()
        text = "A specific climate passage."
        passage_dict = {"text": text, "page_ref": None, "char_start": None,
                        "topic_hint": "hazard", "extraction_note": None}
        stage_b = {
            "category": "hazards", "subcategory": "hazards.physical_acute.extreme_heat_event",
            "seed_category": True, "iro_type": "risk.physical_acute_risk",
            "value_chain_position": "own_operations", "evidence_quality": "anecdotal",
            "time_horizon": "unspecified", "geographic_scope": [], "entities": [],
            "sector_relevance": [], "frameworks_referenced": [], "taxonomy_eligible": None,
            "taxonomy_activity_code": None, "esrs_hazard_ref": None, "scenario_referenced": None,
            "esrs_e2_relevant": False, "confidence": 0.75, "confidence_rationale": "ok",
            "classification_note": None,
        }
        passage = build_classified_passage(passage_dict, stage_b, doc)
        expected_hash = hashlib.sha256(text.encode()).hexdigest()
        assert passage.content_hash == expected_hash


# ── triage ────────────────────────────────────────────────────────────────────

class TestTriage:

    def test_low_confidence_auto_rejected(self) -> None:
        p = _make_passage(confidence=0.30, seed_category=True)
        result = triage(p, source_type="corporate_pdf")
        assert result.validation_status == ValidationStatus.AUTO_REJECTED
        assert result.review_priority is None

    def test_high_confidence_trusted_source_auto_approved(self) -> None:
        p = _make_passage(confidence=0.92, seed_category=True, classification_note=None)
        result = triage(p, source_type="gcf_api")
        assert result.validation_status == ValidationStatus.AUTO_APPROVED
        assert result.review_priority is None

    def test_high_confidence_corporate_pdf_pending_review(self) -> None:
        p = _make_passage(confidence=0.92, seed_category=True, classification_note=None)
        result = triage(p, source_type="corporate_pdf")
        assert result.validation_status == ValidationStatus.PENDING_REVIEW

    def test_client_facing_gets_p1(self) -> None:
        p = _make_passage(confidence=0.75, seed_category=True)
        result = triage(p, source_type="corporate_pdf", client_facing=True)
        assert result.review_priority == ReviewPriority.P1_CLIENT

    def test_quantitative_claim_gets_p2(self) -> None:
        p = _make_passage(confidence=0.75, seed_category=True,
                          classification_note="quantitative_claim")
        result = triage(p, source_type="corporate_pdf")
        assert result.review_priority == ReviewPriority.P2_QUANT

    def test_new_category_gets_p3(self) -> None:
        p = _make_passage(confidence=0.75, seed_category=False, classification_note=None)
        result = triage(p, source_type="corporate_pdf")
        assert result.review_priority == ReviewPriority.P3_NEW_CAT

    def test_standard_passage_gets_p4(self) -> None:
        p = _make_passage(confidence=0.70, seed_category=True, classification_note=None)
        result = triage(p, source_type="corporate_pdf")
        assert result.review_priority == ReviewPriority.P4_STANDARD

    def test_qualifying_language_blocks_auto_approve(self) -> None:
        p = _make_passage(confidence=0.92, seed_category=True,
                          classification_note="qualifying_language_detected")
        result = triage(p, source_type="gcf_api")
        assert result.validation_status == ValidationStatus.PENDING_REVIEW

    def test_quantitative_claim_blocks_auto_approve(self) -> None:
        p = _make_passage(confidence=0.92, seed_category=True,
                          classification_note="quantitative_claim")
        result = triage(p, source_type="gcf_api")
        assert result.validation_status == ValidationStatus.PENDING_REVIEW


# ── run_stage_a ───────────────────────────────────────────────────────────────

class TestRunStageA:

    @pytest.mark.asyncio
    async def test_returns_passages_on_success(self, tmp_path: Path) -> None:
        prompt_file = tmp_path / "collect_v1.txt"
        prompt_file.write_text(
            "SYSTEM:\nYou are a bot.\n---\n\nUSER:\n"
            "Source: {source_name}\nType: {document_type}\n"
            "Year: {reporting_year}\nLang: {language}\n"
            "Company: {company_name}\nText: {document_text}"
        )
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps(
            [{"text": "Water stress passage", "page_ref": 1,
              "char_start": 0, "topic_hint": "hazard", "extraction_note": None}]
        )
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)
        doc = _make_doc()
        with patch("extractor.config") as mock_cfg:
            mock_cfg.STAGE_A_MODEL = "gpt-4o-mini"
            result = await run_stage_a(doc, prompt_file, mock_client)
        assert len(result) == 1
        assert result[0]["text"] == "Water stress passage"

    @pytest.mark.asyncio
    async def test_returns_empty_on_llm_failure(self, tmp_path: Path) -> None:
        prompt_file = tmp_path / "collect_v1.txt"
        prompt_file.write_text(
            "SYSTEM:\nBot.\n---\n\nUSER:\n{source_name} {document_type} "
            "{reporting_year} {language} {company_name} {document_text}"
        )
        mock_client = MagicMock()
        mock_client.chat.completions.create = AsyncMock(side_effect=Exception("API error"))
        doc = _make_doc()
        with patch("extractor.config") as mock_cfg:
            mock_cfg.STAGE_A_MODEL = "gpt-4o-mini"
            result = await run_stage_a(doc, prompt_file, mock_client)
        assert result == []


# ── run_stage_b ───────────────────────────────────────────────────────────────

class TestRunStageB:

    @pytest.mark.asyncio
    async def test_returns_classification_on_success(self, tmp_path: Path) -> None:
        prompt_file = tmp_path / "classify_v1.txt"
        prompt_file.write_text(
            "SYSTEM:\nClassifier.\n---\n\nUSER:\n{taxonomy_excerpt} "
            "{source_name} {document_type} {reporting_year} {company_name} "
            "{topic_hint} {extraction_note} {passage_text}"
        )
        stage_b_response = {
            "category": "hazards",
            "subcategory": "hazards.physical_chronic.water_stress",
            "seed_category": True,
            "iro_type": "risk.physical_chronic_risk",
            "value_chain_position": "own_operations",
            "evidence_quality": "implemented",
            "time_horizon": "medium",
            "geographic_scope": ["FR"],
            "entities": [],
            "sector_relevance": [],
            "frameworks_referenced": [],
            "taxonomy_eligible": None,
            "taxonomy_activity_code": None,
            "esrs_hazard_ref": "water_stress",
            "scenario_referenced": None,
            "esrs_e2_relevant": False,
            "confidence": 0.9,
            "confidence_rationale": "Direct match.",
            "classification_note": None,
        }
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps(stage_b_response)
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)
        doc = _make_doc()

        mock_taxonomy = MagicMock()
        mock_taxonomy.validate_classification.return_value = (True, [])

        with patch("extractor.taxonomy", mock_taxonomy), \
             patch("extractor.config") as mock_cfg:
            mock_cfg.STAGE_B_MODEL = "gpt-4o-mini"
            result = await run_stage_b(
                {"text": "Water stress text", "topic_hint": "hazard", "extraction_note": None},
                doc, "taxonomy yaml here", prompt_file, mock_client,
            )
        assert result is not None
        assert result["category"] == "hazards"
        assert result["confidence"] == 0.9

    @pytest.mark.asyncio
    async def test_sets_confidence_zero_on_invalid_taxonomy(self, tmp_path: Path) -> None:
        prompt_file = tmp_path / "classify_v1.txt"
        prompt_file.write_text(
            "SYSTEM:\nClassifier.\n---\n\nUSER:\n{taxonomy_excerpt} "
            "{source_name} {document_type} {reporting_year} {company_name} "
            "{topic_hint} {extraction_note} {passage_text}"
        )
        bad_response = {
            "category": "not_valid", "subcategory": "bad.path",
            "seed_category": False, "iro_type": "bad_iro",
            "value_chain_position": "bad", "evidence_quality": "bad",
            "time_horizon": "bad", "geographic_scope": [], "entities": [],
            "sector_relevance": [], "frameworks_referenced": [],
            "taxonomy_eligible": None, "taxonomy_activity_code": None,
            "esrs_hazard_ref": None, "scenario_referenced": None,
            "esrs_e2_relevant": False, "confidence": 0.9,
            "confidence_rationale": "test", "classification_note": None,
        }
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps(bad_response)
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)
        doc = _make_doc()

        mock_taxonomy = MagicMock()
        mock_taxonomy.validate_classification.return_value = (False, ["invalid category: 'not_valid'"])

        with patch("extractor.taxonomy", mock_taxonomy), \
             patch("extractor.config") as mock_cfg:
            mock_cfg.STAGE_B_MODEL = "gpt-4o-mini"
            result = await run_stage_b(
                {"text": "some text", "topic_hint": "hazard", "extraction_note": None},
                doc, "taxonomy", prompt_file, mock_client,
            )
        assert result is not None
        assert result["confidence"] == 0.0
        assert result["classification_note"] == "invalid_taxonomy_value"
