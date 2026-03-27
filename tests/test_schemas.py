"""
tests/test_schemas.py

Unit tests for schemas/validation.py, schemas/document.py, schemas/passage.py.

Coverage per PROJECT_BRIEF_v1.1.md Section 7 (schemas/ row):
  - All SOURCE_TYPES values are non-empty strings
  - All DOCUMENT_TYPES values are non-empty strings
  - All IRO_TYPES values are non-empty strings
  - All VALUE_CHAIN_POSITIONS values are non-empty strings
  - Document dataclass instantiates with required fields
  - ClassifiedPassage dataclass instantiates with required fields
"""

from datetime import datetime

import pytest

from schemas.validation import (
    ValidationStatus,
    ReviewPriority,
    TRUSTED_STATUSES,
    AUTO_APPROVE_CONFIDENCE_THRESHOLD,
    AUTO_REJECT_CONFIDENCE_THRESHOLD,
    AUTO_APPROVE_SOURCE_TYPES,
)
from schemas.document import Document, SOURCE_TYPES, DOCUMENT_TYPES
from schemas.passage import (
    ClassifiedPassage,
    IRO_TYPES,
    VALUE_CHAIN_POSITIONS,
    EVIDENCE_QUALITY_LEVELS,
    TIME_HORIZONS,
)


# ── Controlled vocabulary tests ───────────────────────────────────────────────

class TestSourceTypes:
    def test_all_non_empty_strings(self) -> None:
        for value in SOURCE_TYPES:
            assert isinstance(value, str), f"SOURCE_TYPES entry is not a string: {value!r}"
            assert value.strip(), f"SOURCE_TYPES entry is empty or whitespace: {value!r}"

    def test_no_duplicates(self) -> None:
        assert len(SOURCE_TYPES) == len(set(SOURCE_TYPES))

    def test_google_cse_present(self) -> None:
        assert "google_cse" in SOURCE_TYPES

    def test_corporate_pdf_present(self) -> None:
        assert "corporate_pdf" in SOURCE_TYPES


class TestDocumentTypes:
    def test_all_non_empty_strings(self) -> None:
        for value in DOCUMENT_TYPES:
            assert isinstance(value, str), f"DOCUMENT_TYPES entry is not a string: {value!r}"
            assert value.strip(), f"DOCUMENT_TYPES entry is empty or whitespace: {value!r}"

    def test_no_duplicates(self) -> None:
        assert len(DOCUMENT_TYPES) == len(set(DOCUMENT_TYPES))

    def test_corporate_report_present(self) -> None:
        assert "corporate_report" in DOCUMENT_TYPES


class TestIROTypes:
    def test_all_non_empty_strings(self) -> None:
        for value in IRO_TYPES:
            assert isinstance(value, str), f"IRO_TYPES entry is not a string: {value!r}"
            assert value.strip(), f"IRO_TYPES entry is empty or whitespace: {value!r}"

    def test_no_duplicates(self) -> None:
        assert len(IRO_TYPES) == len(set(IRO_TYPES))

    def test_not_specified_present(self) -> None:
        assert "not_specified" in IRO_TYPES


class TestValueChainPositions:
    def test_all_non_empty_strings(self) -> None:
        for value in VALUE_CHAIN_POSITIONS:
            assert isinstance(value, str), f"VALUE_CHAIN_POSITIONS entry is not a string: {value!r}"
            assert value.strip(), f"VALUE_CHAIN_POSITIONS entry is empty: {value!r}"

    def test_no_duplicates(self) -> None:
        assert len(VALUE_CHAIN_POSITIONS) == len(set(VALUE_CHAIN_POSITIONS))

    def test_own_operations_present(self) -> None:
        assert "own_operations" in VALUE_CHAIN_POSITIONS


class TestEvidenceQualityLevels:
    def test_all_non_empty_strings(self) -> None:
        for value in EVIDENCE_QUALITY_LEVELS:
            assert isinstance(value, str)
            assert value.strip()

    def test_no_duplicates(self) -> None:
        assert len(EVIDENCE_QUALITY_LEVELS) == len(set(EVIDENCE_QUALITY_LEVELS))


class TestTimeHorizons:
    def test_all_non_empty_strings(self) -> None:
        for value in TIME_HORIZONS:
            assert isinstance(value, str)
            assert value.strip()

    def test_unspecified_present(self) -> None:
        assert "unspecified" in TIME_HORIZONS


# ── Validation enums ──────────────────────────────────────────────────────────

class TestValidationStatus:
    def test_values_are_strings(self) -> None:
        for status in ValidationStatus:
            assert isinstance(status.value, str)
            assert status.value.strip()

    def test_trusted_statuses_are_subset(self) -> None:
        all_statuses = set(ValidationStatus)
        for ts in TRUSTED_STATUSES:
            assert ts in all_statuses

    def test_raw_not_in_trusted(self) -> None:
        assert ValidationStatus.RAW not in TRUSTED_STATUSES

    def test_rejected_not_in_trusted(self) -> None:
        assert ValidationStatus.REJECTED not in TRUSTED_STATUSES

    def test_auto_approved_in_trusted(self) -> None:
        assert ValidationStatus.AUTO_APPROVED in TRUSTED_STATUSES

    def test_approved_in_trusted(self) -> None:
        assert ValidationStatus.APPROVED in TRUSTED_STATUSES

    def test_edited_in_trusted(self) -> None:
        assert ValidationStatus.EDITED in TRUSTED_STATUSES


class TestValidationConstants:
    def test_approve_threshold_above_reject(self) -> None:
        assert AUTO_APPROVE_CONFIDENCE_THRESHOLD > AUTO_REJECT_CONFIDENCE_THRESHOLD

    def test_thresholds_in_range(self) -> None:
        assert 0.0 < AUTO_REJECT_CONFIDENCE_THRESHOLD < 1.0
        assert 0.0 < AUTO_APPROVE_CONFIDENCE_THRESHOLD < 1.0

    def test_auto_approve_source_types_are_strings(self) -> None:
        for st in AUTO_APPROVE_SOURCE_TYPES:
            assert isinstance(st, str)
            assert st.strip()


# ── Dataclass instantiation ───────────────────────────────────────────────────

class TestDocumentDataclass:
    def _make_document(self, **overrides) -> Document:
        defaults = dict(
            doc_id="test-doc-id",
            content_hash="abc123",
            raw_text="Sample climate text about water stress and drought.",
            title="Test Report 2024",
            language="en",
            source_url="/path/to/report.pdf",
            source_type="corporate_pdf",
            adapter="CorporatePDFAdapter",
            publication_date=None,
            ingestion_date=datetime(2024, 1, 1),
            reporting_year=2024,
            document_type="corporate_report",
            company_name="Test Corp",
            company_id=None,
            csrd_wave=None,
            country=["FR"],
            sector_hint=["food_and_beverage"],
            extraction_status="pending",
            extraction_error=None,
        )
        defaults.update(overrides)
        return Document(**defaults)

    def test_instantiates_with_required_fields(self) -> None:
        doc = self._make_document()
        assert doc.doc_id == "test-doc-id"
        assert doc.source_type == "corporate_pdf"
        assert doc.extraction_status == "pending"

    def test_country_is_list(self) -> None:
        doc = self._make_document(country=["FR", "DE"])
        assert isinstance(doc.country, list)
        assert len(doc.country) == 2

    def test_optional_fields_default_to_none(self) -> None:
        doc = self._make_document()
        assert doc.publication_date is None
        assert doc.extraction_error is None

    def test_source_type_value_in_vocab(self) -> None:
        doc = self._make_document()
        assert doc.source_type in SOURCE_TYPES

    def test_document_type_value_in_vocab(self) -> None:
        doc = self._make_document()
        assert doc.document_type in DOCUMENT_TYPES


class TestClassifiedPassageDataclass:
    def _make_passage(self, **overrides) -> ClassifiedPassage:
        defaults = dict(
            passage_id="pass-001",
            content_hash="def456",
            source_doc_id="doc-001",
            text="Water stress at seven facilities is identified as a primary physical risk.",
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
            evidence_quality="monitored",
            time_horizon="medium",
            geographic_scope=["ZA", "AU"],
            entities=["Test Corp"],
            sector_relevance=["beverages"],
            frameworks_referenced=["tcfd"],
            taxonomy_eligible=None,
            taxonomy_activity_code=None,
            esrs_hazard_ref="E1_AR11_chronic_water_stress",
            scenario_referenced=None,
            esrs_e2_relevant=False,
            confidence=0.91,
            confidence_rationale="Clear water stress language with facility count and geographic scope.",
            classification_note=None,
            classification_model="gpt-4o-mini",
            classified_at=datetime(2024, 6, 1),
            validation_status=ValidationStatus.RAW,
            review_priority=None,
            reviewer_id=None,
            reviewed_at=None,
            review_notes=None,
        )
        defaults.update(overrides)
        return ClassifiedPassage(**defaults)

    def test_instantiates_with_required_fields(self) -> None:
        p = self._make_passage()
        assert p.passage_id == "pass-001"
        assert p.category == "hazards"
        assert p.validation_status == ValidationStatus.RAW

    def test_iro_type_in_vocab(self) -> None:
        p = self._make_passage()
        assert p.iro_type in IRO_TYPES

    def test_value_chain_position_in_vocab(self) -> None:
        p = self._make_passage()
        assert p.value_chain_position in VALUE_CHAIN_POSITIONS

    def test_evidence_quality_in_vocab(self) -> None:
        p = self._make_passage()
        assert p.evidence_quality in EVIDENCE_QUALITY_LEVELS

    def test_time_horizon_in_vocab(self) -> None:
        p = self._make_passage()
        assert p.time_horizon in TIME_HORIZONS

    def test_confidence_in_range(self) -> None:
        p = self._make_passage(confidence=0.91)
        assert 0.0 <= p.confidence <= 1.0

    def test_correction_fields_default_none(self) -> None:
        p = self._make_passage()
        assert p.original_category is None
        assert p.original_subcategory is None
        assert p.error_pattern_tag is None

    def test_geographic_scope_is_list(self) -> None:
        p = self._make_passage()
        assert isinstance(p.geographic_scope, list)
