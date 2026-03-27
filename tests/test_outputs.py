# =============================================================================
# tests/test_outputs.py
# Tests for outputs/ — citations, newsletter, sector_brief, company_assessment
# =============================================================================

import uuid
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from outputs.citations import (
    build_citation_index,
    format_citations_appendix,
    format_passages_for_prompt,
)
from outputs.newsletter import generate_newsletter
from outputs.sector_brief import generate_sector_brief, _sector_title
from outputs.company_assessment import (
    generate_company_assessment,
    _deduplicate,
    _parse_total_score,
)
from schemas.passage import ClassifiedPassage
from schemas.validation import ValidationStatus


def _make_passage(**kwargs) -> ClassifiedPassage:
    defaults = dict(
        passage_id=str(uuid.uuid4()),
        content_hash="hash-" + str(uuid.uuid4())[:8],
        source_doc_id="doc-001",
        text="Water stress affects operations in Southern Europe.",
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
        geographic_scope=["FR"],
        entities=["TestCo"],
        sector_relevance=["food_and_beverage"],
        frameworks_referenced=["csrd_esrs"],
        taxonomy_eligible=None,
        taxonomy_activity_code=None,
        esrs_hazard_ref="water_stress",
        scenario_referenced=None,
        esrs_e2_relevant=False,
        confidence=0.9,
        confidence_rationale="Direct match.",
        classification_note=None,
        classification_model="gpt-4o-mini",
        classified_at=datetime.utcnow(),
        validation_status=ValidationStatus.AUTO_APPROVED,
        review_priority=None,
        reviewer_id=None,
        reviewed_at=None,
        review_notes=None,
    )
    defaults.update(kwargs)
    return ClassifiedPassage(**defaults)


# ── citations.py ──────────────────────────────────────────────────────────────

class TestBuildCitationIndex:

    def test_returns_dict_keyed_by_passage_id(self) -> None:
        p = _make_passage()
        index = build_citation_index([p])
        assert p.passage_id in index

    def test_excerpt_is_first_120_chars(self) -> None:
        long_text = "x" * 200
        p = _make_passage(text=long_text)
        index = build_citation_index([p])
        assert len(index[p.passage_id].text_excerpt) == 120

    def test_empty_list_returns_empty_dict(self) -> None:
        assert build_citation_index([]) == {}

    def test_multiple_passages_all_indexed(self) -> None:
        passages = [_make_passage() for _ in range(5)]
        index = build_citation_index(passages)
        assert len(index) == 5


class TestFormatCitationsAppendix:

    def test_returns_empty_string_for_no_citations(self) -> None:
        assert format_citations_appendix({}) == ""

    def test_contains_citations_heading(self) -> None:
        p = _make_passage()
        index = build_citation_index([p])
        result = format_citations_appendix(index)
        assert "## Citations" in result

    def test_contains_passage_id(self) -> None:
        p = _make_passage()
        index = build_citation_index([p])
        result = format_citations_appendix(index)
        assert p.passage_id[:8] in result


class TestFormatPassagesForPrompt:

    def test_includes_passage_id(self) -> None:
        p = _make_passage()
        result = format_passages_for_prompt([p])
        assert p.passage_id in result

    def test_includes_passage_text(self) -> None:
        p = _make_passage(text="Drought risk in Spain.")
        result = format_passages_for_prompt([p])
        assert "Drought risk in Spain." in result

    def test_includes_category_and_confidence(self) -> None:
        p = _make_passage(category="hazards", confidence=0.88)
        result = format_passages_for_prompt([p])
        assert "hazards" in result
        assert "0.88" in result

    def test_multiple_passages_separated(self) -> None:
        passages = [_make_passage() for _ in range(3)]
        result = format_passages_for_prompt(passages)
        assert result.count("---") >= 2


# ── newsletter.py ─────────────────────────────────────────────────────────────

class TestGenerateNewsletter:

    @pytest.mark.asyncio
    async def test_raises_for_invalid_sector(self) -> None:
        mock_store = MagicMock()
        mock_client = MagicMock()
        with pytest.raises(ValueError, match="Unknown sector"):
            await generate_newsletter("invalid_sector", mock_store, mock_client)

    @pytest.mark.asyncio
    async def test_returns_no_passages_message_when_empty(self) -> None:
        mock_store = MagicMock()
        mock_store.query_trusted = AsyncMock(return_value=[])
        mock_client = MagicMock()
        result = await generate_newsletter("beverages", mock_store, mock_client)
        assert "No trusted passages" in result

    @pytest.mark.asyncio
    async def test_calls_query_trusted_with_sector_filter(self) -> None:
        mock_store = MagicMock()
        mock_store.query_trusted = AsyncMock(return_value=[])
        mock_client = MagicMock()
        await generate_newsletter("beverages", mock_store, mock_client)
        call_kwargs = mock_store.query_trusted.call_args.kwargs
        assert call_kwargs.get("taxonomy_filter", {}).get("sector_relevance") == "beverages"

    @pytest.mark.asyncio
    async def test_output_contains_citations_when_passages_exist(self, tmp_path: Path) -> None:
        prompt_file = tmp_path / "newsletter_v1.txt"
        prompt_file.write_text(
            "SYSTEM:\nBot.\n---\n\nUSER:\n"
            "Sector: {sector} Period: {reporting_period} "
            "Count: {passage_count} Passages: {passages_text}"
        )
        passages = [_make_passage()]
        mock_store = MagicMock()
        mock_store.query_trusted = AsyncMock(return_value=passages)
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "## Physical Hazards\n- Water stress [P:abc]"
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

        with patch("outputs.newsletter.config") as mock_cfg:
            mock_cfg.PROMPTS_DIR = tmp_path
            mock_cfg.NEWSLETTER_PROMPT_VERSION = "v1"
            mock_cfg.OUTPUT_MODEL = "gpt-4o"
            result = await generate_newsletter("beverages", mock_store, mock_client)

        assert "## Citations" in result
        assert passages[0].passage_id[:8] in result


# ── sector_brief.py ───────────────────────────────────────────────────────────

class TestSectorTitle:

    def test_converts_underscores_to_spaces(self) -> None:
        assert _sector_title("food_and_beverage") == "Food And Beverage"

    def test_title_case(self) -> None:
        assert _sector_title("beverages") == "Beverages"


class TestGenerateSectorBrief:

    @pytest.mark.asyncio
    async def test_raises_for_invalid_sector(self) -> None:
        mock_store = MagicMock()
        mock_client = MagicMock()
        with pytest.raises(ValueError, match="Unknown sector"):
            await generate_sector_brief("invalid_sector", mock_store, mock_client)

    @pytest.mark.asyncio
    async def test_raises_for_invalid_time_horizon(self) -> None:
        mock_store = MagicMock()
        mock_client = MagicMock()
        with pytest.raises(ValueError, match="Unknown time_horizon"):
            await generate_sector_brief(
                "beverages", mock_store, mock_client, time_horizon="decade"
            )

    @pytest.mark.asyncio
    async def test_returns_no_passages_message_when_empty(self) -> None:
        mock_store = MagicMock()
        mock_store.query_trusted = AsyncMock(return_value=[])
        mock_client = MagicMock()
        result = await generate_sector_brief("beverages", mock_store, mock_client)
        assert "No trusted passages" in result

    @pytest.mark.asyncio
    async def test_deduplicates_passages_across_dimensions(self) -> None:
        shared_passage = _make_passage()
        mock_store = MagicMock()
        # Return same passage for all dimension queries
        mock_store.query_trusted = AsyncMock(return_value=[shared_passage])
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "## D1\n- content"
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

        with patch("outputs.sector_brief.config") as mock_cfg:
            mock_cfg.PROMPTS_DIR = Path("/tmp")
            mock_cfg.SECTOR_BRIEF_PROMPT_VERSION = "v1"
            mock_cfg.OUTPUT_MODEL = "gpt-4o"
            with patch("outputs.sector_brief._run_prompt", new=AsyncMock(return_value="## D1\n")):
                result = await generate_sector_brief("beverages", mock_store, mock_client)

        # The passage should appear only once in citations
        assert result.count(shared_passage.passage_id[:8]) <= 2


# ── company_assessment.py ─────────────────────────────────────────────────────

class TestDeduplicate:

    def test_removes_duplicate_hashes(self) -> None:
        p1 = _make_passage(content_hash="same-hash", confidence=0.9)
        p2 = _make_passage(content_hash="same-hash", confidence=0.8)
        result = _deduplicate([p1, p2], top_k=10)
        assert len(result) == 1

    def test_keeps_higher_confidence_when_duplicate(self) -> None:
        p1 = _make_passage(content_hash="same-hash", confidence=0.9)
        p2 = _make_passage(content_hash="same-hash", confidence=0.7)
        result = _deduplicate([p1, p2], top_k=10)
        assert result[0].confidence == 0.9

    def test_caps_at_top_k(self) -> None:
        passages = [_make_passage() for _ in range(20)]
        result = _deduplicate(passages, top_k=5)
        assert len(result) == 5

    def test_sorts_by_confidence_descending(self) -> None:
        p_low = _make_passage(confidence=0.5)
        p_high = _make_passage(confidence=0.95)
        result = _deduplicate([p_low, p_high], top_k=10)
        assert result[0].confidence == 0.95


class TestParseTotalScore:

    def test_extracts_score_from_text(self) -> None:
        text = "Some content\n## Overall Score: 18/24 (75%)"
        assert _parse_total_score(text) == "18/24"

    def test_returns_none_when_not_found(self) -> None:
        assert _parse_total_score("No score here") is None

    def test_case_insensitive(self) -> None:
        text = "overall score: 15/24"
        assert _parse_total_score(text) == "15/24"


class TestGenerateCompanyAssessment:

    @pytest.mark.asyncio
    async def test_returns_no_passages_message_when_empty(self) -> None:
        mock_store = MagicMock()
        mock_store.query_by_company = AsyncMock(return_value=[])
        mock_client = MagicMock()
        result = await generate_company_assessment(
            "danone", "Danone", mock_store, mock_client
        )
        assert "No trusted passages" in result

    @pytest.mark.asyncio
    async def test_calls_query_by_company_with_trusted_only(self) -> None:
        mock_store = MagicMock()
        mock_store.query_by_company = AsyncMock(return_value=[])
        mock_client = MagicMock()
        await generate_company_assessment("danone", "Danone", mock_store, mock_client)
        mock_store.query_by_company.assert_called_once_with(
            company_id="danone", trusted_only=True
        )

    @pytest.mark.asyncio
    async def test_output_contains_company_header(self, tmp_path: Path) -> None:
        passages = [_make_passage()]
        mock_store = MagicMock()
        mock_store.query_by_company = AsyncMock(return_value=passages)
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "## D1\n- Water stress [P:abc]\n## Overall Score: 12/24"
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)

        with patch("outputs.company_assessment.config") as mock_cfg:
            mock_cfg.PROMPTS_DIR = tmp_path
            mock_cfg.COMPANY_ASSESSMENT_PROMPT_VERSION = "v1"
            mock_cfg.OUTPUT_MODEL = "gpt-4o"
            prompt_file = tmp_path / "company_assessment_v1.txt"
            prompt_file.write_text(
                "SYSTEM:\nBot.\n---\n\nUSER:\n"
                "{company_name} {reporting_year} {passage_count} {passages_text}"
            )
            result = await generate_company_assessment(
                "danone", "Danone", mock_store, mock_client, reporting_year=2024
            )

        assert "Danone" in result
        assert "2024" in result
