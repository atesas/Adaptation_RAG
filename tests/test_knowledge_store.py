# =============================================================================
# tests/test_knowledge_store.py
# Tests for knowledge_store.py — mocked Azure AI Search
# =============================================================================

import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import pytest

from knowledge_store import KnowledgeStore, KnowledgeStoreError, _passage_to_dict, _dict_to_passage
from schemas.document import Document
from schemas.passage import ClassifiedPassage
from schemas.validation import ValidationStatus, ReviewPriority


def _make_store() -> KnowledgeStore:
    openai_client = MagicMock()
    store = KnowledgeStore(
        search_endpoint="https://test.search.windows.net",
        search_key="test-key",
        openai_client=openai_client,
    )
    return store


def _make_doc(**kwargs) -> Document:
    defaults = dict(
        doc_id=str(uuid.uuid4()),
        content_hash="hash-" + str(uuid.uuid4())[:8],
        raw_text="Climate text.",
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
        content_hash="phash-" + str(uuid.uuid4())[:8],
        source_doc_id="doc-001",
        text="Water stress passage.",
        page_ref="5",
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
        validation_status=ValidationStatus.RAW,
        review_priority=None,
        reviewer_id=None,
        reviewed_at=None,
        review_notes=None,
    )
    defaults.update(kwargs)
    return ClassifiedPassage(**defaults)


def _make_search_results(items: list[dict]):
    """Return an async iterable of search result dicts."""
    async def _aiter():
        for item in items:
            yield item

    mock = MagicMock()
    mock.__aiter__ = lambda self: _aiter()
    return mock


# ── Serialisation round-trip ──────────────────────────────────────────────────

class TestSerialisation:

    def test_passage_roundtrip(self) -> None:
        p = _make_passage()
        d = _passage_to_dict(p)
        p2 = _dict_to_passage(d)
        assert p2.passage_id == p.passage_id
        assert p2.content_hash == p.content_hash
        assert p2.confidence == p.confidence
        assert p2.validation_status == p.validation_status

    def test_passage_to_dict_has_all_required_keys(self) -> None:
        p = _make_passage()
        d = _passage_to_dict(p)
        required = ["passage_id", "content_hash", "source_doc_id", "text",
                    "category", "subcategory", "iro_type", "confidence",
                    "validation_status", "classified_at"]
        for key in required:
            assert key in d

    def test_validation_status_stored_as_string(self) -> None:
        p = _make_passage(validation_status=ValidationStatus.AUTO_APPROVED)
        d = _passage_to_dict(p)
        assert d["validation_status"] == "auto_approved"

    def test_dict_to_passage_handles_missing_optional_fields(self) -> None:
        p = _make_passage()
        d = _passage_to_dict(p)
        d.pop("page_ref", None)
        d.pop("extraction_note", None)
        p2 = _dict_to_passage(d)
        assert p2.page_ref is None
        assert p2.extraction_note is None


# ── register_document ─────────────────────────────────────────────────────────

class TestRegisterDocument:

    @pytest.mark.asyncio
    async def test_returns_doc_id(self) -> None:
        store = _make_store()
        doc = _make_doc()
        store._documents_client.upload_documents = AsyncMock(return_value=None)
        result = await store.register_document(doc)
        assert result == doc.doc_id

    @pytest.mark.asyncio
    async def test_calls_upload_once(self) -> None:
        store = _make_store()
        doc = _make_doc()
        store._documents_client.upload_documents = AsyncMock(return_value=None)
        await store.register_document(doc)
        store._documents_client.upload_documents.assert_called_once()

    @pytest.mark.asyncio
    async def test_raises_knowledge_store_error_on_failure(self) -> None:
        from azure.core.exceptions import HttpResponseError
        store = _make_store()
        doc = _make_doc()
        store._documents_client.upload_documents = AsyncMock(
            side_effect=HttpResponseError(message="fail")
        )
        with pytest.raises(KnowledgeStoreError):
            await store.register_document(doc)


# ── deduplicate_document ──────────────────────────────────────────────────────

class TestDeduplicateDocument:

    @pytest.mark.asyncio
    async def test_returns_true_when_hash_exists(self) -> None:
        store = _make_store()
        store._documents_client.search = AsyncMock(
            return_value=_make_search_results([{"doc_id": "existing"}])
        )
        result = await store.deduplicate_document("some-hash")
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_when_hash_absent(self) -> None:
        store = _make_store()
        store._documents_client.search = AsyncMock(
            return_value=_make_search_results([])
        )
        result = await store.deduplicate_document("new-hash")
        assert result is False


# ── update_document_status ────────────────────────────────────────────────────

class TestUpdateDocumentStatus:

    @pytest.mark.asyncio
    async def test_merges_status_field(self) -> None:
        store = _make_store()
        store._documents_client.merge_documents = AsyncMock(return_value=None)
        await store.update_document_status("doc-001", "extracted")
        call_args = store._documents_client.merge_documents.call_args
        docs = call_args.kwargs.get("documents") or call_args.args[0]
        assert docs[0]["extraction_status"] == "extracted"

    @pytest.mark.asyncio
    async def test_includes_error_message_when_provided(self) -> None:
        store = _make_store()
        store._documents_client.merge_documents = AsyncMock(return_value=None)
        await store.update_document_status("doc-001", "failed", "timeout")
        call_args = store._documents_client.merge_documents.call_args
        docs = call_args.kwargs.get("documents") or call_args.args[0]
        assert docs[0]["extraction_error"] == "timeout"


# ── upsert_passage ────────────────────────────────────────────────────────────

class TestUpsertPassage:

    @pytest.mark.asyncio
    async def test_uploads_new_passage(self) -> None:
        store = _make_store()
        store._openai.embeddings.create = AsyncMock(
            return_value=MagicMock(data=[MagicMock(embedding=[0.1] * 3072)])
        )
        store._passages_client.search = AsyncMock(
            return_value=_make_search_results([])
        )
        store._passages_client.upload_documents = AsyncMock(return_value=None)
        passage = _make_passage()
        result = await store.upsert_passage(passage)
        assert result == passage.passage_id
        store._passages_client.upload_documents.assert_called_once()

    @pytest.mark.asyncio
    async def test_merges_existing_passage(self) -> None:
        store = _make_store()
        store._openai.embeddings.create = AsyncMock(
            return_value=MagicMock(data=[MagicMock(embedding=[0.1] * 3072)])
        )
        store._passages_client.search = AsyncMock(
            return_value=_make_search_results([{"passage_id": "existing"}])
        )
        store._passages_client.merge_documents = AsyncMock(return_value=None)
        passage = _make_passage()
        await store.upsert_passage(passage)
        store._passages_client.merge_documents.assert_called_once()

    @pytest.mark.asyncio
    async def test_embedding_is_stored(self) -> None:
        store = _make_store()
        embedding = [0.5] * 3072
        store._openai.embeddings.create = AsyncMock(
            return_value=MagicMock(data=[MagicMock(embedding=embedding)])
        )
        store._passages_client.search = AsyncMock(
            return_value=_make_search_results([])
        )
        store._passages_client.upload_documents = AsyncMock(return_value=None)
        await store.upsert_passage(_make_passage())
        call_args = store._passages_client.upload_documents.call_args
        docs = call_args.kwargs.get("documents") or call_args.args[0]
        assert docs[0]["text_vector"] == embedding


# ── update_validation_status ──────────────────────────────────────────────────

class TestUpdateValidationStatus:

    @pytest.mark.asyncio
    async def test_sets_status_value(self) -> None:
        store = _make_store()
        store._passages_client.merge_documents = AsyncMock(return_value=None)
        await store.update_validation_status("p-001", ValidationStatus.APPROVED)
        call_args = store._passages_client.merge_documents.call_args
        docs = call_args.kwargs.get("documents") or call_args.args[0]
        assert docs[0]["validation_status"] == "approved"

    @pytest.mark.asyncio
    async def test_includes_reviewer_when_provided(self) -> None:
        store = _make_store()
        store._passages_client.merge_documents = AsyncMock(return_value=None)
        await store.update_validation_status(
            "p-001", ValidationStatus.APPROVED, reviewer_id="user-1"
        )
        call_args = store._passages_client.merge_documents.call_args
        docs = call_args.kwargs.get("documents") or call_args.args[0]
        assert docs[0]["reviewer_id"] == "user-1"
        assert "reviewed_at" in docs[0]


# ── query_trusted ─────────────────────────────────────────────────────────────

class TestQueryTrusted:

    @pytest.mark.asyncio
    async def test_always_applies_trusted_status_filter(self) -> None:
        store = _make_store()
        store._openai.embeddings.create = AsyncMock(
            return_value=MagicMock(data=[MagicMock(embedding=[0.1] * 3072)])
        )
        store._passages_client.search = AsyncMock(
            return_value=_make_search_results([])
        )
        await store.query_trusted("water stress")
        call_kwargs = store._passages_client.search.call_args.kwargs
        odata_filter = call_kwargs.get("filter", "")
        assert "auto_approved" in odata_filter
        assert "approved" in odata_filter
        assert "edited" in odata_filter

    @pytest.mark.asyncio
    async def test_applies_taxonomy_filter(self) -> None:
        store = _make_store()
        store._openai.embeddings.create = AsyncMock(
            return_value=MagicMock(data=[MagicMock(embedding=[0.1] * 3072)])
        )
        store._passages_client.search = AsyncMock(
            return_value=_make_search_results([])
        )
        await store.query_trusted("heat", taxonomy_filter={"category": "hazards"})
        call_kwargs = store._passages_client.search.call_args.kwargs
        assert "hazards" in call_kwargs.get("filter", "")

    @pytest.mark.asyncio
    async def test_returns_list_of_passages(self) -> None:
        store = _make_store()
        passage = _make_passage()
        passage_dict = _passage_to_dict(passage)
        store._openai.embeddings.create = AsyncMock(
            return_value=MagicMock(data=[MagicMock(embedding=[0.1] * 3072)])
        )
        store._passages_client.search = AsyncMock(
            return_value=_make_search_results([passage_dict])
        )
        results = await store.query_trusted("water stress")
        assert len(results) == 1
        assert isinstance(results[0], ClassifiedPassage)


# ── query_pending_review ──────────────────────────────────────────────────────

class TestQueryPendingReview:

    @pytest.mark.asyncio
    async def test_filters_to_pending_review_status(self) -> None:
        store = _make_store()
        store._passages_client.search = AsyncMock(
            return_value=_make_search_results([])
        )
        await store.query_pending_review()
        call_kwargs = store._passages_client.search.call_args.kwargs
        assert "pending_review" in call_kwargs.get("filter", "")

    @pytest.mark.asyncio
    async def test_filters_by_priority_when_specified(self) -> None:
        store = _make_store()
        store._passages_client.search = AsyncMock(
            return_value=_make_search_results([])
        )
        await store.query_pending_review(priority=ReviewPriority.P1_CLIENT)
        call_kwargs = store._passages_client.search.call_args.kwargs
        assert "p1_client" in call_kwargs.get("filter", "")


# ── log_correction ────────────────────────────────────────────────────────────

class TestLogCorrection:

    @pytest.mark.asyncio
    async def test_uploads_to_validation_index(self) -> None:
        store = _make_store()
        store._validation_client.upload_documents = AsyncMock(return_value=None)
        await store.log_correction(
            passage_id="p-001",
            source_doc_id="doc-001",
            document_type="corporate_report",
            original_values={"category": "hazards"},
            corrected_values={"category": "responses"},
            correction_type="category",
            error_pattern_tag="category_boundary",
            reviewer_id="user-1",
            review_notes="Misclassified",
            confidence_at_review=0.72,
        )
        store._validation_client.upload_documents.assert_called_once()

    @pytest.mark.asyncio
    async def test_log_record_has_log_id(self) -> None:
        store = _make_store()
        store._validation_client.upload_documents = AsyncMock(return_value=None)
        await store.log_correction(
            passage_id="p-001", source_doc_id="doc-001",
            document_type="corporate_report",
            original_values={}, corrected_values={},
            correction_type="category", error_pattern_tag=None,
            reviewer_id="user-1", review_notes="note",
            confidence_at_review=0.8,
        )
        call_args = store._validation_client.upload_documents.call_args
        docs = call_args.kwargs.get("documents") or call_args.args[0]
        assert "log_id" in docs[0]
        # Verify it's a valid UUID
        uuid.UUID(docs[0]["log_id"])
