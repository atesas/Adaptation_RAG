# =============================================================================
# knowledge_store.py
# THE ONLY FILE that imports the Azure AI Search SDK.
# All Azure Search operations go through this class.
# =============================================================================

import logging
import uuid
from datetime import datetime
from typing import Optional

from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import HttpResponseError
from azure.search.documents import SearchClient
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
    SemanticConfiguration,
    SemanticField,
    SemanticPrioritizedFields,
    SemanticSearch,
)
from openai import AsyncAzureOpenAI

import config
from schemas.document import Document
from schemas.passage import ClassifiedPassage
from schemas.validation import ValidationStatus, ReviewPriority, TRUSTED_STATUSES

logger = logging.getLogger(__name__)

INDEX_PASSAGES   = config.INDEX_PASSAGES
INDEX_DOCUMENTS  = config.INDEX_DOCUMENTS
INDEX_VALIDATION = config.INDEX_VALIDATION


class KnowledgeStoreError(Exception):
    """Raised when any Azure AI Search operation fails."""


class KnowledgeStore:

    def __init__(
        self,
        search_endpoint: str,
        search_key: str,
        openai_client: AsyncAzureOpenAI,
    ) -> None:
        credential = AzureKeyCredential(search_key)
        self._index_client = SearchIndexClient(
            endpoint=search_endpoint, credential=credential
        )
        self._passages_client = AsyncSearchClient(
            endpoint=search_endpoint,
            index_name=INDEX_PASSAGES,
            credential=credential,
        )
        self._documents_client = AsyncSearchClient(
            endpoint=search_endpoint,
            index_name=INDEX_DOCUMENTS,
            credential=credential,
        )
        self._validation_client = AsyncSearchClient(
            endpoint=search_endpoint,
            index_name=INDEX_VALIDATION,
            credential=credential,
        )
        self._openai = openai_client

    # ── Index provisioning ────────────────────────────────────────────────────

    def ensure_indexes(self) -> None:
        """Create all three indexes if they don't already exist."""
        existing = {idx.name for idx in self._index_client.list_indexes()}
        if INDEX_PASSAGES not in existing:
            self._index_client.create_index(self._passages_index_schema())
            logger.info("Created index: %s", INDEX_PASSAGES)
        if INDEX_DOCUMENTS not in existing:
            self._index_client.create_index(self._documents_index_schema())
            logger.info("Created index: %s", INDEX_DOCUMENTS)
        if INDEX_VALIDATION not in existing:
            self._index_client.create_index(self._validation_index_schema())
            logger.info("Created index: %s", INDEX_VALIDATION)

    def reset_indexes(self) -> None:
        """Delete all three indexes then recreate them with the current schema.

        WARNING: this permanently deletes all stored passages, documents, and
        validation log entries. Use only when the schema has changed and you
        need a clean slate.
        """
        for name in [INDEX_PASSAGES, INDEX_DOCUMENTS, INDEX_VALIDATION]:
            try:
                self._index_client.delete_index(name)
                logger.info("Deleted index: %s", name)
            except Exception:
                pass  # index didn't exist — that's fine
        self._index_client.create_index(self._passages_index_schema())
        logger.info("Created index: %s", INDEX_PASSAGES)
        self._index_client.create_index(self._documents_index_schema())
        logger.info("Created index: %s", INDEX_DOCUMENTS)
        self._index_client.create_index(self._validation_index_schema())
        logger.info("Created index: %s", INDEX_VALIDATION)

    def _passages_index_schema(self) -> SearchIndex:
        fields = [
            SimpleField(name="passage_id", type=SearchFieldDataType.String, key=True, filterable=True),
            SimpleField(name="content_hash", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="source_doc_id", type=SearchFieldDataType.String, filterable=True),
            SearchableField(name="text", type=SearchFieldDataType.String),
            SimpleField(name="page_ref", type=SearchFieldDataType.String, filterable=False),
            SimpleField(name="char_start", type=SearchFieldDataType.Int32, filterable=False),
            SimpleField(name="char_end", type=SearchFieldDataType.Int32, filterable=False),
            SimpleField(name="topic_hint", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="extraction_note", type=SearchFieldDataType.String, filterable=False),
            SimpleField(name="category", type=SearchFieldDataType.String, filterable=True, facetable=True),
            SimpleField(name="subcategory", type=SearchFieldDataType.String, filterable=True, facetable=True),
            SimpleField(name="seed_category", type=SearchFieldDataType.Boolean, filterable=True),
            SimpleField(name="iro_type", type=SearchFieldDataType.String, filterable=True, facetable=True),
            SimpleField(name="value_chain_position", type=SearchFieldDataType.String, filterable=True, facetable=True),
            SimpleField(name="evidence_quality", type=SearchFieldDataType.String, filterable=True, facetable=True),
            SimpleField(name="time_horizon", type=SearchFieldDataType.String, filterable=True, facetable=True),
            SearchField(
                name="geographic_scope",
                type=SearchFieldDataType.Collection(SearchFieldDataType.String),
                filterable=True,
            ),
            SearchField(
                name="entities",
                type=SearchFieldDataType.Collection(SearchFieldDataType.String),
                filterable=True,
            ),
            SearchField(
                name="sector_relevance",
                type=SearchFieldDataType.Collection(SearchFieldDataType.String),
                filterable=True,
                facetable=True,
            ),
            SearchField(
                name="frameworks_referenced",
                type=SearchFieldDataType.Collection(SearchFieldDataType.String),
                filterable=True,
            ),
            SimpleField(name="taxonomy_eligible", type=SearchFieldDataType.Boolean, filterable=True),
            SimpleField(name="taxonomy_activity_code", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="esrs_hazard_ref", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="scenario_referenced", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="esrs_e2_relevant", type=SearchFieldDataType.Boolean, filterable=True),
            SimpleField(name="confidence", type=SearchFieldDataType.Double, filterable=True, sortable=True),
            SearchableField(name="confidence_rationale", type=SearchFieldDataType.String),
            SimpleField(name="classification_note", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="classification_model", type=SearchFieldDataType.String, filterable=False),
            SimpleField(name="classified_at", type=SearchFieldDataType.DateTimeOffset, filterable=True, sortable=True),
            SimpleField(name="validation_status", type=SearchFieldDataType.String, filterable=True, facetable=True, sortable=True),
            SimpleField(name="review_priority", type=SearchFieldDataType.String, filterable=True, sortable=True),
            SimpleField(name="reviewer_id", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="reviewed_at", type=SearchFieldDataType.DateTimeOffset, filterable=True, sortable=True),
            SimpleField(name="review_notes", type=SearchFieldDataType.String, filterable=False),
            SimpleField(name="original_category", type=SearchFieldDataType.String, filterable=False),
            SimpleField(name="original_subcategory", type=SearchFieldDataType.String, filterable=False),
            SimpleField(name="original_iro_type", type=SearchFieldDataType.String, filterable=False),
            SimpleField(name="original_evidence_quality", type=SearchFieldDataType.String, filterable=False),
            SimpleField(name="correction_type", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="error_pattern_tag", type=SearchFieldDataType.String, filterable=True, facetable=True),
            SearchField(
                name="text_vector",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                searchable=True,
                vector_search_dimensions=3072,
                vector_search_profile_name="hnsw_profile",
            ),
        ]
        vector_search = VectorSearch(
            algorithms=[HnswAlgorithmConfiguration(name="hnsw_algo")],
            profiles=[VectorSearchProfile(name="hnsw_profile", algorithm_configuration_name="hnsw_algo")],
        )
        semantic_search = SemanticSearch(
            configurations=[
                SemanticConfiguration(
                    name="default",
                    prioritized_fields=SemanticPrioritizedFields(
                        content_fields=[SemanticField(field_name="text")],
                    ),
                )
            ]
        )
        return SearchIndex(
            name=INDEX_PASSAGES,
            fields=fields,
            vector_search=vector_search,
            semantic_search=semantic_search,
        )

    def _documents_index_schema(self) -> SearchIndex:
        fields = [
            SimpleField(name="doc_id", type=SearchFieldDataType.String, key=True, filterable=True),
            SimpleField(name="content_hash", type=SearchFieldDataType.String, filterable=True),
            SearchableField(name="raw_text", type=SearchFieldDataType.String),
            SearchableField(name="title", type=SearchFieldDataType.String),
            SimpleField(name="language", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="source_url", type=SearchFieldDataType.String, filterable=False),
            SimpleField(name="source_type", type=SearchFieldDataType.String, filterable=True, facetable=True),
            SimpleField(name="adapter", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="publication_date", type=SearchFieldDataType.DateTimeOffset, filterable=True, sortable=True),
            SimpleField(name="ingestion_date", type=SearchFieldDataType.DateTimeOffset, filterable=True, sortable=True),
            SimpleField(name="reporting_year", type=SearchFieldDataType.Int32, filterable=True, sortable=True),
            SimpleField(name="document_type", type=SearchFieldDataType.String, filterable=True, facetable=True),
            SimpleField(name="company_name", type=SearchFieldDataType.String, filterable=True, facetable=True),
            SimpleField(name="company_id", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="csrd_wave", type=SearchFieldDataType.Int32, filterable=True),
            SearchField(
                name="country",
                type=SearchFieldDataType.Collection(SearchFieldDataType.String),
                filterable=True,
            ),
            SearchField(
                name="sector_hint",
                type=SearchFieldDataType.Collection(SearchFieldDataType.String),
                filterable=True,
            ),
            SimpleField(name="extraction_status", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="extraction_error", type=SearchFieldDataType.String, filterable=False),
        ]
        return SearchIndex(name=INDEX_DOCUMENTS, fields=fields)

    def _validation_index_schema(self) -> SearchIndex:
        fields = [
            SimpleField(name="log_id", type=SearchFieldDataType.String, key=True),
            SimpleField(name="passage_id", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="source_doc_id", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="document_type", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="correction_type", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="error_pattern_tag", type=SearchFieldDataType.String, filterable=True, facetable=True),
            SimpleField(name="reviewer_id", type=SearchFieldDataType.String, filterable=True),
            SimpleField(name="review_notes", type=SearchFieldDataType.String, filterable=False),
            SimpleField(name="confidence_at_review", type=SearchFieldDataType.Double, filterable=True),
            SimpleField(name="reviewed_at", type=SearchFieldDataType.DateTimeOffset, filterable=True, sortable=True),
            SimpleField(name="original_values_json", type=SearchFieldDataType.String, filterable=False),
            SimpleField(name="corrected_values_json", type=SearchFieldDataType.String, filterable=False),
        ]
        return SearchIndex(name=INDEX_VALIDATION, fields=fields)

    # ── Embedding ─────────────────────────────────────────────────────────────

    async def _embed(self, text: str) -> list[float]:
        response = await self._openai.embeddings.create(
            input=text,
            model=config.EMBEDDING_DEPLOYMENT,
        )
        return response.data[0].embedding

    # ── Document registry ─────────────────────────────────────────────────────

    async def register_document(self, doc: Document) -> str:
        doc_dict = {
            "doc_id": doc.doc_id,
            "content_hash": doc.content_hash,
            "raw_text": doc.raw_text,
            "title": doc.title,
            "language": doc.language,
            "source_url": doc.source_url,
            "source_type": doc.source_type,
            "adapter": doc.adapter,
            "publication_date": doc.publication_date.isoformat() + "Z" if doc.publication_date else None,
            "ingestion_date": doc.ingestion_date.isoformat() + "Z",
            "reporting_year": doc.reporting_year,
            "document_type": doc.document_type,
            "company_name": doc.company_name,
            "company_id": doc.company_id,
            "csrd_wave": doc.csrd_wave,
            "country": doc.country,
            "sector_hint": doc.sector_hint,
            "extraction_status": doc.extraction_status,
            "extraction_error": doc.extraction_error,
        }
        try:
            await self._documents_client.upload_documents(documents=[doc_dict])
        except HttpResponseError as exc:
            raise KnowledgeStoreError(f"register_document failed: {exc}") from exc
        return doc.doc_id

    async def deduplicate_document(self, content_hash: str) -> bool:
        try:
            results = await self._documents_client.search(
                search_text="*",
                filter=f"content_hash eq '{content_hash}'",
                top=1,
                select=["doc_id"],
            )
            async for _ in results:
                return True
            return False
        except HttpResponseError as exc:
            raise KnowledgeStoreError(f"deduplicate_document failed: {exc}") from exc

    async def update_document_status(
        self,
        doc_id: str,
        extraction_status: str,
        extraction_error: Optional[str] = None,
    ) -> None:
        patch = {
            "doc_id": doc_id,
            "extraction_status": extraction_status,
            "extraction_error": extraction_error,
        }
        try:
            await self._documents_client.merge_documents(documents=[patch])
        except HttpResponseError as exc:
            raise KnowledgeStoreError(f"update_document_status failed: {exc}") from exc

    # ── Passage operations ────────────────────────────────────────────────────

    async def upsert_passage(self, passage: ClassifiedPassage) -> str:
        exists = await self.deduplicate_passage(passage.content_hash)
        embedding = await self._embed(passage.text)
        doc = _passage_to_dict(passage)
        doc["text_vector"] = embedding
        try:
            if exists:
                # Exclude immutable content fields on update
                doc.pop("text", None)
                doc.pop("text_vector", None)
                await self._passages_client.merge_documents(documents=[doc])
            else:
                await self._passages_client.upload_documents(documents=[doc])
        except HttpResponseError as exc:
            raise KnowledgeStoreError(f"upsert_passage failed: {exc}") from exc
        return passage.passage_id

    async def get_passage(self, passage_id: str) -> ClassifiedPassage:
        try:
            result = await self._passages_client.get_document(key=passage_id)
        except HttpResponseError as exc:
            raise KnowledgeStoreError(f"get_passage failed: {exc}") from exc
        return _dict_to_passage(result)

    async def deduplicate_passage(self, content_hash: str) -> bool:
        try:
            results = await self._passages_client.search(
                search_text="*",
                filter=f"content_hash eq '{content_hash}'",
                top=1,
                select=["passage_id"],
            )
            async for _ in results:
                return True
            return False
        except HttpResponseError as exc:
            raise KnowledgeStoreError(f"deduplicate_passage failed: {exc}") from exc

    # ── Validation operations ─────────────────────────────────────────────────

    async def update_validation_status(
        self,
        passage_id: str,
        status: ValidationStatus,
        reviewer_id: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> None:
        patch: dict = {"passage_id": passage_id, "validation_status": status.value}
        if reviewer_id:
            patch["reviewer_id"] = reviewer_id
            patch["reviewed_at"] = datetime.utcnow().isoformat() + "Z"
        if notes:
            patch["review_notes"] = notes
        try:
            await self._passages_client.merge_documents(documents=[patch])
        except HttpResponseError as exc:
            raise KnowledgeStoreError(f"update_validation_status failed: {exc}") from exc

    async def set_review_priority(self, passage_id: str, priority: ReviewPriority) -> None:
        patch = {"passage_id": passage_id, "review_priority": priority.value}
        try:
            await self._passages_client.merge_documents(documents=[patch])
        except HttpResponseError as exc:
            raise KnowledgeStoreError(f"set_review_priority failed: {exc}") from exc

    async def apply_human_correction(
        self,
        passage_id: str,
        corrections: dict,
        reviewer_id: str,
        correction_type: str,
        error_pattern_tag: Optional[str],
        review_notes: str,
    ) -> None:
        current = await self.get_passage(passage_id)
        original_values: dict = {}
        correctable = {
            "category", "subcategory", "iro_type", "evidence_quality",
            "entities", "value_chain_position", "time_horizon",
        }
        for field_name, new_value in corrections.items():
            if field_name in correctable:
                original_values[field_name] = getattr(current, field_name, None)

        patch = dict(corrections)
        patch["passage_id"] = passage_id
        patch["validation_status"] = ValidationStatus.EDITED.value
        patch["correction_type"] = correction_type
        patch["error_pattern_tag"] = error_pattern_tag
        patch["reviewed_at"] = datetime.utcnow().isoformat() + "Z"
        patch["reviewer_id"] = reviewer_id
        patch["review_notes"] = review_notes
        for field_name, orig_val in original_values.items():
            patch[f"original_{field_name}"] = orig_val

        try:
            await self._passages_client.merge_documents(documents=[patch])
        except HttpResponseError as exc:
            raise KnowledgeStoreError(f"apply_human_correction failed: {exc}") from exc

        await self.log_correction(
            passage_id=passage_id,
            source_doc_id=current.source_doc_id,
            document_type="",
            original_values=original_values,
            corrected_values=corrections,
            correction_type=correction_type,
            error_pattern_tag=error_pattern_tag,
            reviewer_id=reviewer_id,
            review_notes=review_notes,
            confidence_at_review=current.confidence,
        )

    async def log_correction(
        self,
        passage_id: str,
        source_doc_id: str,
        document_type: str,
        original_values: dict,
        corrected_values: dict,
        correction_type: str,
        error_pattern_tag: Optional[str],
        reviewer_id: str,
        review_notes: str,
        confidence_at_review: float,
    ) -> None:
        import json as _json
        record = {
            "log_id": str(uuid.uuid4()),
            "passage_id": passage_id,
            "source_doc_id": source_doc_id,
            "document_type": document_type,
            "correction_type": correction_type,
            "error_pattern_tag": error_pattern_tag,
            "reviewer_id": reviewer_id,
            "review_notes": review_notes,
            "confidence_at_review": confidence_at_review,
            "reviewed_at": datetime.utcnow().isoformat() + "Z",
            "original_values_json": _json.dumps(original_values),
            "corrected_values_json": _json.dumps(corrected_values),
        }
        try:
            await self._validation_client.upload_documents(documents=[record])
        except HttpResponseError as exc:
            raise KnowledgeStoreError(f"log_correction failed: {exc}") from exc

    # ── Query operations ──────────────────────────────────────────────────────

    async def query_trusted(
        self,
        text_query: str,
        taxonomy_filter: Optional[dict] = None,
        top_k: int = 20,
        use_hybrid: bool = True,
    ) -> list[ClassifiedPassage]:
        trusted_filter = " or ".join(
            f"validation_status eq '{s.value}'" for s in TRUSTED_STATUSES
        )
        odata_filter = f"({trusted_filter})"
        if taxonomy_filter:
            for field_name, value in taxonomy_filter.items():
                odata_filter += f" and {field_name} eq '{value}'"

        vector_queries = None
        if use_hybrid:
            embedding = await self._embed(text_query)
            from azure.search.documents.models import VectorizedQuery
            vector_queries = [
                VectorizedQuery(vector=embedding, k_nearest_neighbors=top_k, fields="text_vector")
            ]
        try:
            results = await self._passages_client.search(
                search_text=text_query,
                filter=odata_filter,
                top=top_k,
                vector_queries=vector_queries,
                query_type="semantic",
                semantic_configuration_name="default",
            )
            passages = []
            async for r in results:
                passages.append(_dict_to_passage(r))
            return passages
        except HttpResponseError as exc:
            raise KnowledgeStoreError(f"query_trusted failed: {exc}") from exc

    async def query_pending_review(
        self,
        priority: Optional[ReviewPriority] = None,
        limit: int = 50,
    ) -> list[ClassifiedPassage]:
        odata_filter = f"validation_status eq '{ValidationStatus.PENDING_REVIEW.value}'"
        if priority:
            odata_filter += f" and review_priority eq '{priority.value}'"
        try:
            results = await self._passages_client.search(
                search_text="*",
                filter=odata_filter,
                top=limit,
                order_by=["review_priority asc", "classified_at asc"],
            )
            passages = []
            async for r in results:
                passages.append(_dict_to_passage(r))
            return passages
        except HttpResponseError as exc:
            raise KnowledgeStoreError(f"query_pending_review failed: {exc}") from exc

    async def query_by_company(
        self,
        company_id: str,
        trusted_only: bool = True,
    ) -> list[ClassifiedPassage]:
        doc_filter = f"company_id eq '{company_id}'"
        try:
            doc_results = await self._documents_client.search(
                search_text="*",
                filter=doc_filter,
                select=["doc_id"],
                top=1000,
            )
            doc_ids = []
            async for r in doc_results:
                doc_ids.append(r["doc_id"])
        except HttpResponseError as exc:
            raise KnowledgeStoreError(f"query_by_company (docs) failed: {exc}") from exc

        if not doc_ids:
            return []

        id_filter = " or ".join(f"source_doc_id eq '{d}'" for d in doc_ids)
        odata_filter = f"({id_filter})"
        if trusted_only:
            trusted_filter = " or ".join(
                f"validation_status eq '{s.value}'" for s in TRUSTED_STATUSES
            )
            odata_filter += f" and ({trusted_filter})"
        try:
            results = await self._passages_client.search(
                search_text="*",
                filter=odata_filter,
                top=1000,
            )
            passages = []
            async for r in results:
                passages.append(_dict_to_passage(r))
            return passages
        except HttpResponseError as exc:
            raise KnowledgeStoreError(f"query_by_company (passages) failed: {exc}") from exc

    # ── Analytics ─────────────────────────────────────────────────────────────

    async def get_correction_patterns(
        self, date_from: datetime, min_frequency: int = 3
    ) -> dict[str, int]:
        odata_filter = f"reviewed_at ge {date_from.isoformat()}Z"
        try:
            results = await self._validation_client.search(
                search_text="*",
                filter=odata_filter,
                facets=["error_pattern_tag,count:100"],
                top=0,
            )
            await results.get_answers()
            facets = await results.get_facets()
            if not facets or "error_pattern_tag" not in facets:
                return {}
            return {
                f["value"]: f["count"]
                for f in facets["error_pattern_tag"]
                if f["count"] >= min_frequency
            }
        except HttpResponseError as exc:
            raise KnowledgeStoreError(f"get_correction_patterns failed: {exc}") from exc

    async def get_quality_metrics(self, date_from: datetime) -> dict:
        odata_filter = f"classified_at ge {date_from.isoformat()}Z"
        try:
            results = await self._passages_client.search(
                search_text="*",
                filter=odata_filter,
                facets=[
                    "validation_status,count:20",
                    "review_priority,count:10",
                ],
                top=0,
            )
            facets = await results.get_facets()
            if not facets:
                return {}
            status_counts = {
                f["value"]: f["count"]
                for f in facets.get("validation_status", [])
            }
            priority_counts = {
                f["value"]: f["count"]
                for f in facets.get("review_priority", [])
            }
            total = sum(status_counts.values()) or 1
            auto_approved = status_counts.get("auto_approved", 0)
            return {
                "auto_approve_rate": auto_approved / total,
                "pending_review_count_by_priority": priority_counts,
                "status_counts": status_counts,
            }
        except HttpResponseError as exc:
            raise KnowledgeStoreError(f"get_quality_metrics failed: {exc}") from exc

    async def get_passages_for_backfill(self, subcategory_pattern: str) -> list[ClassifiedPassage]:
        prefix = subcategory_pattern.rstrip("*").rstrip(".")
        odata_filter = (
            f"search.ismatch('{prefix}*', 'subcategory') "
            f"and validation_status ne '{ValidationStatus.NEEDS_BACKFILL.value}'"
        )
        try:
            results = await self._passages_client.search(
                search_text="*",
                filter=odata_filter,
                top=1000,
            )
            passages = []
            async for r in results:
                passages.append(_dict_to_passage(r))
        except HttpResponseError as exc:
            raise KnowledgeStoreError(f"get_passages_for_backfill failed: {exc}") from exc

        if passages:
            patches = [
                {"passage_id": p.passage_id, "validation_status": ValidationStatus.NEEDS_BACKFILL.value}
                for p in passages
            ]
            try:
                await self._passages_client.merge_documents(documents=patches)
            except HttpResponseError as exc:
                raise KnowledgeStoreError(f"get_passages_for_backfill (patch) failed: {exc}") from exc
        return passages


# ── Serialisation helpers ─────────────────────────────────────────────────────

def _passage_to_dict(p: ClassifiedPassage) -> dict:
    return {
        "passage_id": p.passage_id,
        "content_hash": p.content_hash,
        "source_doc_id": p.source_doc_id,
        "text": p.text,
        "page_ref": p.page_ref,
        "char_start": p.char_start,
        "char_end": p.char_end,
        "topic_hint": p.topic_hint,
        "extraction_note": p.extraction_note,
        "category": p.category,
        "subcategory": p.subcategory,
        "seed_category": p.seed_category,
        "iro_type": p.iro_type,
        "value_chain_position": p.value_chain_position,
        "evidence_quality": p.evidence_quality,
        "time_horizon": p.time_horizon,
        "geographic_scope": p.geographic_scope,
        "entities": p.entities,
        "sector_relevance": p.sector_relevance,
        "frameworks_referenced": p.frameworks_referenced,
        "taxonomy_eligible": p.taxonomy_eligible,
        "taxonomy_activity_code": p.taxonomy_activity_code,
        "esrs_hazard_ref": p.esrs_hazard_ref,
        "scenario_referenced": p.scenario_referenced,
        "esrs_e2_relevant": p.esrs_e2_relevant,
        "confidence": p.confidence,
        "confidence_rationale": p.confidence_rationale,
        "classification_note": p.classification_note,
        "classification_model": p.classification_model,
        "classified_at": p.classified_at.isoformat() + "Z",
        "validation_status": p.validation_status.value,
        "review_priority": p.review_priority.value if p.review_priority else None,
        "reviewer_id": p.reviewer_id,
        "reviewed_at": p.reviewed_at.isoformat() + "Z" if p.reviewed_at else None,
        "review_notes": p.review_notes,
        "original_category": p.original_category,
        "original_subcategory": p.original_subcategory,
        "original_iro_type": p.original_iro_type,
        "original_evidence_quality": p.original_evidence_quality,
        "correction_type": p.correction_type,
        "error_pattern_tag": p.error_pattern_tag,
    }


def _dict_to_passage(d: dict) -> ClassifiedPassage:
    def _dt(val: Optional[str]) -> Optional[datetime]:
        return datetime.fromisoformat(val.replace("Z", "+00:00")) if val else None

    return ClassifiedPassage(
        passage_id=d["passage_id"],
        content_hash=d["content_hash"],
        source_doc_id=d["source_doc_id"],
        text=d["text"],
        page_ref=d.get("page_ref"),
        char_start=d.get("char_start"),
        char_end=d.get("char_end"),
        topic_hint=d["topic_hint"],
        extraction_note=d.get("extraction_note"),
        category=d["category"],
        subcategory=d["subcategory"],
        seed_category=d.get("seed_category", False),
        iro_type=d["iro_type"],
        value_chain_position=d["value_chain_position"],
        evidence_quality=d["evidence_quality"],
        time_horizon=d["time_horizon"],
        geographic_scope=d.get("geographic_scope") or [],
        entities=d.get("entities") or [],
        sector_relevance=d.get("sector_relevance") or [],
        frameworks_referenced=d.get("frameworks_referenced") or [],
        taxonomy_eligible=d.get("taxonomy_eligible"),
        taxonomy_activity_code=d.get("taxonomy_activity_code"),
        esrs_hazard_ref=d.get("esrs_hazard_ref"),
        scenario_referenced=d.get("scenario_referenced"),
        esrs_e2_relevant=d.get("esrs_e2_relevant", False),
        confidence=d.get("confidence", 0.0),
        confidence_rationale=d.get("confidence_rationale", ""),
        classification_note=d.get("classification_note"),
        classification_model=d.get("classification_model", ""),
        classified_at=_dt(d.get("classified_at")) or datetime.utcnow(),
        validation_status=ValidationStatus(d.get("validation_status", "raw")),
        review_priority=ReviewPriority(d["review_priority"]) if d.get("review_priority") else None,
        reviewer_id=d.get("reviewer_id"),
        reviewed_at=_dt(d.get("reviewed_at")),
        review_notes=d.get("review_notes"),
        original_category=d.get("original_category"),
        original_subcategory=d.get("original_subcategory"),
        original_iro_type=d.get("original_iro_type"),
        original_evidence_quality=d.get("original_evidence_quality"),
        correction_type=d.get("correction_type"),
        error_pattern_tag=d.get("error_pattern_tag"),
    )
