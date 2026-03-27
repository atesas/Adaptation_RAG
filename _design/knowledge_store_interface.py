# =============================================================================
# _design/knowledge_store_interface.py
# Interface contract for knowledge_store.py
# =============================================================================
# This file defines WHAT knowledge_store.py must implement.
# It is NOT the implementation — Claude Code writes the implementation.
#
# RULES FOR CLAUDE CODE:
#   1. knowledge_store.py is the ONLY file that imports the Azure AI Search SDK.
#      No other file calls the SDK directly.
#   2. All three index names are constants defined in this file.
#   3. query_trusted() ALWAYS applies the TRUSTED_STATUSES filter from
#      schemas/validation.py. It is not the caller's responsibility.
#   4. All methods are async (Azure SDK is async-native).
#   5. On Azure Search SDK errors: log the error, raise a KnowledgeStoreError
#      (custom exception defined in this file). Never swallow errors silently.
#   6. upsert_passage() checks content_hash BEFORE writing. If hash exists,
#      update metadata only — do not create a duplicate document.
#   7. The adaptation-validation-log index is append-only. Never update or
#      delete entries from it.
# =============================================================================

from datetime import datetime
from typing import Optional
from schemas.document import Document
from schemas.passage import ClassifiedPassage
from schemas.validation import ValidationStatus, ReviewPriority, TRUSTED_STATUSES


# =============================================================================
# INDEX NAMES (constants)
# =============================================================================

INDEX_PASSAGES   = "adaptation-passages"       # Main knowledge store
INDEX_DOCUMENTS  = "adaptation-documents"      # Source document registry
INDEX_VALIDATION = "adaptation-validation-log" # Correction loop audit trail


# =============================================================================
# CUSTOM EXCEPTION
# =============================================================================

class KnowledgeStoreError(Exception):
    """Raised when any Azure AI Search operation fails."""
    pass


# =============================================================================
# INTERFACE — knowledge_store.py must implement all of these
# =============================================================================

class KnowledgeStoreInterface:
    """
    Abstract interface. Claude Code implements this as KnowledgeStore(KnowledgeStoreInterface).
    Constructor takes: search_endpoint, search_key, openai_client (for embeddings).
    """

    # ── Document registry operations ─────────────────────────────────────────

    async def register_document(self, doc: Document) -> str:
        """
        Add a document to the adaptation-documents index.
        Returns: doc_id
        Raises: KnowledgeStoreError if document with same content_hash exists
                (caller should check deduplicate_document() first)
        """
        ...

    async def deduplicate_document(self, content_hash: str) -> bool:
        """
        Check if a document with this content_hash already exists.
        Returns: True if exists (skip ingestion), False if new.
        """
        ...

    async def update_document_status(
        self,
        doc_id: str,
        extraction_status: str,
        extraction_error: Optional[str] = None
    ) -> None:
        """Update extraction_status on a document after Stage A completes."""
        ...

    # ── Passage operations ────────────────────────────────────────────────────

    async def upsert_passage(self, passage: ClassifiedPassage) -> str:
        """
        Insert or update a passage in the adaptation-passages index.
        If content_hash exists: update metadata fields only, do not duplicate.
        Generates and stores the embedding vector for the passage text.
        Returns: passage_id
        """
        ...

    async def get_passage(self, passage_id: str) -> ClassifiedPassage:
        """
        Retrieve a single passage by ID.
        Raises: KnowledgeStoreError if not found.
        """
        ...

    async def deduplicate_passage(self, content_hash: str) -> bool:
        """
        Check if a passage with this content_hash already exists.
        Returns: True if exists, False if new.
        """
        ...

    # ── Validation operations ─────────────────────────────────────────────────

    async def update_validation_status(
        self,
        passage_id: str,
        status: ValidationStatus,
        reviewer_id: Optional[str] = None,
        notes: Optional[str] = None
    ) -> None:
        """
        Update the validation_status on a passage.
        Sets reviewed_at = datetime.utcnow() if reviewer_id is provided.
        """
        ...

    async def set_review_priority(
        self,
        passage_id: str,
        priority: ReviewPriority
    ) -> None:
        """Set the review_priority field. Called by triage() after Stage B."""
        ...

    async def apply_human_correction(
        self,
        passage_id: str,
        corrections: dict,
        reviewer_id: str,
        correction_type: str,
        error_pattern_tag: Optional[str],
        review_notes: str
    ) -> None:
        """
        Apply a human reviewer's corrections to a passage.
        corrections: dict of field_name → new_value for any corrected fields.

        This method must:
        1. Read the current passage to get original values
        2. For each corrected field, populate the corresponding original_* field
           with the CURRENT (pre-correction) value
        3. Apply the corrections
        4. Set validation_status = ValidationStatus.EDITED
        5. Set correction_type and error_pattern_tag
        6. Call log_correction() to write to adaptation-validation-log
        """
        ...

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
        confidence_at_review: float
    ) -> None:
        """
        Write a correction record to the adaptation-validation-log index.
        This index is APPEND-ONLY. Never update or delete.
        """
        ...

    # ── Query operations ──────────────────────────────────────────────────────

    async def query_trusted(
        self,
        text_query: str,
        taxonomy_filter: Optional[dict] = None,
        top_k: int = 20,
        use_hybrid: bool = True
    ) -> list[ClassifiedPassage]:
        """
        Query the knowledge store for trusted passages.

        ALWAYS applies TRUSTED_STATUSES filter — this is not optional.
        Uses hybrid search (BM25 + vector) when use_hybrid=True.
        Applies Azure L2 semantic re-ranker after retrieval.

        taxonomy_filter: dict of field_name → value for additional filtering.
        Examples:
          {"category": "hazards"}
          {"sector_relevance": "beverages", "time_horizon": "long"}
          {"iro_type": "risk.physical_chronic_risk", "geographic_scope": "FR"}

        Returns passages sorted by re-ranker score descending.
        """
        ...

    async def query_pending_review(
        self,
        priority: Optional[ReviewPriority] = None,
        limit: int = 50
    ) -> list[ClassifiedPassage]:
        """
        Return passages with validation_status = PENDING_REVIEW.
        If priority is specified, filter to that priority only.
        Sorted by priority ascending (P1 first), then classified_at ascending.
        """
        ...

    async def query_by_company(
        self,
        company_id: str,
        trusted_only: bool = True
    ) -> list[ClassifiedPassage]:
        """
        Return all passages from a specific company's documents.
        Used by company_assessment.py for D1–D8 scoring.
        If trusted_only=True, applies TRUSTED_STATUSES filter.
        """
        ...

    # ── Analytics and correction loop ─────────────────────────────────────────

    async def get_correction_patterns(
        self,
        date_from: datetime,
        min_frequency: int = 3
    ) -> dict[str, int]:
        """
        Query adaptation-validation-log for error patterns since date_from.
        Returns: dict of error_pattern_tag → count
        Any pattern with count >= min_frequency is a prompt problem.
        Used by weekly error analysis cycle.
        """
        ...

    async def get_quality_metrics(
        self,
        date_from: datetime
    ) -> dict:
        """
        Return weekly quality metrics dict:
        {
          "acceptance_rate_by_source_type": {source_type: float},
          "edit_rate_by_field": {field_name: float},
          "rejection_rate_by_confidence_band": {
            "0.40-0.60": float,
            "0.60-0.75": float,
            "0.75-0.85": float
          },
          "auto_approve_rate": float,
          "pending_review_count_by_priority": {priority: int}
        }
        """
        ...

    async def get_passages_for_backfill(
        self,
        subcategory_pattern: str
    ) -> list[ClassifiedPassage]:
        """
        Return all passages matching subcategory_pattern with status != NEEDS_BACKFILL.
        Used during taxonomy schema evolution to find passages that need reclassification.
        Sets their status to NEEDS_BACKFILL.
        subcategory_pattern: e.g. "hazards.physical_chronic.*" uses prefix match.
        """
        ...
