# =============================================================================
# schemas/passage.py
# Output of Stage A + Stage B — what lives in the Azure AI Search index
# =============================================================================
# RULES FOR CLAUDE CODE:
#   - Every field marked "filterable" MUST be declared filterable in the
#     Azure AI Search index schema (knowledge_store.py handles this).
#   - A passage has ONE primary category + subcategory. If a passage covers
#     multiple topics, Stage A should have split it. If it wasn't split,
#     set classification_note="multi_topic" and send to PENDING_REVIEW.
#   - The 'original_*' fields are populated ONLY when a human reviewer
#     edits a field. They preserve the original LLM output for error analysis.
#   - content_hash deduplication: if hash already exists in the index,
#     knowledge_store.upsert_passage() updates metadata but does not create
#     a duplicate passage.
# =============================================================================

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from schemas.validation import ValidationStatus, ReviewPriority


@dataclass
class ClassifiedPassage:

    # ── Identity ──────────────────────────────────────────────────────────────
    passage_id: str                    # UUID4
    content_hash: str                  # SHA256(text) — deduplication key
    source_doc_id: str                 # FK → Document.doc_id  [filterable]

    # ── Content ───────────────────────────────────────────────────────────────
    text: str                          # Passage text verbatim from Stage A
    page_ref: Optional[str]            # Page number or section heading
    char_start: Optional[int]          # Character offset in source doc
    char_end: Optional[int]            # Used by validation UI context expander

    # ── Stage A metadata (passed through to Stage B) ──────────────────────────
    topic_hint: str                    # Stage A rough topic signal
                                       # "hazard"|"impact"|"adaptation"|
                                       # "governance"|"finance"|"scenario"|
                                       # "regulatory"|"supply_chain"
    extraction_note: Optional[str]     # Stage A flags: "quantitative_claim",
                                       # "forward_looking_only", etc.

    # ── Stage B: Primary taxonomy classification ──────────────────────────────
    # [filterable]
    category: str                      # Top-level node: "hazards", "responses", etc.
    subcategory: str                   # Dotted path: "hazards.physical_chronic.water_stress"
    seed_category: bool                # True=seed layer; False=data-driven extension

    # ── Stage B: Cross-cutting fields (ALL passages get ALL of these) ─────────
    # [all filterable]
    iro_type: str                      # See IRO_TYPES below
    value_chain_position: str          # See VALUE_CHAIN_POSITIONS below
    evidence_quality: str              # See EVIDENCE_QUALITY_LEVELS below
    time_horizon: str                  # "short"|"medium"|"long"|"unspecified"
    geographic_scope: list[str]        # ISO country codes or region names [filterable]

    # ── Stage B: Optional enrichment fields ───────────────────────────────────
    entities: list[str]                # Named companies, facilities, crops, chemicals
    sector_relevance: list[str]        # From taxonomy sector_tags [filterable]
    frameworks_referenced: list[str]   # e.g. ["csrd_esrs","tcfd"] [filterable]
    taxonomy_eligible: Optional[bool]  # EU Taxonomy alignment flag [filterable]
    taxonomy_activity_code: Optional[str]  # e.g. "3.2" from Climate Delegated Act
    esrs_hazard_ref: Optional[str]     # e.g. "E1_AR11_acute_drought"
    scenario_referenced: Optional[str] # taxonomy scenario node
    esrs_e2_relevant: bool             # True if pollution-climate interaction detected

    # ── Classification quality ────────────────────────────────────────────────
    confidence: float                  # 0.0–1.0 [filterable — range queries]
    confidence_rationale: str          # One sentence from Stage B
    classification_note: Optional[str] # "multi_topic"|"low_source_quality"|
                                       # "qualifying_language_detected"|
                                       # "non_english_fragment"|
                                       # "quantitative_claim"|"forward_looking_only"
    classification_model: str          # Model used for Stage B (audit trail)
    classified_at: datetime

    # ── Validation state ──────────────────────────────────────────────────────
    # [all filterable + sortable]
    validation_status: ValidationStatus    # Default: ValidationStatus.RAW
    review_priority: Optional[ReviewPriority]  # Set by triage(), not Stage B
    reviewer_id: Optional[str]
    reviewed_at: Optional[datetime]
    review_notes: Optional[str]

    # ── Correction loop — populated only when human edits a field ────────────
    # Original LLM values preserved for error pattern analysis
    original_category: Optional[str]           = None
    original_subcategory: Optional[str]        = None
    original_iro_type: Optional[str]           = None
    original_evidence_quality: Optional[str]   = None
    correction_type: Optional[str]             = None
    # "category"|"subcategory"|"iro_type"|"evidence_quality"|"entities"|
    # "value_chain_position"|"time_horizon"
    error_pattern_tag: Optional[str]           = None
    # "evidence_quality_inflation" — planned classified as implemented
    # "iro_misclassification" — risk classified as impact or vice versa
    # "scope_collapse" — upstream risk classified as own_operations
    # "category_boundary" — adjacent categories confused
    # "hallucinated_entity" — entity not present in source text


# =============================================================================
# CONTROLLED VOCABULARIES FOR STAGE B
# These are the ONLY valid values. Stage B prompt uses these exact strings.
# If Stage B returns a value not in these lists → confidence = 0.0,
# status = PENDING_REVIEW, classification_note = "invalid_taxonomy_value"
# =============================================================================

IRO_TYPES: list[str] = [
    "impact.actual_negative",
    "impact.potential_negative",
    "impact.actual_positive",
    "risk.physical_acute_risk",
    "risk.physical_chronic_risk",
    "risk.transition_risk.policy_legal",
    "risk.transition_risk.technology",
    "risk.transition_risk.market",
    "risk.transition_risk.stranded_assets",
    "opportunity.resource_efficiency",
    "opportunity.energy_source",
    "opportunity.products_services",
    "opportunity.markets",
    "opportunity.resilience",
    "not_specified",
]

VALUE_CHAIN_POSITIONS: list[str] = [
    "own_operations",
    "upstream.tier_1_supplier",
    "upstream.tier_2_plus",
    "upstream.raw_material_origin",
    "downstream.distribution",
    "downstream.transport",
    "downstream.storage",
    "not_specified",
]

EVIDENCE_QUALITY_LEVELS: list[str] = [
    "anecdotal",
    "planned",
    "implemented",
    "monitored",
    "satellite_verified",
    "independently_verified",
    "peer_reviewed",
]

TIME_HORIZONS: list[str] = [
    "short",        # < 5 years from reporting year
    "medium",       # 5–10 years
    "long",         # > 10 years (up to 2050)
    "unspecified",
]
