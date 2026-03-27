# =============================================================================
# schemas/validation.py
# Validation state machine enums
# =============================================================================
# RULES FOR CLAUDE CODE:
#   - ValidationStatus values are stored as strings in Azure AI Search
#     (str Enum ensures this automatically).
#   - TRUSTED_STATUSES is the filter applied by knowledge_store.query_trusted().
#     No output engine should ever query without this filter.
#   - State transition rules are enforced in knowledge_store.py, not here.
#   - See AIP_Master_Plan.docx Section 6.1 for full transition rules.
# =============================================================================

from enum import Enum


class ValidationStatus(str, Enum):
    # ── Automated states (set by pipeline, no human involved) ────────────────
    RAW            = "raw"              # Just extracted. Invisible to all outputs.
                                        # Pipeline immediately runs triage() after
                                        # Stage B to assign next status.

    AUTO_APPROVED  = "auto_approved"    # High confidence + seed category + structured
                                        # source. Visible to outputs immediately.
                                        # 5% sample spot-checked monthly.

    AUTO_REJECTED  = "auto_rejected"    # Confidence < 0.40. Stored for correction
                                        # loop analysis but excluded from outputs
                                        # and review queue. Can be manually escalated
                                        # by setting to PENDING_REVIEW.

    # ── Human review states ───────────────────────────────────────────────────
    PENDING_REVIEW = "pending_review"   # Queued for human. Has a review_priority.
                                        # Invisible to outputs.

    IN_REVIEW      = "in_review"        # Reviewer has opened this passage.
                                        # Prevents double-review.
                                        # Auto-reverts to PENDING_REVIEW after 24h
                                        # if not completed (handled by a scheduled
                                        # cleanup job, not in knowledge_store.py).

    # ── Human decision states ─────────────────────────────────────────────────
    APPROVED       = "approved"         # Human confirmed correct. Visible to outputs.

    EDITED         = "edited"           # Human corrected field(s). original_* fields
                                        # populated. Correction logged to
                                        # adaptation-validation-log index.
                                        # Visible to outputs.

    REJECTED       = "rejected"         # Human rejected. Excluded from outputs.
                                        # Logged with rationale. Never deleted —
                                        # kept for error pattern analysis.

    FLAGGED        = "flagged"          # Ambiguous — escalated to senior reviewer.
                                        # Invisible to outputs until resolved.
                                        # review_priority auto-set to P1_CLIENT.

    # ── Maintenance state ─────────────────────────────────────────────────────
    NEEDS_BACKFILL = "needs_backfill"   # Taxonomy has changed (schema evolution).
                                        # Must be reclassified. Temporarily invisible
                                        # to outputs during reclassification.


class ReviewPriority(str, Enum):
    P1_CLIENT    = "p1_client"      # Will appear in client-facing output.
                                    # Review before any output is generated.
                                    # Target: same day.

    P2_QUANT     = "p2_quant"       # Contains specific numbers, dates, amounts,
                                    # or percentages. High hallucination risk.
                                    # Target: within 24 hours of ingestion.

    P3_NEW_CAT   = "p3_new_cat"     # Classified into a data-driven extension
                                    # (seed_category = False). Requires domain
                                    # expert review.
                                    # Target: within the week.

    P4_STANDARD  = "p4_standard"    # Normal passage, seed category, 0.40–0.85
                                    # confidence. Batch review in weekly session.
                                    # Target: within 2 weeks.

    P5_HISTORICAL = "p5_historical" # Retrospective reclassification (NEEDS_BACKFILL).
                                    # Does not affect current outputs.
                                    # Target: monthly batch.


# =============================================================================
# CONSTANTS
# =============================================================================

# Statuses visible to ALL output engines.
# knowledge_store.query_trusted() always filters to these.
TRUSTED_STATUSES: list[ValidationStatus] = [
    ValidationStatus.AUTO_APPROVED,
    ValidationStatus.APPROVED,
    ValidationStatus.EDITED,
]

# Auto-approval conditions (ALL must be true — see knowledge_store.py triage()):
# 1. confidence >= AUTO_APPROVE_CONFIDENCE_THRESHOLD
# 2. seed_category = True
# 3. source_type in AUTO_APPROVE_SOURCE_TYPES
# 4. No quantitative claims in text (classification_note != "quantitative_claim")
# 5. No qualifying language (classification_note != "qualifying_language_detected")
AUTO_APPROVE_CONFIDENCE_THRESHOLD: float = 0.85

AUTO_APPROVE_SOURCE_TYPES: list[str] = [
    "gcf_api",
    "oecd_api",
    "world_bank_api",
    "unfccc_api",
    "gef_api",
]

AUTO_REJECT_CONFIDENCE_THRESHOLD: float = 0.40
