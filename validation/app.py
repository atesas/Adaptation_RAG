# =============================================================================
# validation/app.py
# Streamlit human review interface for classified passages.
# Run: streamlit run validation/app.py
# =============================================================================

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
from openai import AsyncAzureOpenAI

import config
from knowledge_store import KnowledgeStore
from schemas.passage import (
    ClassifiedPassage,
    EVIDENCE_QUALITY_LEVELS,
    IRO_TYPES,
    TIME_HORIZONS,
    VALUE_CHAIN_POSITIONS,
)
from schemas.validation import ReviewPriority, ValidationStatus
from taxonomy import taxonomy


# ── Streamlit page config ─────────────────────────────────────────────────────

st.set_page_config(
    page_title="AIP — Passage Review",
    page_icon="🌱",
    layout="wide",
)


# ── Store singleton ───────────────────────────────────────────────────────────

@st.cache_resource
def get_store() -> KnowledgeStore:
    from utils.clients import build_openai_client, build_store
    return build_store(build_openai_client())


@st.cache_resource
def _get_event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop


def run_async(coro):
    return _get_event_loop().run_until_complete(coro)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_index(options: list[str], value: str) -> int:
    try:
        return options.index(value)
    except ValueError:
        return 0


def _handle_decision(
    store: KnowledgeStore,
    passage: ClassifiedPassage,
    action: str,
    new_subcategory: str,
    new_iro: str,
    new_evidence: str,
    new_vcp: str,
    new_time: str,
    error_pattern: str | None,
    review_notes: str,
    reviewer_id: str,
) -> None:
    pid = passage.passage_id

    if action == "Approve":
        run_async(store.update_validation_status(
            pid, ValidationStatus.APPROVED,
            reviewer_id=reviewer_id, notes=review_notes or None,
        ))
        st.success(f"Approved passage {pid[:8]}…")

    elif action == "Edit & Approve":
        corrections: dict = {}
        if new_subcategory != passage.subcategory:
            category = new_subcategory.split(".")[0] if "." in new_subcategory else new_subcategory
            corrections["category"] = category
            corrections["subcategory"] = new_subcategory
        if new_iro != passage.iro_type:
            corrections["iro_type"] = new_iro
        if new_evidence != passage.evidence_quality:
            corrections["evidence_quality"] = new_evidence
        if new_vcp != passage.value_chain_position:
            corrections["value_chain_position"] = new_vcp
        if new_time != passage.time_horizon:
            corrections["time_horizon"] = new_time

        if corrections:
            correction_type = next(iter(corrections))
            run_async(store.apply_human_correction(
                passage_id=pid,
                corrections=corrections,
                reviewer_id=reviewer_id,
                correction_type=correction_type,
                error_pattern_tag=error_pattern,
                review_notes=review_notes or "Human correction",
            ))
            st.success(f"Edited and approved passage {pid[:8]}… ({len(corrections)} fields changed)")
        else:
            run_async(store.update_validation_status(
                pid, ValidationStatus.APPROVED,
                reviewer_id=reviewer_id, notes=review_notes or None,
            ))
            st.success(f"Approved passage {pid[:8]}… (no changes)")

    elif action == "Reject":
        run_async(store.update_validation_status(
            pid, ValidationStatus.REJECTED,
            reviewer_id=reviewer_id, notes=review_notes or None,
        ))
        st.warning(f"Rejected passage {pid[:8]}…")

    elif action == "Flag for escalation":
        run_async(store.update_validation_status(
            pid, ValidationStatus.FLAGGED,
            reviewer_id=reviewer_id, notes=review_notes or None,
        ))
        run_async(store.set_review_priority(pid, ReviewPriority.P1_CLIENT))
        st.warning(f"Flagged passage {pid[:8]}… for escalation")


# ── Sidebar — filters ─────────────────────────────────────────────────────────

st.sidebar.title("AIP Review Queue")
priority_options = {
    "All priorities": None,
    "P1 — Client-facing": ReviewPriority.P1_CLIENT,
    "P2 — Quantitative claims": ReviewPriority.P2_QUANT,
    "P3 — New categories": ReviewPriority.P3_NEW_CAT,
    "P4 — Standard": ReviewPriority.P4_STANDARD,
}
selected_priority_label = st.sidebar.selectbox("Priority filter", list(priority_options.keys()))
selected_priority = priority_options[selected_priority_label]
reviewer_id = st.sidebar.text_input("Your reviewer ID", value="reviewer-1")
limit = st.sidebar.slider("Passages per page", min_value=5, max_value=50, value=20)

# ── Load queue ────────────────────────────────────────────────────────────────

store = get_store()

if st.sidebar.button("Refresh queue"):
    st.rerun()

try:
    passages: list[ClassifiedPassage] = run_async(
        store.query_pending_review(priority=selected_priority, limit=limit)
    )
except Exception as exc:
    st.error(f"Failed to load review queue: {exc}")
    passages = []

st.title(f"Review Queue ({len(passages)} passages)")

if not passages:
    st.info("No passages pending review for this filter.")
    st.stop()

# ── Passage cards ─────────────────────────────────────────────────────────────

for idx, passage in enumerate(passages):
    priority_label = passage.review_priority.value if passage.review_priority else "—"
    confidence_pct = f"{passage.confidence:.0%}"

    with st.expander(
        f"[{priority_label.upper()}] {passage.category} / {passage.subcategory}  "
        f"| conf {confidence_pct} | {passage.source_doc_id[:8]}…",
        expanded=(idx == 0),
    ):
        col_text, col_form = st.columns([3, 2])

        # ── Left: passage text + metadata ─────────────────────────────────
        with col_text:
            st.markdown("**Passage text**")
            st.info(passage.text)

            meta_cols = st.columns(3)
            meta_cols[0].metric("Confidence", confidence_pct)
            meta_cols[1].metric("IRO type", passage.iro_type)
            meta_cols[2].metric("Evidence quality", passage.evidence_quality)

            st.caption(
                f"Source: `{passage.source_doc_id}` | "
                f"Page: {passage.page_ref or '—'} | "
                f"Topic hint: {passage.topic_hint} | "
                f"Model: {passage.classification_model}"
            )
            if passage.confidence_rationale:
                st.caption(f"Rationale: {passage.confidence_rationale}")
            if passage.classification_note:
                st.warning(f"Classification note: {passage.classification_note}")

        # ── Right: review form ─────────────────────────────────────────────
        with col_form:
            st.markdown("**Review**")
            form_key = f"form_{passage.passage_id}"
            with st.form(key=form_key):
                action = st.radio(
                    "Decision",
                    ["Approve", "Edit & Approve", "Reject", "Flag for escalation"],
                    key=f"action_{passage.passage_id}",
                )

                # Editable fields — shown pre-filled with Stage B values
                new_category = st.selectbox(
                    "Category",
                    taxonomy.get_all_subcategory_paths(),
                    index=_find_index(taxonomy.get_all_subcategory_paths(), passage.subcategory),
                    key=f"cat_{passage.passage_id}",
                )
                new_iro = st.selectbox(
                    "IRO type",
                    IRO_TYPES,
                    index=_find_index(IRO_TYPES, passage.iro_type),
                    key=f"iro_{passage.passage_id}",
                )
                new_evidence = st.selectbox(
                    "Evidence quality",
                    EVIDENCE_QUALITY_LEVELS,
                    index=_find_index(EVIDENCE_QUALITY_LEVELS, passage.evidence_quality),
                    key=f"ev_{passage.passage_id}",
                )
                new_vcp = st.selectbox(
                    "Value chain position",
                    VALUE_CHAIN_POSITIONS,
                    index=_find_index(VALUE_CHAIN_POSITIONS, passage.value_chain_position),
                    key=f"vcp_{passage.passage_id}",
                )
                new_time = st.selectbox(
                    "Time horizon",
                    TIME_HORIZONS,
                    index=_find_index(TIME_HORIZONS, passage.time_horizon),
                    key=f"time_{passage.passage_id}",
                )
                error_pattern = st.selectbox(
                    "Error pattern (if editing)",
                    ["—", "evidence_quality_inflation", "iro_misclassification",
                     "scope_collapse", "category_boundary", "hallucinated_entity",
                     "multi_topic"],
                    key=f"err_{passage.passage_id}",
                )
                review_notes = st.text_area(
                    "Notes",
                    key=f"notes_{passage.passage_id}",
                    height=80,
                )

                submitted = st.form_submit_button("Submit decision")

            if submitted:
                _handle_decision(
                    store=store,
                    passage=passage,
                    action=action,
                    new_subcategory=new_category,
                    new_iro=new_iro,
                    new_evidence=new_evidence,
                    new_vcp=new_vcp,
                    new_time=new_time,
                    error_pattern=None if error_pattern == "—" else error_pattern,
                    review_notes=review_notes,
                    reviewer_id=reviewer_id,
                )
                st.rerun()
