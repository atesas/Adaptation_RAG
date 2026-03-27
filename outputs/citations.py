# =============================================================================
# outputs/citations.py
# Shared citation utilities for all output engines.
# Every claim in every output must be traceable to a passage + source doc.
# =============================================================================

from dataclasses import dataclass
from typing import Optional

from schemas.passage import ClassifiedPassage


@dataclass
class Citation:
    passage_id: str
    source_doc_id: str
    source_url: str
    company_name: Optional[str]
    reporting_year: Optional[int]
    page_ref: Optional[str]
    text_excerpt: str          # First 120 chars of passage text


def build_citation_index(passages: list[ClassifiedPassage]) -> dict[str, Citation]:
    """
    Build a mapping of passage_id → Citation for a set of passages.
    Used by output engines to resolve [P:<id>] tags in generated text.
    """
    return {
        p.passage_id: Citation(
            passage_id=p.passage_id,
            source_doc_id=p.source_doc_id,
            source_url="",          # Populated by output engine from document registry
            company_name=None,      # Populated by output engine from document registry
            reporting_year=None,    # Populated by output engine from document registry
            page_ref=p.page_ref,
            text_excerpt=p.text[:120],
        )
        for p in passages
    }


def format_citations_appendix(citations: dict[str, Citation]) -> str:
    """
    Render a numbered citations appendix for inclusion at the end of outputs.
    Maps [P:<id>] tags to full source references.
    """
    if not citations:
        return ""
    lines = ["## Citations\n"]
    for i, (pid, cit) in enumerate(citations.items(), start=1):
        company_part = f"{cit.company_name}, " if cit.company_name else ""
        year_part = str(cit.reporting_year) if cit.reporting_year else "year unknown"
        page_part = f", p. {cit.page_ref}" if cit.page_ref else ""
        lines.append(
            f"[P:{pid[:8]}…] {company_part}{year_part}{page_part}\n"
            f"  Source: {cit.source_url or cit.source_doc_id}\n"
            f"  Excerpt: \"{cit.text_excerpt}…\""
        )
    return "\n".join(lines)


def format_passages_for_prompt(passages: list[ClassifiedPassage]) -> str:
    """
    Render passages into the {passages_text} template variable.
    Each passage is formatted with its ID so the LLM can cite it.
    """
    blocks = []
    for p in passages:
        meta = (
            f"[P:{p.passage_id}] "
            f"category={p.category} | "
            f"subcategory={p.subcategory} | "
            f"iro={p.iro_type} | "
            f"evidence={p.evidence_quality} | "
            f"confidence={p.confidence:.2f}"
        )
        blocks.append(f"{meta}\n{p.text}")
    return "\n\n---\n\n".join(blocks)
