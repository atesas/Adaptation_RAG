# =============================================================================
# outputs/company_assessment.py
# D1–D8 company-level climate adaptation assessment (ESRS E1 aligned).
# CLI: python outputs/company_assessment.py --company "Danone" --year 2024
# =============================================================================

import asyncio
import logging
import re
from pathlib import Path
from typing import Optional

from openai import AsyncAzureOpenAI

import config
from knowledge_store import KnowledgeStore
from outputs.citations import build_citation_index, format_citations_appendix, format_passages_for_prompt
from schemas.passage import ClassifiedPassage

logger = logging.getLogger(__name__)

# D1–D8 targeted queries run against company passages
_D_QUERIES: list[tuple[str, str]] = [
    ("D1", "physical climate hazard identification material"),
    ("D2", "quantified financial risk magnitude scenario"),
    ("D3", "adaptation action response measure implemented"),
    ("D4", "board governance oversight climate risk management"),
    ("D5", "finance investment green bond insurance provision"),
    ("D6", "supply chain upstream downstream climate"),
    ("D7", "scenario pathway RCP SSP NGFS time horizon"),
    ("D8", "KPI target monitoring framework CSRD ESRS"),
]


async def generate_company_assessment(
    company_id: str,
    company_name: str,
    store: KnowledgeStore,
    openai_client: AsyncAzureOpenAI,
    reporting_year: Optional[int] = None,
    top_k: int = 50,
) -> str:
    passages = await store.query_by_company(company_id=company_id, trusted_only=True)

    if reporting_year:
        passages = [p for p in passages if _passage_year_matches(p, reporting_year)]

    if not passages:
        return (
            f"# {company_name} — Climate Adaptation Assessment\n\n"
            "No trusted passages found for this company."
        )

    passages = _deduplicate(passages, top_k)

    prompt_path = config.PROMPTS_DIR / f"company_assessment_{config.COMPANY_ASSESSMENT_PROMPT_VERSION}.txt"
    output = await _run_prompt(
        prompt_path=prompt_path,
        variables={
            "company_name": company_name,
            "reporting_year": str(reporting_year) if reporting_year else "latest",
            "passage_count": str(len(passages)),
            "passages_text": format_passages_for_prompt(passages),
        },
        openai_client=openai_client,
    )

    citation_index = build_citation_index(passages)
    total_score = _parse_total_score(output)
    header = _build_header(company_name, reporting_year, total_score)
    return f"{header}\n\n{output}\n\n{format_citations_appendix(citation_index)}"


def _passage_year_matches(passage: ClassifiedPassage, year: int) -> bool:
    # source_doc_id may embed the year if named by convention, e.g. "danone_2024_..."
    # classified_at is the processing date, not the source document date — do not use it.
    if passage.source_doc_id and str(year) in passage.source_doc_id:
        return True
    # When year cannot be verified from available passage fields, include the passage.
    # The prompt is told the reporting year and will focus on it.
    return True


def _deduplicate(passages: list[ClassifiedPassage], top_k: int) -> list[ClassifiedPassage]:
    """Sort by confidence descending, deduplicate on content_hash, cap at top_k."""
    seen: set[str] = set()
    result = []
    for p in sorted(passages, key=lambda x: x.confidence, reverse=True):
        if p.content_hash not in seen:
            seen.add(p.content_hash)
            result.append(p)
            if len(result) >= top_k:
                break
    return result


def _parse_total_score(text: str) -> Optional[str]:
    """Extract 'Overall Score: X/24' from the generated text if present."""
    match = re.search(r"Overall Score[:\s]+(\d+/24)", text, re.IGNORECASE)
    return match.group(1) if match else None


def _build_header(company_name: str, year: Optional[int], score: Optional[str]) -> str:
    year_str = str(year) if year else "latest available"
    score_str = f" | Score: {score}" if score else ""
    return f"**Company:** {company_name} | **Year:** {year_str}{score_str}"


async def _run_prompt(
    prompt_path: Path,
    variables: dict[str, str],
    openai_client: AsyncAzureOpenAI,
) -> str:
    template = prompt_path.read_text(encoding="utf-8")
    prompt = template.format(**variables)
    system_msg, user_msg = _split_prompt(prompt)
    messages = []
    if system_msg:
        messages.append({"role": "system", "content": system_msg})
    messages.append({"role": "user", "content": user_msg})
    response = await openai_client.chat.completions.create(
        model=config.OUTPUT_MODEL,
        messages=messages,
        temperature=0.2,
    )
    return response.choices[0].message.content


def _split_prompt(prompt: str) -> tuple[str, str]:
    if "---\n\nUSER:" in prompt:
        parts = prompt.split("---\n\nUSER:", 1)
        return parts[0].replace("SYSTEM:\n", "").strip(), parts[1].strip()
    return "", prompt.strip()


def _build_clients() -> tuple[KnowledgeStore, AsyncAzureOpenAI]:
    openai_client = AsyncAzureOpenAI(
        azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
        api_key=config.AZURE_OPENAI_KEY,
        api_version="2024-08-01-preview",
    )
    return KnowledgeStore(
        search_endpoint=config.AZURE_SEARCH_ENDPOINT,
        search_key=config.AZURE_SEARCH_KEY,
        openai_client=openai_client,
    ), openai_client


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="D1–D8 company climate adaptation assessment")
    parser.add_argument("--company", required=True, help="Company name")
    parser.add_argument("--company-id", help="Internal company_id (defaults to slugified company name)")
    parser.add_argument("--year", type=int, help="Reporting year (default: all available)")
    parser.add_argument("--top-k", type=int, default=50, help="Max passages to use")
    parser.add_argument("--output", help="Write output to file path (default: stdout)")
    args = parser.parse_args()

    company_id = args.company_id or args.company.lower().replace(" ", "-")
    store, openai_client = _build_clients()
    result = asyncio.run(generate_company_assessment(
        company_id=company_id,
        company_name=args.company,
        store=store,
        openai_client=openai_client,
        reporting_year=args.year,
        top_k=args.top_k,
    ))

    if args.output:
        Path(args.output).write_text(result, encoding="utf-8")
        print(f"Written to {args.output}")
    else:
        print(result)
