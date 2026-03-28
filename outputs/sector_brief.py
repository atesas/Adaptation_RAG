# =============================================================================
# outputs/sector_brief.py
# Climate adaptation sector brief generator — D1–D8 dimensions (ESRS E1).
# CLI: python outputs/sector_brief.py --sector beverages
# =============================================================================

import asyncio
import logging
from pathlib import Path
from typing import Optional

from openai import AsyncAzureOpenAI

import config
from knowledge_store import KnowledgeStore
from outputs.citations import build_citation_index, format_citations_appendix, format_passages_for_prompt
from schemas.passage import TIME_HORIZONS

logger = logging.getLogger(__name__)

_VALID_SECTORS = [
    "food_agriculture", "beverages", "food_and_beverage",
    "water", "ecosystems", "retail_consumer_goods",
]

# D1–D8 taxonomy queries — each maps to the relevant taxonomy nodes
_D_QUERIES: dict[str, str] = {
    "D1": "physical climate hazard identification material risk",
    "D2": "financial quantified risk magnitude scenario analysis",
    "D3": "adaptation response measure implemented resilience",
    "D4": "board governance oversight climate risk management",
    "D5": "finance investment green bond insurance instrument",
    "D6": "supply chain upstream downstream climate vulnerability",
    "D7": "scenario pathway RCP SSP NGFS time horizon",
    "D8": "KPI target monitoring reporting framework CSRD ESRS",
}

_D_FILTERS: dict[str, dict] = {
    "D1": {"category": "hazards"},
    "D2": {"iro_type": "risk.physical_chronic_risk"},
    "D3": {"category": "responses"},
    "D4": {"category": "governance"},
    "D5": {"category": "finance"},
    "D6": {"value_chain_position": "upstream.tier_1_supplier"},
    "D7": {"category": "scenarios"},
    "D8": {"category": "governance"},
}


async def generate_sector_brief(
    sector: str,
    store: KnowledgeStore,
    openai_client: AsyncAzureOpenAI,
    time_horizon: str = "all",
    top_k_per_dimension: int = 8,
) -> str:
    if sector not in _VALID_SECTORS:
        raise ValueError(f"Unknown sector '{sector}'. Valid: {_VALID_SECTORS}")
    if time_horizon not in TIME_HORIZONS + ["all"]:
        raise ValueError(f"Unknown time_horizon '{time_horizon}'")

    all_passages = []
    seen_ids: set[str] = set()

    for dim, query in _D_QUERIES.items():
        tax_filter = dict(_D_FILTERS[dim])
        tax_filter["sector_relevance"] = sector
        try:
            results = await store.query_trusted(
                text_query=f"{query} {sector}",
                taxonomy_filter=tax_filter,
                top_k=top_k_per_dimension,
            )
        except Exception:
            # Fallback: query without the dimension-specific filter
            results = await store.query_trusted(
                text_query=f"{query} {sector}",
                taxonomy_filter={"sector_relevance": sector},
                top_k=top_k_per_dimension,
            )
        for p in results:
            if p.passage_id not in seen_ids:
                if time_horizon == "all" or p.time_horizon == time_horizon:
                    all_passages.append(p)
                    seen_ids.add(p.passage_id)

    if not all_passages:
        return (
            f"# {_sector_title(sector)} Sector Climate Adaptation Brief\n\n"
            "No trusted passages available for this sector."
        )

    prompt_path = config.PROMPTS_DIR / f"sector_brief_{config.SECTOR_BRIEF_PROMPT_VERSION}.txt"
    output = await _run_prompt(
        prompt_path=prompt_path,
        variables={
            "sector": sector,
            "sector_title": _sector_title(sector),
            "time_horizon": time_horizon,
            "passage_count": str(len(all_passages)),
            "passages_text": format_passages_for_prompt(all_passages),
        },
        openai_client=openai_client,
    )

    citation_index = build_citation_index(all_passages)
    return f"{output}\n\n{format_citations_appendix(citation_index)}"


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


def _sector_title(sector: str) -> str:
    return sector.replace("_", " ").title()


def _build_clients() -> tuple[KnowledgeStore, AsyncAzureOpenAI]:
    config.require_credentials()
    openai_client = AsyncAzureOpenAI(
        azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
        api_key=config.AZURE_OPENAI_KEY,
        api_version="2024-08-01-preview",
    )
    store = KnowledgeStore(
        search_endpoint=config.AZURE_SEARCH_ENDPOINT,
        search_key=config.AZURE_SEARCH_KEY,
        openai_client=openai_client,
    )
    store.ensure_indexes()
    return store, openai_client


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="Generate climate adaptation sector brief (D1–D8)")
    parser.add_argument("--sector", required=True, help=f"Sector: {_VALID_SECTORS}")
    parser.add_argument("--time-horizon", default="all", help="short | medium | long | unspecified | all")
    parser.add_argument("--top-k", type=int, default=8, help="Passages per D dimension")
    parser.add_argument("--output", help="Write output to file path (default: stdout)")
    args = parser.parse_args()

    store, openai_client = _build_clients()
    result = asyncio.run(generate_sector_brief(
        sector=args.sector,
        store=store,
        openai_client=openai_client,
        time_horizon=args.time_horizon,
        top_k_per_dimension=args.top_k,
    ))

    if args.output:
        Path(args.output).write_text(result, encoding="utf-8")
        print(f"Written to {args.output}")
    else:
        print(result)
