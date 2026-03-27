# =============================================================================
# outputs/newsletter.py
# Climate adaptation newsletter generator.
# Queries trusted passages for a sector and synthesises using GPT-4o.
# CLI: python outputs/newsletter.py --sector food_agriculture
# =============================================================================

import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path

from openai import AsyncAzureOpenAI

import config
from knowledge_store import KnowledgeStore
from outputs.citations import build_citation_index, format_citations_appendix, format_passages_for_prompt
from schemas.passage import ClassifiedPassage

logger = logging.getLogger(__name__)

_VALID_SECTORS = [
    "food_agriculture", "beverages", "food_and_beverage",
    "water", "ecosystems", "retail_consumer_goods",
]


async def generate_newsletter(
    sector: str,
    store: KnowledgeStore,
    openai_client: AsyncAzureOpenAI,
    top_k: int = 40,
    days_back: int = 90,
) -> str:
    if sector not in _VALID_SECTORS:
        raise ValueError(f"Unknown sector '{sector}'. Valid: {_VALID_SECTORS}")

    passages = await store.query_trusted(
        text_query=f"climate adaptation {sector}",
        taxonomy_filter={"sector_relevance": sector},
        top_k=top_k,
    )

    if not passages:
        logger.warning("No trusted passages found for sector: %s", sector)
        return f"# {sector.replace('_', ' ').title()} Climate Adaptation Newsletter\n\nNo trusted passages available for this sector."

    prompt_path = config.PROMPTS_DIR / f"newsletter_{config.NEWSLETTER_PROMPT_VERSION}.txt"
    output = await _run_prompt(
        prompt_path=prompt_path,
        variables={
            "sector": sector.replace("_", " ").title(),
            "reporting_period": _reporting_period(days_back),
            "passage_count": str(len(passages)),
            "passages_text": format_passages_for_prompt(passages),
        },
        openai_client=openai_client,
    )

    citation_index = build_citation_index(passages)
    citations_appendix = format_citations_appendix(citation_index)
    return f"{output}\n\n{citations_appendix}"


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
        temperature=0.3,
    )
    return response.choices[0].message.content


def _split_prompt(prompt: str) -> tuple[str, str]:
    if "---\n\nUSER:" in prompt:
        parts = prompt.split("---\n\nUSER:", 1)
        return parts[0].replace("SYSTEM:\n", "").strip(), parts[1].strip()
    return "", prompt.strip()


def _reporting_period(days_back: int) -> str:
    end = datetime.utcnow()
    start = end - timedelta(days=days_back)
    return f"{start.strftime('%d %b %Y')} – {end.strftime('%d %b %Y')}"


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
    parser = argparse.ArgumentParser(description="Generate climate adaptation newsletter")
    parser.add_argument("--sector", required=True, help=f"Sector: {_VALID_SECTORS}")
    parser.add_argument("--top-k", type=int, default=40, help="Max passages to retrieve")
    parser.add_argument("--days-back", type=int, default=90, help="Reporting period in days")
    parser.add_argument("--output", help="Write output to this file path (default: stdout)")
    args = parser.parse_args()

    store, openai_client = _build_clients()
    result = asyncio.run(generate_newsletter(
        sector=args.sector,
        store=store,
        openai_client=openai_client,
        top_k=args.top_k,
        days_back=args.days_back,
    ))

    if args.output:
        Path(args.output).write_text(result, encoding="utf-8")
        print(f"Written to {args.output}")
    else:
        print(result)
