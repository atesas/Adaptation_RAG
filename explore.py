# =============================================================================
# explore.py
# Unstructured Q&A exploration mode.
#
# Usage (interactive REPL):
#   python explore.py
#
# Usage (single question):
#   python explore.py --question "What water efficiency targets have companies set?"
#
# Usage (search all statuses, not just trusted):
#   python explore.py --all
#
# Options:
#   --question TEXT   Question to answer (if omitted, enters interactive REPL)
#   --top-k N         Number of passages to retrieve (default: 15)
#   --all             Search ALL indexed passages, not only trusted/approved ones
#   --filter KEY=VAL  Optional OData filter pair, repeatable
#                     e.g. --filter category=responses --filter company_name=Danone
# =============================================================================

import argparse
import asyncio
import textwrap
from pathlib import Path

from openai import AsyncAzureOpenAI

import config
from knowledge_store import KnowledgeStore
from schemas.passage import ClassifiedPassage

EXPLORE_PROMPT_PATH = Path("prompts/explore_v1.txt")
PROMPT_TEMPLATE     = EXPLORE_PROMPT_PATH.read_text(encoding="utf-8")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _format_passages(passages: list[ClassifiedPassage]) -> str:
    """Render retrieved passages into a numbered block for the prompt."""
    lines = []
    for i, p in enumerate(passages, 1):
        doc  = p.source_doc_id or "unknown"
        page = p.page_ref or "?"
        cat  = f"{p.category}/{p.subcategory}" if p.category else "unclassified"
        conf = f"{p.confidence:.0%}" if p.confidence else "n/a"
        lines.append(
            f"[{i}] DOC:{doc}  page:{page}  category:{cat}  confidence:{conf}\n"
            f"    {p.text.strip()}"
        )
    return "\n\n".join(lines)


async def answer_question(
    question: str,
    store: KnowledgeStore,
    openai_client: AsyncAzureOpenAI,
    top_k: int = 15,
    search_all: bool = False,
    extra_filters: dict | None = None,
) -> tuple[str, list[ClassifiedPassage]]:
    """
    Retrieve relevant passages and synthesise an answer.

    Returns (answer_text, passages_used).
    """
    if search_all:
        passages = await store.query_any(
            text_query=question,
            top_k=top_k,
            extra_filters=extra_filters,
        )
    else:
        taxonomy_filter = extra_filters or None
        passages = await store.query_trusted(
            text_query=question,
            taxonomy_filter=taxonomy_filter,
            top_k=top_k,
            use_hybrid=True,
        )

    if not passages:
        return "No relevant passages found in the knowledge base for that question.", []

    passages_block = _format_passages(passages)
    prompt = PROMPT_TEMPLATE.format(
        question=question,
        n_passages=len(passages),
        passages_block=passages_block,
    )

    response = await openai_client.chat.completions.create(
        model=config.OUTPUT_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=1500,
    )
    answer = response.choices[0].message.content.strip()
    return answer, passages


# ── CLI ───────────────────────────────────────────────────────────────────────

def _build_clients():
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
    return store, openai_client


def _parse_filters(raw: list[str]) -> dict:
    """Parse ['category=responses', 'company_name=Danone'] into a dict."""
    out = {}
    for item in raw or []:
        if "=" not in item:
            raise ValueError(f"--filter must be KEY=VALUE, got: {item!r}")
        k, v = item.split("=", 1)
        out[k.strip()] = v.strip()
    return out


async def _run_once(args):
    store, openai_client = _build_clients()
    try:
        filters = _parse_filters(args.filter)
        answer, passages = await answer_question(
            question=args.question,
            store=store,
            openai_client=openai_client,
            top_k=args.top_k,
            search_all=args.all,
            extra_filters=filters or None,
        )
        print("\n" + "="*72)
        print(textwrap.fill(answer, width=72))
        print("="*72)
        print(f"\n({len(passages)} passages retrieved)")
    finally:
        await store.close()
        await openai_client.close()


async def _repl(args):
    store, openai_client = _build_clients()
    filters = _parse_filters(args.filter)
    print("Adaptation RAG — Exploration Mode  (type 'quit' to exit)")
    print(f"  top_k={args.top_k}  search_all={args.all}  filters={filters or 'none'}")
    print("-"*72)
    try:
        while True:
            try:
                question = input("\nQuestion: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nBye.")
                break
            if not question:
                continue
            if question.lower() in ("quit", "exit", "q"):
                break
            answer, passages = await answer_question(
                question=question,
                store=store,
                openai_client=openai_client,
                top_k=args.top_k,
                search_all=args.all,
                extra_filters=filters or None,
            )
            print("\n" + "-"*72)
            print(textwrap.fill(answer, width=72))
            print("-"*72)
            print(f"({len(passages)} passages retrieved)")
    finally:
        await store.close()
        await openai_client.close()


def main():
    parser = argparse.ArgumentParser(
        description="Explore the Adaptation RAG knowledge base with free-form questions."
    )
    parser.add_argument("--question", "-q", default=None, help="Single question to answer")
    parser.add_argument("--top-k",    "-k", type=int, default=15, metavar="N",
                        help="Number of passages to retrieve (default: 15)")
    parser.add_argument("--all",      "-a", action="store_true",
                        help="Search all passages, not just trusted/approved")
    parser.add_argument("--filter",   "-f", action="append", default=[],
                        metavar="KEY=VALUE",
                        help="OData filter pair (repeatable). E.g. --filter category=responses")
    args = parser.parse_args()

    if args.question:
        asyncio.run(_run_once(args))
    else:
        asyncio.run(_repl(args))


if __name__ == "__main__":
    main()
