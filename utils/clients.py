"""
Shared Azure OpenAI + KnowledgeStore client initialisation.
Import from here instead of duplicating setup across ingest, explore, qdc, validation.
"""
from openai import AsyncAzureOpenAI

import config
from knowledge_store import KnowledgeStore


def build_openai_client(max_retries: int = 3, timeout: float = 120.0) -> AsyncAzureOpenAI:
    """Return a configured AsyncAzureOpenAI client."""
    return AsyncAzureOpenAI(
        azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
        api_key=config.AZURE_OPENAI_KEY,
        api_version="2024-08-01-preview",
        max_retries=max_retries,
        timeout=timeout,
    )


def build_store(openai_client: AsyncAzureOpenAI, ensure_indexes: bool = False) -> KnowledgeStore:
    """Return a configured KnowledgeStore, optionally creating indexes."""
    store = KnowledgeStore(
        search_endpoint=config.AZURE_SEARCH_ENDPOINT,
        search_key=config.AZURE_SEARCH_KEY,
        openai_client=openai_client,
    )
    if ensure_indexes:
        store.ensure_indexes()
    return store


def build_clients(ensure_indexes: bool = False) -> tuple[KnowledgeStore, AsyncAzureOpenAI]:
    """Build both clients together. Used by CLI entry points."""
    config.require_credentials()
    client = build_openai_client(max_retries=6)
    store  = build_store(client, ensure_indexes=ensure_indexes)
    return store, client
