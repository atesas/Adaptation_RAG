from abc import ABC, abstractmethod
from typing import AsyncIterator
import asyncio

from schemas.document import Document


class AdapterAuthError(Exception):
    """API key missing, expired, or invalid."""
    pass


class AdapterFetchError(Exception):
    """Network or API error after retries exhausted."""
    pass


class AdapterParseError(Exception):
    """Content could not be parsed into a Document."""
    pass


class BaseAdapter(ABC):
    """
    Abstract base class for all source adapters.

    Subclasses implement fetch() to yield Document objects from a specific
    source type. Adapters do not call Azure AI Search or any LLM directly.
    All fetched documents flow through ingest.py for normalization,
    extraction, and storage.

    Subclasses: CorporatePDFAdapter, GoogleCSEAdapter, GCFAPIAdapter (Phase 2),
                OECDAPIAdapter (Phase 2)
    """

    # Must match one of SOURCE_TYPES in schemas/document.py
    source_type: str = NotImplemented

    def __init__(self, config: dict) -> None:
        """
        Args:
            config: Dict loaded from the relevant entry in sources.yaml.
                    Each adapter reads only the keys it needs.
        """
        self.config = config
        self._request_times: list[float] = []

    @abstractmethod
    async def fetch(self, query_or_path: str) -> AsyncIterator[Document]:
        """
        Main adapter method. Fetches content and yields Document objects.

        query_or_path:
          - Local file path for CorporatePDFAdapter
          - Search query string for GoogleCSEAdapter
          - API query string for structured API adapters (Phase 2)

        Yields Document objects with all fields populated except:
          - doc_id     (set by normalize() in ingest.py)
          - content_hash (set by normalize() in ingest.py)

        Raises:
            AdapterAuthError: if API key is missing or rejected
            AdapterFetchError: if network/API error persists after retries
            AdapterParseError: if content cannot be parsed into a Document
        """
        ...

    async def rate_limit_wait(self, requests_per_minute: int) -> None:
        """
        Token bucket rate limiter. Call before each outbound API request.

        Args:
            requests_per_minute: Maximum allowed requests per 60-second window.
        """
        import time

        now = time.monotonic()
        window = 60.0
        min_gap = window / requests_per_minute

        # Drop timestamps outside the rolling window
        self._request_times = [t for t in self._request_times if now - t < window]

        if len(self._request_times) >= requests_per_minute:
            # Window is full — wait until the oldest slot frees
            sleep_for = self._request_times[0] + window - now
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)

        elif self._request_times:
            # Enforce minimum gap between consecutive requests
            elapsed = now - self._request_times[-1]
            if elapsed < min_gap:
                await asyncio.sleep(min_gap - elapsed)

        self._request_times.append(time.monotonic())
