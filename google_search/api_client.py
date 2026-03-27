"""
Google Custom Search API Client - IMPROVED VERSION
Handles API requests with URL logging and proper error handling.
"""

import requests
import time
import logging
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass, field
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

class APIError(Exception):
    """Base exception for API errors."""
    pass

class QuotaExceededError(APIError):
    """Raised when all API keys are exhausted."""
    pass

@dataclass
class SearchResult:
    """Represents a single search result with metadata."""
    metadata: dict

    @classmethod
    def from_api_item(cls, item: dict, extractor: Callable[[dict], dict]):
        """Create SearchResult from API response item."""
        return cls(metadata=extractor(item))

@dataclass
class SearchResponse:
    """Complete response from a search request."""
    results: List[SearchResult]
    total_results: int
    start_index: int
    items_per_page: int
    api_key_used: str
    search_time: float
    request_url: str  # Added for debugging

    @classmethod
    def from_api_response(
        cls, 
        data: dict, 
        api_key: str, 
        search_time: float,
        request_url: str,
        extractor: Callable[[dict], dict]
    ) -> 'SearchResponse':
        """Create SearchResponse from API JSON response."""
        search_info = data.get('searchInformation', {})
        queries = data.get('queries', {})
        request_info = queries.get('request', [{}])[0]

        results = [
            SearchResult.from_api_item(item, extractor) 
            for item in data.get('items', [])
        ]

        return cls(
            results=results,
            total_results=int(search_info.get('totalResults', 0)),
            start_index=int(request_info.get('startIndex', 1)),
            items_per_page=int(request_info.get('count', 10)),
            api_key_used=api_key,
            search_time=search_time,
            request_url=request_url
        )

class GoogleCustomSearchClient:
    """Client for Google Custom Search API with automatic key rotation."""

    def __init__(
        self, 
        search_engine_id: str, 
        api_keys: List[str],
        results_per_page: int = 10,
        rate_limit_delay: float = 1.0
    ):
        """
        Initialize the API client.

        Args:
            search_engine_id: Google Custom Search Engine ID
            api_keys: List of API keys for rotation
            results_per_page: Number of results per page (1 or 10)
            rate_limit_delay: Delay between requests in seconds
        """
        if not search_engine_id:
            raise ValueError("search_engine_id cannot be empty")
        if not api_keys:
            raise ValueError("At least one API key is required")
        if results_per_page not in [1, 10]:
            raise ValueError("results_per_page must be 1 or 10")

        self.search_engine_id = search_engine_id
        self.api_keys = api_keys
        self.results_per_page = results_per_page
        self.rate_limit_delay = rate_limit_delay

        self.key_index = 0
        self.exhausted_keys = set()
        self.base_url = "https://www.googleapis.com/customsearch/v1"

        logger.info(f"API Client initialized with {len(api_keys)} keys")

    def get_api_key(self) -> str:
        """Get current API key, skipping exhausted ones."""
        attempts = 0
        max_attempts = len(self.api_keys)

        while attempts < max_attempts:
            key = self.api_keys[self.key_index % len(self.api_keys)]

            if key not in self.exhausted_keys:
                return key

            self.key_index += 1
            attempts += 1

        # All keys exhausted
        raise QuotaExceededError(
            f"All {len(self.api_keys)} API keys have been exhausted"
        )

    def mark_key_exhausted(self, key: str) -> None:
        """Mark an API key as exhausted."""
        self.exhausted_keys.add(key)
        logger.warning(
            f"API key marked as exhausted. "
            f"{len(self.exhausted_keys)}/{len(self.api_keys)} keys exhausted"
        )

    def rotate_key(self) -> None:
        """Rotate to next API key."""
        self.key_index += 1
        logger.info(f"Rotated to API key index {self.key_index % len(self.api_keys)}")

    def build_params(
        self,
        query: str,
        start_index: int = 1,
        exact_term: str = "",
        file_type: str = "",
        language: str = "",
        country: str = "",
        date_restrict: str = ""
    ) -> Dict[str, str]:
        """Build query parameters for API request."""
        params = {
            'cx': self.search_engine_id,
            'key': self.get_api_key(),
            'q': query,
            'num': str(self.results_per_page),
            'start': str(start_index)
        }

        # Add optional parameters
        if exact_term:
            params['exactTerms'] = exact_term
        if file_type:
            params['fileType'] = file_type
        if language:
            params['lr'] = f'lang_{language}'
        if country:
            params['cr'] = f'country{country.upper()}'
        if date_restrict:
            params['sort'] = date_restrict

        return params

    def _mask_api_key(self, url: str) -> str:
        """Mask API key in URL for logging."""
        import re
        return re.sub(r'key=[^&]+', 'key=***MASKED***', url)

    def search(
        self,
        query: str,
        start_index: int = 1,
        exact_term: str = "",
        file_type: str = "",
        language: str = "",
        country: str = "",
        date_restrict: str = "",
        metadata_extractor: Optional[Callable[[dict], dict]] = None,
        max_retries: int = 3
    ) -> SearchResponse:
        """
        Perform a search request.

        Args:
            query: Search query string
            start_index: Starting result index (1-based)
            exact_term: Exact phrase to match
            file_type: File type filter (e.g., 'pdf')
            language: Language filter
            country: Country filter
            date_restrict: Date restriction in format 'date:r:YYYYMMDD:YYYYMMDD'
            metadata_extractor: Function to extract metadata from API items
            max_retries: Maximum number of retry attempts

        Returns:
            SearchResponse object with results and metadata
        """
        # Default extractor if none provided
        if metadata_extractor is None:
            metadata_extractor = lambda item: item

        params = self.build_params(
            query=query,
            start_index=start_index,
            exact_term=exact_term,
            file_type=file_type,
            language=language,
            country=country,
            date_restrict=date_restrict
        )

        for attempt in range(max_retries):
            current_key = params['key']

            # Build full URL for logging
            full_url = f"{self.base_url}?{urlencode(params)}"
            masked_url = self._mask_api_key(full_url)

            # logger.info(f"API Request URL: {masked_url}")
            logger.debug(f"Full URL (unmasked): {full_url}")

            try:
                start_time = time.time()
                response = requests.get(self.base_url, params=params, timeout=30)
                search_time = time.time() - start_time

                # Success
                if response.status_code == 200:
                    data = response.json()

                    # Rate limiting
                    time.sleep(self.rate_limit_delay)

                    return SearchResponse.from_api_response(
                        data=data,
                        api_key=current_key,
                        search_time=search_time,
                        request_url=full_url,
                        extractor=metadata_extractor
                    )

                # Quota exceeded - try next key
                elif response.status_code == 429:
                    logger.warning(f"Rate limit hit for key ending in ...{current_key[-4:]}")
                    self.mark_key_exhausted(current_key)
                    self.rotate_key()
                    # Update params with new key
                    params['key'] = self.get_api_key()
                    time.sleep(2)  # Backoff before retry

                # Forbidden - key exhausted or invalid
                elif response.status_code == 403:
                    error_data = response.json()
                    error_reason = error_data.get('error', {}).get('errors', [{}])[0].get('reason', '')

                    if 'dailyLimitExceeded' in error_reason or 'quotaExceeded' in error_reason:
                        logger.warning(f"Quota exceeded for key ...{current_key[-4:]}")
                        self.mark_key_exhausted(current_key)
                        self.rotate_key()
                        params['key'] = self.get_api_key()
                        time.sleep(1)
                    else:
                        raise APIError(f"API returned 403: {response.text}")

                # Other client errors
                elif response.status_code >= 400:
                    logger.error(f"API error {response.status_code}: {response.text}")
                    raise APIError(f"API returned {response.status_code}: {response.text}")

            except QuotaExceededError:
                raise  # Re-raise quota errors immediately

            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise APIError(f"Request failed after {max_retries} attempts: {e}")

        raise APIError(f"Failed to complete search after {max_retries} attempts")
