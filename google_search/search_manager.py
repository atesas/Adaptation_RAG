"""
Search Manager - FIXED VERSION
Manages search operations with proper date handling and deduplication.
"""

import logging
import math
from datetime import datetime, timedelta
from typing import List, Optional, Callable, Set
from dataclasses import dataclass, field

from api_client import GoogleCustomSearchClient, SearchResult, SearchResponse

logger = logging.getLogger(__name__)

@dataclass
class DateRange:
    """Represents a date range for search."""
    start: datetime
    end: datetime

    @classmethod
    def from_strings(cls, start_str: str, end_str: str, format: str = "%d/%m/%Y") -> 'DateRange':
        """
        Create DateRange from date strings.

        Args:
            start_str: Start date string
            end_str: End date string  
            format: Date format (default: DD/MM/YYYY)
        """
        return cls(
            start=datetime.strptime(start_str, format),
            end=datetime.strptime(end_str, format)
        )

    def to_api_format(self) -> str:
        """Convert to Google Custom Search API date format."""
        start_str = self.start.strftime("%Y%m%d")
        end_str = self.end.strftime("%Y%m%d")
        return f"date:r:{start_str}:{end_str}"

    def to_string(self) -> str:
        """Convert to human-readable string."""
        return f"{self.start.strftime('%Y-%m-%d')} to {self.end.strftime('%Y-%m-%d')}"

@dataclass
class SearchJob:
    """Represents a complete search job with results."""
    query: str
    date_range: Optional[DateRange]
    results: List[SearchResult] = field(default_factory=list)
    total_results: int = 0
    pages_fetched: int = 0
    api_keys_used: Set[str] = field(default_factory=set)
    errors: List[str] = field(default_factory=list)
    unique_urls: Set[str] = field(default_factory=set)
    duplicates_filtered: int = 0

    def add_response(self, response: SearchResponse) -> int:
        """
        Add a search response to this job.

        Returns:
            Number of new (non-duplicate) results added
        """
        added = 0

        for result in response.results:
            # Get URL for deduplication
            url = result.metadata.get('link', result.metadata.get('url', ''))

            if url and url not in self.unique_urls:
                self.results.append(result)
                self.unique_urls.add(url)
                added += 1
            elif url:
                self.duplicates_filtered += 1

        self.total_results = response.total_results
        self.pages_fetched += 1
        self.api_keys_used.add(response.api_key_used)

        return added

class SearchManager:
    """Manages search operations across date ranges and pagination."""

    def __init__(self, client: GoogleCustomSearchClient):
        """
        Initialize SearchManager.

        Args:
            client: GoogleCustomSearchClient instance
        """
        self.client = client
        logger.info("SearchManager initialized")

    def _generate_date_ranges(
        self,
        start_date: datetime,
        end_date: datetime,
        days_per_chunk: int
    ) -> List[DateRange]:
        """Generate list of date ranges for chunked searching."""
        ranges = []
        current = start_date
        delta = timedelta(days=days_per_chunk - 1)  # Inclusive range

        while current <= end_date:
            chunk_end = min(current + delta, end_date)
            ranges.append(DateRange(start=current, end=chunk_end))
            current = chunk_end + timedelta(days=1)

        logger.info(f"Generated {len(ranges)} date ranges")
        return ranges

    def search_single(
        self,
        query: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        exact_term: str = "",
        file_type: str = "",
        language: str = "",
        country: str = "",
        max_results: Optional[int] = None,
        metadata_extractor: Optional[Callable[[dict], dict]] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> SearchJob:
        """
        Perform a single search query with pagination.

        Args:
            query: Search query string
            start_date: Start date string (DD/MM/YYYY format)
            end_date: End date string (DD/MM/YYYY format)
            exact_term: Exact phrase to match
            file_type: File type filter
            language: Language filter
            country: Country filter
            max_results: Maximum number of results to fetch
            metadata_extractor: Function to extract metadata from results
            progress_callback: Callback function(current_page, total_pages)
        """
        # Convert date strings to DateRange if provided
        date_range = None
        date_restrict = ""

        if start_date and end_date:
            date_range = DateRange.from_strings(start_date, end_date)
            date_restrict = date_range.to_api_format()
            logger.info(f"Searching with date range: {date_range.to_string()}")

        job = SearchJob(query=query, date_range=date_range)

        try:
            # First page
            first_response = self.client.search(
                query=query,
                start_index=1,
                exact_term=exact_term,
                file_type=file_type,
                language=language,
                country=country,
                date_restrict=date_restrict,
                metadata_extractor=metadata_extractor
            )

            job.add_response(first_response)

            if first_response.total_results == 0:
                logger.info(f"No results found for query: {query}")
                return job

            # Determine total results to fetch
            total_results = first_response.total_results
            if max_results:
                total_results = min(total_results, max_results)

            # Google limits results to 100 (10 pages)
            total_results = min(total_results, 100)

            total_pages = math.ceil(total_results / self.client.results_per_page)

            logger.info(
                f"Query '{query}' has {first_response.total_results} total results, "
                f"fetching up to {total_pages} pages"
            )

            # Fetch remaining pages
            for page in range(2, total_pages + 1):
                start_index = (page - 1) * self.client.results_per_page + 1

                try:
                    response = self.client.search(
                        query=query,
                        start_index=start_index,
                        exact_term=exact_term,
                        file_type=file_type,
                        language=language,
                        country=country,
                        date_restrict=date_restrict,
                        metadata_extractor=metadata_extractor
                    )

                    added = job.add_response(response)
                    logger.debug(f"Page {page}: added {added} new results")

                    if progress_callback:
                        progress_callback(page, total_pages)

                    # Stop if we've reached max_results
                    if max_results and len(job.results) >= max_results:
                        job.results = job.results[:max_results]
                        logger.info(f"Reached max_results limit of {max_results}")
                        break

                except Exception as e:
                    error_msg = f"Error fetching page {page}: {e}"
                    logger.error(error_msg)
                    job.errors.append(error_msg)
                    # Continue with next page

        except Exception as e:
            error_msg = f"Error in search: {e}"
            logger.error(error_msg)
            job.errors.append(error_msg)

        logger.info(
            f"Search complete: {len(job.results)} unique results "
            f"({job.duplicates_filtered} duplicates filtered)"
        )

        return job

    def search_date_range(
        self,
        query: str,
        start_date: datetime,
        end_date: datetime,
        days_per_chunk: int = 7,
        exact_term: str = "",
        file_type: str = "",
        language: str = "",
        country: str = "",
        max_results_per_chunk: Optional[int] = None,
        metadata_extractor: Optional[Callable[[dict], dict]] = None,
        progress_callback: Optional[Callable[[int, int, DateRange], None]] = None
    ) -> List[SearchJob]:
        """
        Perform search across a date range, chunked into smaller periods.

        Args:
            query: Search query string
            start_date: Start datetime
            end_date: End datetime
            days_per_chunk: Number of days per chunk
            exact_term: Exact phrase to match
            file_type: File type filter
            language: Language filter
            country: Country filter
            max_results_per_chunk: Max results per chunk
            metadata_extractor: Function to extract metadata
            progress_callback: Callback function(chunk_num, total_chunks, date_range)
        """
        date_ranges = self._generate_date_ranges(start_date, end_date, days_per_chunk)
        jobs = []

        logger.info(
            f"Starting date range search: {start_date.date()} to {end_date.date()}, "
            f"{len(date_ranges)} chunks"
        )

        for i, date_range in enumerate(date_ranges, 1):
            logger.info(f"Searching chunk {i}/{len(date_ranges)}: {date_range.to_string()}")

            job = self.search_single(
                query=query,
                start_date=date_range.start.strftime("%d/%m/%Y"),
                end_date=date_range.end.strftime("%d/%m/%Y"),
                exact_term=exact_term,
                file_type=file_type,
                language=language,
                country=country,
                max_results=max_results_per_chunk,
                metadata_extractor=metadata_extractor
            )

            jobs.append(job)

            if progress_callback:
                progress_callback(i, len(date_ranges), date_range)

        total_results = sum(len(job.results) for job in jobs)
        total_duplicates = sum(job.duplicates_filtered for job in jobs)

        logger.info(
            f"Date range search complete: {total_results} unique results "
            f"({total_duplicates} duplicates filtered) "
            f"across {len(jobs)} chunks"
        )

        return jobs
