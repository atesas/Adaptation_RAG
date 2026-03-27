"""
Main Script - FINAL INTEGRATED VERSION
Search + Download with improved file type detection from CSV metadata.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from query_manager import QueryManager
from utils import split_date_range
from search_manager import SearchManager, SearchJob
from api_client import GoogleCustomSearchClient, QuotaExceededError
from config import AppConfig
from results_writer import write_results_to_csv
from metadata import extract_metadata

# Import improved downloader
try:
    from file_downloader import ImprovedFileDownloader
    DOWNLOADER_AVAILABLE = True
except ImportError:
    DOWNLOADER_AVAILABLE = False
    print("⚠️  File downloader not available. Install: pip install selenium webdriver-manager pdfplumber beautifulsoup4")


# Configure logging with UTF-8 encoding for Windows compatibility
def setup_logging():
    """Setup logging with proper UTF-8 encoding."""
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # File handler with UTF-8 encoding
    file_handler = logging.FileHandler('search_tool.log', encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    # Console handler with UTF-8 encoding
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # Force UTF-8 output on Windows
    if sys.platform == 'win32':
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

setup_logging()
logger = logging.getLogger(__name__)


def aggregate_jobs(jobs: List[SearchJob]) -> List[dict]:
    """
    Aggregate results from multiple SearchJob objects, removing duplicates.

    Args:
        jobs: List of SearchJob objects from different date chunks

    Returns:
        List of unique result metadata dictionaries
    """
    all_results = []
    seen_urls = set()
    total_duplicates = 0

    for job in jobs:
        for result in job.results:
            url = result.metadata.get('link', result.metadata.get('url', ''))

            if url and url not in seen_urls:
                all_results.append(result.metadata)
                seen_urls.add(url)
            elif url:
                total_duplicates += 1

    logger.info(
        f"Aggregated {len(all_results)} unique results "
        f"({total_duplicates} duplicates removed across chunks)"
    )

    return all_results


def download_files_from_csv(
    csv_path: Path,
    output_dir: Path,
    limit: Optional[int] = None,
    headless: bool = True,
    use_browser: bool = False
) -> dict:
    """
    Download files from search results CSV using improved downloader.

    Args:
        csv_path: Path to CSV file with search results
        output_dir: Directory to save downloads
        limit: Maximum number of files to download (None = all)
        headless: Run browser in headless mode
        use_browser: Force browser downloads (slower but handles complex pages)

    Returns:
        Statistics dictionary
    """
    if not DOWNLOADER_AVAILABLE:
        logger.error("File downloader not available. Skipping downloads.")
        return {'downloaded': 0, 'failed': 0, 'skipped': 0}

    logger.info("\n" + "="*70)
    logger.info("STARTING FILE DOWNLOADS")
    logger.info("="*70)
    logger.info(f"Source CSV: {csv_path.name}")
    logger.info(f"Download directory: {output_dir}")

    downloader = None
    try:
        # Initialize downloader
        downloader = ImprovedFileDownloader(
            base_dir=output_dir,
            headless=headless
        )

        # Download from CSV
        successful, failed, results = downloader.download_from_csv(
            csv_path=csv_path,
            limit=limit,
            use_browser=use_browser
        )

        logger.info("\n" + "="*70)
        logger.info("DOWNLOAD SUMMARY")
        logger.info("="*70)
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Total processed: {successful + failed}")

        # Log file type breakdown
        file_types = {}
        for r in results:
            if r['success'] and r['file_type']:
                file_types[r['file_type']] = file_types.get(r['file_type'], 0) + 1

        if file_types:
            logger.info("\nFile types downloaded:")
            for ft, count in file_types.items():
                logger.info(f"  {ft.upper()}: {count}")

        return {
            'downloaded': successful,
            'failed': failed,
            'total': successful + failed,
            'file_types': file_types
        }

    finally:
        if downloader:
            downloader.close()


def search_query_with_chunks(
    query: str,
    start_date: str,
    end_date: str,
    days_per_chunk: int = 5,
    file_type: str = None,
    limit_per_chunk: int = 50,
    config: AppConfig = None
) -> List[SearchJob]:
    """
    Execute a search query split into date chunks.

    Args:
        query: Search query string
        start_date: Start date (DD/MM/YYYY format)
        end_date: End date (DD/MM/YYYY format)
        days_per_chunk: Number of days per chunk
        file_type: Optional file type filter (e.g., 'pdf')
        limit_per_chunk: Maximum results per chunk
        config: Application configuration

    Returns:
        List of SearchJob objects (one per chunk)
    """
    # Load config if not provided
    if config is None:
        config = AppConfig.from_file("config.yaml")

    # Initialize API client
    api_client = GoogleCustomSearchClient(
        search_engine_id=config.api.search_engine_id,
        api_keys=config.api.api_keys,
        results_per_page=config.api.results_per_page,
        rate_limit_delay=config.api.rate_limit_delay
    )

    search_manager = SearchManager(api_client)

    # Parse dates
    try:
        dt_start = datetime.strptime(start_date, "%d/%m/%Y")
        dt_end = datetime.strptime(end_date, "%d/%m/%Y")
    except ValueError as e:
        logger.error(f"Invalid date format: {e}")
        return []

    # Split into chunks
    chunks = split_date_range(dt_start, dt_end, days=days_per_chunk)
    logger.info(f"Split date range into {len(chunks)} chunks of {days_per_chunk} days each")

    jobs = []

    # Process each chunk
    for chunk_num, (chunk_start, chunk_end) in enumerate(chunks, 1):
        logger.info(
            f"[Chunk {chunk_num}/{len(chunks)}] "
            f"Searching: {chunk_start.date()} to {chunk_end.date()}"
        )

        try:
            job = search_manager.search_single(
                query=query,
                start_date=chunk_start.strftime("%d/%m/%Y"),
                end_date=chunk_end.strftime("%d/%m/%Y"),
                file_type=file_type,
                max_results=limit_per_chunk,
                metadata_extractor=extract_metadata
            )

            jobs.append(job)

            logger.info(
                f"[Chunk {chunk_num}/{len(chunks)}] "
                f"Complete: {len(job.results)} results, "
                f"{job.duplicates_filtered} duplicates filtered"
            )

        except QuotaExceededError as e:
            logger.error(f"[Chunk {chunk_num}/{len(chunks)}] All API keys exhausted: {e}")
            break

        except Exception as e:
            logger.error(
                f"[Chunk {chunk_num}/{len(chunks)}] Error: {e}",
                exc_info=True
            )
            # Continue with next chunk

    return jobs


def main():
    """Main execution function with integrated downloading."""
    logger.info("=" * 70)
    logger.info("Google Custom Search Tool - FINAL INTEGRATED VERSION")
    logger.info("=" * 70)

    if DOWNLOADER_AVAILABLE:
        logger.info("File downloader: AVAILABLE")
    else:
        logger.info("File downloader: NOT AVAILABLE (search only)")

    try:
        # Load configuration
        config = AppConfig.from_file("config.yaml")
        logger.info(f"Configuration loaded: {len(config.api.api_keys)} API keys available")

        # Load queries
        queries = QueryManager()
        queries.load_from_csv("queries.csv")
        logger.info(f"Loaded {queries.get_count()} queries")

        # Create results directory
        output_dir = Path(config.paths.results_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        # ========================================
        # DOWNLOAD SETTINGS (CONFIGURABLE)
        # ========================================
        ENABLE_DOWNLOADS = True      # Set to False to disable downloads
        DOWNLOAD_LIMIT = None        # Set to number to limit downloads per query (None = all)
        HEADLESS_BROWSER = False      # Set to False to see browser window
        USE_BROWSER = True          # Set to True to force browser downloads (slower but more reliable)

        total_queries = 0
        successful_queries = 0
        total_downloads = 0
        total_download_failures = 0
        all_file_types = {}

        # Process each query
        for q in queries.get_queries():
            total_queries += 1

            logger.info(f"\n{'='*70}")
            logger.info(f"Query {total_queries}/{queries.get_count()}: {q.name}")
            logger.info(f"{'='*70}")
            logger.info(f"Search term: {q.query}")
            logger.info(f"Date range: {q.start_date} to {q.end_date}")
            logger.info(f"Chunk size: {q.days_per_chunk} days")

            # ========================================
            # PHASE 1: SEARCH
            # ========================================
            jobs = search_query_with_chunks(
                query=q.query,
                start_date=q.start_date,
                end_date=q.end_date,
                days_per_chunk=q.days_per_chunk,
                file_type=getattr(q, "file_type", None),
                limit_per_chunk=getattr(q, "max_results", 50),
                config=config
            )

            if not jobs:
                logger.warning(f"[SKIPPED] No results for query: {q.name}")
                continue

            # Aggregate results from all chunks
            aggregated_results = aggregate_jobs(jobs)

            if not aggregated_results:
                logger.warning(f"[SKIPPED] No results after aggregation for: {q.name}")
                continue

            # Save to single CSV file per query
            csv_filename = output_dir / (
                f"results_{q.name.replace(' ', '_')}_"
                f"{q.start_date.replace('/', '-')}_to_"
                f"{q.end_date.replace('/', '-')}.csv"
            )

            write_results_to_csv(aggregated_results, str(csv_filename))

            # Calculate search statistics
            total_api_keys_used = len(set(
                key for job in jobs for key in job.api_keys_used
            ))
            total_pages_fetched = sum(job.pages_fetched for job in jobs)
            total_errors = sum(len(job.errors) for job in jobs)

            logger.info(f"\n[SEARCH SUCCESS] Query completed: {q.name}")
            logger.info(f"  CSV file: {csv_filename}")
            logger.info(f"  Total unique results: {len(aggregated_results)}")
            logger.info(f"  Date chunks processed: {len(jobs)}")
            logger.info(f"  API pages fetched: {total_pages_fetched}")
            logger.info(f"  API keys used: {total_api_keys_used}")

            if total_errors > 0:
                logger.warning(f"  Errors encountered: {total_errors}")

            successful_queries += 1

            # ========================================
            # PHASE 2: DOWNLOAD FILES (if enabled)
            # ========================================
            if ENABLE_DOWNLOADS and DOWNLOADER_AVAILABLE:
                logger.info(f"\n[DOWNLOADS] Starting for query: {q.name}")

                download_dir = output_dir / f"downloads_{q.name.replace(' ', '_')}"

                download_stats = download_files_from_csv(
                    csv_path=csv_filename,
                    output_dir=download_dir,
                    limit=DOWNLOAD_LIMIT,
                    headless=HEADLESS_BROWSER,
                    use_browser=USE_BROWSER
                )

                total_downloads += download_stats.get('downloaded', 0)
                total_download_failures += download_stats.get('failed', 0)

                # Aggregate file types
                for ft, count in download_stats.get('file_types', {}).items():
                    all_file_types[ft] = all_file_types.get(ft, 0) + count

                logger.info(f"[DOWNLOADS] Completed for {q.name}: "
                           f"{download_stats.get('downloaded', 0)} successful, "
                           f"{download_stats.get('failed', 0)} failed")

            elif ENABLE_DOWNLOADS and not DOWNLOADER_AVAILABLE:
                logger.warning("[DOWNLOADS] Skipped - downloader not available")

        # ========================================
        # FINAL SUMMARY
        # ========================================
        logger.info("\n" + "=" * 70)
        logger.info("FINAL SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Total queries: {total_queries}")
        logger.info(f"Successful searches: {successful_queries}")
        logger.info(f"Failed searches: {total_queries - successful_queries}")
        logger.info(f"Results saved to: {output_dir}")

        if ENABLE_DOWNLOADS and DOWNLOADER_AVAILABLE:
            logger.info(f"\nDownload Statistics:")
            logger.info(f"  Files downloaded: {total_downloads}")
            logger.info(f"  Download failures: {total_download_failures}")

            if all_file_types:
                logger.info(f"\n  File types downloaded:")
                for ft, count in all_file_types.items():
                    logger.info(f"    {ft.upper()}: {count}")

    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
