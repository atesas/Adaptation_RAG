"""
GoogleCSEAdapter

Two-step adapter: search → download → extract text.

Step 1 (search):
    Uses GoogleCustomSearchClient + SearchManager from google_search/ to
    query the Google Custom Search Engine API. Returns a list of result URLs
    with MIME type metadata.

Step 2 (download + extract):
    Uses ImprovedFileDownloader from google_search/ to download each result
    to tmp/. PDFs are extracted with PyPDF2 (same logic as CorporatePDFAdapter).
    HTML pages are stored as raw HTML text.

Yields one Document per successfully downloaded and extracted result.

API keys and CSE ID come from config.GOOGLE_CSE_API_KEY / config.GOOGLE_CSE_ID.
All downloads go to config.TMP_DIR.
"""

import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator

import PyPDF2

import config
from adapters.base import BaseAdapter, AdapterAuthError, AdapterFetchError, AdapterParseError
from schemas.document import Document

# google_search/ modules are on the path via sys.path manipulation below.
# They are retained until Phase 0 wrapping is confirmed, then the folder
# will be deleted (see PROJECT_BRIEF.md Section 4 delete list).
_GOOGLE_SEARCH_DIR = Path(__file__).parent.parent / "google_search"
if str(_GOOGLE_SEARCH_DIR) not in sys.path:
    sys.path.insert(0, str(_GOOGLE_SEARCH_DIR))

logger = logging.getLogger(__name__)


class GoogleCSEAdapter(BaseAdapter):
    """
    Adapter for Google Custom Search Engine + PDF/HTML download.

    fetch(query) two-step flow:
      Step 1: CSE API call via GoogleCustomSearchClient → list of result URLs
      Step 2: Download each URL to tmp/ via ImprovedFileDownloader
              → extract text (PDF: PyPDF2, HTML: raw page source)
      → yield Document per successful download
    """

    source_type: str = "google_cse"

    def __init__(self, config_dict: dict) -> None:
        super().__init__(config_dict)
        self._validate_credentials()

    def _validate_credentials(self) -> None:
        """Raise AdapterAuthError if CSE credentials are missing."""
        try:
            _ = config.GOOGLE_CSE_API_KEY
            _ = config.GOOGLE_CSE_ID
        except KeyError as exc:
            raise AdapterAuthError(
                "GOOGLE_CSE_API_KEY and GOOGLE_CSE_ID must be set in environment"
            ) from exc

        if not config.GOOGLE_CSE_API_KEY or not config.GOOGLE_CSE_ID:
            raise AdapterAuthError(
                "GOOGLE_CSE_API_KEY and GOOGLE_CSE_ID must not be empty"
            )

    async def fetch(self, query_or_path: str) -> AsyncIterator[Document]:
        """
        Args:
            query_or_path: Google search query string.

        Yields:
            Document objects with extraction_status="pending".

        Raises:
            AdapterAuthError: if CSE credentials are invalid.
            AdapterFetchError: if the CSE API call fails after retries.
            AdapterParseError: if a downloaded file cannot be parsed.
        """
        results = self._search(query_or_path)

        if not results:
            logger.warning("GoogleCSEAdapter: no results for query %r", query_or_path)
            return

        config.TMP_DIR.mkdir(parents=True, exist_ok=True)
        ingestion_date = datetime.utcnow()

        for result in results:
            url = result.get("link", "")
            if not url:
                continue

            downloaded_path = self._download(result)
            if downloaded_path is None:
                logger.warning("GoogleCSEAdapter: download failed for %s", url)
                continue

            raw_text = self._extract_text(downloaded_path)
            if not raw_text.strip():
                logger.warning("GoogleCSEAdapter: no text from %s", downloaded_path.name)
                continue

            doc_type = self._infer_document_type(result, downloaded_path)
            pub_date = self._parse_date(result.get("published_date") or result.get("creation_date"))

            doc = Document(
                doc_id="",
                content_hash="",
                raw_text=raw_text[:config.MAX_DOCUMENT_CHARS],
                title=result.get("title") or downloaded_path.stem,
                language="en",
                source_url=url,
                source_type=self.source_type,
                adapter=self.__class__.__name__,
                publication_date=pub_date,
                ingestion_date=ingestion_date,
                reporting_year=None,
                document_type=doc_type,
                company_name=None,
                company_id=None,
                csrd_wave=None,
                country=self.config.get("country", []),
                sector_hint=self.config.get("sector_hint", []),
                extraction_status="pending",
                extraction_error=None,
            )
            yield doc

    # ── Step 1: Search ────────────────────────────────────────────────────────

    def _search(self, query: str) -> list[dict]:
        """
        Run CSE search and return a flat list of result metadata dicts.

        Uses GoogleCustomSearchClient for API calls and SearchManager for
        pagination. Applies date-range chunking when date params are configured.

        Returns list of result dicts with keys: link, title, mime_type,
        file_format, published_date, creation_date, snippet.
        """
        try:
            from api_client import GoogleCustomSearchClient, QuotaExceededError
            from search_manager import SearchManager
            from metadata import extract_metadata
        except ImportError as exc:
            raise AdapterFetchError(
                "google_search modules not found. Ensure google_search/ is present."
            ) from exc

        api_keys = self._get_api_keys()
        results_per_page = self.config.get("results_per_page", 10)
        rate_limit_delay = self.config.get("rate_limit_delay", 1.0)

        try:
            client = GoogleCustomSearchClient(
                search_engine_id=config.GOOGLE_CSE_ID,
                api_keys=api_keys,
                results_per_page=results_per_page,
                rate_limit_delay=rate_limit_delay,
            )
        except ValueError as exc:
            raise AdapterAuthError(f"Invalid CSE configuration: {exc}") from exc

        manager = SearchManager(client)
        max_results = self.config.get("max_results", 50)
        file_type = self.config.get("file_type", "pdf")

        try:
            job = manager.search_single(
                query=query,
                file_type=file_type,
                max_results=max_results,
                metadata_extractor=extract_metadata,
            )
        except QuotaExceededError as exc:
            raise AdapterFetchError(f"All CSE API keys exhausted: {exc}") from exc
        except Exception as exc:
            raise AdapterFetchError(f"CSE search failed: {exc}") from exc

        return [r.metadata for r in job.results]

    def _get_api_keys(self) -> list[str]:
        """
        Return CSE API key(s) from environment.
        Supports a single key or a comma-separated list.
        """
        raw = config.GOOGLE_CSE_API_KEY
        keys = [k.strip() for k in raw.split(",") if k.strip()]
        if not keys:
            raise AdapterAuthError("GOOGLE_CSE_API_KEY is empty")
        return keys

    # ── Step 2: Download ──────────────────────────────────────────────────────

    def _download(self, result: dict) -> "Path | None":
        """
        Download a single search result to tmp/.

        Uses ImprovedFileDownloader from google_search/.
        Returns the local Path of the downloaded file, or None on failure.
        """
        try:
            from file_downloader import ImprovedFileDownloader
        except ImportError as exc:
            raise AdapterFetchError(
                "file_downloader module not found in google_search/"
            ) from exc

        downloader = ImprovedFileDownloader(
            base_dir=config.TMP_DIR / "cse_downloads",
            headless=self.config.get("headless", True),
        )
        try:
            status = downloader.download_result(result, use_browser=False)
        finally:
            downloader.close()

        if status.get("success") and status.get("downloaded_file"):
            return Path(status["downloaded_file"])

        return None

    # ── Text extraction ───────────────────────────────────────────────────────

    def _extract_text(self, path: Path) -> str:
        """
        Extract text from a downloaded file.
        PDFs: PyPDF2. HTML: raw page source as-is.
        """
        if path.suffix.lower() == ".pdf":
            return self._extract_pdf_text(path)
        return self._read_html_text(path)

    def _extract_pdf_text(self, path: Path) -> str:
        """Extract text from PDF with PyPDF2."""
        try:
            with open(path, "rb") as fh:
                reader = PyPDF2.PdfReader(fh)
                pages = [p.extract_text() or "" for p in reader.pages]
            return "\n".join(pages).strip()
        except Exception as exc:
            raise AdapterParseError(f"PDF extraction failed for {path.name}: {exc}") from exc

    def _read_html_text(self, path: Path) -> str:
        """Read HTML file as plain text."""
        try:
            return path.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            raise AdapterParseError(f"HTML read failed for {path.name}: {exc}") from exc

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _infer_document_type(self, result: dict, path: Path) -> str:
        """Infer document_type from MIME type and file extension."""
        mime = (result.get("mime_type") or "").lower()
        if "pdf" in mime or path.suffix.lower() == ".pdf":
            return self.config.get("document_type", "guidance")
        return "news"

    def _parse_date(self, date_str: str | None) -> "datetime | None":
        """Parse a date string to datetime. Returns None if unparseable."""
        if not date_str:
            return None
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%Y"):
            try:
                return datetime.strptime(str(date_str)[:len(fmt)], fmt)
            except ValueError:
                continue
        return None
