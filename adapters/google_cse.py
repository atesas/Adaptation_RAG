"""
GoogleCSEAdapter

Two-step adapter: search → download → extract text.

Step 1 (search):
    Calls the Google Custom Search JSON API directly via requests.
    Endpoint: https://www.googleapis.com/customsearch/v1
    Paginates up to max_results. Rotates through comma-separated API keys.

Step 2 (download + extract):
    Downloads each result URL to tmp/cse_downloads/ with requests.
    PDFs are extracted with PyPDF2. HTML files are read as plain text.

Yields one Document per successfully downloaded and extracted result.

API keys and CSE ID come from config.GOOGLE_CSE_API_KEY / config.GOOGLE_CSE_ID.
"""

import asyncio
import hashlib
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator, Iterator

import requests
import PyPDF2

import config
from adapters.base import BaseAdapter, AdapterAuthError, AdapterFetchError, AdapterParseError
from schemas.document import Document

logger = logging.getLogger(__name__)

_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1"
_MAX_RESULTS_PER_PAGE = 10
_REQUEST_TIMEOUT = 30
_DOWNLOAD_TIMEOUT = 60


class GoogleCSEAdapter(BaseAdapter):
    """
    Adapter for Google Custom Search Engine + PDF/HTML download.

    fetch(query) two-step flow:
      Step 1: CSE API call → list of result metadata dicts
      Step 2: Download each URL to tmp/ → extract text
      → yield Document per successful download
    """

    source_type: str = "google_cse"

    def __init__(self, config_dict: dict) -> None:
        super().__init__(config_dict)
        self._api_keys = self._get_api_keys()
        self._cse_id = self._get_cse_id()
        self._key_index = 0

    def _get_api_keys(self) -> list[str]:
        api_key = config.GOOGLE_CSE_API_KEY
        if not api_key:
            raise AdapterAuthError("GOOGLE_CSE_API_KEY must not be empty")
        keys = [k.strip() for k in api_key.split(",") if k.strip()]
        if not keys:
            raise AdapterAuthError("GOOGLE_CSE_API_KEY contains no valid keys")
        return keys

    def _get_cse_id(self) -> str:
        cse_id = config.GOOGLE_CSE_ID
        if not cse_id:
            raise AdapterAuthError("GOOGLE_CSE_ID must not be empty")
        return cse_id

    async def fetch(self, query_or_path: str) -> AsyncIterator[Document]:
        results = list(self._search(query_or_path))

        if not results:
            logger.warning("GoogleCSEAdapter: no results for query %r", query_or_path)
            return

        download_dir = config.TMP_DIR / "cse_downloads"
        download_dir.mkdir(parents=True, exist_ok=True)
        ingestion_date = datetime.utcnow()

        for result in results:
            url = result.get("link", "")
            if not url:
                continue

            await self.rate_limit_wait(self.config.get("rate_limit_rpm", 60))

            downloaded_path = self._download(url, download_dir)
            if downloaded_path is None:
                logger.warning("GoogleCSEAdapter: download failed for %s", url)
                continue

            try:
                raw_text = self._extract_text(downloaded_path)
            except AdapterParseError as exc:
                logger.warning("GoogleCSEAdapter: %s", exc)
                continue

            if not raw_text.strip():
                logger.warning("GoogleCSEAdapter: no text from %s", downloaded_path.name)
                continue

            doc_type = self.config.get("document_type", "guidance")
            pub_date = _parse_date(result.get("snippet", ""))

            yield Document(
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
                country=self.config.get("country_hints", []),
                sector_hint=self.config.get("sector_hints", []),
                extraction_status="pending",
                extraction_error=None,
            )

    # ── Step 1: Search ────────────────────────────────────────────────────────

    def _search(self, query: str) -> Iterator[dict]:
        """
        Paginate through CSE results up to max_results.
        Rotates API keys on quota errors.
        Yields one result metadata dict per search hit.

        Config keys that control the search:
          max_results    int    Max results to fetch (default 20)
          file_type      str    e.g. "pdf" — added as fileType param
          date_restrict  str    CSE dateRestrict format: d[N], w[N], m[N], y[N]
                                e.g. "m12" = last 12 months, "y2" = last 2 years
                                (default: "y2" — avoids stale results)
        """
        max_results: int = self.config.get("max_results", 20)
        file_type: str = self.config.get("file_type", "")
        # Default y2 (last 2 years) prevents drowning in old results.
        # Set date_restrict: "" in sources.yaml to disable.
        date_restrict: str = self.config.get("date_restrict", "y2")
        fetched = 0
        start = 1

        while fetched < max_results:
            batch_size = min(_MAX_RESULTS_PER_PAGE, max_results - fetched)
            params: dict = {
                "key": self._current_key(),
                "cx": self._cse_id,
                "q": query,
                "num": batch_size,
                "start": start,
            }
            if file_type:
                params["fileType"] = file_type
            if date_restrict:
                params["dateRestrict"] = date_restrict

            try:
                resp = requests.get(_CSE_ENDPOINT, params=params, timeout=_REQUEST_TIMEOUT)
            except requests.RequestException as exc:
                raise AdapterFetchError(f"CSE API request failed: {exc}") from exc

            if resp.status_code == 429 or resp.status_code == 403:
                self._rotate_key()
                if self._key_index == 0:
                    raise AdapterFetchError("All CSE API keys exhausted (quota exceeded)")
                continue

            if not resp.ok:
                raise AdapterFetchError(f"CSE API error {resp.status_code}: {resp.text[:200]}")

            data = resp.json()
            items = data.get("items", [])
            if not items:
                break

            for item in items:
                yield {
                    "link": item.get("link", ""),
                    "title": item.get("title", ""),
                    "snippet": item.get("snippet", ""),
                    "mime": item.get("mime", ""),
                }
                fetched += 1
                if fetched >= max_results:
                    return

            if len(items) < batch_size:
                break

            start += batch_size
            time.sleep(0.5)

    def _current_key(self) -> str:
        return self._api_keys[self._key_index % len(self._api_keys)]

    def _rotate_key(self) -> None:
        self._key_index += 1
        logger.info("Rotating to CSE API key %d", self._key_index % len(self._api_keys))

    # ── Step 2: Download ──────────────────────────────────────────────────────

    def _download(self, url: str, download_dir: Path) -> "Path | None":
        """
        Download a URL to download_dir. Returns the local Path or None on failure.
        Filename is derived from the URL hash to avoid collisions.
        """
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        suffix = ".pdf" if url.lower().endswith(".pdf") else ".html"
        dest = download_dir / f"{url_hash}{suffix}"

        if dest.exists():
            return dest

        try:
            resp = requests.get(
                url,
                timeout=_DOWNLOAD_TIMEOUT,
                stream=True,
                headers={"User-Agent": "Mozilla/5.0 (compatible; AIPBot/1.0)"},
            )
            resp.raise_for_status()
            content_type = resp.headers.get("Content-Type", "")
            if "pdf" in content_type:
                dest = download_dir / f"{url_hash}.pdf"
            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=65536):
                    fh.write(chunk)
            return dest
        except Exception as exc:
            logger.warning("Download failed for %s: %s", url, exc)
            return None

    # ── Text extraction ───────────────────────────────────────────────────────

    def _extract_text(self, path: Path) -> str:
        if path.suffix.lower() == ".pdf":
            return self._extract_pdf_text(path)
        return self._read_html_text(path)

    def _extract_pdf_text(self, path: Path) -> str:
        try:
            with open(path, "rb") as fh:
                reader = PyPDF2.PdfReader(fh)
                pages = [p.extract_text() or "" for p in reader.pages]
            return "\n".join(pages).strip()
        except Exception as exc:
            raise AdapterParseError(f"PDF extraction failed for {path.name}: {exc}") from exc

    def _read_html_text(self, path: Path) -> str:
        try:
            return path.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            raise AdapterParseError(f"HTML read failed for {path.name}: {exc}") from exc


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_date(snippet: str) -> "datetime | None":
    """Best-effort: extract a year from the snippet text and return Jan 1 of that year."""
    import re
    match = re.search(r"\b(20\d{2})\b", snippet)
    if match:
        try:
            return datetime(int(match.group(1)), 1, 1)
        except ValueError:
            pass
    return None
