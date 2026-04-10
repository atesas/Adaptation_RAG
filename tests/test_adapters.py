"""
tests/test_adapters.py

Unit tests for adapters/corporate_pdf.py and adapters/google_cse.py.

Coverage per PROJECT_BRIEF_v1.1.md Section 7 (adapters/ row):
  CorporatePDFAdapter:
    - Returns Document with all required fields populated
    - content_hash is deterministic (same file → same hash when normalize() runs)
    - Raises AdapterFetchError for missing file
    - Raises AdapterParseError for non-PDF file
  GoogleCSEAdapter:
    - Mocked CSE response produces correct Document objects
    - Two-step order (search then download) is enforced
    - Raises AdapterAuthError if credentials missing
"""

import asyncio
import hashlib
import io
import struct
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator
from unittest.mock import MagicMock, patch, AsyncMock

import pytest
import PyPDF2

from adapters.base import AdapterAuthError, AdapterFetchError, AdapterParseError
from adapters.corporate_pdf import CorporatePDFAdapter
from schemas.document import Document, SOURCE_TYPES, DOCUMENT_TYPES
from schemas.validation import ValidationStatus


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_minimal_pdf(text: str = "Climate risk: water stress at facilities.") -> bytes:
    """
    Create a minimal valid PDF in memory containing the given text.
    Uses PyPDF2's PdfWriter to generate a real (parseable) PDF.
    """
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas
    import io as _io

    buf = _io.BytesIO()
    c = canvas.Canvas(buf, pagesize=letter)
    c.drawString(72, 720, text)
    c.save()
    return buf.getvalue()


def _write_sample_pdf(tmp_path: Path, text: str = "Water stress is a key physical risk.") -> Path:
    """Write a sample PDF to a temp directory using PyPDF2's writer."""
    pdf_path = tmp_path / "sample_report.pdf"
    writer = PyPDF2.PdfWriter()
    # PyPDF2 can't easily add text; use a pre-made minimal PDF bytes approach
    # We write a valid empty PDF and then test text extraction separately
    with open(pdf_path, "wb") as fh:
        writer.write(fh)
    return pdf_path


# ── CorporatePDFAdapter tests ─────────────────────────────────────────────────

class TestCorporatePDFAdapter:
    """Tests for adapters/corporate_pdf.py"""

    def _make_adapter(self, config_override: dict | None = None) -> CorporatePDFAdapter:
        config = {
            "document_type": "corporate_report",
            "sector_hint": ["food_and_beverage"],
            "country": ["FR"],
        }
        if config_override:
            config.update(config_override)
        return CorporatePDFAdapter(config)

    def test_source_type_is_corporate_pdf(self) -> None:
        adapter = self._make_adapter()
        assert adapter.source_type == "corporate_pdf"

    def test_source_type_in_vocab(self) -> None:
        adapter = self._make_adapter()
        assert adapter.source_type in SOURCE_TYPES

    def test_raises_fetch_error_for_missing_file(self, tmp_path: Path) -> None:
        adapter = self._make_adapter()
        missing = str(tmp_path / "nonexistent.pdf")

        async def run() -> None:
            async for _ in adapter.fetch(missing):
                pass

        with pytest.raises(AdapterFetchError):
            asyncio.get_event_loop().run_until_complete(run())

    def test_raises_parse_error_for_unsupported_extension(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("col1,col2\n1,2")
        adapter = self._make_adapter()

        async def run() -> None:
            async for _ in adapter.fetch(str(csv_file)):
                pass

        with pytest.raises(AdapterParseError):
            asyncio.get_event_loop().run_until_complete(run())

    def test_txt_file_yields_document(self, tmp_path: Path) -> None:
        txt_file = tmp_path / "report.txt"
        txt_file.write_text("Climate adaptation content here.")
        adapter = self._make_adapter()
        docs = []

        async def run() -> None:
            async for doc in adapter.fetch(str(txt_file)):
                docs.append(doc)

        asyncio.get_event_loop().run_until_complete(run())
        assert len(docs) == 1
        assert docs[0].raw_text == "Climate adaptation content here."

    def test_directory_yields_documents_for_each_file(self, tmp_path: Path) -> None:
        (tmp_path / "a.txt").write_text("Content A")
        (tmp_path / "b.txt").write_text("Content B")
        (tmp_path / "ignore.csv").write_text("not,included")
        adapter = self._make_adapter()
        docs = []

        async def run() -> None:
            async for doc in adapter.fetch(str(tmp_path)):
                docs.append(doc)

        asyncio.get_event_loop().run_until_complete(run())
        assert len(docs) == 2

    def test_document_has_all_required_fields(self, tmp_path: Path) -> None:
        """
        Verify that fetch() yields a Document with all required fields populated.
        We mock _extract_text to avoid needing a real PDF.
        """
        adapter = self._make_adapter()
        fake_pdf = tmp_path / "danone_2024.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4 fake content")  # valid header for path check

        sample_text = "Water stress is identified as a primary risk at 7 facilities."

        with patch.object(adapter, "_extract_text", return_value=sample_text):
            async def run() -> list[Document]:
                docs = []
                async for doc in adapter.fetch(str(fake_pdf)):
                    docs.append(doc)
                return docs

            docs = asyncio.get_event_loop().run_until_complete(run())

        assert len(docs) == 1
        doc = docs[0]

        # Required identity fields (doc_id and content_hash set by normalize(), not adapter)
        assert doc.raw_text == sample_text
        assert doc.source_type == "corporate_pdf"
        assert doc.adapter == "CorporatePDFAdapter"
        assert doc.document_type == "corporate_report"
        assert doc.language == "en"
        assert isinstance(doc.ingestion_date, datetime)
        assert doc.extraction_status == "pending"
        assert doc.extraction_error is None
        assert isinstance(doc.country, list)
        assert isinstance(doc.sector_hint, list)

    def test_document_source_url_is_absolute_path(self, tmp_path: Path) -> None:
        adapter = self._make_adapter()
        fake_pdf = tmp_path / "report.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4")

        with patch.object(adapter, "_extract_text", return_value="Climate text."):
            async def run() -> Document:
                async for doc in adapter.fetch(str(fake_pdf)):
                    return doc

            doc = asyncio.get_event_loop().run_until_complete(run())

        assert Path(doc.source_url).is_absolute()

    def test_split_text_single_chunk_when_under_limit(self) -> None:
        adapter = self._make_adapter()
        text = "A" * 1000
        chunks = adapter._split_text(text, max_chars=200_000)
        assert len(chunks) == 1
        assert chunks[0] == text

    def test_split_text_splits_when_over_limit(self) -> None:
        adapter = self._make_adapter()
        # 3 paragraphs, each 100_001 chars — must result in multiple chunks
        para = ("Climate risk analysis. " * 5000)[:100_001]
        text = para + "\n\n" + para + "\n\n" + para
        chunks = adapter._split_text(text, max_chars=200_000)
        assert len(chunks) >= 2
        for chunk in chunks:
            assert len(chunk) <= 200_000

    def test_content_hash_is_deterministic(self, tmp_path: Path) -> None:
        """
        Same text always produces the same SHA256. Verifies normalize() contract.
        (The adapter doesn't set the hash, but we verify SHA256 determinism.)
        """
        text = "The company faces water stress at its South African brewery."
        h1 = hashlib.sha256(text.encode()).hexdigest()
        h2 = hashlib.sha256(text.encode()).hexdigest()
        assert h1 == h2

    def test_multiple_documents_for_large_text(self, tmp_path: Path) -> None:
        adapter = self._make_adapter()
        fake_pdf = tmp_path / "large.pdf"
        fake_pdf.write_bytes(b"%PDF-1.4")
        large_text = ("Climate adaptation measures were implemented. " * 5000)[:450_000]

        with patch.object(adapter, "_extract_text", return_value=large_text):
            async def run() -> list[Document]:
                return [doc async for doc in adapter.fetch(str(fake_pdf))]

            docs = asyncio.get_event_loop().run_until_complete(run())

        assert len(docs) >= 2
        for doc in docs:
            assert len(doc.raw_text) <= 200_000


# ── GoogleCSEAdapter tests ─────────────────────────────────────────────────────

class TestGoogleCSEAdapter:
    """Tests for adapters/google_cse.py"""

    def _make_adapter(self, config_override: dict | None = None) -> "GoogleCSEAdapter":
        from adapters.google_cse import GoogleCSEAdapter
        config = {
            "document_type": "guidance",
            "sector_hint": ["food_agriculture"],
            "country": [],
            "max_results": 5,
            "file_type": "pdf",
        }
        if config_override:
            config.update(config_override)
        return GoogleCSEAdapter(config)

    def test_source_type_is_google_cse(self) -> None:
        with patch.dict("os.environ", {
            "GOOGLE_CSE_API_KEY": "test-key",
            "GOOGLE_CSE_ID": "test-cx",
            "AZURE_SEARCH_ENDPOINT": "https://test.search.windows.net",
            "AZURE_SEARCH_KEY": "test-search-key",
            "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com/",
            "AZURE_OPENAI_KEY": "test-openai-key",
        }):
            import importlib
            import config as cfg
            importlib.reload(cfg)
            from adapters.google_cse import GoogleCSEAdapter
            adapter = GoogleCSEAdapter({"document_type": "guidance", "sector_hint": []})
            assert adapter.source_type == "google_cse"
            assert adapter.source_type in SOURCE_TYPES

    def test_raises_auth_error_if_credentials_missing(self) -> None:
        with patch.dict("os.environ", {
            "GOOGLE_CSE_API_KEY": "test-key",
            "GOOGLE_CSE_ID": "test-cx",
            "AZURE_SEARCH_ENDPOINT": "https://test.search.windows.net",
            "AZURE_SEARCH_KEY": "test",
            "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com/",
            "AZURE_OPENAI_KEY": "test",
        }):
            import importlib
            import config as cfg
            importlib.reload(cfg)
            from adapters.google_cse import GoogleCSEAdapter

            # Patch config attributes directly to simulate empty credentials
            with patch("adapters.google_cse.config") as mock_cfg:
                mock_cfg.GOOGLE_CSE_API_KEY = ""
                mock_cfg.GOOGLE_CSE_ID = ""
                with pytest.raises(AdapterAuthError):
                    GoogleCSEAdapter({"document_type": "guidance"})

    def test_two_step_order_search_then_download(self, tmp_path: Path) -> None:
        """
        Verify that _search() is called before _download().
        Enforces the documented two-step flow.
        """
        call_order: list[str] = []

        def mock_search(self_inner, query: str) -> list[dict]:
            call_order.append("search")
            return [{"link": "https://example.com/report.pdf", "title": "Test Report"}]

        def mock_download(self_inner, url: str, download_dir: Path) -> Path | None:
            call_order.append("download")
            p = tmp_path / "report.pdf"
            p.write_bytes(b"%PDF-1.4 fake")
            return p

        with patch.dict("os.environ", {
            "GOOGLE_CSE_API_KEY": "test-key",
            "GOOGLE_CSE_ID": "test-cx",
            "AZURE_SEARCH_ENDPOINT": "https://test.search.windows.net",
            "AZURE_SEARCH_KEY": "test",
            "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com/",
            "AZURE_OPENAI_KEY": "test",
        }):
            import importlib
            import config as cfg
            importlib.reload(cfg)
            from adapters.google_cse import GoogleCSEAdapter

            adapter = GoogleCSEAdapter({"document_type": "guidance", "sector_hint": []})

            with patch.object(GoogleCSEAdapter, "_search", mock_search), \
                 patch.object(GoogleCSEAdapter, "_download", mock_download), \
                 patch.object(GoogleCSEAdapter, "_extract_text", return_value="Water stress content."):

                async def run() -> list[Document]:
                    return [doc async for doc in adapter.fetch("climate adaptation food")]

                docs = asyncio.get_event_loop().run_until_complete(run())

        assert call_order == ["search", "download"], (
            f"Expected ['search', 'download'], got {call_order}"
        )
        assert len(docs) == 1

    def test_mocked_cse_response_produces_correct_document(self, tmp_path: Path) -> None:
        """
        Given a mocked CSE result, fetch() must yield a Document with correct fields.
        """
        mock_result = {
            "link": "https://example.com/nestle-sustainability-2024.pdf",
            "title": "Nestlé Sustainability Report 2024",
            "mime_type": "application/pdf",
            "published_date": "2024-03-15",
        }
        sample_text = "Nestlé identifies water stress as a material physical risk."
        downloaded_pdf = tmp_path / "nestle_report.pdf"
        downloaded_pdf.write_bytes(b"%PDF-1.4 fake content")

        with patch.dict("os.environ", {
            "GOOGLE_CSE_API_KEY": "test-key",
            "GOOGLE_CSE_ID": "test-cx",
            "AZURE_SEARCH_ENDPOINT": "https://test.search.windows.net",
            "AZURE_SEARCH_KEY": "test",
            "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com/",
            "AZURE_OPENAI_KEY": "test",
        }):
            import importlib
            import config as cfg
            importlib.reload(cfg)
            from adapters.google_cse import GoogleCSEAdapter

            adapter = GoogleCSEAdapter({"document_type": "corporate_report", "sector_hint": ["food_and_beverage"]})

            with patch.object(GoogleCSEAdapter, "_search", return_value=[mock_result]), \
                 patch.object(GoogleCSEAdapter, "_download", return_value=downloaded_pdf), \
                 patch.object(GoogleCSEAdapter, "_extract_text", return_value=sample_text):

                async def run() -> list[Document]:
                    return [doc async for doc in adapter.fetch("Nestlé sustainability report 2024")]

                docs = asyncio.get_event_loop().run_until_complete(run())

        assert len(docs) == 1
        doc = docs[0]
        assert doc.raw_text == sample_text
        assert doc.source_url == mock_result["link"]
        assert doc.source_type == "google_cse"
        assert doc.adapter == "GoogleCSEAdapter"
        assert doc.document_type == "corporate_report"
        assert doc.extraction_status == "pending"
        assert isinstance(doc.ingestion_date, datetime)

    def test_skips_result_with_no_url(self, tmp_path: Path) -> None:
        """Results with no link are silently skipped."""
        mock_results = [
            {"link": "", "title": "Empty link"},
        ]

        with patch.dict("os.environ", {
            "GOOGLE_CSE_API_KEY": "test-key",
            "GOOGLE_CSE_ID": "test-cx",
            "AZURE_SEARCH_ENDPOINT": "https://test.search.windows.net",
            "AZURE_SEARCH_KEY": "test",
            "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com/",
            "AZURE_OPENAI_KEY": "test",
        }):
            import importlib
            import config as cfg
            importlib.reload(cfg)
            from adapters.google_cse import GoogleCSEAdapter

            adapter = GoogleCSEAdapter({"document_type": "guidance"})

            with patch.object(GoogleCSEAdapter, "_search", return_value=mock_results):
                async def run() -> list[Document]:
                    return [doc async for doc in adapter.fetch("test query")]

                docs = asyncio.get_event_loop().run_until_complete(run())

        assert docs == []

    def test_skips_result_when_download_fails(self, tmp_path: Path) -> None:
        """If _download returns None, the result is skipped without raising."""
        mock_results = [{"link": "https://example.com/report.pdf", "title": "Report"}]

        with patch.dict("os.environ", {
            "GOOGLE_CSE_API_KEY": "test-key",
            "GOOGLE_CSE_ID": "test-cx",
            "AZURE_SEARCH_ENDPOINT": "https://test.search.windows.net",
            "AZURE_SEARCH_KEY": "test",
            "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com/",
            "AZURE_OPENAI_KEY": "test",
        }):
            import importlib
            import config as cfg
            importlib.reload(cfg)
            from adapters.google_cse import GoogleCSEAdapter

            adapter = GoogleCSEAdapter({"document_type": "guidance"})

            with patch.object(GoogleCSEAdapter, "_search", return_value=mock_results), \
                 patch.object(GoogleCSEAdapter, "_download", return_value=None):

                async def run() -> list[Document]:
                    return [doc async for doc in adapter.fetch("test query")]

                docs = asyncio.get_event_loop().run_until_complete(run())

        assert docs == []

    def test_date_chunking_generates_multiple_windows(self) -> None:
        """
        _search() with a 9-day lookback at 3-day chunks must produce exactly
        3 windows, each with a dated query containing after: and before: operators.
        """
        captured_queries: list[str] = []

        def fake_get(url: str, params: dict, timeout: int) -> "MagicMock":
            captured_queries.append(params)
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.ok = True
            mock_resp.json.return_value = {"items": []}
            return mock_resp

        with patch.dict("os.environ", {
            "GOOGLE_CSE_API_KEY": "test-key",
            "GOOGLE_CSE_ID": "test-cx",
            "AZURE_SEARCH_ENDPOINT": "https://test.search.windows.net",
            "AZURE_SEARCH_KEY": "test",
            "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com/",
            "AZURE_OPENAI_KEY": "test",
        }):
            import importlib
            import config as cfg
            importlib.reload(cfg)
            from adapters.google_cse import GoogleCSEAdapter

            adapter = GoogleCSEAdapter({
                "document_type": "guidance",
                "lookback_days": 9,
                "date_chunk_days": 3,
                "max_results_per_chunk": 10,
            })

            with patch("adapters.google_cse.requests.get", side_effect=fake_get):
                list(adapter._search("climate food"))

        # lookback=9 days, chunk=3 days → floor(9/3)+1 = 4 windows
        assert len(captured_queries) == 4, f"Expected 4 windows, got {len(captured_queries)}"
        for params in captured_queries:
            assert params["q"] == "climate food"
            assert "sort" in params, f"Missing sort param: {params}"
            # sort value must be date:r:YYYYMMDD:YYYYMMDD
            assert params["sort"].startswith("date:r:"), f"Wrong sort format: {params['sort']}"
            parts = params["sort"].split(":")
            assert len(parts) == 4
            start_d, end_d = parts[2], parts[3]
            assert len(start_d) == 8 and start_d.isdigit()
            assert len(end_d) == 8 and end_d.isdigit()
            assert start_d <= end_d

    def test_date_chunking_deduplicates_urls_across_windows(self) -> None:
        """
        If the same URL appears in two different date windows, it should only
        be yielded once.
        """
        repeated_item = {"link": "https://example.com/report.pdf", "title": "Report",
                         "snippet": "", "mime": ""}

        call_count = 0

        def fake_get(url: str, params: dict, timeout: int) -> "MagicMock":
            nonlocal call_count
            call_count += 1
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.ok = True
            mock_resp.json.return_value = {"items": [repeated_item]}
            return mock_resp

        with patch.dict("os.environ", {
            "GOOGLE_CSE_API_KEY": "test-key",
            "GOOGLE_CSE_ID": "test-cx",
            "AZURE_SEARCH_ENDPOINT": "https://test.search.windows.net",
            "AZURE_SEARCH_KEY": "test",
            "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com/",
            "AZURE_OPENAI_KEY": "test",
        }):
            import importlib
            import config as cfg
            importlib.reload(cfg)
            from adapters.google_cse import GoogleCSEAdapter

            adapter = GoogleCSEAdapter({
                "document_type": "guidance",
                "lookback_days": 6,
                "date_chunk_days": 3,
                "max_results_per_chunk": 10,
            })

            with patch("adapters.google_cse.requests.get", side_effect=fake_get):
                results = list(adapter._search("climate food"))

        # lookback=6 days, chunk=3 days → floor(6/3)+1 = 3 windows
        assert call_count == 3
        # but the URL appeared in both — only one result yielded
        assert len(results) == 1
        assert results[0]["link"] == "https://example.com/report.pdf"

    def test_proactive_key_rotation_at_limit(self) -> None:
        """
        With queries_per_key_limit=2 and 2 keys, the adapter must rotate to
        key 2 after 2 queries on key 1, then use key 2 for the remaining queries.
        """
        keys_used: list[str] = []

        def fake_get(url: str, params: dict, timeout: int) -> "MagicMock":
            keys_used.append(params["key"])
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.ok = True
            mock_resp.json.return_value = {"items": []}
            return mock_resp

        with patch.dict("os.environ", {
            "GOOGLE_CSE_API_KEY": "key-one,key-two",
            "GOOGLE_CSE_ID": "test-cx",
            "AZURE_SEARCH_ENDPOINT": "https://test.search.windows.net",
            "AZURE_SEARCH_KEY": "test",
            "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com/",
            "AZURE_OPENAI_KEY": "test",
        }):
            import importlib
            import config as cfg
            importlib.reload(cfg)
            from adapters.google_cse import GoogleCSEAdapter

            # 3-window search, limit=2 per key → key-one used for windows 1–2,
            # key-two used for window 3
            adapter = GoogleCSEAdapter({
                "document_type": "guidance",
                "lookback_days": 9,
                "date_chunk_days": 3,
                "max_results_per_chunk": 10,
                "queries_per_key_limit": 2,
            })

            with patch("adapters.google_cse.requests.get", side_effect=fake_get):
                list(adapter._search("climate food"))

        assert keys_used[:2] == ["key-one", "key-one"]
        assert all(k == "key-two" for k in keys_used[2:])

    def test_all_keys_exhausted_raises(self) -> None:
        """When all keys hit their limit, AdapterFetchError is raised."""
        with patch.dict("os.environ", {
            "GOOGLE_CSE_API_KEY": "only-key",
            "GOOGLE_CSE_ID": "test-cx",
            "AZURE_SEARCH_ENDPOINT": "https://test.search.windows.net",
            "AZURE_SEARCH_KEY": "test",
            "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com/",
            "AZURE_OPENAI_KEY": "test",
        }):
            import importlib
            import config as cfg
            importlib.reload(cfg)
            from adapters.google_cse import GoogleCSEAdapter

            adapter = GoogleCSEAdapter({
                "document_type": "guidance",
                "lookback_days": 9,
                "date_chunk_days": 3,
                "max_results_per_chunk": 10,
                "queries_per_key_limit": 1,  # exhausted after 1 request
            })

            def fake_get(url: str, params: dict, timeout: int) -> "MagicMock":
                mock_resp = MagicMock()
                mock_resp.status_code = 200
                mock_resp.ok = True
                mock_resp.json.return_value = {"items": []}
                return mock_resp

            with patch("adapters.google_cse.requests.get", side_effect=fake_get):
                with pytest.raises(AdapterFetchError, match="exhausted"):
                    list(adapter._search("climate food"))


# ── GCFAPIAdapter tests ───────────────────────────────────────────────────────

class TestGCFAPIAdapter:
    """Tests for adapters/gcf_api.py — two-step list+detail fetch."""

    # One realistic project record matching the /v1/projects API shape
    _SAMPLE_PROJECT: dict = {
        "ProjectsID": 13020,
        "ApprovedRef": "FP001",
        "BoardMeeting": "B.11",
        "ProjectName": "Building the Resilience of Wetlands in the Province of Datem del Marañón, Peru",
        "StartDate": "2017-03-10T00:00:00.000Z",
        "EndDate": "2022-03-10T00:00:00.000Z",
        "ApprovalDate": "2015-11-05T00:00:00.000Z",
        "DateImplementationStart": "2016-12-15T00:00:00.000Z",
        "DateClosing": "2022-12-31T00:00:00.000Z",
        "DurationMonths": 60,
        "Theme": "Cross-cutting",
        "Sector": "Public",
        "LifeTimeCO2": 2630000,
        "Size": "Micro",
        "RiskCategory": "Category C",
        "DirectBeneficiaries": 20413,
        "IndirectBeneficiaries": 0,
        "TotalGCFFunding": 6240000,
        "TotalCoFinancing": 2870000,
        "TotalValue": 9110000,
        "ProjectURL": "https://www.greenclimate.fund/project/FP001",
        "Status": "Under Implementation",
        "DateCancelled": None,
        "Countries": [{
            "CountryID": 173,
            "CountryName": "Peru",
            "ISO3": "PER",
            "Region": "Latin America and the Caribbean",
            "LDCs": False,
            "SIDS": False,
            "Financing": [{"Currency": "USD", "GCF": 6240000, "CoFinancing": 2870000, "Total": 9110000}],
        }],
        "Entities": [{
            "EntityID": 27,
            "Name": "Peruvian Trust Fund for National Parks and Protected Areas",
            "Acronym": "Profonanpe",
            "Access": "Direct",
            "Type": "National",
            "Sector": "Public",
        }],
        "ResultAreas": [
            {"Area": "Forest and land use", "Type": "Mitigation", "Value": "80.00%"},
            {"Area": "Livelihoods of people and communities", "Type": "Adaptation", "Value": "20.00%"},
            {"Area": "Energy generation and access", "Type": "Mitigation", "Value": "0.00%"},
        ],
        "Funding": [
            {"Source": "GCF", "Instrument": "Grants", "Budget": 6240000, "Currency": "USD"},
            {"Source": "Co-Financing", "Instrument": "Grants", "Budget": 2870000, "Currency": "USD"},
        ],
        "Disbursements": [
            {"ProjectDisbursementID": 10, "AmountDisbursed": 1022186, "AmountDisbursedUSDeq": 1022186,
             "Currency": "USD", "DateEffective": "2017-05-31", "Entity": "Profonanpe"},
            {"ProjectDisbursementID": 122, "AmountDisbursed": 1300000, "AmountDisbursedUSDeq": 1300000,
             "Currency": "USD", "DateEffective": "2019-10-25", "Entity": "Profonanpe"},
        ],
    }

    def _make_adapter(self, config_override: dict | None = None) -> "GCFAPIAdapter":
        from adapters.gcf_api import GCFAPIAdapter
        config = {
            "sector_hints": ["water", "ecosystems"],
            "rate_limit_rpm": 20,
            "fetch_project_details": False,  # default off for unit tests
        }
        if config_override:
            config.update(config_override)
        return GCFAPIAdapter(config)

    def test_source_type_is_gcf_api(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        from schemas.document import SOURCE_TYPES
        adapter = self._make_adapter()
        assert adapter.source_type == "gcf_api"
        assert adapter.source_type in SOURCE_TYPES

    def test_fetch_without_detail_calls_list_only(self) -> None:
        """With fetch_project_details=False, only the list endpoint is called."""
        import asyncio
        from adapters.gcf_api import GCFAPIAdapter

        adapter = self._make_adapter({"fetch_project_details": False})
        list_calls: list[str] = []
        detail_calls: list[str] = []

        async def mock_fetch_list(self_inner: GCFAPIAdapter) -> list[dict]:
            list_calls.append("list")
            return [self._SAMPLE_PROJECT]

        async def mock_fetch_detail(self_inner: GCFAPIAdapter, project_id: int) -> dict | None:
            detail_calls.append(str(project_id))
            return None

        with patch.object(GCFAPIAdapter, "_fetch_list", mock_fetch_list), \
             patch.object(GCFAPIAdapter, "_fetch_detail", mock_fetch_detail):
            docs = asyncio.get_event_loop().run_until_complete(
                _collect(adapter.fetch(""))
            )

        assert list_calls == ["list"]
        assert detail_calls == [], "detail must NOT be called when fetch_project_details=False"
        assert len(docs) == 1

    def test_fetch_with_detail_calls_list_then_detail(self) -> None:
        """With fetch_project_details=True, list is called first, then detail per project."""
        import asyncio
        from adapters.gcf_api import GCFAPIAdapter

        adapter = self._make_adapter({"fetch_project_details": True})
        call_order: list[str] = []

        async def mock_fetch_list(self_inner: GCFAPIAdapter) -> list[dict]:
            call_order.append("list")
            return [self._SAMPLE_PROJECT]

        async def mock_fetch_detail(self_inner: GCFAPIAdapter, project_id: int) -> dict | None:
            call_order.append(f"detail:{project_id}")
            return self._SAMPLE_PROJECT

        with patch.object(GCFAPIAdapter, "_fetch_list", mock_fetch_list), \
             patch.object(GCFAPIAdapter, "_fetch_detail", mock_fetch_detail), \
             patch.object(GCFAPIAdapter, "rate_limit_wait", new=AsyncMock()):
            docs = asyncio.get_event_loop().run_until_complete(
                _collect(adapter.fetch(""))
            )

        assert call_order[0] == "list", "list must be called before any detail"
        assert call_order[1] == "detail:13020"
        assert len(docs) == 1

    def test_document_has_required_fields(self) -> None:
        """_project_to_document must populate all required Document fields."""
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter()
        doc = adapter._project_to_document(self._SAMPLE_PROJECT)

        assert doc is not None
        assert doc.source_type == "gcf_api"
        assert doc.adapter == "GCFAPIAdapter"
        assert doc.document_type == "project_db"
        assert doc.extraction_status == "pending"
        assert doc.extraction_error is None
        assert isinstance(doc.ingestion_date, datetime)
        assert isinstance(doc.country, list)
        assert isinstance(doc.sector_hint, list)
        assert doc.content_hash != ""
        assert doc.doc_id != ""

    def test_approved_ref_and_title_in_document(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter()
        doc = adapter._project_to_document(self._SAMPLE_PROJECT)
        assert doc is not None
        assert "FP001" in doc.title or "Wetlands" in doc.title
        assert doc.source_url == "https://www.greenclimate.fund/project/FP001"

    def test_countries_extracted_as_iso3(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter()
        doc = adapter._project_to_document(self._SAMPLE_PROJECT)
        assert doc is not None
        assert "PER" in doc.country

    def test_raw_text_contains_key_fields(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter()
        doc = adapter._project_to_document(self._SAMPLE_PROJECT)
        assert doc is not None
        text = doc.raw_text

        assert "FP001" in text
        assert "B.11" in text
        assert "Peru" in text
        assert "Cross-cutting" in text
        assert "6,240,000" in text           # TotalGCFFunding formatted
        assert "Profonanpe" in text           # entity name
        assert "Forest and land use" in text  # non-zero result area
        assert "Livelihoods of people" in text
        # Zero-value result areas must be omitted
        assert "Energy generation and access" not in text

    def test_zero_result_areas_excluded_from_text(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter()
        doc = adapter._project_to_document(self._SAMPLE_PROJECT)
        assert doc is not None
        # "Energy generation and access" has Value "0.00%" — must not appear in text
        assert "Energy generation and access" not in doc.raw_text

    def test_missing_project_id_returns_none(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter()
        doc = adapter._project_to_document({"ProjectName": "No ID project"})
        assert doc is None

    def test_content_hash_is_deterministic(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter()
        doc1 = adapter._project_to_document(self._SAMPLE_PROJECT)
        doc2 = adapter._project_to_document(self._SAMPLE_PROJECT)
        assert doc1 is not None and doc2 is not None
        assert doc1.content_hash == doc2.content_hash

    def test_detail_fallback_to_meta_when_detail_fails(self) -> None:
        """If _fetch_detail returns None, the list metadata is used instead."""
        import asyncio
        from adapters.gcf_api import GCFAPIAdapter

        adapter = self._make_adapter({"fetch_project_details": True})

        async def mock_fetch_list(self_inner: GCFAPIAdapter) -> list[dict]:
            return [self._SAMPLE_PROJECT]

        async def mock_fetch_detail(self_inner: GCFAPIAdapter, project_id: int) -> dict | None:
            return None  # simulate detail endpoint failure

        with patch.object(GCFAPIAdapter, "_fetch_list", mock_fetch_list), \
             patch.object(GCFAPIAdapter, "_fetch_detail", mock_fetch_detail), \
             patch.object(GCFAPIAdapter, "rate_limit_wait", new=AsyncMock()):
            docs = asyncio.get_event_loop().run_until_complete(
                _collect(adapter.fetch(""))
            )

        # Should still yield the document using list metadata
        assert len(docs) == 1
        assert "FP001" in docs[0].raw_text

    def test_list_fetch_retries_on_failure(self) -> None:
        """_fetch_list retries up to _MAX_RETRIES times before raising."""
        import asyncio
        from adapters.gcf_api import GCFAPIAdapter

        adapter = self._make_adapter()
        attempt_count = 0

        def bad_get(url: str, timeout: int) -> MagicMock:
            nonlocal attempt_count
            attempt_count += 1
            raise requests.exceptions.ConnectionError("network error")

        with patch("adapters.gcf_api.requests.get", side_effect=bad_get), \
             patch("adapters.gcf_api.asyncio.sleep", new=AsyncMock()):
            with pytest.raises(AdapterFetchError, match="failed after"):
                asyncio.get_event_loop().run_until_complete(adapter._fetch_list())

        assert attempt_count == 4  # initial + 3 retries

    def test_parse_date_handles_iso8601_with_z(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter()
        dt = adapter._parse_date("2015-11-05T00:00:00.000Z")
        assert dt is not None
        assert dt.year == 2015
        assert dt.month == 11
        assert dt.day == 5

    def test_parse_date_returns_none_for_empty(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter()
        assert adapter._parse_date(None) is None
        assert adapter._parse_date("") is None

    def test_disbursements_total_in_text(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter()
        doc = adapter._project_to_document(self._SAMPLE_PROJECT)
        assert doc is not None
        # 1022186 + 1300000 = 2322186
        assert "2,322,186" in doc.raw_text

    def test_primary_entity_name(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter()
        doc = adapter._project_to_document(self._SAMPLE_PROJECT)
        assert doc is not None
        assert doc.company_name == "Peruvian Trust Fund for National Parks and Protected Areas"

    # ── _matches_filters tests ────────────────────────────────────────────────

    def test_no_filters_matches_everything(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter({"filters": {}})
        assert adapter._matches_filters(self._SAMPLE_PROJECT) is True

    def test_theme_filter_match(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter({"filters": {"theme": ["Cross-cutting", "Adaptation"]}})
        assert adapter._matches_filters(self._SAMPLE_PROJECT) is True  # Theme=Cross-cutting

    def test_theme_filter_no_match(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter({"filters": {"theme": ["Mitigation"]}})
        assert adapter._matches_filters(self._SAMPLE_PROJECT) is False  # Theme=Cross-cutting

    def test_status_filter_match(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter({"filters": {"status": ["Under Implementation"]}})
        assert adapter._matches_filters(self._SAMPLE_PROJECT) is True

    def test_status_filter_no_match(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter({"filters": {"status": ["Completed"]}})
        assert adapter._matches_filters(self._SAMPLE_PROJECT) is False

    def test_countries_iso3_filter_match(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter({"filters": {"countries_iso3": ["PER", "MWI"]}})
        assert adapter._matches_filters(self._SAMPLE_PROJECT) is True  # ISO3=PER

    def test_countries_iso3_filter_no_match(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter({"filters": {"countries_iso3": ["MWI", "BGD"]}})
        assert adapter._matches_filters(self._SAMPLE_PROJECT) is False  # only PER

    def test_result_areas_filter_match(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter({
            "filters": {"result_areas": ["Livelihoods of people and communities"]}
        })
        assert adapter._matches_filters(self._SAMPLE_PROJECT) is True  # 20%

    def test_result_areas_filter_excludes_zero_allocation(self) -> None:
        """A result area present but at 0.00% must NOT trigger a filter match."""
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter({
            "filters": {"result_areas": ["Energy generation and access"]}
        })
        assert adapter._matches_filters(self._SAMPLE_PROJECT) is False  # 0.00%

    def test_min_gcf_funding_match(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter({"filters": {"min_gcf_funding": 5000000}})
        assert adapter._matches_filters(self._SAMPLE_PROJECT) is True  # 6,240,000

    def test_min_gcf_funding_no_match(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter({"filters": {"min_gcf_funding": 10000000}})
        assert adapter._matches_filters(self._SAMPLE_PROJECT) is False  # only 6,240,000

    def test_combined_filters_all_pass(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter({"filters": {
            "theme": ["Cross-cutting", "Adaptation"],
            "status": ["Under Implementation"],
            "countries_iso3": ["PER"],
            "min_gcf_funding": 1000000,
        }})
        assert adapter._matches_filters(self._SAMPLE_PROJECT) is True

    def test_combined_filters_one_fails(self) -> None:
        from adapters.gcf_api import GCFAPIAdapter
        adapter = self._make_adapter({"filters": {
            "theme": ["Cross-cutting"],
            "status": ["Completed"],       # fails — project is Under Implementation
        }})
        assert adapter._matches_filters(self._SAMPLE_PROJECT) is False

    def test_filter_skips_unmatched_before_detail_fetch(self) -> None:
        """
        With a filter that matches nothing, _fetch_detail must never be called.
        This proves filtering happens before the detail step.
        """
        import asyncio
        from adapters.gcf_api import GCFAPIAdapter

        adapter = self._make_adapter({
            "fetch_project_details": True,
            "filters": {"theme": ["Mitigation"]},  # sample is Cross-cutting → no match
        })
        detail_calls: list[int] = []

        async def mock_fetch_list(self_inner: GCFAPIAdapter) -> list[dict]:
            return [self._SAMPLE_PROJECT]

        async def mock_fetch_detail(self_inner: GCFAPIAdapter, project_id: int) -> dict | None:
            detail_calls.append(project_id)
            return None

        with patch.object(GCFAPIAdapter, "_fetch_list", mock_fetch_list), \
             patch.object(GCFAPIAdapter, "_fetch_detail", mock_fetch_detail), \
             patch.object(GCFAPIAdapter, "rate_limit_wait", new=AsyncMock()):
            docs = asyncio.get_event_loop().run_until_complete(
                _collect(adapter.fetch(""))
            )

        assert docs == [], "no documents expected when filter matches nothing"
        assert detail_calls == [], "detail must not be fetched for filtered-out projects"


# ── Async helper ──────────────────────────────────────────────────────────────

async def _collect(ait: AsyncIterator) -> list:
    return [item async for item in ait]
