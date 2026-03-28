"""
CorporatePDFAdapter

Wraps PyPDF2-based PDF text extraction. Accepts a local file path and
yields a single Document object containing the full extracted text.

No Azure AI Search calls. No embeddings. No LLM calls.
All downstream processing happens in ingest.py.
"""

import hashlib
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator

import PyPDF2

from adapters.base import BaseAdapter, AdapterFetchError, AdapterParseError
from schemas.document import Document, SOURCE_TYPES, DOCUMENT_TYPES


class CorporatePDFAdapter(BaseAdapter):
    """
    Adapter for corporate sustainability, annual, and TCFD/CSRD PDF reports.

    fetch(path) flow:
      1. Open the PDF at the given path
      2. Extract text from all pages with PyPDF2
      3. Yield one Document (or multiple if text exceeds MAX_DOCUMENT_CHARS)
    """

    source_type: str = "corporate_pdf"

    async def fetch(self, query_or_path: str) -> AsyncIterator[Document]:
        """
        Args:
            query_or_path: Path to a PDF file, a plain-text (.txt) file, or a
                           directory. Directories are expanded to all .pdf and
                           .txt files they contain (non-recursive).

        Yields:
            Document objects with extraction_status="pending".

        Raises:
            AdapterFetchError: if the file does not exist or cannot be opened.
            AdapterParseError: if PyPDF2 cannot read the PDF structure.
        """
        path = Path(query_or_path)

        if not path.exists():
            raise AdapterFetchError(f"Path not found: {path}")

        # Directory — expand to all .pdf and .txt files inside it
        if path.is_dir():
            files = sorted(
                p for p in path.iterdir()
                if p.suffix.lower() in {".pdf", ".txt"} and p.is_file()
            )
            if not files:
                raise AdapterFetchError(f"No .pdf or .txt files found in directory: {path}")
            for file_path in files:
                async for doc in self.fetch(str(file_path)):
                    yield doc
            return

        suffix = path.suffix.lower()
        if suffix not in {".pdf", ".txt"}:
            raise AdapterParseError(f"Expected a .pdf or .txt file, got: {path.suffix}")

        raw_text = self._extract_text(path)

        if not raw_text.strip():
            raise AdapterParseError(f"No text extracted from: {path.name}")

        ingestion_date = datetime.utcnow()

        # Split into chunks of MAX_DOCUMENT_CHARS if needed
        chunks = self._split_text(raw_text)

        for i, chunk in enumerate(chunks):
            title = path.stem if i == 0 else f"{path.stem} (part {i + 1})"
            source_url = str(path.resolve())

            doc = Document(
                doc_id="",               # set by normalize()
                content_hash="",         # set by normalize()
                raw_text=chunk,
                title=title,
                language="en",           # detected by normalize(); default en
                source_url=source_url,
                source_type=self.source_type,
                adapter=self.__class__.__name__,
                publication_date=None,   # not extractable from PDF metadata reliably
                ingestion_date=ingestion_date,
                reporting_year=None,     # extracted by Stage A
                document_type=self.config.get("document_type", "corporate_report"),
                company_name=self.config.get("company_name", None),
                company_id=self.config.get("company_id", None),
                csrd_wave=self.config.get("csrd_wave", None),
                country=self.config.get("country", []),
                sector_hint=self.config.get("sector_hint", []),
                extraction_status="pending",
                extraction_error=None,
            )
            yield doc

    def _extract_text(self, path: Path) -> str:
        """Extract text from a PDF or plain-text file."""
        if path.suffix.lower() == ".txt":
            try:
                return path.read_text(encoding="utf-8", errors="replace")
            except Exception as exc:
                raise AdapterFetchError(f"Cannot read text file: {path}") from exc

        # PDF
        try:
            with open(path, "rb") as fh:
                reader = PyPDF2.PdfReader(fh)
                pages = []
                for page in reader.pages:
                    text = page.extract_text()
                    if text:
                        pages.append(text)
                return "\n".join(pages).strip()
        except FileNotFoundError as exc:
            raise AdapterFetchError(f"Cannot open PDF: {path}") from exc
        except Exception as exc:
            raise AdapterParseError(f"PyPDF2 failed to parse {path.name}: {exc}") from exc

    def _split_text(self, text: str, max_chars: int = 30_000) -> list[str]:
        """
        Split text into segments of at most max_chars characters.
        Splits on paragraph boundaries where possible.

        Args:
            text: Full document text.
            max_chars: Maximum characters per segment.

        Returns:
            List of text segments.
        """
        if len(text) <= max_chars:
            return [text]

        segments: list[str] = []
        while text:
            if len(text) <= max_chars:
                segments.append(text)
                break

            # Try to split at a paragraph boundary within the window
            window = text[:max_chars]
            split_pos = window.rfind("\n\n")
            if split_pos == -1:
                split_pos = window.rfind("\n")
            if split_pos == -1:
                split_pos = max_chars

            segments.append(text[:split_pos].strip())
            text = text[split_pos:].strip()

        return [s for s in segments if s]
