# =============================================================================
# utils/ocr.py
# Azure Document Intelligence OCR for scanned PDFs
# =============================================================================
import logging
from pathlib import Path
from typing import Optional

from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest, ContentSource
from azure.core.credentials import AzureKeyCredential

import config

log = logging.getLogger("ocr")


def build_ocr_client() -> Optional[DocumentIntelligenceClient]:
    """Build Document Intelligence client if credentials are available."""
    if not config.AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT or not config.AZURE_DOCUMENT_INTELLIGENCE_KEY:
        return None
    
    return DocumentIntelligenceClient(
        endpoint=config.AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT,
        credential=AzureKeyCredential(config.AZURE_DOCUMENT_INTELLIGENCE_KEY),
    )


def extract_text_with_ocr(pdf_path: Path) -> str:
    """
    Extract text from a PDF using Azure Document Intelligence.
    Falls back to PyPDF2 if OCR not available or fails.
    """
    client = build_ocr_client()
    
    if client is None:
        log.warning("No Document Intelligence credentials - falling back to PyPDF2")
        return _extract_with_pypdf(pdf_path)
    
    try:
        log.info(f"Running OCR on {pdf_path.name}...")
        
        with open(pdf_path, "rb") as f:
            poller = client.begin_analyze_document(
                "prebuilt-layout",
                AnalyzeDocumentRequest(source=ContentSource(content=f)),
            )
        
        result = poller.result()
        
        text_parts = []
        for page in result.pages:
            for line in page.lines:
                text_parts.append(line.content)
        
        full_text = "\n".join(text_parts)
        log.info(f"OCR extracted {len(full_text):,} chars from {len(result.pages)} pages")
        
        return full_text
        
    except Exception as e:
        log.error(f"OCR failed for {pdf_path.name}: {e}, falling back to PyPDF2")
        return _extract_with_pypdf(pdf_path)


def _extract_with_pypdf(pdf_path: Path) -> str:
    """Extract text using PyPDF2 as fallback."""
    import PyPDF2
    
    try:
        with open(pdf_path, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            pages = []
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
            return "\n".join(pages)
    except Exception as e:
        log.error(f"PyPDF2 extraction failed: {e}")
        return ""
