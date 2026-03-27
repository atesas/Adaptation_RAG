# =============================================================================
# schemas/document.py
# Output of every adapter — common schema before any LLM processing
# =============================================================================
# RULES FOR CLAUDE CODE:
#   - Every adapter must produce a Document object
#   - No adapter writes to Azure AI Search directly — all go through ingest.py
#   - raw_text max: 200,000 characters. Larger docs split by section BEFORE
#     creating the Document object, each section becomes its own Document.
#   - content_hash is SHA256 of raw_text. If hash already exists in the
#     adaptation-documents index → skip ingestion (deduplication).
#   - publication_date is the SOURCE document date, not ingestion date.
#   - language: detect automatically (langdetect library), never infer from URL.
# =============================================================================

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Document:

    # ── Identity ─────────────────────────────────────────────────────────────
    doc_id: str                        # UUID4, generated at ingestion
    content_hash: str                  # SHA256(raw_text) — deduplication key

    # ── Content ───────────────────────────────────────────────────────────────
    raw_text: str                      # Full extracted text, minimally cleaned
                                       # No summarisation — Stage A reads this
                                       # Max 200,000 chars (see rules above)
    title: Optional[str]               # Document title if extractable, else None
    language: str                      # ISO 639-1: "en", "fr", "es"

    # ── Source provenance ─────────────────────────────────────────────────────
    source_url: str                    # Original URL or absolute file path
    source_type: str                   # Must be one of SOURCE_TYPES below
    adapter: str                       # Adapter class name, e.g. "CorporatePDFAdapter"

    # ── Publication metadata ──────────────────────────────────────────────────
    publication_date: Optional[datetime]   # Date of the source document
    ingestion_date: datetime               # datetime.utcnow() at ingestion
    reporting_year: Optional[int]          # Financial/reporting year (corporate docs)

    # ── Pre-classification metadata ───────────────────────────────────────────
    # Set by the adapter from known context — NOT by any LLM call
    document_type: str                 # Must be one of DOCUMENT_TYPES below
    company_name: Optional[str]        # Corporate docs only, else None
    company_id: Optional[str]          # Internal ID for cross-document linking
    csrd_wave: Optional[int]           # 1, 2, or 3. Corporate docs only, else None
    country: list[str]                 # ISO 3166-1 alpha-2 codes
                                       # Countries the document RELATES TO
                                       # (not necessarily company HQ country)
    sector_hint: list[str]             # From adapter context, not LLM
                                       # Must be values from taxonomy sector_tags

    # ── Processing state ──────────────────────────────────────────────────────
    extraction_status: str             # "pending" | "extracted" | "failed"
    extraction_error: Optional[str]    # Error message if extraction_status="failed"


# =============================================================================
# CONTROLLED VOCABULARIES
# These are the only valid values for source_type and document_type.
# Adding a new value requires updating both this file AND sources.yaml.
# =============================================================================

SOURCE_TYPES: list[str] = [
    "corporate_pdf",        # Annual/sustainability/TCFD/CSRD reports
    "gcf_api",              # GCF Project Browser API
    "oecd_api",             # OECD CRS (Common Reporting Standard)
    "world_bank_api",       # World Bank Projects API
    "unfccc_api",           # UNFCCC NDC Registry bulk download
    "gef_api",              # GEF Project Portal
    "semantic_scholar",     # Semantic Scholar API (academic)
    "openalexAPI",          # OpenAlex API (academic)
    "playwright_scrape",    # Targeted site crawl via Playwright
    "google_cse",           # Google CSE result + PDF download
    "exa_search",           # Exa.ai neural similarity result
    "rss_news",             # RSS feed / Google News API
    "cdp_export",           # CDP bulk data download
]

DOCUMENT_TYPES: list[str] = [
    "corporate_report",     # Annual report, sustainability report, TCFD/CSRD disclosure
    "policy",               # Government policy, NAP, NDC, regulatory guidance
    "academic",             # Peer-reviewed paper, IPCC report chapter
    "project_db",           # GCF/GEF/World Bank project entry (structured)
    "news",                 # News article, press release
    "regulatory",           # EU regulation text, ESMA filing, ESEF disclosure
    "guidance",             # Industry guidance, sector framework document
]
