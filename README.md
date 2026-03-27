# Adaptation Intelligence Platform

A climate knowledge intelligence platform for extracting, classifying, and querying climate adaptation data from corporate reports and external sources.

## What It Does

Three-pillar architecture:

| Pillar | Description |
|--------|-------------|
| **Unified Ingestion** | Adapter-based ingestion supporting corporate PDFs and Google CSE search. All sources share a common `Document` schema. |
| **Taxonomy Classification** | Two-stage LLM extraction: Stage A collects climate passages; Stage B classifies each into a seeded taxonomy aligned to CSRD / CSDDD / TCFD / IPCC AR6. |
| **Output Generation** | Newsletter engine and sector brief engine querying a validated Azure AI Search knowledge store. *(Phase 2)* |

## Phase Status

| Phase | Status | Description |
|-------|--------|-------------|
| **Phase 0** | Complete | Repo restructure, schemas, adapters, tests |
| **Phase 1** | Complete | `ingest.py`, `taxonomy.py`, `knowledge_store.py`, two-stage extraction |
| **Phase 2** | Complete | GCF API, OECD API adapters, Streamlit review UI, GitHub Actions |

## Repository Structure

```
adapters/
  base.py              # BaseAdapter ABC + exceptions
  corporate_pdf.py     # Wraps PyPDF2 text extraction
  google_cse.py        # Two-step: CSE search -> download -> extract
  gcf_api.py           # Phase 2 stub
  oecd_api.py          # Phase 2 stub
schemas/
  document.py          # Document dataclass, SOURCE_TYPES, DOCUMENT_TYPES
  passage.py           # ClassifiedPassage dataclass, controlled vocabularies
  validation.py        # ValidationStatus enum, TRUSTED_STATUSES, thresholds
prompts/
  collect_v1.txt       # Stage A prompt (collect all climate passages)
  classify_v1.txt      # Stage B prompt (classify each passage)
tests/
  test_schemas.py      # 40 tests — schemas and controlled vocabularies
  test_adapters.py     # 16 tests — CorporatePDFAdapter + GoogleCSEAdapter
  test_taxonomy.py     # 24 tests — TaxonomyLoader
  test_extractor.py    # 20 tests — Stage A/B, triage, build_classified_passage
_design/               # Read-only design specs and interfaces
config.py              # Single source for all env var reads
taxonomy.py            # TaxonomyLoader singleton — loads _design/taxonomy.yaml
knowledge_store.py     # Azure AI Search client (only file importing the SDK)
extractor.py           # Stage A (collect) + Stage B (classify) LLM logic
ingest.py              # Pipeline entry point + CLI
sources.yaml           # Active and disabled source definitions
```

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment variables

Copy `.env.example` to `.env` and fill in all values:

```bash
cp .env.example .env
```

Required variables:

| Variable | Description |
|----------|-------------|
| `AZURE_SEARCH_ENDPOINT` | Azure AI Search service endpoint |
| `AZURE_SEARCH_KEY` | Azure AI Search admin key |
| `AZURE_OPENAI_ENDPOINT` | Azure OpenAI service endpoint |
| `AZURE_OPENAI_KEY` | Azure OpenAI API key |
| `GOOGLE_CSE_API_KEY` | Google Custom Search API key (comma-separated for multiple) |
| `GOOGLE_CSE_ID` | Google Custom Search Engine ID |

Optional variables (have defaults):

| Variable | Default | Description |
|----------|---------|-------------|
| `EMBEDDING_DEPLOYMENT` | `text-embedding-3-large` | Azure OpenAI embedding deployment name |
| `GPT4O_DEPLOYMENT` | `gpt-4o` | Azure OpenAI GPT-4o deployment name |
| `GPT4O_MINI_DEPLOYMENT` | `gpt-4o-mini` | Azure OpenAI GPT-4o-mini deployment name |

### 3. Run tests

```bash
python -m pytest tests/ -v
```

All 56 tests should pass.

## Adapters

### CorporatePDFAdapter

Ingests a local PDF file and yields `Document` objects. Splits large documents at `200_000` characters per chunk.

```python
from adapters.corporate_pdf import CorporatePDFAdapter

adapter = CorporatePDFAdapter({
    "document_type": "corporate_report",
    "sector_hint": ["food_and_beverage"],
    "country": ["FR"],
})

async for doc in adapter.fetch("/path/to/report.pdf"):
    print(doc.raw_text[:200])
```

### GoogleCSEAdapter

Two-step adapter: searches Google CSE for matching documents, downloads each result, extracts text, and yields `Document` objects.

```python
from adapters.google_cse import GoogleCSEAdapter

adapter = GoogleCSEAdapter({
    "document_type": "corporate_report",
    "sector_hint": ["food_and_beverage"],
    "max_results": 10,
    "file_type": "pdf",
})

async for doc in adapter.fetch("Nestlé climate adaptation 2024"):
    print(doc.source_url, doc.raw_text[:200])
```

Downloads are saved to `tmp/cse_downloads/` (gitignored).

## Key Design Rules

- `config.py` is the only file that reads `os.environ`
- `knowledge_store.py` *(Phase 1)* will be the only file importing the Azure Search SDK
- Every function raises a named exception on failure — no silent swallowing
- No hardcoded credentials anywhere in the codebase
- Taxonomy is read-only at `_design/taxonomy.yaml` until `taxonomy.py` is implemented in Phase 1
