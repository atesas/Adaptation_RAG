# ADAPTATION INTELLIGENCE PLATFORM
## Project Brief — Governing Document
**Version 1.1 | March 2026 | Drop this file in repo root as `PROJECT_BRIEF.md`**

---

## 1. What We Are Building and Why

### The Problem
We have a working but limited RAG system that extracts climate risk data from
corporate PDFs using 24 fixed Q&A questions, producing 13 CSV tables.
Three structural problems:

1. **Disconnected pipelines** — search/download lives in a separate folder
   with no clean interface to the RAG system
2. **Redundant extraction** — 24 questions cause the same data to appear
   under multiple questions (hazards appear under Q4, Q7, Q8, Q9).
   Noise, not signal.
3. **Limited scope** — only corporate PDFs, no external sources,
   no output generation

### The Target
A **climate knowledge intelligence platform** with three pillars:

| Pillar | What it does |
|--------|-------------|
| **Unified Ingestion** | One entry point (`ingest.py`) accepting any source via a clean adapter pattern. Google CSE is the primary discovery mechanism for PDFs. A small number of structured APIs provide clean JSON data where PDFs would lose information. |
| **Taxonomy Classification** | Replace 24 Q&A with two-stage extraction: Stage A collects all climate passages; Stage B classifies each into a seeded taxonomy aligned to CSRD/CSDDD/EU Taxonomy vocabulary |
| **Output Generation** | Newsletter engine + sector brief engine querying a validated knowledge store |

---

## 2. Guiding Principles (Non-Negotiable)

These govern every implementation decision. Claude Code must not override them.

### 2.1 No Fallbacks, No Dead Code
Every function either works correctly or raises a named exception.
No `except: pass`, no `try/except` that swallows errors silently.
No commented-out code blocks. No `TODO` left in merged code.
If something is not implemented yet, it does not exist in the codebase —
it will be added in its phase.

### 2.2 One Entry Point Per Operation
- Documents enter the system only through `ingest.py`
- The knowledge store is accessed only through `knowledge_store.py`
- Taxonomy is loaded only through `taxonomy.py`
- No other file imports Azure SDK, Azure Search SDK, or OpenAI SDK directly

### 2.3 Everything Is Testable
Every function has a clear input type, output type, and a corresponding
test in `tests/`. No function takes `**kwargs` unless the argument space
is genuinely open-ended and documented. No global mutable state outside
of the singleton loaders (taxonomy, knowledge store client).

### 2.4 Validation Is Not Optional
Every classified passage enters the knowledge store with a
`validation_status`. Output engines query only `TRUSTED_STATUSES`.
The validation filter is in `knowledge_store.query_trusted()` — not in
the caller. Callers cannot bypass it.

### 2.5 Prompts Are Version-Controlled Files
Prompt text lives in `prompts/collect_v1.txt` and
`prompts/classify_v1.txt`. It is never hardcoded in Python.
When a prompt changes, a new version file is created (`v2`, `v3`) —
the old one is never modified. The active prompt version is set in
`config.py`, not scattered across files.

### 2.6 Schema Drives Everything
`schemas/` is the single source of truth for data shapes. Any field
that appears in Azure AI Search must be defined in `schemas/passage.py`
first. Any controlled vocabulary (source_type, document_type, iro_type,
etc.) is defined as a constant list in the schema file — not inline
in adapter code.

### 2.7 Adapters Are Minimal and Focused
An adapter does exactly one thing: fetch content from one source type
and yield `Document` objects. No adapter contains extraction logic,
classification logic, or Azure Search calls. If an adapter needs to
download a PDF after searching, that download is part of the adapter's
`fetch()` method — not a separate pipeline step.

---

## 3. Adapter Strategy

### Primary Discovery: Google CSE
Google CSE is the primary mechanism for finding and downloading PDFs.
It handles:
- Corporate sustainability reports, TCFD/CSRD disclosures
- Policy documents (NAPs, EU Adapt, regulatory guidance)
- Sector briefs and industry reports
- Academic PDFs not available through structured APIs
- News articles and press releases

The `google_cse.py` adapter is a **two-step adapter**:
search → download → extract text. It wraps the existing Google
search/download code and yields `Document` objects directly.

```
GoogleCSEAdapter.fetch(query)
  Step 1: CSE API call → list of result URLs
  Step 2: download each PDF/HTML to tmp/ directory
  Step 3: extract text (same logic as CorporatePDFAdapter)
  → yield Document objects
```

### Direct Structured APIs (Phase 2 Only)
Three sources expose structured data that would be degraded by going
through PDFs. Built in Phase 2, disabled until then.

| Adapter | Source | Why direct, not CSE |
|---------|--------|-------------------|
| `gcf_api.py` | GCF Project Browser | Public API returns structured JSON: funding amounts, result areas, countries, sectors. Maps directly to taxonomy `finance` node. |
| `oecd_api.py` | OECD CRS | Structured finance flows by instrument, recipient, sector code. Pre-structured for Report 3 (Finance Flow Tracker). |
| `cdp_export.py` | CDP Bulk Export | Pre-answered climate questions already mapped to fields — if CDP access is available. Add Phase 2. |

### What Google CSE Covers (No Separate Adapters Needed)
EU Adapt, IPCC reports, FAO/CGIAR, academic papers, UNFCCC guidance,
news and events — all reachable as PDFs or HTML via CSE.
If academic volume becomes insufficient, add OpenAlex adapter in
Phase 2. Not before.

---

## 4. Repository Structure (Target State)

```
adaptation-rag/
│
├── PROJECT_BRIEF.md              ← this file (governing document)
│
├── _design/                      ← architecture specs (READ-ONLY)
│   ├── README_FOR_CLAUDE_CODE.md
│   ├── AIP_Master_Plan.docx
│   ├── taxonomy.yaml
│   ├── sources.yaml
│   ├── schemas/
│   ├── prompts/
│   ├── knowledge_store_interface.py
│   ├── ingest_interface.py
│   └── taxonomy_interface.py
│
├── schemas/                      ← implementation (from _design/schemas/)
│   ├── __init__.py
│   ├── document.py
│   ├── passage.py
│   └── validation.py
│
├── adapters/
│   ├── __init__.py
│   ├── base.py                   ← Phase 0
│   ├── corporate_pdf.py          ← Phase 0: direct PDF ingestion
│   ├── google_cse.py             ← Phase 0: search + download + extract
│   ├── gcf_api.py                ← Phase 2: disabled until then
│   └── oecd_api.py               ← Phase 2: disabled until then
│
├── prompts/
│   ├── collect_v1.txt            ← Stage A extraction
│   └── classify_v1.txt           ← Stage B classification
│
├── tests/
│   ├── __init__.py
│   ├── test_schemas.py
│   ├── test_taxonomy.py
│   ├── test_extractor.py
│   ├── test_knowledge_store.py
│   ├── test_adapters.py
│   └── fixtures/
│       ├── sample_corporate.pdf
│       └── sample_passages.json
│
├── outputs/                      ← Phase 3 only — do not create until then
├── validation/                   ← Phase 2 only — do not create until then
│
├── taxonomy.py
├── knowledge_store.py
├── extractor.py
├── ingest.py
├── config.py
├── sources.yaml                  ← source registry (root copy)
│
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

### What Gets Deleted in the Restructure

| File / folder | Reason |
|--------------|--------|
| `questions_loader.py`, `question_batcher.py` | Replaced by `taxonomy.py` |
| Old `extractor.py` Q&A logic | Replaced by Stage A/B in new `extractor.py` |
| Old `transformer.py` CSV writer | Replaced by `knowledge_store.py` |
| CSV output files in `output/` or `results/` | Archived or deleted |
| Google search code not in `adapters/` | Logic moves to `adapters/google_cse.py` |
| Notebook files (`.ipynb`) | Move useful logic to modules first, then delete |
| Hardcoded credentials anywhere | Moved to `.env`, read only through `config.py` |
| `__pycache__/`, `*.pyc` | Added to `.gitignore` |
| Duplicate utility functions | One canonical version per utility |

Nothing is deleted without being listed explicitly and confirmed first.

---

## 5. Implementation Phases

### Phase 0 — Restructure (Before Any New Features)
**Goal:** Clean repo. Existing functionality preserved but properly
organised. `pytest tests/` passes.

- [ ] Create target folder structure
- [ ] Implement `schemas/` from `_design/schemas/`
- [ ] Implement `config.py` (all env vars, constants, paths)
- [ ] Implement `adapters/base.py` (BaseAdapter ABC)
- [ ] Implement `adapters/corporate_pdf.py` (wraps existing PDF logic)
- [ ] Implement `adapters/google_cse.py` (wraps existing search/download)
- [ ] Copy `_design/prompts/` → `prompts/`
- [ ] Write `requirements.txt` and `.env.example`
- [ ] Write `.gitignore`
- [ ] Write `tests/test_schemas.py` and `tests/test_adapters.py`
- [ ] Delete all replaced files (after explicit confirmation)

**Exit criteria:** `pytest tests/` passes. A corporate PDF returns a valid
`Document` through `adapters/corporate_pdf.py`. Google CSE adapter returns
`Document` objects for a test query against a mocked CSE response.

---

### Phase 1 — Taxonomy + Classification + Knowledge Store
**Goal:** Stage A/B extraction operational. Passages in Azure AI Search.

- [ ] Implement `taxonomy.py` (reads `_design/taxonomy.yaml`)
- [ ] Implement `knowledge_store.py` (three Azure indexes)
- [ ] Implement `extractor.py` (Stage A + Stage B)
- [ ] Implement `ingest.py` (full pipeline orchestration)
- [ ] Write `tests/test_taxonomy.py`, `tests/test_extractor.py`,
       `tests/test_knowledge_store.py`
- [ ] Run against 3 existing corporate PDFs
- [ ] Manual review of first 50 passages to calibrate auto-approve threshold

**Exit criteria:** `python ingest.py --source corporate_pdf --path
documents/sample.pdf` runs end-to-end. Passages appear in Azure AI Search
with correct `validation_status`. `query_trusted()` returns results.
Running the same file twice produces no duplicate passages.

---

### Phase 2 — Google CSE Pipeline + Structured APIs + Validation UI
**Goal:** Multi-source ingestion. Human review interface operational.

- [ ] `adapters/google_cse.py` fully tested end-to-end (live CSE calls)
- [ ] `adapters/gcf_api.py` (enabled in `sources.yaml`)
- [ ] `adapters/oecd_api.py` (enabled in `sources.yaml`)
- [ ] `validation/app.py` Streamlit review interface
- [ ] GitHub Actions cron for scheduled ingestion
- [ ] Quality metrics visible

**Exit criteria:** `python ingest.py --source google_cse --query
"Danone CSRD physical risk 2024"` downloads, extracts, classifies,
and indexes results. Reviewer can approve/edit/reject in Streamlit UI.

---

### Phase 3 — Output Generation
**Goal:** Client-ready outputs from the validated knowledge store.

- [ ] `outputs/newsletter.py`
- [ ] `outputs/sector_brief.py`
- [ ] `outputs/company_assessment.py` (D1–D8 rubric)
- [ ] Citation generation (every claim traced to passage + source)

**Exit criteria:** `python outputs/sector_brief.py --sector beverages`
produces a structured brief with citations, drawn only from approved
passages, covering D1–D8 dimensions aligned to ESRS E1.

---

## 6. Configuration (`config.py`)

The only file that reads `os.environ`. All other files import from `config`.

```python
import os
from pathlib import Path

# ── Azure AI Search ───────────────────────────────────────────────────
AZURE_SEARCH_ENDPOINT  = os.environ["AZURE_SEARCH_ENDPOINT"]
AZURE_SEARCH_KEY       = os.environ["AZURE_SEARCH_KEY"]

# ── Azure OpenAI ──────────────────────────────────────────────────────
AZURE_OPENAI_ENDPOINT  = os.environ["AZURE_OPENAI_ENDPOINT"]
AZURE_OPENAI_KEY       = os.environ["AZURE_OPENAI_KEY"]

# ── Model deployments ─────────────────────────────────────────────────
EMBEDDING_DEPLOYMENT   = os.environ.get("EMBEDDING_DEPLOYMENT",
                                        "text-embedding-3-large")
GPT4O_DEPLOYMENT       = os.environ.get("GPT4O_DEPLOYMENT", "gpt-4o")
GPT4O_MINI_DEPLOYMENT  = os.environ.get("GPT4O_MINI_DEPLOYMENT",
                                        "gpt-4o-mini")

# Model allocation by task
STAGE_A_MODEL          = GPT4O_MINI_DEPLOYMENT  # extraction — cost-sensitive
STAGE_B_MODEL          = GPT4O_MINI_DEPLOYMENT  # classification — cost-sensitive
OUTPUT_MODEL           = GPT4O_DEPLOYMENT        # synthesis — quality-sensitive

# ── Google CSE (Phase 0) ──────────────────────────────────────────────
GOOGLE_CSE_API_KEY     = os.environ["GOOGLE_CSE_API_KEY"]
GOOGLE_CSE_ID          = os.environ["GOOGLE_CSE_ID"]

# ── Structured APIs (Phase 2) ─────────────────────────────────────────
GCF_API_BASE    = "https://www.greenclimate.fund/projects/api"
OECD_API_BASE   = "https://stats.oecd.org/SDMX-JSON/data/CRS"

# ── Active prompt versions ────────────────────────────────────────────
COLLECT_PROMPT_VERSION  = "v1"
CLASSIFY_PROMPT_VERSION = "v1"

# ── Paths ─────────────────────────────────────────────────────────────
TAXONOMY_PATH  = Path("_design/taxonomy.yaml")
SOURCES_PATH   = Path("sources.yaml")
PROMPTS_DIR    = Path("prompts/")
TMP_DIR        = Path("tmp/")   # temp downloads — gitignored

# ── Validation thresholds ─────────────────────────────────────────────
AUTO_APPROVE_THRESHOLD = 0.85
AUTO_REJECT_THRESHOLD  = 0.40

# ── Document limits ───────────────────────────────────────────────────
MAX_DOCUMENT_CHARS = 200_000

# ── Azure AI Search index names ───────────────────────────────────────
INDEX_PASSAGES      = "adaptation-passages"
INDEX_DOCUMENTS     = "adaptation-documents"
INDEX_VALIDATION    = "adaptation-validation-log"
```

---

## 7. Testing Standards

- Framework: `pytest`
- No test touches the live Azure Search index — use
  `adaptation-passages-test` or mock the client
- No test requires a live API call unless marked `@pytest.mark.integration`
- Unit tests complete in under 30 seconds
- Fixtures in `tests/fixtures/`

| Module | Minimum test coverage |
|--------|-----------------------|
| `schemas/` | Controlled vocab values are strings; dataclass instantiation works |
| `taxonomy.py` | `get_taxonomy_excerpt_for_hint()` non-empty for all hints; invalid values caught; `is_seed_category()` correct |
| `adapters/corporate_pdf.py` | Returns `Document` with all fields; `content_hash` is deterministic |
| `adapters/google_cse.py` | Mocked CSE response → correct `Document` objects; two-step order enforced |
| `extractor.py` | Stage A returns valid JSON for fixture; Stage B returns all fields; invalid taxonomy caught; retry on parse failure |
| `knowledge_store.py` | `query_trusted()` never returns RAW/REJECTED; `upsert` deduplicates on hash; corrections logged |
| `ingest.py` | Full pipeline on fixture PDF produces passages; duplicate document skipped |

---

## 8. Key Reference Files

| Question | File |
|----------|------|
| What does each taxonomy node mean? | `_design/taxonomy.yaml` |
| What should function X do exactly? | `_design/knowledge_store_interface.py` or `_design/ingest_interface.py` |
| What fields does a passage have? | `_design/schemas/passage.py` |
| What do the prompts say? | `_design/prompts/collect_v1.txt`, `_design/prompts/classify_v1.txt` |
| What sources are configured? | `sources.yaml` (root) |
| Full architecture context | `_design/AIP_Master_Plan.docx` |
