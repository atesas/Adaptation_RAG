# Adaptation Intelligence Platform

A climate knowledge intelligence platform that finds, downloads, extracts, classifies, and queries climate adaptation data from corporate reports and policy documents — aligned to ESRS E1, CSRD, TCFD, and IPCC AR6.

## What It Does

| Pillar | What happens |
|--------|-------------|
| **Ingestion** | PDFs and web pages are downloaded, text is extracted, and documents are saved to Azure AI Search |
| **Classification** | Two-stage LLM pipeline: Stage A pulls out every climate passage; Stage B classifies each into a seeded taxonomy |
| **Output Generation** | Sector newsletters, D1–D8 sector briefs, and D1–D8 company assessments — all with source citations |

---

## Before You Start — Set Up Your `.env`

Everything the platform needs comes from environment variables. You never hardcode credentials anywhere.

### Step 1 — Copy the example file

```bash
cp .env.example .env
```

### Step 2 — Fill in each value

Open `.env` in any text editor and fill in the values below. Here is where to find each one.

```env
# ── Azure AI Search ────────────────────────────────────────────────────────────
# Where to find: Azure portal → your Search resource → Overview tab
AZURE_SEARCH_ENDPOINT=https://your-search-name.search.windows.net
# Where to find: Azure portal → your Search resource → Settings → Keys → Admin key
AZURE_SEARCH_KEY=abc123...

# ── Azure OpenAI ──────────────────────────────────────────────────────────────
# Where to find: Azure portal → your OpenAI resource → Overview tab → Endpoint
AZURE_OPENAI_ENDPOINT=https://your-openai-name.openai.azure.com/
# Where to find: Azure portal → your OpenAI resource → Keys and Endpoint → KEY 1
AZURE_OPENAI_KEY=abc123...

# ── Model deployment names ────────────────────────────────────────────────────
# These must match the deployment names you created in Azure OpenAI Studio.
# Leave as-is if you used the default names.
EMBEDDING_DEPLOYMENT=text-embedding-3-large
GPT4O_DEPLOYMENT=gpt-4o
GPT4O_MINI_DEPLOYMENT=gpt-4o-mini

# ── Google Custom Search Engine ───────────────────────────────────────────────
# Where to find API key: console.cloud.google.com → APIs & Services → Credentials
# You need the "Custom Search API" enabled on your project.
GOOGLE_CSE_API_KEY=AIza...
# Where to find CSE ID: programmablesearchengine.google.com → your engine → cx value
GOOGLE_CSE_ID=abc123:xyz
```

You can add multiple Google API keys separated by commas — the platform rotates through them automatically when quota is hit:

```env
GOOGLE_CSE_API_KEY=AIzaKey1,AIzaKey2,AIzaKey3
```

### Step 3 — Install dependencies

```bash
pip install -r requirements.txt
```

---

## Verify Everything Works

### Test without Azure (no credentials needed)

The test suite mocks all Azure and OpenAI calls. Run this first to confirm the codebase is intact:

```bash
python -m pytest tests/ -v
```

You should see all tests pass (0 failures). If any tests fail, check the error — it's almost always a missing package.

### Test your CLI works

```bash
python ingest.py --help
```

You should see the full help text with all flags. No credentials needed for this.

### Test your Azure connection

Once your `.env` is filled in, run a quick connection check:

```bash
python - <<'EOF'
import asyncio, config
from knowledge_store import KnowledgeStore
from openai import AsyncAzureOpenAI

client = AsyncAzureOpenAI(
    azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
    api_key=config.AZURE_OPENAI_KEY,
    api_version="2024-08-01-preview",
)
store = KnowledgeStore(
    search_endpoint=config.AZURE_SEARCH_ENDPOINT,
    search_key=config.AZURE_SEARCH_KEY,
    openai_client=client,
)
print("Connected. Indexes will be created on first upsert.")
EOF
```

No error = credentials work.

---

## How to Start: Step-by-Step Walkthrough

### Option A — You have a PDF already

If you have a corporate report PDF on disk, this is the fastest path.

```bash
# Process a single PDF end-to-end
python ingest.py --source corporate_pdf_direct --path /path/to/danone_2024.pdf
```

What happens:
1. PDF text is extracted
2. Stage A: GPT-4o-mini reads the text and pulls out every climate-related passage
3. Stage B: GPT-4o-mini classifies each passage into the taxonomy (physical hazard, adaptation response, governance, etc.)
4. High-confidence passages are auto-approved and saved to Azure AI Search
5. Lower-confidence passages are saved as `pending_review` for human review

Output example:
```
{'documents_processed': 1, 'documents_skipped_duplicate': 0,
 'passages_extracted': 47, 'passages_auto_approved': 31,
 'passages_pending_review': 12, 'passages_auto_rejected': 4, 'errors': []}
```

### Option B — Search the web first, review, then process (recommended)

This is the two-step workflow. You search first without spending any LLM tokens, review what was found, then decide what to process.

**Step 1 — Search and download** (no LLM, no Azure)

```bash
python ingest.py --source google_cse_corporate \
                 --path "Danone CSRD climate report 2024 filetype:pdf" \
                 --download-only
```

What you see:
```
Searching: 'Danone CSRD climate report 2024 filetype:pdf'
Source:    google_cse_corporate

  ✓ Danone Universal Registration Document 2024
    URL:  https://www.danone.com/content/dam/danone-corp/...pdf
    Size: 312,450 chars | lang: fr
    File: tmp/staged/Danone_Universal_Registra_a1b2c3d4.txt

  ✓ Danone Climate Transition Plan 2024
    URL:  https://www.danone.com/content/dam/...climate.pdf
    Size: 89,220 chars | lang: en
    File: tmp/staged/Danone_Climate_Transition__e5f6g7h8.txt

2 document(s) staged in tmp/staged/
Manifest:  tmp/staged/manifest.json
```

**Step 2 — Review what was downloaded**

Open the `.txt` files in `tmp/staged/` to check they contain useful content. The manifest JSON lists all metadata.

**Step 3 — Process one file**

```bash
python ingest.py --source corporate_pdf_direct \
                 --path tmp/staged/Danone_Climate_Transition__e5f6g7h8.txt
```

**Or process all staged files at once**

```bash
python ingest.py --source corporate_pdf_direct --path tmp/staged/ --all-staged
```

---

## All CLI Commands

### `ingest.py` — Add documents to the knowledge store

```bash
# Process a local PDF
python ingest.py --source corporate_pdf_direct --path documents/report.pdf

# Process a PDF and mark all passages as high-priority (P1_CLIENT)
python ingest.py --source corporate_pdf_direct --path documents/report.pdf --client-facing

# Reprocess even if already ingested (skips duplicate check)
python ingest.py --source corporate_pdf_direct --path documents/report.pdf --force

# Control how many LLM calls run in parallel (default 5; lower to 2-3 on S0 tier)
python ingest.py --source corporate_pdf_direct --path documents/report.pdf --concurrency 3

# Search → download only (review before processing)
python ingest.py --source google_cse_corporate \
                 --path "Nestle water stress adaptation 2024" \
                 --download-only

# Search policy documents
python ingest.py --source google_cse_policy \
                 --path "EU climate adaptation food sector guidance" \
                 --download-only

# Process all staged files
python ingest.py --source corporate_pdf_direct --path tmp/staged/ --all-staged

# Delete and recreate all Azure AI Search indexes (wipes all data)
# Use after a schema change, or to start fresh
python ingest.py --reset-indexes

# Re-run Stage B classification on all auto_rejected passages
# Use after updating taxonomy.yaml (see Taxonomy Evolution section)
python ingest.py --reclassify
```

Available source keys (from `sources.yaml`):

| Source key | What it does |
|------------|-------------|
| `corporate_pdf_direct` | Process a local PDF or `.txt` file you already have |
| `google_cse_corporate` | Search for corporate sustainability/CSRD/TCFD reports |
| `google_cse_policy` | Search for policy docs, NAPs, FAO/IPCC/EU guidance |
| `google_cse_targeted` | Search specific high-quality domains (EEA, UNFCCC, FAO) |
| `gcf_projects` | GCF Portfolio API — all approved projects with funding detail (Phase 2) |
| `oecd_crs` | OECD CRS climate finance flows — agriculture and water sectors (Phase 2) |

---

### `gcf_projects` — GCF Approved Projects (Phase 2)

The GCF adapter fetches all approved projects from the [GCF Portfolio API](https://api.gcfund.org/v1/projects) and converts each one into a `Document`. No API key required.

#### How it works (three steps)

```
Step 1 — GET /v1/projects
         Fetches the complete approved project list in one request.
         Each record already includes Countries, Entities, Disbursements,
         Funding, and ResultAreas.
         ~300+ projects returned as JSON.

Step 2 — Filter (in memory)
         Your filters from sources.yaml are applied here.
         Only matching projects proceed to Step 3.
         Unmatched projects are skipped entirely — no detail requests made for them.

Step 3 — GET /v1/projects/{ProjectsID}   (fetch_project_details: true)
         Fetches the authoritative per-project record for each match.
         Rate-limited to 20 requests/minute by default.
         Set fetch_project_details: false to skip this and use the list data only.
```

#### Enabling the adapter

In `sources.yaml`, set `enabled: true` for `gcf_projects`:

```yaml
gcf_projects:
  enabled: true
```

Then run:

```bash
python ingest.py --source gcf_projects
```

#### Filtering projects before fetching details

Configure filters in `sources.yaml` under `gcf_projects.filters`. All filters are optional and AND-ed together. Values within a filter are OR-ed.

```yaml
gcf_projects:
  fetch_project_details: true
  filters:
    # Only adaptation and cross-cutting projects
    theme:
      - Adaptation
      - Cross-cutting

    # Only active projects
    status:
      - Under Implementation

    # Only projects with non-zero allocation to these result areas
    result_areas:
      - Livelihoods of people and communities
      - Health, food, and water security
      - Ecosystems and ecosystem services

    # Only specific countries (ISO3 codes)
    countries_iso3:
      - PER
      - MWI
      - BGD
      - ETH

    # Only Small/Medium/Large (exclude Micro)
    size:
      - Small
      - Medium
      - Large

    # Only projects with at least $5M GCF funding
    min_gcf_funding: 5000000
```

Available filter keys:

| Filter key | Matches against | Values |
|------------|----------------|--------|
| `theme` | `Theme` field | `Adaptation`, `Mitigation`, `Cross-cutting` |
| `status` | `Status` field | `Under Implementation`, `Completed`, `Under Development` |
| `result_areas` | `ResultAreas[].Area` (non-zero only) | See GCF result area taxonomy |
| `countries_iso3` | `Countries[].ISO3` | Any ISO 3166-1 alpha-3 code |
| `size` | `Size` field | `Micro`, `Small`, `Medium`, `Large` |
| `sector` | `Sector` field | `Public`, `Private`, `Mixed` |
| `min_gcf_funding` | `TotalGCFFunding` (USD) | Any integer |

#### Fetch modes

| Config | Behaviour | When to use |
|--------|-----------|-------------|
| `fetch_project_details: true` | Step 1 → filter → Step 3 (per-project detail) | Default. Most complete data. Detail requests only for filtered matches. |
| `fetch_project_details: false` | Step 1 → filter → done | Faster. Use when you only need the fields already in the list payload. |

#### Exporting to CSV for exploration

Before ingesting into Azure AI Search, you can export the raw GCF data to CSV files for manual exploration. No Azure or OpenAI credentials needed.

```bash
# Dump everything — list metadata only (one fast request)
python gcf_export.py --no-details

# All projects with full per-project detail (300+ HTTP requests, rate-limited)
python gcf_export.py

# Filter first, then fetch detail only for matching projects
python gcf_export.py \
    --theme Adaptation Cross-cutting \
    --status "Under Implementation" \
    --result-areas "Livelihoods of people and communities" \
                   "Health, food, and water security" \
    --min-funding 1000000 \
    --output exports/gcf_adaptation/

# Just specific countries, fast
python gcf_export.py --countries-iso3 PER MWI BGD ETH --no-details

# Also save the raw JSON alongside the CSVs
python gcf_export.py --save-json --output exports/gcf_full/
```

Output files (all in `--output` directory, default `tmp/gcf_export/`):

| File | Contents | Join key |
|------|----------|----------|
| `gcf_projects.csv` | One row per project — all scalar fields + nested arrays summarised | — |
| `gcf_result_areas.csv` | One row per project × result area (all areas incl. zero-allocation) | `ProjectsID` |
| `gcf_countries.csv` | One row per project × country with financing breakdown | `ProjectsID` |
| `gcf_entities.csv` | One row per project × implementing entity | `ProjectsID` |
| `gcf_disbursements.csv` | One row per disbursement tranche | `ProjectsID` |
| `gcf_funding.csv` | One row per funding instrument / source | `ProjectsID` |
| `gcf_raw.json` | Full raw API response (with `--save-json`) | — |

The filter flags (`--theme`, `--status`, `--result-areas`, `--countries-iso3`, `--size`, `--sector`, `--min-funding`) mirror the `filters:` keys in `sources.yaml` exactly, so the same filter you validate in the CSV export can be copied directly into your ingestion config.

---

#### What ends up in the Document

Each project becomes one `Document` with `source_type: gcf_api` and `document_type: project_db`. The `raw_text` field contains structured prose that Stage A reads for passage extraction:

```
GCF Approved Project: Building the Resilience of Wetlands in the Province of Datem del Marañón, Peru
Reference: FP001 | Board Meeting: B.11
Status: Under Implementation
Theme: Cross-cutting | Sector: Public
Size: Micro | Risk Category: Category C
Approval Date: 2015-11-05 | Implementation Start: 2016-12-15 | ...
Total GCF Funding: USD 6,240,000 | Co-Financing: USD 2,870,000 | Total: USD 9,110,000
Direct Beneficiaries: 20,413 | Indirect Beneficiaries: 0 | Lifetime CO2 (tCO2eq): 2,630,000
Countries: Peru (PER) – Latin America and the Caribbean | GCF: USD 6,240,000
Implementing Entities: Peruvian Trust Fund for National Parks and Protected Areas (Profonanpe) – Access: Direct / Type: National
Result Areas: Forest and land use (Mitigation): 80.00%; Livelihoods of people and communities (Adaptation): 20.00%
Funding Instruments: GCF – Grants: USD 6,240,000; Co-Financing – Grants: USD 2,870,000
Disbursements: 5 tranches, total USD 6,240,000 disbursed (latest: 2022-12-15)
```

---

### `validation/app.py` — Human review UI

After ingestion, passages with confidence between 0.40 and 0.85 are marked `pending_review`. Review them here:

```bash
streamlit run validation/app.py
```

Opens at `http://localhost:8501`. You can filter by priority level and either approve, edit, reject, or flag each passage.

---

### `outputs/newsletter.py` — Sector newsletter

Generates a narrative newsletter summarising recent climate adaptation intelligence for a sector.

```bash
# Basic newsletter for food and beverage
python outputs/newsletter.py --sector food_and_beverage

# With options
python outputs/newsletter.py \
    --sector food_agriculture \
    --top-k 40 \
    --days-back 30 \
    --output newsletter_march2026.md
```

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--sector` | required | Sector tag to filter passages |
| `--top-k` | 30 | Max passages to use |
| `--days-back` | 30 | Only use passages ingested in last N days |
| `--output` | stdout | Write to file instead of printing |

---

### `outputs/sector_brief.py` — D1–D8 sector brief

A structured 8-dimension brief covering all aspects of climate adaptation for a sector.

```bash
# Basic brief
python outputs/sector_brief.py --sector food_and_beverage

# With options
python outputs/sector_brief.py \
    --sector food_agriculture \
    --time-horizon 2030 \
    --top-k 60 \
    --output sector_brief_food_2026.md
```

The 8 dimensions (D1–D8):

| Dimension | What it covers |
|-----------|---------------|
| D1 | Physical hazard identification — which climate hazards are material |
| D2 | Risk quantification — financial magnitude and scenario analysis |
| D3 | Adaptation responses — actions and measures being implemented |
| D4 | Governance — board oversight and climate risk management |
| D5 | Finance — investments, green bonds, insurance, adaptation spending |
| D6 | Supply chain — upstream/downstream climate exposure |
| D7 | Scenarios — RCP/SSP/NGFS pathways and time horizons |
| D8 | Monitoring — KPIs, targets, and CSRD/ESRS reporting |

---

### `outputs/company_assessment.py` — D1–D8 company assessment

Same 8-dimension framework but focused on a single company, scored out of 24.

```bash
# Assess a company
python outputs/company_assessment.py --company "Danone"

# With year filter and file output
python outputs/company_assessment.py \
    --company "Nestle" \
    --year 2024 \
    --top-k 50 \
    --output nestle_assessment_2024.md
```

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--company` | required | Company name |
| `--company-id` | slugified name | Internal ID if different from name |
| `--year` | all available | Focus on a specific reporting year |
| `--top-k` | 50 | Max passages |
| `--output` | stdout | Write to file |

Sample output header:
```
**Company:** Danone | **Year:** 2024 | **Score:** 16/24

## D1 — Physical Hazard Identification (Score: 3/3)
Danone discloses water stress as a primary physical hazard across...
[P:a1b2c3d4]

## Sources
[P:a1b2c3d4] Danone Universal Registration Document 2024, p.87
  URL: https://www.danone.com/...
```

---

## Repository Structure

```
adapters/
  base.py              # BaseAdapter ABC + AdapterAuthError, AdapterFetchError, AdapterParseError
  corporate_pdf.py     # Local PDF → Document objects (PyPDF2)
  google_cse.py        # Search → download → extract → Document objects
  gcf_api.py           # GCF Portfolio API: list → filter → per-project detail (Phase 2)
  oecd_api.py          # OECD CRS finance flows API (Phase 2)
schemas/
  document.py          # Document dataclass, SOURCE_TYPES, DOCUMENT_TYPES
  passage.py           # ClassifiedPassage dataclass, controlled vocabularies
  validation.py        # ValidationStatus enum, TRUSTED_STATUSES
outputs/
  citations.py         # Citation index + [P:id] appendix rendering
  newsletter.py        # Sector newsletter generator
  sector_brief.py      # D1–D8 sector brief generator
  company_assessment.py # D1–D8 company scoring
validation/
  app.py               # Streamlit human review UI
prompts/
  collect_v1.txt       # Stage A prompt — extract all climate passages
  classify_v1.txt      # Stage B prompt — classify each passage into taxonomy
  newsletter_v1.txt    # Newsletter generation prompt
  sector_brief_v1.txt  # Sector brief generation prompt
  company_assessment_v1.txt # Company assessment prompt
tests/
  conftest.py          # Stub env vars for test collection
  test_schemas.py      # 40 tests — schemas and controlled vocabularies
  test_adapters.py     # 38 tests — CorporatePDFAdapter, GoogleCSEAdapter, GCFAPIAdapter
  test_taxonomy.py     # 24 tests — TaxonomyLoader
  test_extractor.py    # 20 tests — Stage A/B, triage, build_classified_passage
  test_knowledge_store.py # 23 tests — KnowledgeStore (mocked Azure)
  test_outputs.py      # 31 tests — citations, newsletter, brief, assessment
_design/               # Read-only design specs — do not modify
  taxonomy.yaml        # 11-node climate adaptation taxonomy
  PROJECT_BRIEF.md     # Full specification
gcf_export.py          # Export GCF project data to CSV for exploration (no credentials needed)
config.py              # Single source for all env var reads
taxonomy.py            # TaxonomyLoader singleton
knowledge_store.py     # Azure AI Search client (only file importing the SDK)
extractor.py           # Stage A + Stage B LLM extraction logic
ingest.py              # Pipeline entry point + CLI
sources.yaml           # Source registry (what to ingest and how)
```

---

## How the Pipeline Works

```
PDF / URL / query
       │
       ▼
   Adapter.fetch()          ← CorporatePDFAdapter or GoogleCSEAdapter
       │                      extracts raw text, yields Document objects
       ▼
   normalize()              ← NFKC normalisation, dedup hash, language detect
       │
       ▼
   store.deduplicate()      ← skip if content_hash already in Azure
       │
       ▼
   run_stage_a()            ← GPT-4o-mini reads document, returns list of
       │                      climate passages (text + topic_hint)
       ▼
   run_stage_b()            ← GPT-4o-mini classifies each passage:
       │                      taxonomy_node, confidence, hazard_type, etc.
       ▼
   triage()                 ← auto_approved / pending_review / auto_rejected
       │                      based on confidence + source_type + seed_category
       ▼
   store.upsert_passage()   ← saves to Azure AI Search with HNSW vector index
```

Auto-approval requires **all three** conditions:
- Confidence ≥ 0.85
- `seed_category = true` (taxonomy node marked as seeded)
- Source type is a structured API (`gcf_api`, `oecd_api`, `world_bank_api`, `unfccc_api`, `gef_api`)

**Corporate PDFs and Google CSE documents always go to `pending_review`** (not auto-approved), because free-text documents have higher hallucination risk and require human verification.

---

## Taxonomy Evolution Loop

The taxonomy in `_design/taxonomy.yaml` has two layers:
- **Seed layer**: ~100 nodes pre-defined from ESRS E1, TCFD, IPCC AR6, CSRD. Stable. Every node has a `seed_source` field.
- **Extension layer**: data-driven additions approved by a human reviewer after observing what the model finds in practice.

The evolution loop has three phases:

### Phase 1 — Emerge (automatic)
During every ingest run, when Stage B classifies a passage into a category that doesn't exist in the taxonomy, the pipeline automatically logs it to `candidate_extensions.jsonl`:

```jsonl
{"value": "acute_water_shortage", "hint": "hazard", "source_doc_id": "unilever_2025_abc", "frequency": 1}
{"value": "transition_risk_supply_chain", "hint": "impact", "source_doc_id": "nestle_2024_xyz", "frequency": 3}
```

Check this file periodically after running ingestion on new documents.

### Phase 2 — Review (human)
Open `candidate_extensions.jsonl` in any text editor. Group entries by `value` + `hint`. Decide for each:
- **Approve**: add as a new node under the correct parent in `_design/taxonomy.yaml`
- **Reject**: the model was confused; leave taxonomy unchanged
- **Merge**: map to an existing node the model was just labelling differently

When adding a new node, follow the extension pattern already in taxonomy.yaml:
```yaml
hazards:
  extensions:
    acute_water_shortage:
      label: Acute water shortage event
      seed_mapping: hazards.physical_acute.flood_event   # closest seed node
      added: "2026-03-28"
      frequency: 12
      reviewer: "your-name"
```

### Phase 3 — Propagate (targeted re-run)
After saving taxonomy.yaml, run:
```bash
python ingest.py --reclassify
```

This re-runs Stage B classification **only** on the `auto_rejected` passages that failed due to an invalid taxonomy value. It does **not** re-parse any PDFs or re-run Stage A — it uses the passage text already stored in Azure AI Search. A corpus of 1,000 passages typically reclassifies in under 5 minutes.

---

## GitHub Actions (Automated Ingestion)

### CI — runs on every push

Tests run automatically. See `.github/workflows/tests.yml`.

### Weekly ingestion — runs every Monday 06:00 UTC

Configured in `.github/workflows/ingest.yml`. To activate it, add these secrets to your GitHub repository:

**Settings → Secrets and variables → Actions → New repository secret**

| Secret name | Value |
|-------------|-------|
| `AZURE_SEARCH_ENDPOINT` | Your Azure Search endpoint |
| `AZURE_SEARCH_KEY` | Your Azure Search admin key |
| `AZURE_OPENAI_ENDPOINT` | Your Azure OpenAI endpoint |
| `AZURE_OPENAI_KEY` | Your Azure OpenAI key |
| `GOOGLE_CSE_API_KEY` | Your Google CSE API key(s) |
| `GOOGLE_CSE_ID` | Your Google CSE ID |

You can also trigger ingestion manually from the GitHub Actions tab with a custom source and query.

---

## Phase Status

| Phase | Status | What was built |
|-------|--------|---------------|
| Phase 0 | Complete | Schemas, adapters, tests, repo structure |
| Phase 1 | Complete | `ingest.py`, taxonomy, knowledge store, two-stage extraction |
| Phase 2 | Complete | GCF API, OECD API adapters, Streamlit review UI, GitHub Actions |
| Phase 3 | Complete | Newsletter, sector brief, company assessment (D1–D8), citations |

Phase 2 API adapters (GCF, OECD) are fully implemented but disabled by default. Enable them in `sources.yaml` by setting `enabled: true`. See the **GCF Approved Projects** section above for full filtering and fetch-mode documentation.

---

## Key Design Rules

- `config.py` is the **only** file that reads `os.environ` — never use `os.getenv` anywhere else
- `knowledge_store.py` is the **only** file that imports the Azure Search SDK
- Every function raises a named exception on failure — no silent swallowing
- No hardcoded credentials anywhere in the codebase
- Taxonomy seed nodes at `_design/taxonomy.yaml` are **stable** — never remove or rename a seed node (it breaks existing passage subcategory paths). Add new nodes under `extensions:` only after human review (see Taxonomy Evolution Loop above)
- Prompt files are versioned (`collect_v1.txt`, `classify_v2.txt`, etc.) — never modify in-place; bump the version
- `TRUSTED_STATUSES` filter in `query_trusted()` is non-bypassable — only auto-approved, approved, or edited passages feed LLM outputs
