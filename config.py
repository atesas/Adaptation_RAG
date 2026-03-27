import os
from pathlib import Path

# ── Azure AI Search ───────────────────────────────────────────────────────────
AZURE_SEARCH_ENDPOINT  = os.environ["AZURE_SEARCH_ENDPOINT"]
AZURE_SEARCH_KEY       = os.environ["AZURE_SEARCH_KEY"]

# ── Azure OpenAI ──────────────────────────────────────────────────────────────
AZURE_OPENAI_ENDPOINT  = os.environ["AZURE_OPENAI_ENDPOINT"]
AZURE_OPENAI_KEY       = os.environ["AZURE_OPENAI_KEY"]

# ── Model deployments ─────────────────────────────────────────────────────────
EMBEDDING_DEPLOYMENT   = os.environ.get("EMBEDDING_DEPLOYMENT", "text-embedding-3-large")
GPT4O_DEPLOYMENT       = os.environ.get("GPT4O_DEPLOYMENT", "gpt-4o")
GPT4O_MINI_DEPLOYMENT  = os.environ.get("GPT4O_MINI_DEPLOYMENT", "gpt-4o-mini")

# Model allocation by task
STAGE_A_MODEL          = GPT4O_MINI_DEPLOYMENT  # extraction — cost-sensitive
STAGE_B_MODEL          = GPT4O_MINI_DEPLOYMENT  # classification — cost-sensitive
OUTPUT_MODEL           = GPT4O_DEPLOYMENT        # synthesis — quality-sensitive

# ── Google CSE (Phase 0) ──────────────────────────────────────────────────────
GOOGLE_CSE_API_KEY     = os.environ["GOOGLE_CSE_API_KEY"]
GOOGLE_CSE_ID          = os.environ["GOOGLE_CSE_ID"]

# ── Structured APIs (Phase 2) ─────────────────────────────────────────────────
GCF_API_BASE    = "https://www.greenclimate.fund/projects/api"
OECD_API_BASE   = "https://stats.oecd.org/SDMX-JSON/data/CRS"

# ── Active prompt versions ────────────────────────────────────────────────────
COLLECT_PROMPT_VERSION            = "v1"
CLASSIFY_PROMPT_VERSION           = "v1"
NEWSLETTER_PROMPT_VERSION         = "v1"
SECTOR_BRIEF_PROMPT_VERSION       = "v1"
COMPANY_ASSESSMENT_PROMPT_VERSION = "v1"

# ── Paths ─────────────────────────────────────────────────────────────────────
TAXONOMY_PATH  = Path("_design/taxonomy.yaml")
SOURCES_PATH   = Path("sources.yaml")
PROMPTS_DIR    = Path("prompts/")
TMP_DIR        = Path("tmp/")   # temp downloads — gitignored

# ── Validation thresholds ─────────────────────────────────────────────────────
AUTO_APPROVE_THRESHOLD = 0.85
AUTO_REJECT_THRESHOLD  = 0.40

# ── Document limits ───────────────────────────────────────────────────────────
MAX_DOCUMENT_CHARS = 200_000

# ── Azure AI Search index names ───────────────────────────────────────────────
INDEX_PASSAGES      = "adaptation-passages"
INDEX_DOCUMENTS     = "adaptation-documents"
INDEX_VALIDATION    = "adaptation-validation-log"
