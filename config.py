import os
from pathlib import Path

# ── Auto-load .env if present ─────────────────────────────────────────────────
# This makes every CLI entry point work without manually exporting env vars.
# Has no effect if the variables are already set in the environment (e.g. CI).
try:
    from dotenv import load_dotenv
    load_dotenv(override=False)  # don't override vars already set in environment
except ImportError:
    pass  # python-dotenv not installed — rely on env vars being set externally

# ── Azure AI Search ───────────────────────────────────────────────────────────
AZURE_SEARCH_ENDPOINT  = os.environ.get("AZURE_SEARCH_ENDPOINT", "")
AZURE_SEARCH_KEY       = os.environ.get("AZURE_SEARCH_KEY", "")

# ── Azure OpenAI ──────────────────────────────────────────────────────────────
AZURE_OPENAI_ENDPOINT  = os.environ.get("AZURE_OPENAI_ENDPOINT", "")
AZURE_OPENAI_KEY       = os.environ.get("AZURE_OPENAI_KEY", "")

# ── Model deployments ─────────────────────────────────────────────────────────
EMBEDDING_DEPLOYMENT   = os.environ.get("EMBEDDING_DEPLOYMENT", "text-embedding-3-large")
GPT4O_DEPLOYMENT       = os.environ.get("GPT4O_DEPLOYMENT", "gpt-4o")
GPT4O_MINI_DEPLOYMENT  = os.environ.get("GPT4O_MINI_DEPLOYMENT", "gpt-4o-mini")

# Model allocation by task
STAGE_A_MODEL          = GPT4O_MINI_DEPLOYMENT  # extraction — cost-sensitive
STAGE_B_MODEL          = GPT4O_MINI_DEPLOYMENT  # classification — cost-sensitive
OUTPUT_MODEL           = GPT4O_DEPLOYMENT        # synthesis — quality-sensitive

# ── Google CSE ────────────────────────────────────────────────────────────────
GOOGLE_CSE_API_KEY     = os.environ.get("GOOGLE_CSE_API_KEY", "")
GOOGLE_CSE_ID          = os.environ.get("GOOGLE_CSE_ID", "")

# ── Structured APIs (Phase 2) ─────────────────────────────────────────────────
GCF_API_BASE    = "https://www.greenclimate.fund/projects/api"
OECD_API_BASE   = "https://stats.oecd.org/SDMX-JSON/data/CRS"

# ── Active prompt versions ────────────────────────────────────────────────────
COLLECT_PROMPT_VERSION            = "v2"
CLASSIFY_PROMPT_VERSION           = "v1"
NEWSLETTER_PROMPT_VERSION         = "v1"
SECTOR_BRIEF_PROMPT_VERSION       = "v1"
COMPANY_ASSESSMENT_PROMPT_VERSION = "v1"

# ── Paths ─────────────────────────────────────────────────────────────────────
TAXONOMY_PATH  = Path(os.environ.get("TAXONOMY_PATH", "_design/taxonomy.yaml"))
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


def require_credentials() -> None:
    """
    Call this before making any Azure or Google API calls.
    Raises a clear error listing every missing variable.
    Not called at import time — so --help and tests work without credentials.
    """
    missing = []
    for name, value in [
        ("AZURE_SEARCH_ENDPOINT",  AZURE_SEARCH_ENDPOINT),
        ("AZURE_SEARCH_KEY",       AZURE_SEARCH_KEY),
        ("AZURE_OPENAI_ENDPOINT",  AZURE_OPENAI_ENDPOINT),
        ("AZURE_OPENAI_KEY",       AZURE_OPENAI_KEY),
        ("GOOGLE_CSE_API_KEY",     GOOGLE_CSE_API_KEY),
        ("GOOGLE_CSE_ID",          GOOGLE_CSE_ID),
    ]:
        if not value:
            missing.append(name)
    if missing:
        raise EnvironmentError(
            "Missing required environment variables:\n"
            + "\n".join(f"  {m}" for m in missing)
            + "\n\nCopy .env.example to .env and fill in the values."
        )
