# =============================================================================
# tests/conftest.py
# Sets required environment variables before any module import so that
# config.py does not raise KeyError during test collection.
# =============================================================================

import os

_TEST_ENV = {
    "AZURE_SEARCH_ENDPOINT": "https://test.search.windows.net",
    "AZURE_SEARCH_KEY": "test-search-key",
    "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com/",
    "AZURE_OPENAI_KEY": "test-openai-key",
    "GOOGLE_CSE_API_KEY": "test-cse-key",
    "GOOGLE_CSE_ID": "test-cse-cx",
}

for key, value in _TEST_ENV.items():
    os.environ.setdefault(key, value)
