"""
Delete Azure Search Indexes
Run this to clean up old indexes before starting fresh
"""
import os
from dotenv import load_dotenv
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient

load_dotenv()

# Get credentials from .env
search_endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
search_key = os.getenv("AZURE_SEARCH_KEY")

# Initialize client
search_credential = AzureKeyCredential(search_key)
index_client = SearchIndexClient(endpoint=search_endpoint, credential=search_credential)

print("\n" + "="*70)
print("DELETE AZURE SEARCH INDEXES")
print("="*70)

# List current indexes
print("\nCurrent indexes in your Azure Search service:")
indexes = list(index_client.list_indexes())
for idx in indexes:
    print(f"  - {idx.name}")

if not indexes:
    print("  (No indexes found)")
    exit(0)

print("\n" + "="*70)

# Delete documents-index
try:
    index_client.delete_index("documents-index")
    print("✓ Deleted 'documents-index'")
except Exception as e:
    print(f"⚠️ Could not delete 'documents-index': {e}")

# Delete temp-documents-index
try:
    index_client.delete_index("temp-documents-index")
    print("✓ Deleted 'temp-documents-index'")
except Exception as e:
    print(f"⚠️ Could not delete 'temp-documents-index': {e}")

print("\n" + "="*70)
print("Remaining indexes:")
indexes = list(index_client.list_indexes())
for idx in indexes:
    print(f"  - {idx.name}")

if not indexes:
    print("  (All indexes deleted)")

print("="*70)
print("\n✓ Done! Now run your dual-batch command again.")
print("  The system will create new indexes with the correct schema.\n")
