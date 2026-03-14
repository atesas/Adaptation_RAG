# Document Q&A System - Dual-Index Strategy

A sophisticated document Q&A system implementing a **dual-index strategy** for maximum accuracy while building a permanent knowledge base.

## 🎯 Why Dual-Index?

### The Problem
When all documents are indexed together, vector search can retrieve chunks from the wrong documents, reducing answer accuracy.

### The Solution
**Dual-Index Strategy:**
1. **Persistent KB** (`documents-index`): All documents stored permanently for future cross-document queries
2. **Ephemeral Processing** (`temp-documents-index`): Each document processed in isolation for Q&A, then deleted

### Benefits
✅ **Maximum Accuracy**: Each document processed in isolation, eliminating cross-document noise  
✅ **Permanent KB**: All documents stored for future queries  
✅ **Guaranteed Precision**: Vector search only retrieves from target document  
✅ **Focused Context**: LLM gets only relevant chunks from the specific document

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   AZURE SEARCH SERVICE                       │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  INDEX 1: "documents-index" (PERSISTENT KB)                 │
│  ┌────────────────────────────────────────────────────┐    │
│  │ • All documents stored permanently                 │    │
│  │ • Used for future cross-document queries          │    │
│  │ • Built once, queried many times                  │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  INDEX 2: "temp-documents-index" (EPHEMERAL)                │
│  ┌────────────────────────────────────────────────────┐    │
│  │ • One document at a time                           │    │
│  │ • Process → Answer → Delete                        │    │
│  │ • Maximum accuracy (no interference)               │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## 📦 Project Structure

```
document_qa_system_v2/
├── config.py                        # Configuration management
├── indexing.py                      # Document extraction, chunking, embedding
├── question_router.py               # Question type detection
├── qa.py                            # Q&A and classification logic
├── dual_index_batch_processor.py   # Dual-index strategy (MAIN)
├── cli.py                           # Command-line interface
├── requirements.txt                 # Dependencies
└── .env.example                     # Environment template
```

## 🚀 Installation

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env with your Azure credentials

# 3. Create documents folder
mkdir documents
# Add your PDF files
```

## 📖 Usage

### RECOMMENDED: Dual-Index Batch Processing

Best accuracy for extracting data from multiple documents:

```bash
python cli.py dual-batch documents/ --questions-file questions.txt --parallel
```

**What happens:**
1. ✅ Builds persistent KB with all documents (parallel)
2. ✅ For each document:
   - Index to temp-documents-index (isolated)
   - Answer all questions (no cross-document noise)
   - Delete from temp index
3. ✅ Saves results to CSV
4. ✅ Persistent KB remains for future queries

**Output:** 
- `output/qa_results.csv` with all answers
- Persistent KB ready for future use

### Example Questions File

```text
# questions.txt
What is the main focus and purpose of this document?
Which climate risks are identified, and in which locations or sectors?
Is this document related to adaptation or mitigation?
What type of document is this?
Which climate hazards are mentioned?
What climate adaptation strategies are proposed?
```

### Other Commands

**Build Persistent KB Only:**
```bash
python cli.py index-all documents/ --parallel
```

**Query Persistent KB:**
```bash
python cli.py ask "What are the main risks?" --document climate_plan_2024
```

**Single-Index Batch (Faster, Slightly Less Accurate):**
```bash
python cli.py single-batch documents/ --questions-file questions.txt --parallel
```

**One-Off Ephemeral Query:**
```bash
python cli.py ephemeral document.pdf "What is this about?"
```

## 🔄 Workflow Comparison

### Dual-Index (RECOMMENDED)
```
For each document:
  1. Index to persistent KB (parallel, once)
  2. Index to temp (isolated)
  3. Answer questions (accurate)
  4. Delete from temp

Result: Accurate CSV + Permanent KB
```

### Single-Index (Traditional)
```
1. Index all documents to one index
2. For each document:
   - Query with document_name filter
   - May retrieve wrong chunks if names similar

Result: Faster but less accurate
```

## 📊 Output Format

CSV with columns:

| document | Q1: Main focus? | Q1_type | Q2: Adaptation/mitigation? | Q2_type | Q2_dimension | Q2_confidence | Q2_new_category |
|----------|----------------|---------|---------------------------|---------|--------------|---------------|-----------------|
| doc1.pdf | Climate risks... | exploratory | loss and damage | classification | topic | high | True |

## 🎓 Question Types

The system handles **mixed question types**:

### Exploratory Questions
Extract information from documents:
- "What are the main climate risks?"
- "Which actors are mentioned?"
- "What financing mechanisms are described?"

### Classification Questions
Categorize documents with dynamic expansion:
- "Is this about adaptation or mitigation?"
- "What type of document is this?"
- "Which sector does this focus on?"

**Dynamic Categories:** If a document doesn't fit existing categories, the system creates new ones automatically.

## ⚙️ Configuration

Edit `config.py` or set environment variables:

```python
CHUNK_SIZE = 1000              # Characters per chunk
CHUNK_OVERLAP = 200            # Overlap between chunks
MAX_SEARCH_RESULTS = 5         # Chunks to retrieve
PARALLEL_WORKERS = 4           # Parallel indexing workers
EMBEDDING_BATCH_SIZE = 50      # Embeddings per batch
```

## 🔍 Advanced: When to Use Which Mode

| Use Case | Command | Index Strategy |
|----------|---------|----------------|
| **Extract data from multiple docs (BEST ACCURACY)** | `dual-batch` | Both: KB + isolated temp |
| **Build searchable knowledge base** | `index-all` | Persistent only |
| **Query existing knowledge base** | `ask` | Persistent only |
| **Quick batch processing** | `single-batch` | Single index |
| **One-off question** | `ephemeral` | Temp only |

## 💡 Tips for Maximum Accuracy

1. **Use dual-batch** for production data extraction
2. **Enable parallel KB indexing** (`--parallel`) for speed
3. **Review dynamic categories** in `categories.json` periodically
4. **Tune chunk size** based on document structure
5. **Monitor vector search scores** (stored in metadata)

## 🔧 Programmatic Use

```python
from dual_index_batch_processor import DualIndexBatchProcessor

questions = [
    "What are the main climate risks?",
    "Is this about adaptation or mitigation?",
    "What type of document is this?"
]

processor = DualIndexBatchProcessor()
processor.process_documents_with_questions(
    document_folder="documents",
    questions=questions,
    output_file="results.csv",
    build_persistent_kb=True,      # Also build permanent KB
    parallel_kb_indexing=True      # Parallel indexing
)
```

## 📈 Storage Costs

### Persistent KB
- Stores: All documents permanently
- Cost: ~$1-5/month for 100-500 documents
- Benefit: Query anytime without re-indexing

### Ephemeral Temp Index
- Stores: One document at a time
- Cost: Negligible (immediate deletion)
- Benefit: Maximum accuracy

## 🐛 Troubleshooting

**No text extracted:**
- PDF may be scanned (needs OCR)
- Try converting to searchable PDF

**Rate limits:**
- Reduce `EMBEDDING_BATCH_SIZE`
- Reduce `PARALLEL_WORKERS`

**Low accuracy:**
- Use dual-batch instead of single-batch
- Increase `MAX_SEARCH_RESULTS`
- Adjust `CHUNK_SIZE` for your documents

**Memory errors:**
- Process fewer documents at once
- Reduce `PARALLEL_WORKERS`

## 📝 License

MIT License
