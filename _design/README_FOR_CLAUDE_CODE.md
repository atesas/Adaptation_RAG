# Adaptation Intelligence Platform — Design Package
## Instructions for Claude Code

This `_design/` folder contains the complete architecture specification.
**Read this file first, then read the files in the order listed below.**

---

## What This Is

This is a design-first repository. All architectural decisions have been made
and documented here. Claude Code's job is to implement — not to make architecture
decisions. If something is unclear, read the relevant design file before asking.

The `_design/` folder contains:
- Schemas (what data looks like)
- Interface contracts (what functions exist and what they do)
- Prompt templates (what to send to the LLM)
- The taxonomy (the classification vocabulary)
- The master plan (the full architecture in one document)

---

## Files in This Package

Read them in this order before implementing anything:

1. `AIP_Master_Plan.docx`
   The full architecture document. Sections 1–11. Read this first.
   Covers: current state, three pillars, regulatory seeds, taxonomy design,
   Azure AI Search config, validation workflow, ingestion adapters, output
   generation, implementation phases, design decisions.

2. `taxonomy.yaml`
   The taxonomy dictionary. All classification categories with definitions,
   examples, exclusions, and seed source references. Stage B reads this.

3. `schemas/validation.py`
   ValidationStatus and ReviewPriority enums. Constants for auto-approval.
   Implement exactly as written — these are enum values stored in Azure Search.

4. `schemas/document.py`
   Document dataclass — output of every adapter. Controlled vocabularies for
   source_type and document_type.

5. `schemas/passage.py`
   ClassifiedPassage dataclass — what lives in the Azure AI Search index.
   Controlled vocabularies for iro_type, value_chain_position, etc.

6. `taxonomy_interface.py`
   Interface contract for taxonomy.py. Implement TaxonomyLoader class.

7. `knowledge_store_interface.py`
   Interface contract for knowledge_store.py. Implement KnowledgeStore class.
   Three Azure AI Search indexes. See INDEX NAMES constants in the file.

8. `ingest_interface.py`
   Interface contracts for ingest.py and adapters/base.py.
   The pipeline order: fetch → normalize → Stage A → Stage B → triage → upsert.

9. `prompts/collect_v1.txt`
   Stage A extraction prompt template. Template variables in {curly_braces}.
   Do not modify the prompt — use it exactly as written.

10. `prompts/classify_v1.txt`
    Stage B classification prompt template. Template variables in {curly_braces}.
    The {taxonomy_excerpt} variable is populated by taxonomy.py, not hardcoded.

---

## Implementation Order (Phase 1 first)

Implement in this order — each file depends on the ones before it:

```
Phase 0 (setup):
  1. schemas/validation.py     ← no dependencies
  2. schemas/document.py       ← no dependencies
  3. schemas/passage.py        ← depends on schemas/validation.py
  4. taxonomy.py               ← depends on taxonomy.yaml + schemas/passage.py

Phase 1 (knowledge store + extraction):
  5. knowledge_store.py        ← depends on all schemas
  6. extractor.py              ← depends on taxonomy.py, schemas, prompts/
                                  (Stage A + Stage B logic lives here)
  7. ingest.py + adapters/     ← depends on everything above

Phase 2 (validation UI):
  8. validation/app.py         ← depends on knowledge_store.py

Phase 3 (outputs):
  9. outputs/newsletter.py     ← depends on knowledge_store.py
  10. outputs/sector_brief.py  ← depends on knowledge_store.py
  11. outputs/company_assessment.py ← depends on knowledge_store.py
```

---

## Azure AI Search — Three Indexes

Create these indexes before writing any other code:

| Index name                  | Purpose                              |
|-----------------------------|--------------------------------------|
| `adaptation-passages`       | Main knowledge store (ClassifiedPassage) |
| `adaptation-documents`      | Source document registry (Document)  |
| `adaptation-validation-log` | Correction loop audit trail (append-only) |

Index schema definitions: derive from the dataclasses in schemas/.
All fields marked `# [filterable]` in schemas/passage.py MUST be declared
filterable in the Azure Search index schema.
Embedding field: `text_vector`, 3072 dimensions (text-embedding-3-large).

---

## Environment Variables Required

```
AZURE_SEARCH_ENDPOINT=
AZURE_SEARCH_KEY=
AZURE_OPENAI_ENDPOINT=
AZURE_OPENAI_KEY=
AZURE_OPENAI_EMBEDDING_DEPLOYMENT=text-embedding-3-large
AZURE_OPENAI_GPT4O_DEPLOYMENT=gpt-4o
AZURE_OPENAI_GPT4O_MINI_DEPLOYMENT=gpt-4o-mini
GOOGLE_CSE_API_KEY=          # Phase 2 — google_cse adapter
GOOGLE_CSE_ID=               # Phase 2 — google_cse adapter
SEMANTIC_SCHOLAR_API_KEY=    # Phase 2 — academic adapter
EXA_API_KEY=                 # Phase 2 — exa adapter
```

---

## Key Rules (Do Not Override These)

1. `knowledge_store.py` is the ONLY file that imports the Azure AI Search SDK
2. `ingest.py` is the ONLY entry point for adding documents
3. `query_trusted()` ALWAYS applies TRUSTED_STATUSES filter — never bypass this
4. All methods in knowledge_store.py are async
5. Stage A and B use GPT-4o-mini (not GPT-4o — that's for output generation only)
6. Prompt files are version-controlled: never modify collect_v1.txt or classify_v1.txt
   in place — create collect_v2.txt and update the path reference in extractor.py
7. The adaptation-validation-log index is append-only — no updates, no deletes
