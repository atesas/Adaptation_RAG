"""
Stage 1: Extract climate risk data with automatic PDF renaming
Usage: 
  python stage1_extract.py documents/                    # With auto-rename
  python stage1_extract.py documents/ --no-rename        # Without auto-rename
  python stage1_extract.py documents/ --no-backup        # Without backup
  python stage1_extract.py documents/ --no-incremental   # Reprocess all
"""
from extractor import ClimateRiskExtractor
import sys

def main():
    # Parse arguments
    args = sys.argv[1:]

    # Documents folder
    docs_folder = args[0] if args and not args[0].startswith('--') else "documents"

    # Options
    rename_pdfs = "--no-rename" not in args
    backup_originals = "--no-backup" not in args
    incremental = "--no-incremental" not in args

    print("="*70)
    print("STAGE 1: EXTRACTION CONFIGURATION")
    print("="*70)
    print(f"Documents folder: {docs_folder}")
    print(f"Auto-rename PDFs: {rename_pdfs}")
    print(f"Backup originals: {backup_originals}")
    print(f"Incremental mode: {incremental}")
    print()

    # Initialize extractor
    extractor = ClimateRiskExtractor(
        rename_pdfs=rename_pdfs,
        backup_originals=backup_originals
    )

    # Load questions
    extractor.load_questions("questions.txt")

    # Run extraction
    extractor.extract_batch(docs_folder, incremental=incremental)

if __name__ == "__main__":
    main()
