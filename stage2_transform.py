"""
Stage 2: Transform raw JSON to normalized CSV tables
Usage: python stage2_transform.py [raw_folder]
"""
from transformer import ClimateDataTransformer
import sys

def main():
    # Parse arguments
    raw_folder = sys.argv[1] if len(sys.argv) > 1 else None

    # Initialize transformer
    transformer = ClimateDataTransformer()

    # Run transformation
    transformer.transform_all(raw_folder=raw_folder)

    print("\nTransformation complete!")
    print("\nGenerated CSV tables:")
    print("  - Risk Identification: output/tables/risk_identification/")
    print("  - Financial: output/tables/financial/")
    print("  - Responses: output/tables/responses/")
    print("  - Management: output/tables/management/")
    print("  - Metadata: output/tables/metadata/")

if __name__ == "__main__":
    main()
