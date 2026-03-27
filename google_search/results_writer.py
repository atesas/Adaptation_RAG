"""
Results Writer - Enhanced with ID column
Writes search results to CSV with sequential ID for matching with downloaded files.
"""

import csv
from pathlib import Path
from typing import List, Dict


def write_results_to_csv(results: List[Dict], filename: str) -> None:
    """
    Write search results to CSV file with ID column.

    Args:
        results: List of result dictionaries
        filename: Output CSV filename
    """
    if not results:
        print(f"No results to write to {filename}")
        return

    # Add ID column to each result (1-indexed)
    for idx, result in enumerate(results, start=1):
        result['ID'] = idx

    # Get all unique keys across all results
    all_keys = set()
    for result in results:
        all_keys.update(result.keys())

    # Define column order - ID first, then the rest
    ordered_keys = ['ID'] + sorted([k for k in all_keys if k != 'ID'])

    # Write to CSV
    output_path = Path(filename)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=ordered_keys)
        writer.writeheader()
        writer.writerows(results)

    print(f"Wrote {len(results)} results to {filename} (with ID column)")
