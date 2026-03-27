# =============================================================================
# _design/taxonomy_interface.py
# Interface contract for taxonomy.py
# =============================================================================
# RULES FOR CLAUDE CODE:
#   1. taxonomy.py loads taxonomy.yaml once at startup (not on every call).
#      Use a module-level singleton pattern.
#   2. get_taxonomy_excerpt_for_hint() is called once per Stage B invocation.
#      It must be fast — no I/O, no API calls. Pure dict traversal.
#   3. The excerpt must be formatted as readable YAML (not JSON) — the Stage B
#      prompt is more accurate with YAML taxonomy context.
#   4. Target excerpt size: 800–1200 tokens. Measure with tiktoken if needed.
#   5. validate_classification() is called inside run_stage_b() to check
#      that Stage B output fields are in controlled vocabularies.
# =============================================================================

from pathlib import Path
from typing import Optional


# =============================================================================
# HINT → TAXONOMY NODES MAPPING
# Which top-level nodes to include for each topic_hint value.
# "always" nodes are included regardless of hint.
# =============================================================================

HINT_TO_NODES: dict[str, list[str]] = {
    "hazard":       ["hazards"],
    "impact":       ["impacts", "hazards"],  # impacts reference hazards
    "adaptation":   ["responses"],
    "governance":   ["governance"],
    "finance":      ["finance", "responses.financial_instruments"],
    "scenario":     ["scenarios"],
    "regulatory":   ["frameworks"],
    "supply_chain": ["value_chain_position", "responses.supply_chain_resilience"],
}

# Always included — cross-cutting, needed for every classification
ALWAYS_INCLUDE_NODES: list[str] = [
    "iro_type",
    "value_chain_position",
    "evidence_quality",
]

# Always excluded — too long, not needed for classification
NEVER_INCLUDE_NODES: list[str] = [
    "schema_evolution_log",
    "sector_tags",      # Included separately as a short list
    "frameworks",       # Included only for hint="regulatory"
]


# =============================================================================
# INTERFACE
# =============================================================================

class TaxonomyLoader:
    """
    Loads taxonomy.yaml and provides query methods.
    Instantiated once as a module-level singleton in taxonomy.py.
    """

    def __init__(self, taxonomy_path: Path):
        """Load and parse taxonomy.yaml. Cache the result."""
        ...

    def get_taxonomy_excerpt_for_hint(self, topic_hint: str) -> str:
        """
        Return a focused YAML excerpt of the taxonomy for Stage B.

        Uses HINT_TO_NODES to select relevant nodes.
        Always includes ALWAYS_INCLUDE_NODES.
        Never includes NEVER_INCLUDE_NODES.
        Appends a short flat list of valid sector_tags values at the end.
        Appends the IRO_TYPES, VALUE_CHAIN_POSITIONS, and EVIDENCE_QUALITY_LEVELS
        controlled vocabulary lists from schemas/passage.py.

        Returns: YAML string, 800–1200 tokens
        """
        ...

    def validate_classification(self, stage_b_output: dict) -> tuple[bool, list[str]]:
        """
        Validate Stage B output against taxonomy and controlled vocabularies.

        Checks:
        - category is a valid top-level taxonomy node
        - subcategory dotted path exists in taxonomy
        - iro_type is in IRO_TYPES (schemas/passage.py)
        - value_chain_position is in VALUE_CHAIN_POSITIONS
        - evidence_quality is in EVIDENCE_QUALITY_LEVELS
        - time_horizon is in TIME_HORIZONS
        - All values in sector_relevance are valid sector_tags
        - All values in frameworks_referenced are valid framework keys

        Returns: (is_valid: bool, errors: list[str])
        If not valid, the caller sets confidence = 0.0 and
        classification_note = "invalid_taxonomy_value"
        """
        ...

    def get_node(self, dotted_path: str) -> Optional[dict]:
        """
        Retrieve a taxonomy node by dotted path.
        e.g. get_node("hazards.physical_chronic.water_stress")
        Returns None if path does not exist.
        """
        ...

    def is_seed_category(self, subcategory_path: str) -> bool:
        """
        Returns True if the subcategory is in the seed layer.
        Returns False if it is a data-driven extension.
        Checks for presence of 'seed_source' key on the node.
        """
        ...

    def get_all_subcategory_paths(self) -> list[str]:
        """
        Return all valid dotted subcategory paths in the taxonomy.
        Used to build dropdown options in the validation UI.
        """
        ...

    def record_candidate_extension(
        self,
        value: str,
        hint: str,
        source_doc_id: str,
        frequency: int = 1
    ) -> None:
        """
        Log a value that doesn't match any taxonomy node.
        Writes to a local candidate_extensions.jsonl file.
        Used for schema evolution Step 1 (emergence detection).
        Does NOT modify taxonomy.yaml — human review required first.
        """
        ...
