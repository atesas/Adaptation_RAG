# =============================================================================
# taxonomy.py
# Loads _design/taxonomy.yaml and provides query methods for Stage B.
# Singleton pattern — loaded once at module import time.
# =============================================================================

import json
import logging
from pathlib import Path
from typing import Optional

import yaml

from schemas.passage import IRO_TYPES, VALUE_CHAIN_POSITIONS, EVIDENCE_QUALITY_LEVELS, TIME_HORIZONS
from _design.taxonomy_interface import HINT_TO_NODES, ALWAYS_INCLUDE_NODES, NEVER_INCLUDE_NODES

logger = logging.getLogger(__name__)

_CANDIDATE_EXTENSIONS_FILE = Path("candidate_extensions.jsonl")


class TaxonomyLoader:

    def __init__(self, taxonomy_path: Path) -> None:
        with open(taxonomy_path, "r", encoding="utf-8") as fh:
            self._taxonomy: dict = yaml.safe_load(fh)
        self._top_level_nodes: list[str] = [
            k for k in self._taxonomy
            if isinstance(self._taxonomy[k], dict)
            and k not in ("taxonomy_version", "last_updated", "sector_focus")
        ]
        logger.info("Taxonomy loaded: %d top-level nodes", len(self._top_level_nodes))

    def get_taxonomy_excerpt_for_hint(self, topic_hint: str) -> str:
        nodes_to_include: list[str] = list(ALWAYS_INCLUDE_NODES)
        extra = HINT_TO_NODES.get(topic_hint, [])
        for node in extra:
            top = node.split(".")[0]
            if top not in nodes_to_include:
                nodes_to_include.append(top)

        excerpt: dict = {}
        for node in nodes_to_include:
            if node in NEVER_INCLUDE_NODES:
                continue
            if node in self._taxonomy:
                excerpt[node] = self._taxonomy[node]

        sector_tags = self._taxonomy.get("sector_tags", {})
        if isinstance(sector_tags, dict):
            tag_list = list(sector_tags.keys())
        else:
            tag_list = self._taxonomy.get("sector_focus", [])

        valid_categories = {
            "VALID_CATEGORIES__use_exactly_one_as_category_field": self._top_level_nodes,
        }
        controlled_vocab = {
            "IRO_TYPES": IRO_TYPES,
            "VALUE_CHAIN_POSITIONS": VALUE_CHAIN_POSITIONS,
            "EVIDENCE_QUALITY_LEVELS": EVIDENCE_QUALITY_LEVELS,
            "TIME_HORIZONS": TIME_HORIZONS,
            "sector_tags": tag_list,
        }
        return yaml.dump(valid_categories, allow_unicode=True, sort_keys=False) + \
               "\n" + \
               yaml.dump(excerpt, allow_unicode=True, sort_keys=False) + \
               "\n# --- CONTROLLED VOCABULARIES ---\n" + \
               yaml.dump(controlled_vocab, allow_unicode=True, sort_keys=False)

    def validate_classification(self, stage_b_output: dict) -> tuple[bool, list[str]]:
        errors: list[str] = []
        category = stage_b_output.get("category", "")
        if category not in self._top_level_nodes:
            errors.append(f"invalid category: '{category}'")

        subcategory = stage_b_output.get("subcategory", "")
        if subcategory and self.get_node(subcategory) is None:
            errors.append(f"subcategory path not found: '{subcategory}'")

        iro = stage_b_output.get("iro_type", "")
        if iro not in IRO_TYPES:
            errors.append(f"invalid iro_type: '{iro}'")

        vcp = stage_b_output.get("value_chain_position", "")
        if vcp not in VALUE_CHAIN_POSITIONS:
            errors.append(f"invalid value_chain_position: '{vcp}'")

        eq = stage_b_output.get("evidence_quality", "")
        if eq not in EVIDENCE_QUALITY_LEVELS:
            errors.append(f"invalid evidence_quality: '{eq}'")

        th = stage_b_output.get("time_horizon", "")
        if th not in TIME_HORIZONS:
            errors.append(f"invalid time_horizon: '{th}'")

        sector_tags = self._get_valid_sector_tags()
        for tag in stage_b_output.get("sector_relevance", []):
            if tag not in sector_tags:
                errors.append(f"invalid sector_relevance value: '{tag}'")

        valid_frameworks = {
            "csrd_esrs", "eu_taxonomy", "csddd", "tcfd",
            "ifrs_s2", "cdp", "tnfd", "gri", "sfdr",
        }
        for fw in stage_b_output.get("frameworks_referenced", []):
            if fw not in valid_frameworks:
                errors.append(f"invalid framework: '{fw}'")

        return len(errors) == 0, errors

    def get_node(self, dotted_path: str) -> Optional[dict]:
        parts = dotted_path.split(".")
        node = self._taxonomy
        for part in parts:
            if not isinstance(node, dict) or part not in node:
                return None
            node = node[part]
        return node if isinstance(node, dict) else None

    def is_seed_category(self, subcategory_path: str) -> bool:
        node = self.get_node(subcategory_path)
        if node is None:
            return False
        return "seed_source" in node

    def get_all_subcategory_paths(self) -> list[str]:
        paths: list[str] = []
        self._collect_paths(self._taxonomy, [], paths)
        return paths

    def _collect_paths(self, node: dict, prefix: list[str], result: list[str]) -> None:
        for key, value in node.items():
            if not isinstance(value, dict):
                continue
            current = prefix + [key]
            if "seed_source" in value or "label" in value:
                result.append(".".join(current))
            self._collect_paths(value, current, result)

    def record_candidate_extension(
        self,
        value: str,
        hint: str,
        source_doc_id: str,
        frequency: int = 1,
    ) -> None:
        record = {
            "value": value,
            "hint": hint,
            "source_doc_id": source_doc_id,
            "frequency": frequency,
        }
        with open(_CANDIDATE_EXTENSIONS_FILE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")

    def _get_valid_sector_tags(self) -> set[str]:
        sector_tags_node = self._taxonomy.get("sector_tags")
        if isinstance(sector_tags_node, dict) and sector_tags_node:
            return set(sector_tags_node.keys())
        return set(self._taxonomy.get("sector_focus", []))


# Module-level singleton — loaded lazily on first access.
_taxonomy_singleton: Optional[TaxonomyLoader] = None


def _get_taxonomy() -> TaxonomyLoader:
    global _taxonomy_singleton
    if _taxonomy_singleton is None:
        import config as _config
        _taxonomy_singleton = TaxonomyLoader(_config.TAXONOMY_PATH)
    return _taxonomy_singleton


class _TaxonomyProxy:
    """Proxy that forwards attribute access to the lazily-loaded singleton."""

    def __getattr__(self, name: str):
        return getattr(_get_taxonomy(), name)

    def __call__(self, *args, **kwargs):
        return _get_taxonomy()(*args, **kwargs)


taxonomy: TaxonomyLoader = _TaxonomyProxy()  # type: ignore[assignment]
