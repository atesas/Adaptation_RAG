# =============================================================================
# tests/test_taxonomy.py
# Tests for taxonomy.py — TaxonomyLoader
# =============================================================================

import pytest
from pathlib import Path
from unittest.mock import patch
import yaml

from taxonomy import TaxonomyLoader
from schemas.passage import IRO_TYPES, VALUE_CHAIN_POSITIONS, EVIDENCE_QUALITY_LEVELS, TIME_HORIZONS


@pytest.fixture
def minimal_taxonomy(tmp_path: Path) -> Path:
    data = {
        "taxonomy_version": "1.0",
        "last_updated": "2026-03",
        "sector_focus": ["food_and_beverage", "agriculture_food_systems"],
        "hazards": {
            "label": "Climate-Related Physical Hazards",
            "seed_source": "ESRS E1 AR11",
            "physical_acute": {
                "label": "Acute Physical Hazards",
                "seed_source": "ESRS E1 AR11",
                "extreme_heat_event": {
                    "label": "Extreme heat event",
                    "seed_source": "ESRS E1 AR11",
                    "seed_ref": "row 1",
                },
            },
            "physical_chronic": {
                "label": "Chronic Physical Hazards",
                "seed_source": "ESRS E1 AR11",
                "water_stress": {
                    "label": "Water stress",
                    "seed_source": "ESRS E1 AR11",
                    "seed_ref": "row 12",
                },
            },
        },
        "responses": {
            "label": "Adaptation Responses",
            "seed_source": "TCFD",
            "operational_adaptation": {
                "label": "Operational adaptation",
                "seed_source": "TCFD",
            },
        },
        "iro_type": {
            "label": "IRO Type",
            "seed_source": "CSRD",
        },
        "value_chain_position": {
            "label": "Value Chain Position",
            "seed_source": "CSDDD",
        },
        "evidence_quality": {
            "label": "Evidence Quality",
            "seed_source": "internal",
        },
    }
    path = tmp_path / "taxonomy.yaml"
    with open(path, "w") as fh:
        yaml.dump(data, fh)
    return path


class TestTaxonomyLoaderInit:

    def test_loads_without_error(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        assert loader is not None

    def test_detects_top_level_nodes(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        assert "hazards" in loader._top_level_nodes
        assert "responses" in loader._top_level_nodes

    def test_excludes_metadata_from_nodes(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        assert "taxonomy_version" not in loader._top_level_nodes
        assert "last_updated" not in loader._top_level_nodes
        assert "sector_focus" not in loader._top_level_nodes


class TestGetNode:

    def test_returns_top_level_node(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        node = loader.get_node("hazards")
        assert node is not None
        assert node["label"] == "Climate-Related Physical Hazards"

    def test_returns_nested_node(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        node = loader.get_node("hazards.physical_acute.extreme_heat_event")
        assert node is not None
        assert node["label"] == "Extreme heat event"

    def test_returns_none_for_missing_path(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        assert loader.get_node("hazards.nonexistent") is None

    def test_returns_none_for_empty_path(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        assert loader.get_node("") is None


class TestIsSeedCategory:

    def test_returns_true_for_seed_node(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        assert loader.is_seed_category("hazards.physical_acute.extreme_heat_event") is True

    def test_returns_false_for_missing_path(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        assert loader.is_seed_category("hazards.nonexistent.node") is False


class TestGetAllSubcategoryPaths:

    def test_returns_list(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        paths = loader.get_all_subcategory_paths()
        assert isinstance(paths, list)
        assert len(paths) > 0

    def test_includes_known_path(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        paths = loader.get_all_subcategory_paths()
        assert any("extreme_heat_event" in p for p in paths)


class TestValidateClassification:

    def test_valid_classification_passes(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        stage_b = {
            "category": "hazards",
            "subcategory": "hazards.physical_acute.extreme_heat_event",
            "iro_type": "risk.physical_acute_risk",
            "value_chain_position": "own_operations",
            "evidence_quality": "implemented",
            "time_horizon": "medium",
            "sector_relevance": ["food_and_beverage"],
            "frameworks_referenced": ["csrd_esrs"],
        }
        is_valid, errors = loader.validate_classification(stage_b)
        assert is_valid is True
        assert errors == []

    def test_invalid_category_fails(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        stage_b = {
            "category": "not_a_category",
            "subcategory": "hazards.physical_acute.extreme_heat_event",
            "iro_type": "risk.physical_acute_risk",
            "value_chain_position": "own_operations",
            "evidence_quality": "implemented",
            "time_horizon": "medium",
            "sector_relevance": [],
            "frameworks_referenced": [],
        }
        is_valid, errors = loader.validate_classification(stage_b)
        assert is_valid is False
        assert any("category" in e for e in errors)

    def test_invalid_iro_type_fails(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        stage_b = {
            "category": "hazards",
            "subcategory": "hazards.physical_acute.extreme_heat_event",
            "iro_type": "not_a_valid_iro",
            "value_chain_position": "own_operations",
            "evidence_quality": "implemented",
            "time_horizon": "medium",
            "sector_relevance": [],
            "frameworks_referenced": [],
        }
        is_valid, errors = loader.validate_classification(stage_b)
        assert is_valid is False
        assert any("iro_type" in e for e in errors)

    def test_invalid_evidence_quality_fails(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        stage_b = {
            "category": "responses",
            "subcategory": "responses.operational_adaptation",
            "iro_type": "opportunity.resilience",
            "value_chain_position": "own_operations",
            "evidence_quality": "not_a_level",
            "time_horizon": "short",
            "sector_relevance": [],
            "frameworks_referenced": [],
        }
        is_valid, errors = loader.validate_classification(stage_b)
        assert is_valid is False


class TestGetTaxonomyExcerpt:

    def test_returns_string(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        excerpt = loader.get_taxonomy_excerpt_for_hint("hazard")
        assert isinstance(excerpt, str)
        assert len(excerpt) > 0

    def test_contains_always_include_nodes(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        excerpt = loader.get_taxonomy_excerpt_for_hint("hazard")
        assert "iro_type" in excerpt or "IRO_TYPES" in excerpt

    def test_contains_controlled_vocab(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        excerpt = loader.get_taxonomy_excerpt_for_hint("adaptation")
        assert "EVIDENCE_QUALITY_LEVELS" in excerpt or "implemented" in excerpt

    def test_unknown_hint_still_returns_excerpt(self, minimal_taxonomy: Path) -> None:
        loader = TaxonomyLoader(minimal_taxonomy)
        excerpt = loader.get_taxonomy_excerpt_for_hint("unknown_hint")
        assert isinstance(excerpt, str)
        assert len(excerpt) > 0


class TestRecordCandidateExtension:

    def test_writes_jsonl_file(self, minimal_taxonomy: Path, tmp_path: Path, monkeypatch) -> None:
        import taxonomy as tx_module
        candidate_file = tmp_path / "candidate_extensions.jsonl"
        monkeypatch.setattr(tx_module, "_CANDIDATE_EXTENSIONS_FILE", candidate_file)

        loader = TaxonomyLoader(minimal_taxonomy)
        loader.record_candidate_extension(
            value="novel_heat_event",
            hint="hazard",
            source_doc_id="doc-001",
            frequency=1,
        )
        assert candidate_file.exists()
        import json
        with open(candidate_file) as fh:
            record = json.loads(fh.readline())
        assert record["value"] == "novel_heat_event"
        assert record["hint"] == "hazard"
