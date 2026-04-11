# =============================================================================
# suggest_taxonomy.py
# Analyze Q&A results and suggest taxonomy improvements.
#
# Usage:
#   python suggest_taxonomy.py \
#     --input results/qa_results.json \
#     --output results/taxonomy_suggestions.json \
#     --taxonomy _design/taxonomy.yaml
# =============================================================================
import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml


def load_taxonomy(taxonomy_path: Path) -> dict:
    """Load taxonomy from YAML file."""
    with open(taxonomy_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_qa_results(results_path: Path) -> dict:
    """Load Q&A results from JSON."""
    with open(results_path, encoding="utf-8") as f:
        return json.load(f)


def build_taxonomy_tree(taxonomy: dict) -> dict:
    """Build a searchable tree of all taxonomy categories and subcategories."""
    tree = {
        "categories": {},  # category_name -> {subcategories}
        "all_nodes": {},  # full_path -> node_info
        "keywords": {},   # keyword -> taxonomy_path
    }
    
    # Define top-level categories we want to track
    top_level_mappings = {
        "hazards": "hazards",
        "responses": "responses",
        "impacts": "impacts",
        "governance": "governance",
        "finance": "finance",
        "strategy": "strategy",
        "evidence_quality": "evidence_quality",
    }
    
    # Keywords that map to taxonomy nodes
    keyword_mappings = {
        # Hazards - Acute
        "extreme heat": "hazards.physical_acute.extreme_heat_event",
        "heatwave": "hazards.physical_acute.extreme_heat_event",
        "flood": "hazards.physical_acute.flood_event",
        "storm": "hazards.physical_acute.storm_wind",
        "wildfire": "hazards.physical_acute.wildfire",
        "drought": "hazards.physical_acute.drought",
        "cold spell": "hazards.physical_acute.cold_spell_frost",
        
        # Hazards - Chronic
        "water stress": "hazards.physical_chronic.water_stress",
        "temperature shift": "hazards.physical_chronic.temperature_shift",
        "biodiversity loss": "hazards.physical_chronic.biodiversity_loss",
        "sea level": "hazards.physical_chronic.sea_level_rise",
        
        # Responses - Adaptation
        "regenerative agriculture": "responses.adaptation.regenerative_agriculture",
        "no-till": "responses.adaptation.regenerative_agriculture",
        "cover cropping": "responses.adaptation.regenerative_agriculture",
        "agroecology": "responses.adaptation.regenerative_agriculture",
        "water efficiency": "responses.adaptation.water_efficiency",
        "drip irrigation": "responses.adaptation.water_efficiency",
        "water recycling": "responses.adaptation.water_efficiency",
        "flood barriers": "responses.adaptation.infrastructure_resilience",
        "cooling systems": "responses.adaptation.heat_management",
        "heat management": "responses.adaptation.heat_management",
        "early warning": "responses.adaptation.digital_early_warning",
        "satellite monitoring": "responses.adaptation.digital_early_warning",
        "sensor network": "responses.adaptation.digital_early_warning",
        "insurance": "responses.adaptation.insurance_risk_transfer",
        "parametric insurance": "responses.adaptation.insurance_risk_transfer",
        
        # Responses - Nature-based
        "nature-based": "responses.nature_based",
        "agroforestry": "responses.nature_based",
        "mangrove restoration": "responses.nature_based",
        "watershed restoration": "responses.nature_based",
        
        # Supply chain
        "supply chain": "responses.supply_chain_resilience",
        "supplier adaptation": "responses.supply_chain_resilience",
        
        # Impacts
        "revenue impact": "impacts.financial",
        "cost impact": "impacts.financial",
        "operational impact": "impacts.operational",
        
        # Governance
        "board oversight": "governance.board_oversight",
        "climate risk committee": "governance.board_oversight",
        
        # Finance
        "green bond": "finance.green_bond",
        "climate finance": "finance.climate_finance",
        "adaptation investment": "finance.adaptation_investment",
    }
    
    tree["keywords"] = keyword_mappings
    return tree


def extract_concepts_with_taxonomy_paths(answer: str, taxonomy_tree: dict) -> list[dict]:
    """Extract concepts and map them to taxonomy paths."""
    found_concepts = []
    answer_lower = answer.lower()
    
    for keyword, taxonomy_path in taxonomy_tree["keywords"].items():
        if keyword in answer_lower:
            # Extract a snippet around the keyword
            pos = answer_lower.find(keyword)
            start = max(0, pos - 50)
            end = min(len(answer), pos + len(keyword) + 50)
            snippet = answer[start:end].strip()
            
            found_concepts.append({
                "keyword": keyword,
                "taxonomy_path": taxonomy_path,
                "snippet": snippet,
            })
    
    return found_concepts


def find_framework_mappings(text: str, taxonomy: dict) -> list[dict]:
    """Find framework references in text."""
    frameworks = taxonomy.get("frameworks", {})
    mappings = []
    
    text_lower = text.lower()
    skip_fields = {"label", "definition"}
    
    for fw_key, fw_data in frameworks.items():
        if fw_key in skip_fields:
            continue
        
        if isinstance(fw_data, str):
            fw_label = fw_data.lower()
        else:
            fw_label = fw_data.get("label", "").lower()
        
        # Check for references
        if fw_key == "tcfd":
            if any(word in text_lower for word in ["tcfd", "task force", "climate-related financial"]):
                mappings.append({
                    "framework": "tcfd",
                    "dimension": _infer_tcfd_dimension(text),
                })
        elif fw_key in ["esrs", "csrd_esrs"]:
            if any(word in text_lower for word in ["esrs", "csrd", "e1", "e2", "e3", "e4"]):
                mappings.append({
                    "framework": "csrd_esrs",
                    "standard": _infer_esrs_standard(text),
                })
        elif fw_key == "tnfd":
            if any(word in text_lower for word in ["tnfd", "nature-related", "nature positive"]):
                mappings.append({
                    "framework": "tnfd",
                })
        elif fw_key == "gri":
            if "gri" in text_lower or "global reporting" in text_lower:
                mappings.append({
                    "framework": "gri",
                })
    
    return mappings


def _infer_tcfd_dimension(text: str) -> str:
    """Infer TCFD dimension from text."""
    text_lower = text.lower()
    
    if any(word in text_lower for word in ["governance", "board", "management", "oversight"]):
        return "Governance"
    elif any(word in text_lower for word in ["strategy", "scenario", "risk", "opportunity"]):
        return "Strategy"
    elif any(word in text_lower for word in ["risk management", "identify", "assess", "manage"]):
        return "Risk Management"
    elif any(word in text_lower for word in ["metric", "target", "kpi", "indicator", "disclose"]):
        return "Metrics & Targets"
    
    return "Unknown"


def _infer_esrs_standard(text: str) -> str:
    """Infer ESRS standard from text."""
    text_lower = text.lower()
    
    if "e1" in text_lower or "climate change" in text_lower:
        return "ESRS E1"
    elif "e2" in text_lower or "pollution" in text_lower:
        return "ESRS E2"
    elif "e3" in text_lower or "water" in text_lower:
        return "ESRS E3"
    elif "e4" in text_lower or "biodiversity" in text_lower:
        return "ESRS E4"
    elif "s1" in text_lower or "workforce" in text_lower:
        return "ESRS S1"
    
    return "ESRS General"


def analyze_answers(qa_results: dict, taxonomy: dict) -> dict:
    """Analyze Q&A answers and generate taxonomy suggestions."""
    
    taxonomy_tree = build_taxonomy_tree(taxonomy)
    
    suggestions = {
        "new_subcategories": [],
        "new_categories": [],
        "new_framework_mappings": [],
        "taxonomy_gaps": [],
    }
    
    # Track mappings found
    taxonomy_matches = {}  # path -> [sources]
    unmatched_concepts = {}  # concept -> [sources]
    framework_sources = {}
    
    # Process each document's answers
    for doc_result in qa_results.get("results", []):
        doc_title = doc_result.get("document", {}).get("title", "Unknown")
        
        for answer_obj in doc_result.get("answers", []):
            answer_text = answer_obj.get("answer", "")
            question = answer_obj.get("question", "")
            
            # Skip empty answers
            if not answer_text or "does not provide" in answer_text.lower() or "does not mention" in answer_text.lower():
                continue
            
            # Extract concepts with taxonomy paths
            concepts = extract_concepts_with_taxonomy_paths(answer_text, taxonomy_tree)
            
            for concept in concepts:
                path = concept["taxonomy_path"]
                if path:
                    if path not in taxonomy_matches:
                        taxonomy_matches[path] = []
                    taxonomy_matches[path].append({
                        "document": doc_title,
                        "question": question,
                        "keyword": concept["keyword"],
                        "snippet": concept["snippet"],
                    })
                else:
                    keyword = concept["keyword"]
                    if keyword not in unmatched_concepts:
                        unmatched_concepts[keyword] = []
                    unmatched_concepts[keyword].append({
                        "document": doc_title,
                        "question": question,
                        "snippet": concept["snippet"],
                    })
            
            # Find framework mappings
            fw_mappings = find_framework_mappings(answer_text, taxonomy)
            for fw in fw_mappings:
                fw_key = fw.get("framework", "")
                if fw_key not in framework_sources:
                    framework_sources[fw_key] = []
                framework_sources[fw_key].append({
                    "document": doc_title,
                    "question": question,
                    "dimension": fw.get("dimension", ""),
                    "standard": fw.get("standard", ""),
                })
    
    # Process taxonomy matches into suggestions format
    for path, sources in taxonomy_matches.items():
        parts = path.split(".")
        category = parts[0] if parts else "unknown"
        subcategory = parts[-1] if len(parts) > 1 else path
        
        # Get unique documents
        docs = list(set(s["document"] for s in sources))
        
        # Get first 3 snippets as evidence
        evidence = []
        for s in sources[:3]:
            evidence.append({
                "document": s["document"],
                "question": s["question"],
                "snippet": s["snippet"],
            })
        
        suggestions["new_subcategories"].append({
            "taxonomy_path": path,
            "category": category,
            "subcategory": subcategory,
            "documents_found": docs,
            "mention_count": len(sources),
            "evidence": evidence,
        })
    
    # Process unmatched concepts as gaps
    for concept, sources in unmatched_concepts.items():
        docs = list(set(s["document"] for s in sources))
        evidence = []
        for s in sources[:2]:
            evidence.append({
                "document": s["document"],
                "question": s["question"],
                "snippet": s["snippet"][:200],
            })
        
        suggestions["taxonomy_gaps"].append({
            "concept": concept,
            "documents_found": docs,
            "mention_count": len(sources),
            "evidence": evidence,
            "suggested_path": _suggest_path(concept),
        })
    
    # Process framework mappings
    for fw_key, sources in framework_sources.items():
        docs = list(set(s["document"] for s in sources))
        evidence = []
        for s in sources[:3]:
            evidence.append({
                "document": s["document"],
                "question": s["question"],
                "dimension": s.get("dimension", ""),
                "standard": s.get("standard", ""),
            })
        
        suggestions["new_framework_mappings"].append({
            "framework": fw_key,
            "documents_found": docs,
            "mention_count": len(sources),
            "evidence": evidence,
        })
    
    # Sort by mention count
    suggestions["new_subcategories"].sort(key=lambda x: x["mention_count"], reverse=True)
    suggestions["taxonomy_gaps"].sort(key=lambda x: x["mention_count"], reverse=True)
    suggestions["new_framework_mappings"].sort(key=lambda x: x["mention_count"], reverse=True)
    
    return suggestions


def _suggest_path(concept: str) -> str:
    """Suggest a taxonomy path for an unmatched concept."""
    concept_lower = concept.lower()
    
    suggestions = {
        "biodiversity": "responses.biodiversity_strategy",
        "supply chain resilience": "responses.supply_chain_resilience",
        "carbon capture": "responses.mitigation.carbon_capture",
        "carbon sequestration": "responses.adaptation.regenerative_agriculture",
        "soil health": "responses.adaptation.regenerative_agriculture",
        "climate finance": "finance.climate_finance",
        "emissions reduction": "responses.mitigation.ghg_reduction",
        "renewable energy": "responses.mitigation.renewable_energy",
    }
    
    for key, path in suggestions.items():
        if key in concept_lower:
            return path
    
    return "responses.adaptation.new_subcategory"


def main():
    parser = argparse.ArgumentParser(
        description="Analyze Q&A results and suggest taxonomy improvements",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python suggest_taxonomy.py --input results/qa_results.json
  python suggest_taxonomy.py --input results/qa_results.json --taxonomy _design/taxonomy.yaml
""",
    )
    
    parser.add_argument("--input", "-i", type=str, required=True,
                        help="Path to Q&A results JSON file")
    parser.add_argument("--output", "-o", type=str, default=None,
                        help="Path for output suggestions JSON")
    parser.add_argument("--taxonomy", "-t", type=str, default="_design/taxonomy.yaml",
                        help="Path to taxonomy YAML file")
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    output_path = Path(args.output) if args.output else Path("results/taxonomy_suggestions.json")
    taxonomy_path = Path(args.taxonomy)
    
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)
    
    if not taxonomy_path.exists():
        print(f"Error: Taxonomy file not found: {taxonomy_path}")
        sys.exit(1)
    
    print(f"Loading Q&A results from: {input_path}")
    qa_results = load_qa_results(input_path)
    
    print(f"Loading taxonomy from: {taxonomy_path}")
    taxonomy = load_taxonomy(taxonomy_path)
    
    print("Analyzing answers against taxonomy...")
    suggestions = analyze_answers(qa_results, taxonomy)
    
    output = {
        "generated_at": datetime.utcnow().isoformat(),
        "source_file": str(input_path),
        "taxonomy_file": str(taxonomy_path),
        "total_documents": qa_results.get("total_documents", 0),
        "total_questions": len(qa_results.get("questions", [])),
        "suggestions": suggestions,
    }
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    
    print(f"\nTaxonomy suggestions saved to: {output_path}")
    print(f"\nSummary:")
    print(f"  Taxonomy matches (existing nodes): {len(suggestions['new_subcategories'])}")
    print(f"  Framework mappings: {len(suggestions['new_framework_mappings'])}")
    print(f"  Taxonomy gaps (new concepts): {len(suggestions['taxonomy_gaps'])}")
    
    if suggestions["new_subcategories"]:
        print(f"\nTop taxonomy matches:")
        for item in suggestions["new_subcategories"][:5]:
            print(f"  - {item['taxonomy_path']}: {item['mention_count']} mentions")
    
    if suggestions["taxonomy_gaps"]:
        print(f"\nTop gaps (concepts not in taxonomy):")
        for gap in suggestions["taxonomy_gaps"][:5]:
            print(f"  - {gap['concept']}: {gap['mention_count']} mentions")


if __name__ == "__main__":
    main()
