"""
Stage 2: Transform raw JSON to normalized CSV tables
Creates 13 tables from extracted climate risk data
"""
import json
import csv
from pathlib import Path
from typing import Dict, List, Any
from config import Config

class ClimateDataTransformer:
    """Transforms raw JSON extractions into normalized CSV tables"""

    def __init__(self):
        Config.create_folders()
        self.validation_log = []

    def transform_all(self, raw_folder: str = None):
        """Transform all raw JSON files to CSV tables"""
        raw_folder = raw_folder or Config.RAW_FOLDER
        json_files = list(Path(raw_folder).glob("*.json"))

        print(f"\n{'='*70}")
        print("STAGE 2: TRANSFORMATION")
        print(f"{'='*70}")
        print(f"Raw JSON files: {len(json_files)}")
        print(f"Output: {Config.TABLES_FOLDER}")
        print()

        # Initialize CSV files with headers
        self._initialize_csv_files()

        # Transform each document
        for i, json_file in enumerate(json_files, 1):
            print(f"[{i}/{len(json_files)}] {json_file.stem}")
            try:
                self._transform_document(json_file)
                print(f"  ✓ Transformed")
            except Exception as e:
                print(f"  ⚠️ Error: {str(e)[:100]}")

        # Save validation log
        self._save_validation_log()

        print(f"\n{'='*70}")
        print("TRANSFORMATION COMPLETE")
        print(f"{'='*70}")
        self._print_summary()

    def _print_summary(self):
        """Print summary of generated tables"""
        print(f"\nGenerated tables:")

        tables = {
            "Risk Identification": Config.RISK_ID_FOLDER,
            "Financial": Config.FINANCIAL_FOLDER,
            "Responses": Config.RESPONSES_FOLDER,
            "Management": Config.MANAGEMENT_FOLDER,
            "Metadata": Config.METADATA_FOLDER
        }

        for category, folder in tables.items():
            csv_files = list(Path(folder).glob("*.csv"))
            print(f"  {category}: {len(csv_files)} tables")
            for f in csv_files:
                row_count = sum(1 for _ in open(f, encoding='utf-8')) - 1  # Exclude header
                print(f"    - {f.name} ({row_count} rows)")

    def _initialize_csv_files(self):
        """Create CSV files with headers"""

        # 1. Frameworks (Q2)
        self._write_csv_header(
            Path(Config.RISK_ID_FOLDER) / "frameworks.csv",
            ["document_id", "framework_name", "mentioned", "section_name", "page_range",
             "tcfd_alignment_claimed", "external_assurance", "found", "source_text"]
        )

        # 2. Coverage (Q3)
        self._write_csv_header(
            Path(Config.RISK_ID_FOLDER) / "coverage.csv",
            ["document_id", "operations_coverage_pct", "supply_chain_coverage_pct",
             "geographies_covered", "business_units_covered", "assessment_methodology",
             "source_pages", "found"]
        )

        # 3. Hazards (Q4, Q12)
        self._write_csv_header(
            Path(Config.RISK_ID_FOLDER) / "hazards.csv",
            ["document_id", "hazard_type", "location", "affected_assets", "time_horizon",
             "severity_or_likelihood", "source_page", "found", "source_text"]
        )

        # 4. Scenarios (Q5)
        self._write_csv_header(
            Path(Config.RISK_ID_FOLDER) / "scenarios.csv",
            ["document_id", "scenario_name", "temperature_pathway", "time_horizon",
             "methodology_described", "scenario_analysis_scope", "source_pages", "found"]
        )

        # 5. Vulnerable Assets (Q6)
        self._write_csv_header(
            Path(Config.RISK_ID_FOLDER) / "vulnerable_assets.csv",
            ["document_id", "asset_type", "location", "climate_risk", "potential_impact",
             "materiality_assessment", "source_page", "found", "source_text"]
        )

        # 6. Supply Chain Risks (Q7)
        self._write_csv_header(
            Path(Config.RISK_ID_FOLDER) / "supply_chain_risks.csv",
            ["document_id", "commodity_or_material", "sourcing_region", "climate_hazard",
             "impact_on_supply", "tier_level", "resilience_program", "source_page", "found", "source_text"]
        )

        # 7. Comprehensive Risks (Q8, Q9, Q10)
        self._write_csv_header(
            Path(Config.RISK_ID_FOLDER) / "comprehensive_risks.csv",
            ["document_id", "risk_category", "risk_type", "risk_description", "affected_operations",
             "geographic_locations", "value_chain_segment", "potential_magnitude", "likelihood",
             "materiality_level", "materiality_justification", "source_page", "found", "source_text"]
        )

        # 8. Financial Impacts (Q11, Q18, Q23)
        self._write_csv_header(
            Path(Config.FINANCIAL_FOLDER) / "financial_impacts.csv",
            ["document_id", "impact_type", "risk_description", "financial_amount", "currency",
             "cost_type", "time_period", "geographic_location", "estimation_method",
             "climate_scenario", "modeling_approach", "capex_disclosed", "source_page", "found", "source_text"]
        )

        # 9. Adaptation (Q13-Q17, Q22)
        self._write_csv_header(
            Path(Config.RESPONSES_FOLDER) / "adaptation.csv",
            ["document_id", "adaptation_type", "measure_description", "risk_addressed",
             "location_or_scope", "implementation_status", "implementation_timeline",
             "investment_amount", "effectiveness", "initiative_name", "technology_area",
             "partners", "metric_name", "baseline_value", "current_value", "target_value",
             "target_year", "source_page", "found", "source_text"]
        )

        # 10. Governance (Q19)
        self._write_csv_header(
            Path(Config.MANAGEMENT_FOLDER) / "governance.csv",
            ["document_id", "responsible_committee", "meeting_frequency", "climate_expertise_on_board",
             "executive_role", "executive_name", "climate_committees_or_teams", "source_pages", "found"]
        )

        # 11. Business Integration (Q20)
        self._write_csv_header(
            Path(Config.MANAGEMENT_FOLDER) / "business_integration.csv",
            ["document_id", "integration_area", "business_continuity_measures", "procurement_changes",
             "site_selection_criteria", "strategic_examples", "source_pages", "found", "source_text"]
        )

        # 12. Stakeholder Engagement (Q21)
        self._write_csv_header(
            Path(Config.MANAGEMENT_FOLDER) / "stakeholder_engagement.csv",
            ["document_id", "stakeholder_type", "engagement_activity", "purpose_or_focus",
             "geographic_focus", "source_page", "found", "source_text"]
        )

        # 13. Extraction Metadata
        self._write_csv_header(
            Path(Config.METADATA_FOLDER) / "extraction_metadata.csv",
            ["document_id", "question_id", "question_status", "answer_preview",
             "found", "has_error", "validation_flags"]
        )

    def _write_csv_header(self, file_path: Path, headers: List[str]):
        """Write CSV header"""
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(headers)

    def _transform_document(self, json_file: Path):
        """Transform single document JSON to CSV rows"""
        doc_id = json_file.stem

        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Transform each question group
        self._transform_q2_frameworks(doc_id, data.get("Q2", {}))
        self._transform_q3_coverage(doc_id, data.get("Q3", {}))
        self._transform_q4_hazards(doc_id, data.get("Q4", {}))
        self._transform_q5_scenarios(doc_id, data.get("Q5", {}))
        self._transform_q6_assets(doc_id, data.get("Q6", {}))
        self._transform_q7_supply_chain(doc_id, data.get("Q7", {}))
        self._transform_q8_q9_q10_risks(doc_id, data)
        self._transform_q11_financial(doc_id, data.get("Q11", {}))
        self._transform_q12_location_risks(doc_id, data.get("Q12", {}))
        self._transform_q13_q17_q22_adaptation(doc_id, data)
        self._transform_q18_financial_disclosure(doc_id, data.get("Q18", {}))
        self._transform_q19_governance(doc_id, data.get("Q19", {}))
        self._transform_q20_integration(doc_id, data.get("Q20", {}))
        self._transform_q21_stakeholders(doc_id, data.get("Q21", {}))
        self._transform_q23_quantified_impacts(doc_id, data.get("Q23", {}))

        # Log metadata
        self._log_extraction_metadata(doc_id, data)

    def _transform_q2_frameworks(self, doc_id: str, q2: Dict):
        """Q2: Frameworks"""
        file_path = Path(Config.RISK_ID_FOLDER) / "frameworks.csv"

        frameworks = q2.get("frameworks_referenced", [])
        sections = q2.get("climate_sections", [])
        tcfd = q2.get("tcfd_alignment_claimed", False)
        assurance = q2.get("external_assurance", False)
        found = q2.get("found", True) if (frameworks or sections) else False

        if not found or not frameworks:
            self._append_csv_row(file_path, [doc_id, "", False, "", "", tcfd, assurance, False, ""])
            return

        for fw in frameworks:
            for section in (sections if sections else [{}]):
                row = [
                    doc_id, fw, True,
                    section.get("section_name", ""),
                    section.get("page_range", ""),
                    tcfd, assurance, True, ""
                ]
                self._append_csv_row(file_path, row)

    def _transform_q3_coverage(self, doc_id: str, q3: Dict):
        """Q3: Coverage"""
        file_path = Path(Config.RISK_ID_FOLDER) / "coverage.csv"
        found = q3.get("found", True) if q3 else False

        row = [
            doc_id,
            q3.get("operations_coverage_pct", ""),
            q3.get("supply_chain_coverage_pct", ""),
            "|".join(q3.get("geographies_covered", [])),
            "|".join(q3.get("business_units_covered", [])),
            q3.get("assessment_methodology", ""),
            "|".join([str(p) for p in q3.get("source_pages", [])]),
            found
        ]
        self._append_csv_row(file_path, row)

    def _transform_q4_hazards(self, doc_id: str, q4: Dict):
        """Q4: Hazards"""
        file_path = Path(Config.RISK_ID_FOLDER) / "hazards.csv"
        hazards = q4.get("hazards", [])
        found = q4.get("found", True) if hazards else False

        if not found or not hazards:
            self._append_csv_row(file_path, [doc_id, "", "", "", "", "", 0, False, ""])
            return

        for h in hazards:
            row = [
                doc_id,
                h.get("hazard_type", ""),
                h.get("location", ""),
                h.get("affected_assets", ""),
                h.get("time_horizon", ""),
                h.get("severity_or_likelihood", ""),
                h.get("source_page", 0),
                True, ""
            ]
            self._append_csv_row(file_path, row)

    def _transform_q5_scenarios(self, doc_id: str, q5: Dict):
        """Q5: Scenarios"""
        file_path = Path(Config.RISK_ID_FOLDER) / "scenarios.csv"
        scenarios = q5.get("scenarios_used", [])
        pathways = q5.get("temperature_pathways", [])
        horizons = q5.get("time_horizons", [])
        found = q5.get("found", True) if scenarios else False

        if not found or not scenarios:
            self._append_csv_row(file_path, [doc_id, "", "", "", False, "", "", False])
            return

        for scenario in scenarios:
            row = [
                doc_id, scenario,
                "|".join([str(p) for p in pathways]),
                "|".join([str(h) for h in horizons]),
                q5.get("methodology_described", False),
                q5.get("scenario_analysis_scope", ""),
                "|".join([str(p) for p in q5.get("source_pages", [])]),
                True
            ]
            self._append_csv_row(file_path, row)

    def _transform_q6_assets(self, doc_id: str, q6: Dict):
        """Q6: Vulnerable Assets"""
        file_path = Path(Config.RISK_ID_FOLDER) / "vulnerable_assets.csv"
        assets = q6.get("vulnerable_assets", [])
        found = q6.get("found", True) if assets else False

        if not found or not assets:
            self._append_csv_row(file_path, [doc_id, "", "", "", "", "", 0, False, ""])
            return

        for asset in assets:
            row = [
                doc_id,
                asset.get("asset_type", ""),
                asset.get("location", ""),
                asset.get("climate_risk", ""),
                asset.get("potential_impact", ""),
                asset.get("materiality_assessment", ""),
                asset.get("source_page", 0),
                True, ""
            ]
            self._append_csv_row(file_path, row)

    def _transform_q7_supply_chain(self, doc_id: str, q7: Dict):
        """Q7: Supply Chain Risks"""
        file_path = Path(Config.RISK_ID_FOLDER) / "supply_chain_risks.csv"
        risks = q7.get("supply_chain_risks", [])
        found = q7.get("found", True) if risks else False

        if not found or not risks:
            self._append_csv_row(file_path, [doc_id, "", "", "", "", "", "", 0, False, ""])
            return

        for risk in risks:
            row = [
                doc_id,
                risk.get("commodity_or_material", ""),
                risk.get("sourcing_region", ""),
                risk.get("climate_hazard", ""),
                risk.get("impact_on_supply", ""),
                risk.get("tier_level", ""),
                "", 
                risk.get("source_page", 0),
                True, ""
            ]
            self._append_csv_row(file_path, row)

    def _transform_q8_q9_q10_risks(self, doc_id: str, data: Dict):
        """Q8, Q9, Q10: Comprehensive Risks"""
        file_path = Path(Config.RISK_ID_FOLDER) / "comprehensive_risks.csv"

        # Q8: Identified risks
        q8 = data.get("Q8", {})
        risks = q8.get("identified_risks", [])

        for risk in risks:
            row = [
                doc_id,
                risk.get("risk_category", ""),
                risk.get("risk_type", ""),
                risk.get("risk_description", ""),
                "|".join(risk.get("affected_operations", [])),
                "|".join(risk.get("geographic_locations", [])),
                risk.get("value_chain_segment", ""),
                risk.get("potential_magnitude", ""),
                risk.get("likelihood", ""),
                "", "",
                risk.get("source_page", 0),
                True, ""
            ]
            self._append_csv_row(file_path, row)

        # Q9: Vulnerabilities
        q9 = data.get("Q9", {})
        vulns = q9.get("vulnerabilities", [])

        for vuln in vulns:
            row = [
                doc_id, "vulnerability", "physical",
                vuln.get("vulnerability_reason", ""),
                "", vuln.get("location", ""),
                "", vuln.get("exposure_level", ""),
                "", "", "",
                vuln.get("source_page", 0),
                True, ""
            ]
            self._append_csv_row(file_path, row)

    def _transform_q11_financial(self, doc_id: str, q11: Dict):
        """Q11: Financial Costs"""
        file_path = Path(Config.FINANCIAL_FOLDER) / "financial_impacts.csv"
        costs = q11.get("cost_estimates", [])
        found = q11.get("found", True) if costs else False

        if not found or not costs:
            self._append_csv_row(file_path, [doc_id, "cost_estimate", "", "", "", "", "", "", "", "", "", False, 0, False, ""])
            return

        for cost in costs:
            row = [
                doc_id, "cost_estimate",
                cost.get("risk_description", ""),
                cost.get("cost_amount", ""),
                cost.get("currency", ""),
                cost.get("cost_type", ""),
                cost.get("time_period", ""),
                cost.get("geographic_location", ""),
                cost.get("estimation_method", ""),
                cost.get("scenario_if_applicable", ""),
                "", False,
                cost.get("source_page", 0),
                True, ""
            ]
            self._append_csv_row(file_path, row)

    def _transform_q12_location_risks(self, doc_id: str, q12: Dict):
        """Q12: Location-specific risks (add to hazards)"""
        file_path = Path(Config.RISK_ID_FOLDER) / "hazards.csv"
        locations = q12.get("location_specific_risks", [])

        for loc in locations:
            for risk in loc.get("climate_risks_identified", []):
                row = [
                    doc_id, risk,
                    loc.get("location_name", ""),
                    "", "",
                    loc.get("risk_level", ""),
                    loc.get("source_page", 0),
                    True, ""
                ]
                self._append_csv_row(file_path, row)

    def _transform_q13_q17_q22_adaptation(self, doc_id: str, data: Dict):
        """Q13-Q17, Q22: Adaptation measures"""
        file_path = Path(Config.RESPONSES_FOLDER) / "adaptation.csv"

        # Q13: Risk-adaptation linkage
        q13 = data.get("Q13", {})
        for link in q13.get("risk_adaptation_linkage", []):
            for measure in link.get("adaptation_measures", []):
                row = [
                    doc_id, "risk_linked",
                    measure.get("measure_description", ""),
                    link.get("climate_risk", ""),
                    measure.get("measure_location", ""),
                    measure.get("implementation_status", ""),
                    measure.get("implementation_timeline", ""),
                    measure.get("investment_amount", ""),
                    measure.get("effectiveness_or_impact", ""),
                    "", "", "", "", "", "", "", "",
                    measure.get("source_page", 0),
                    True, ""
                ]
                self._append_csv_row(file_path, row)

        # Q14: Implemented measures
        q14 = data.get("Q14", {})
        for measure in q14.get("implemented_measures", []):
            row = [
                doc_id, "implemented",
                measure.get("measure_description", ""),
                measure.get("risk_addressed", ""),
                measure.get("location_or_scope", ""),
                measure.get("implementation_status", ""),
                measure.get("implementation_year", ""),
                "", "", "", "", "", "", "", "", "", "",
                measure.get("source_page", 0),
                True, ""
            ]
            self._append_csv_row(file_path, row)

        # Q15: Resilience programs
        q15 = data.get("Q15", {})
        for program in q15.get("resilience_programs", []):
            row = [
                doc_id, "supply_chain_program",
                program.get("description", ""),
                "", program.get("geographic_focus", ""),
                "", "", "", "",
                program.get("program_name", ""),
                "", "", "", "", "", "", "",
                program.get("source_page", 0),
                True, ""
            ]
            self._append_csv_row(file_path, row)

        # Q16: Nature-based solutions
        q16 = data.get("Q16", {})
        for nbs in q16.get("nature_based_solutions", []):
            row = [
                doc_id, "nature_based",
                nbs.get("description", ""),
                "", nbs.get("geographic_location", ""),
                "", "", "",
                nbs.get("climate_adaptation_benefit", ""),
                nbs.get("initiative_name", ""),
                "", "", "", "", "", "", "",
                nbs.get("source_page", 0),
                True, ""
            ]
            self._append_csv_row(file_path, row)

        # Q17: Innovation initiatives
        q17 = data.get("Q17", {})
        for init in q17.get("innovation_initiatives", []):
            row = [
                doc_id, "innovation", "",
                init.get("climate_risk_addressed", ""),
                "", init.get("stage", ""),
                "", "", "",
                init.get("initiative_name", ""),
                init.get("technology_or_research_area", ""),
                "|".join(init.get("partners_or_collaborators", [])),
                "", "", "", "", "",
                init.get("source_page", 0),
                True, ""
            ]
            self._append_csv_row(file_path, row)

        # Q22: KPIs/Metrics
        q22 = data.get("Q22", {})
        for metric in q22.get("adaptation_metrics", []):
            row = [
                doc_id, "kpi", "", "", "", "", "", "", "", "", "",
                "", metric.get("metric_name", ""),
                metric.get("baseline_value", ""),
                metric.get("current_value", ""),
                metric.get("target_value", ""),
                metric.get("target_year", ""),
                metric.get("source_page", 0),
                True, ""
            ]
            self._append_csv_row(file_path, row)

    def _transform_q18_financial_disclosure(self, doc_id: str, q18: Dict):
        """Q18: Financial disclosure (CapEx/OpEx)"""
        file_path = Path(Config.FINANCIAL_FOLDER) / "financial_impacts.csv"
        fd = q18.get("financial_disclosure", {})

        if not fd:
            return

        row = [
            doc_id, "capex_opex",
            "Adaptation investment",
            fd.get("adaptation_investment_amount", ""),
            fd.get("currency", ""),
            "capex" if fd.get("capex_disclosed", False) else "unknown",
            fd.get("time_period", ""),
            "", "", "", "",
            fd.get("capex_disclosed", False),
            fd.get("source_page", 0),
            True, ""
        ]
        self._append_csv_row(file_path, row)

    def _transform_q19_governance(self, doc_id: str, q19: Dict):
        """Q19: Governance"""
        file_path = Path(Config.MANAGEMENT_FOLDER) / "governance.csv"
        board = q19.get("board_oversight", {})
        exec_resp = q19.get("executive_responsibility", {})
        teams = q19.get("climate_committees_or_teams", [])
        found = q19.get("found", True) if (board or exec_resp) else False

        row = [
            doc_id,
            board.get("responsible_committee", ""),
            board.get("meeting_frequency", ""),
            board.get("climate_expertise_on_board", ""),
            exec_resp.get("role_or_title", ""),
            exec_resp.get("name_if_disclosed", ""),
            "|".join(teams),
            "|".join([str(p) for p in q19.get("source_pages", [])]),
            found
        ]
        self._append_csv_row(file_path, row)

    def _transform_q20_integration(self, doc_id: str, q20: Dict):
        """Q20: Business Integration"""
        file_path = Path(Config.MANAGEMENT_FOLDER) / "business_integration.csv"
        found = q20.get("found", True) if q20 else False

        row = [
            doc_id,
            "|".join(q20.get("integration_areas", [])),
            q20.get("business_continuity_measures", ""),
            q20.get("procurement_or_sourcing_changes", ""),
            q20.get("site_selection_criteria", ""),
            "|".join(q20.get("strategic_examples", [])),
            "|".join([str(p) for p in q20.get("source_pages", [])]),
            found, ""
        ]
        self._append_csv_row(file_path, row)

    def _transform_q21_stakeholders(self, doc_id: str, q21: Dict):
        """Q21: Stakeholder Engagement"""
        file_path = Path(Config.MANAGEMENT_FOLDER) / "stakeholder_engagement.csv"
        stakeholders = q21.get("stakeholders_engaged", [])
        found = q21.get("found", True) if stakeholders else False

        if not found or not stakeholders:
            self._append_csv_row(file_path, [doc_id, "", "", "", "", 0, False, ""])
            return

        for sh in stakeholders:
            row = [
                doc_id,
                sh.get("stakeholder_type", ""),
                sh.get("engagement_activity", ""),
                sh.get("purpose_or_focus", ""),
                sh.get("geographic_focus", ""),
                sh.get("source_page", 0),
                True, ""
            ]
            self._append_csv_row(file_path, row)

    def _transform_q23_quantified_impacts(self, doc_id: str, q23: Dict):
        """Q23: Quantified financial impacts"""
        file_path = Path(Config.FINANCIAL_FOLDER) / "financial_impacts.csv"
        impacts = q23.get("quantified_impacts", [])

        for impact in impacts:
            row = [
                doc_id, "scenario_impact",
                impact.get("risk_type", ""),
                impact.get("financial_impact_amount", ""),
                impact.get("currency", ""),
                "scenario_based",
                impact.get("time_horizon", ""),
                "",
                impact.get("modeling_approach", ""),
                impact.get("climate_scenario", ""),
                impact.get("modeling_approach", ""),
                False,
                impact.get("source_page", 0),
                True, ""
            ]
            self._append_csv_row(file_path, row)

    def _log_extraction_metadata(self, doc_id: str, data: Dict):
        """Log metadata for each question"""
        file_path = Path(Config.METADATA_FOLDER) / "extraction_metadata.csv"

        for qid in data.keys():
            q_data = data[qid]
            found = q_data.get("found", True)
            has_error = "error" in q_data
            answer_preview = str(q_data)[:100]

            row = [
                doc_id, qid,
                "completed" if not has_error else "error",
                answer_preview,
                found, has_error, ""
            ]
            self._append_csv_row(file_path, row)

    def _append_csv_row(self, file_path: Path, row: List[Any]):
        """Append row to CSV file"""
        with open(file_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            cleaned_row = []
            for item in row:
                if isinstance(item, (list, dict)):
                    cleaned_row.append(str(item))
                elif isinstance(item, str):
                    cleaned_row.append(item.replace(',', ';').replace('"', "'"))
                else:
                    cleaned_row.append(item)
            writer.writerow(cleaned_row)

    def _save_validation_log(self):
        """Save validation log"""
        if not self.validation_log:
            return

        log_path = Path(Config.METADATA_FOLDER) / "validation_log.csv"
        with open(log_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["document_id", "field", "issue", "severity"])
            writer.writerows(self.validation_log)
