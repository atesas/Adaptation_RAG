#!/usr/bin/env python3
# =============================================================================
# gcf_export.py
# Export GCF approved project data to CSV files for exploration.
#
# No Azure or OpenAI credentials required — calls the public GCF Portfolio API.
#
# Usage
# -----
# All projects (list metadata only, fast):
#   python gcf_export.py --no-details
#
# All projects with full per-project detail:
#   python gcf_export.py
#
# With filters (only fetch details for matching projects):
#   python gcf_export.py \
#       --theme Adaptation Cross-cutting \
#       --status "Under Implementation" \
#       --result-areas "Livelihoods of people and communities" \
#                      "Health, food, and water security" \
#       --min-funding 1000000
#
# Custom output directory:
#   python gcf_export.py --output exports/gcf_2026/
#
# Output files (all in --output directory)
# -----------------------------------------
#   gcf_projects.csv        One row per project — all scalar fields + nested
#                           arrays summarised into human-readable columns.
#   gcf_result_areas.csv    One row per project × result area.
#   gcf_countries.csv       One row per project × country.
#   gcf_entities.csv        One row per project × implementing entity.
#   gcf_disbursements.csv   One row per disbursement tranche.
#   gcf_funding.csv         One row per funding instrument / source.
# =============================================================================

import argparse
import csv
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

_GCF_LIST_URL = "https://api.gcfund.org/v1/projects"
_GCF_DETAIL_URL = "https://api.gcfund.org/v1/projects/{project_id}"
_RETRY_BACKOFF = [0, 2, 4, 8]
_RATE_LIMIT_RPM = 20  # conservative default
_MIN_GAP = 60.0 / _RATE_LIMIT_RPM  # seconds between detail requests


# =============================================================================
# Network helpers
# =============================================================================

def _get(url: str, timeout: int = 60) -> dict | list:
    """GET with simple retry backoff. Raises on persistent failure."""
    last_exc: Exception = RuntimeError("No attempts made")
    for attempt, wait in enumerate(_RETRY_BACKOFF):
        if wait:
            logger.debug("Retry %d — waiting %ds", attempt, wait)
            time.sleep(wait)
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.warning("Attempt %d failed for %s: %s", attempt + 1, url, exc)
            last_exc = exc
    raise RuntimeError(f"GET {url} failed after {len(_RETRY_BACKOFF)} attempts: {last_exc}") from last_exc


def fetch_list() -> list[dict]:
    """GET /v1/projects — returns all approved GCF projects."""
    logger.info("Fetching project list from %s", _GCF_LIST_URL)
    data = _get(_GCF_LIST_URL)
    projects = data if isinstance(data, list) else data.get("results", [])
    logger.info("  %d projects returned", len(projects))
    return projects


def fetch_detail(project_id: int) -> dict | None:
    """GET /v1/projects/{project_id} — authoritative detail for one project."""
    url = _GCF_DETAIL_URL.format(project_id=project_id)
    try:
        return _get(url, timeout=30)
    except Exception as exc:
        logger.warning("  Detail fetch failed for project %s: %s", project_id, exc)
        return None


# =============================================================================
# Filtering (same logic as GCFAPIAdapter._matches_filters)
# =============================================================================

def matches_filters(project: dict, filters: dict) -> bool:
    """Return True if the project passes all configured filters."""
    if not filters:
        return True

    if "theme" in filters:
        if project.get("Theme") not in filters["theme"]:
            return False

    if "status" in filters:
        if project.get("Status") not in filters["status"]:
            return False

    if "size" in filters:
        if project.get("Size") not in filters["size"]:
            return False

    if "sector" in filters:
        if project.get("Sector") not in filters["sector"]:
            return False

    if "min_gcf_funding" in filters:
        if (project.get("TotalGCFFunding") or 0) < filters["min_gcf_funding"]:
            return False

    if "countries_iso3" in filters:
        project_isos = {
            c.get("ISO3", "") for c in (project.get("Countries") or [])
            if isinstance(c, dict)
        }
        if not project_isos.intersection(filters["countries_iso3"]):
            return False

    if "result_areas" in filters:
        allowed = set(filters["result_areas"])
        non_zero = {
            ra.get("Area", "")
            for ra in (project.get("ResultAreas") or [])
            if ra.get("Value") not in ("0.00%", "0%", None, 0, "")
            and ra.get("Value", "0.00%") != "0.00%"
        }
        if not non_zero.intersection(allowed):
            return False

    return True


# =============================================================================
# CSV writers
# =============================================================================

def _fmt_date(value: str | None) -> str:
    """Strip the time component from ISO 8601 timestamps."""
    if not value:
        return ""
    return value.split("T")[0]


def _csv_list(items: list[str]) -> str:
    """Join a list as a semicolon-separated string for CSV cells."""
    return "; ".join(str(i) for i in items if i)


def write_projects_csv(projects: list[dict], path: Path) -> None:
    """
    One row per project. Nested arrays are summarised into readable columns:
      - countries:     semicolon-separated CountryName (ISO3) pairs
      - entities:      semicolon-separated entity acronyms
      - result_areas:  semicolon-separated non-zero area allocations
      - disbursements: count + total USD disbursed
    """
    fields = [
        "ProjectsID", "ApprovedRef", "BoardMeeting", "ProjectName",
        "Theme", "Sector", "Size", "RiskCategory", "Status",
        "ApprovalDate", "DateImplementationStart", "StartDate", "EndDate",
        "DateClosing", "DateCompletion", "DateCancelled", "DurationMonths",
        "TotalGCFFunding_USD", "TotalCoFinancing_USD", "TotalValue_USD",
        "DirectBeneficiaries", "IndirectBeneficiaries", "LifeTimeCO2_tCO2eq",
        "Countries", "Countries_ISO3", "Regions", "HasLDC", "HasSIDS",
        "PrimaryEntity", "PrimaryEntity_Acronym", "PrimaryEntity_Access",
        "PrimaryEntity_Type", "AllEntities",
        "NonZeroResultAreas",
        "TotalDisbursed_USD", "DisbursementCount",
        "GCF_Grant_USD", "CoFinancing_Grant_USD",
        "ProjectURL",
    ]

    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()

        for p in projects:
            countries = p.get("Countries") or []
            entities  = p.get("Entities") or []
            ras       = p.get("ResultAreas") or []
            disbs     = p.get("Disbursements") or []
            funding   = p.get("Funding") or []

            # Countries
            country_labels = [
                f"{c.get('CountryName', '?')} ({c.get('ISO3', '?')})"
                for c in countries if isinstance(c, dict)
            ]
            isos    = [c.get("ISO3", "") for c in countries if isinstance(c, dict)]
            regions = list({c.get("Region", "") for c in countries if isinstance(c, dict)})
            has_ldc  = any(c.get("LDCs") for c in countries if isinstance(c, dict))
            has_sids = any(c.get("SIDS") for c in countries if isinstance(c, dict))

            # Entities
            primary = entities[0] if entities else {}
            all_entity_names = [e.get("Name", "") for e in entities if isinstance(e, dict)]

            # Non-zero result areas
            non_zero_ras = [
                f"{ra.get('Area')} ({ra.get('Type')}): {ra.get('Value')}"
                for ra in ras
                if ra.get("Value") not in ("0.00%", "0%", None, 0, "")
                and ra.get("Value", "0.00%") != "0.00%"
            ]

            # Disbursements
            total_disbursed = sum(d.get("AmountDisbursedUSDeq") or 0 for d in disbs)

            # Funding by source
            gcf_grants    = sum(f.get("Budget") or 0 for f in funding if f.get("Source") == "GCF")
            cofin_grants  = sum(f.get("Budget") or 0 for f in funding if f.get("Source") == "Co-Financing")

            writer.writerow({
                "ProjectsID":               p.get("ProjectsID", ""),
                "ApprovedRef":              p.get("ApprovedRef", ""),
                "BoardMeeting":             p.get("BoardMeeting", ""),
                "ProjectName":              p.get("ProjectName", ""),
                "Theme":                    p.get("Theme", ""),
                "Sector":                   p.get("Sector", ""),
                "Size":                     p.get("Size", ""),
                "RiskCategory":             p.get("RiskCategory", ""),
                "Status":                   p.get("Status", ""),
                "ApprovalDate":             _fmt_date(p.get("ApprovalDate")),
                "DateImplementationStart":  _fmt_date(p.get("DateImplementationStart")),
                "StartDate":                _fmt_date(p.get("StartDate")),
                "EndDate":                  _fmt_date(p.get("EndDate")),
                "DateClosing":              _fmt_date(p.get("DateClosing")),
                "DateCompletion":           _fmt_date(p.get("DateCompletion")),
                "DateCancelled":            _fmt_date(p.get("DateCancelled")),
                "DurationMonths":           p.get("DurationMonths", ""),
                "TotalGCFFunding_USD":      p.get("TotalGCFFunding", ""),
                "TotalCoFinancing_USD":     p.get("TotalCoFinancing", ""),
                "TotalValue_USD":           p.get("TotalValue", ""),
                "DirectBeneficiaries":      p.get("DirectBeneficiaries", ""),
                "IndirectBeneficiaries":    p.get("IndirectBeneficiaries", ""),
                "LifeTimeCO2_tCO2eq":       p.get("LifeTimeCO2", ""),
                "Countries":                _csv_list(country_labels),
                "Countries_ISO3":           _csv_list(isos),
                "Regions":                  _csv_list(regions),
                "HasLDC":                   has_ldc,
                "HasSIDS":                  has_sids,
                "PrimaryEntity":            primary.get("Name", ""),
                "PrimaryEntity_Acronym":    primary.get("Acronym", ""),
                "PrimaryEntity_Access":     primary.get("Access", ""),
                "PrimaryEntity_Type":       primary.get("Type", ""),
                "AllEntities":              _csv_list(all_entity_names),
                "NonZeroResultAreas":       _csv_list(non_zero_ras),
                "TotalDisbursed_USD":       round(total_disbursed, 2),
                "DisbursementCount":        len(disbs),
                "GCF_Grant_USD":            gcf_grants,
                "CoFinancing_Grant_USD":    cofin_grants,
                "ProjectURL":               p.get("ProjectURL", ""),
            })

    logger.info("  gcf_projects.csv          %d rows", len(projects))


def write_result_areas_csv(projects: list[dict], path: Path) -> None:
    """One row per project × result area (all areas, including zero-allocation)."""
    fields = [
        "ProjectsID", "ApprovedRef", "ProjectName",
        "Area", "Type", "Value_Pct",
    ]
    rows = 0
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for p in projects:
            for ra in (p.get("ResultAreas") or []):
                writer.writerow({
                    "ProjectsID":   p.get("ProjectsID", ""),
                    "ApprovedRef":  p.get("ApprovedRef", ""),
                    "ProjectName":  p.get("ProjectName", ""),
                    "Area":         ra.get("Area", ""),
                    "Type":         ra.get("Type", ""),
                    "Value_Pct":    ra.get("Value", ""),
                })
                rows += 1
    logger.info("  gcf_result_areas.csv       %d rows", rows)


def write_countries_csv(projects: list[dict], path: Path) -> None:
    """One row per project × country, including financing breakdown."""
    fields = [
        "ProjectsID", "ApprovedRef", "ProjectName",
        "CountryID", "CountryName", "ISO3", "Region", "LDCs", "SIDS",
        "GCF_Financing_USD", "CoFinancing_USD", "Total_USD", "Currency",
    ]
    rows = 0
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for p in projects:
            for c in (p.get("Countries") or []):
                financing = (c.get("Financing") or [{}])[0]
                writer.writerow({
                    "ProjectsID":        p.get("ProjectsID", ""),
                    "ApprovedRef":       p.get("ApprovedRef", ""),
                    "ProjectName":       p.get("ProjectName", ""),
                    "CountryID":         c.get("CountryID", ""),
                    "CountryName":       c.get("CountryName", ""),
                    "ISO3":              c.get("ISO3", ""),
                    "Region":            c.get("Region", ""),
                    "LDCs":              c.get("LDCs", ""),
                    "SIDS":              c.get("SIDS", ""),
                    "GCF_Financing_USD": financing.get("GCF", ""),
                    "CoFinancing_USD":   financing.get("CoFinancing", ""),
                    "Total_USD":         financing.get("Total", ""),
                    "Currency":          financing.get("Currency", "USD"),
                })
                rows += 1
    logger.info("  gcf_countries.csv          %d rows", rows)


def write_entities_csv(projects: list[dict], path: Path) -> None:
    """One row per project × implementing entity."""
    fields = [
        "ProjectsID", "ApprovedRef", "ProjectName",
        "EntityID", "Name", "Acronym", "Access", "Type",
        "AccreditationDate", "Sector", "ESS",
    ]
    rows = 0
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for p in projects:
            for e in (p.get("Entities") or []):
                writer.writerow({
                    "ProjectsID":        p.get("ProjectsID", ""),
                    "ApprovedRef":       p.get("ApprovedRef", ""),
                    "ProjectName":       p.get("ProjectName", ""),
                    "EntityID":          e.get("EntityID", ""),
                    "Name":              e.get("Name", ""),
                    "Acronym":           e.get("Acronym", ""),
                    "Access":            e.get("Access", ""),
                    "Type":              e.get("Type", ""),
                    "AccreditationDate": e.get("AccreditationDate", ""),
                    "Sector":            e.get("Sector", ""),
                    "ESS":               e.get("ESS", ""),
                })
                rows += 1
    logger.info("  gcf_entities.csv           %d rows", rows)


def write_disbursements_csv(projects: list[dict], path: Path) -> None:
    """One row per disbursement tranche."""
    fields = [
        "ProjectsID", "ApprovedRef", "ProjectName",
        "ProjectDisbursementID", "AmountDisbursed", "AmountDisbursedUSDeq",
        "Currency", "DateEffective", "Entity",
    ]
    rows = 0
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for p in projects:
            for d in (p.get("Disbursements") or []):
                writer.writerow({
                    "ProjectsID":              p.get("ProjectsID", ""),
                    "ApprovedRef":             p.get("ApprovedRef", ""),
                    "ProjectName":             p.get("ProjectName", ""),
                    "ProjectDisbursementID":   d.get("ProjectDisbursementID", ""),
                    "AmountDisbursed":         d.get("AmountDisbursed", ""),
                    "AmountDisbursedUSDeq":    d.get("AmountDisbursedUSDeq", ""),
                    "Currency":               d.get("Currency", ""),
                    "DateEffective":          d.get("DateEffective", ""),
                    "Entity":                 d.get("Entity", ""),
                })
                rows += 1
    logger.info("  gcf_disbursements.csv      %d rows", rows)


def write_funding_csv(projects: list[dict], path: Path) -> None:
    """One row per funding instrument / source."""
    fields = [
        "ProjectsID", "ApprovedRef", "ProjectName",
        "ProjectBudgetID", "BM", "Source", "Instrument",
        "Budget", "BudgetUSDeq", "Currency",
    ]
    rows = 0
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for p in projects:
            for f in (p.get("Funding") or []):
                writer.writerow({
                    "ProjectsID":      p.get("ProjectsID", ""),
                    "ApprovedRef":     p.get("ApprovedRef", ""),
                    "ProjectName":     p.get("ProjectName", ""),
                    "ProjectBudgetID": f.get("ProjectBudgetID", ""),
                    "BM":              f.get("BM", ""),
                    "Source":          f.get("Source", ""),
                    "Instrument":      f.get("Instrument", ""),
                    "Budget":          f.get("Budget", ""),
                    "BudgetUSDeq":     f.get("BudgetUSDeq", ""),
                    "Currency":        f.get("Currency", ""),
                })
                rows += 1
    logger.info("  gcf_funding.csv            %d rows", rows)


def write_raw_json(projects: list[dict], path: Path) -> None:
    """Dump the full raw JSON for reference / further scripting."""
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(projects, fh, indent=2, ensure_ascii=False)
    logger.info("  gcf_raw.json               %d projects", len(projects))


# =============================================================================
# CLI
# =============================================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Export GCF approved project data to CSV files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Filter examples
---------------
All adaptation/cross-cutting projects currently under implementation:
  python gcf_export.py --theme Adaptation Cross-cutting --status "Under Implementation"

Food and water result areas only, minimum $5M GCF funding:
  python gcf_export.py \\
      --result-areas "Livelihoods of people and communities" \\
                     "Health, food, and water security" \\
      --min-funding 5000000

Specific countries, skip per-project detail fetch (faster):
  python gcf_export.py --countries-iso3 PER MWI BGD ETH --no-details

Filter keys match sources.yaml gcf_projects.filters exactly.
""",
    )
    p.add_argument(
        "--output", "-o",
        default="tmp/gcf_export",
        metavar="DIR",
        help="Output directory for CSV files (default: tmp/gcf_export)",
    )
    p.add_argument(
        "--no-details",
        action="store_true",
        help="Skip per-project detail fetch — use list metadata only (faster)",
    )
    p.add_argument(
        "--save-json",
        action="store_true",
        help="Also save the raw JSON alongside the CSVs",
    )
    p.add_argument(
        "--rate-limit", type=int, default=_RATE_LIMIT_RPM, metavar="RPM",
        help=f"Detail requests per minute (default: {_RATE_LIMIT_RPM})",
    )

    # Filters
    fg = p.add_argument_group("filters (all optional, AND-ed together)")
    fg.add_argument(
        "--theme", nargs="+", metavar="THEME",
        help="e.g. --theme Adaptation Cross-cutting Mitigation",
    )
    fg.add_argument(
        "--status", nargs="+", metavar="STATUS",
        help='e.g. --status "Under Implementation" Completed',
    )
    fg.add_argument(
        "--result-areas", nargs="+", metavar="AREA",
        help='e.g. --result-areas "Livelihoods of people and communities"',
    )
    fg.add_argument(
        "--countries-iso3", nargs="+", metavar="ISO3",
        help="e.g. --countries-iso3 PER MWI BGD",
    )
    fg.add_argument(
        "--size", nargs="+", metavar="SIZE",
        help="e.g. --size Small Medium Large  (excludes Micro)",
    )
    fg.add_argument(
        "--sector", nargs="+", metavar="SECTOR",
        help="e.g. --sector Public Private Mixed",
    )
    fg.add_argument(
        "--min-funding", type=float, metavar="USD",
        help="Minimum TotalGCFFunding in USD, e.g. --min-funding 5000000",
    )
    return p.parse_args()


def build_filters(args: argparse.Namespace) -> dict:
    filters: dict = {}
    if args.theme:
        filters["theme"] = args.theme
    if args.status:
        filters["status"] = args.status
    if args.result_areas:
        filters["result_areas"] = args.result_areas
    if args.countries_iso3:
        filters["countries_iso3"] = args.countries_iso3
    if args.size:
        filters["size"] = args.size
    if args.sector:
        filters["sector"] = args.sector
    if args.min_funding is not None:
        filters["min_gcf_funding"] = args.min_funding
    return filters


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── Step 1: fetch list ────────────────────────────────────────────────────
    all_projects = fetch_list()

    # ── Step 2: filter ────────────────────────────────────────────────────────
    filters = build_filters(args)
    if filters:
        matched = [p for p in all_projects if matches_filters(p, filters)]
        logger.info(
            "Filter: %d/%d projects matched (%d skipped)",
            len(matched), len(all_projects), len(all_projects) - len(matched),
        )
        logger.info("Active filters: %s", json.dumps(filters, indent=2))
    else:
        matched = all_projects
        logger.info("No filters — exporting all %d projects", len(matched))

    if not matched:
        logger.warning("No projects matched your filters. Nothing to export.")
        sys.exit(0)

    # ── Step 3: optionally enrich with per-project detail ─────────────────────
    if not args.no_details:
        min_gap = 60.0 / args.rate_limit
        logger.info(
            "Fetching per-project detail for %d projects "
            "(rate limit: %d rpm, ~%ds gap)…",
            len(matched), args.rate_limit, int(min_gap),
        )
        enriched: list[dict] = []
        last_request = 0.0
        for i, meta in enumerate(matched, 1):
            pid = meta.get("ProjectsID")
            # Rate limiting
            elapsed = time.monotonic() - last_request
            if elapsed < min_gap:
                time.sleep(min_gap - elapsed)
            last_request = time.monotonic()

            detail = fetch_detail(pid)
            enriched.append(detail if detail else meta)
            if i % 10 == 0 or i == len(matched):
                logger.info("  %d/%d done", i, len(matched))
        matched = enriched
    else:
        logger.info("Skipping per-project detail (--no-details)")

    # ── Step 4: write CSVs ────────────────────────────────────────────────────
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    logger.info("Writing CSVs to %s/", output_dir)

    write_projects_csv(matched,      output_dir / "gcf_projects.csv")
    write_result_areas_csv(matched,  output_dir / "gcf_result_areas.csv")
    write_countries_csv(matched,     output_dir / "gcf_countries.csv")
    write_entities_csv(matched,      output_dir / "gcf_entities.csv")
    write_disbursements_csv(matched, output_dir / "gcf_disbursements.csv")
    write_funding_csv(matched,       output_dir / "gcf_funding.csv")

    if args.save_json:
        write_raw_json(matched, output_dir / "gcf_raw.json")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\nExported {len(matched)} projects → {output_dir}/")
    print(f"  gcf_projects.csv        (overview — one row per project)")
    print(f"  gcf_result_areas.csv    (allocation by area — join on ProjectsID)")
    print(f"  gcf_countries.csv       (country financing — join on ProjectsID)")
    print(f"  gcf_entities.csv        (implementing entities — join on ProjectsID)")
    print(f"  gcf_disbursements.csv   (disbursement tranches — join on ProjectsID)")
    print(f"  gcf_funding.csv         (funding instruments — join on ProjectsID)")
    if args.save_json:
        print(f"  gcf_raw.json            (full raw API response)")
    print(f"\nAll files use ProjectsID + ApprovedRef as join keys.")


if __name__ == "__main__":
    main()
