# =============================================================================
# adapters/gcf_api.py
# GCF Approved Projects API adapter
# Two-step fetch: list metadata then per-project detail
# Docs: https://api.gcfund.org/v1/projects
# =============================================================================

import asyncio
import hashlib
import logging
import uuid
from datetime import datetime
from typing import AsyncIterator

import requests

from adapters.base import AdapterFetchError, BaseAdapter
from schemas.document import Document

logger = logging.getLogger(__name__)

_GCF_LIST_URL = "https://api.gcfund.org/v1/projects"
_GCF_DETAIL_URL = "https://api.gcfund.org/v1/projects/{project_id}"
_MAX_RETRIES = 3
_RETRY_BACKOFF = [2, 4, 8]


class GCFAPIAdapter(BaseAdapter):
    """
    Fetches GCF approved project records from the public GCF Portfolio API.
    No authentication required.

    Three-step strategy:
      Step 1 — GET /v1/projects
          Fetches the complete list of approved projects. Each record already
          contains Countries, Entities, Disbursements, Funding, and ResultAreas,
          which is sufficient for filtering.

      Step 2 — Filter (client-side, in memory)
          Apply any filters configured under the ``filters`` key in sources.yaml
          before making any per-project detail requests. Supported filters:

            theme           list of Theme values     e.g. ["Adaptation","Cross-cutting"]
            status          list of Status values    e.g. ["Under Implementation"]
            result_areas    list of ResultArea names  (matches if any non-zero area matches)
            countries_iso3  list of ISO3 codes        (matches if any country matches)
            size            list of Size values       e.g. ["Small","Medium","Large"]
            sector          list of Sector values     e.g. ["Public","Private","Mixed"]
            min_gcf_funding minimum TotalGCFFunding    e.g. 5000000  (USD)

      Step 3 — GET /v1/projects/{ProjectsID}  (when fetch_project_details=true)
          Fetches the authoritative per-project detail only for projects that
          passed the filters. Skipping this for non-matching projects avoids
          hundreds of unnecessary HTTP requests.

    Each matching project yields one Document. raw_text is structured prose
    for Stage A extraction.
    """

    source_type: str = "gcf_api"

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self._list_url: str = config.get("api_base_url", _GCF_LIST_URL)
        self._detail_url: str = config.get("api_detail_url", _GCF_DETAIL_URL)
        self._fetch_details: bool = config.get("fetch_project_details", True)
        self._rate_limit_rpm: int = config.get("rate_limit_rpm", 20)
        self._sector_hint: list[str] = config.get("sector_hints", [])
        self._filters: dict = config.get("filters", {})

    async def fetch(self, query_or_path: str) -> AsyncIterator[Document]:
        """
        Step 1: fetch full project list from /v1/projects.
        Step 2: apply in-memory filters — only matching projects proceed.
        Step 3: for each match, optionally fetch /v1/projects/{ProjectsID}.
        Yields one Document per matching project.
        """
        all_projects = await self._fetch_list()

        matched = [p for p in all_projects if self._matches_filters(p)]
        skipped = len(all_projects) - len(matched)
        if skipped:
            logger.info(
                "GCF filter: %d/%d projects matched, %d skipped",
                len(matched), len(all_projects), skipped,
            )

        for meta in matched:
            project_id = meta.get("ProjectsID")
            if not project_id:
                logger.warning("GCF project missing ProjectsID, skipping")
                continue

            if self._fetch_details:
                await self.rate_limit_wait(self._rate_limit_rpm)
                detail = await self._fetch_detail(project_id)
                project = detail if detail else meta
            else:
                project = meta

            doc = self._project_to_document(project)
            if doc is not None:
                yield doc

    # ── Filter ────────────────────────────────────────────────────────────────

    def _matches_filters(self, project: dict) -> bool:
        """
        Return True if the project passes all configured filters.
        An empty filters dict matches everything.

        Filters are AND-ed together. Within each filter the values are OR-ed
        (e.g. theme: [Adaptation, Cross-cutting] matches either).
        """
        f = self._filters
        if not f:
            return True

        # theme: ["Adaptation", "Cross-cutting"]
        if "theme" in f:
            if project.get("Theme") not in f["theme"]:
                return False

        # status: ["Under Implementation", "Completed"]
        if "status" in f:
            if project.get("Status") not in f["status"]:
                return False

        # size: ["Small", "Medium", "Large"]
        if "size" in f:
            if project.get("Size") not in f["size"]:
                return False

        # sector: ["Public", "Private", "Mixed"]
        if "sector" in f:
            if project.get("Sector") not in f["sector"]:
                return False

        # min_gcf_funding: 5000000
        if "min_gcf_funding" in f:
            if (project.get("TotalGCFFunding") or 0) < f["min_gcf_funding"]:
                return False

        # countries_iso3: ["PER", "MWI", "BGD"]
        if "countries_iso3" in f:
            project_isos = {
                c.get("ISO3", "") for c in (project.get("Countries") or [])
                if isinstance(c, dict)
            }
            if not project_isos.intersection(f["countries_iso3"]):
                return False

        # result_areas: ["Livelihoods of people and communities"]
        # Matches if any non-zero result area name is in the filter list
        if "result_areas" in f:
            allowed = set(f["result_areas"])
            non_zero_areas = {
                ra.get("Area", "")
                for ra in (project.get("ResultAreas") or [])
                if ra.get("Value") not in ("0.00%", "0%", None, 0, "")
                and ra.get("Value", "0.00%") != "0.00%"
            }
            if not non_zero_areas.intersection(allowed):
                return False

        return True

    # ── Network helpers ───────────────────────────────────────────────────────

    async def _fetch_list(self) -> list[dict]:
        """GET /v1/projects — returns all approved GCF projects."""
        last_exc: Exception = AdapterFetchError("No attempts made")
        for attempt, wait in enumerate([0] + _RETRY_BACKOFF):
            if wait:
                await asyncio.sleep(wait)
            try:
                resp = requests.get(self._list_url, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                projects = data if isinstance(data, list) else data.get("results", [])
                logger.info("GCF list API returned %d projects", len(projects))
                return projects
            except Exception as exc:
                logger.warning("GCF list attempt %d failed: %s", attempt + 1, exc)
                last_exc = exc
        raise AdapterFetchError(
            f"GCF list API failed after {_MAX_RETRIES} retries: {last_exc}"
        ) from last_exc

    async def _fetch_detail(self, project_id: int) -> dict | None:
        """GET /v1/projects/{project_id} — full detail for one project."""
        url = self._detail_url.format(project_id=project_id)
        last_exc: Exception = AdapterFetchError("No attempts made")
        for attempt, wait in enumerate([0] + _RETRY_BACKOFF):
            if wait:
                await asyncio.sleep(wait)
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:
                logger.warning(
                    "GCF detail %s attempt %d failed: %s",
                    project_id, attempt + 1, exc,
                )
                last_exc = exc
        logger.error(
            "GCF detail %s failed after %d retries: %s",
            project_id, _MAX_RETRIES, last_exc,
        )
        return None

    # ── Document construction ─────────────────────────────────────────────────

    def _project_to_document(self, project: dict) -> Document | None:
        project_id = project.get("ProjectsID")
        if not project_id:
            return None

        approved_ref = project.get("ApprovedRef") or f"FP{project_id}"
        title = project.get("ProjectName") or f"GCF Project {approved_ref}"
        source_url = (
            project.get("ProjectURL")
            or f"https://www.greenclimate.fund/project/{approved_ref}"
        )
        pub_date = self._parse_date(
            project.get("ApprovalDate") or project.get("StartDate")
        )
        countries = self._extract_countries(project)
        raw_text = self._project_to_text(project, title, approved_ref)
        content_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()

        return Document(
            doc_id=str(uuid.uuid4()),
            content_hash=content_hash,
            raw_text=raw_text,
            title=title,
            language="en",
            source_url=source_url,
            source_type=self.source_type,
            adapter=self.__class__.__name__,
            publication_date=pub_date,
            ingestion_date=datetime.utcnow(),
            reporting_year=pub_date.year if pub_date else None,
            document_type="project_db",
            company_name=self._primary_entity_name(project),
            company_id=None,
            csrd_wave=None,
            country=countries,
            sector_hint=self._sector_hint,
            extraction_status="pending",
            extraction_error=None,
        )

    def _project_to_text(self, project: dict, title: str, approved_ref: str) -> str:
        """
        Convert a GCF project dict to structured prose for Stage A extraction.
        Includes all fields returned by /v1/projects and /v1/projects/{id}.
        """
        parts: list[str] = []

        # ── Header ────────────────────────────────────────────────────────────
        parts.append(f"GCF Approved Project: {title}")
        parts.append(
            f"Reference: {approved_ref} | "
            f"Board Meeting: {project.get('BoardMeeting', 'N/A')}"
        )
        parts.append(f"Status: {project.get('Status', 'N/A')}")
        parts.append(
            f"Theme: {project.get('Theme', 'N/A')} | "
            f"Sector: {project.get('Sector', 'N/A')}"
        )
        parts.append(
            f"Size: {project.get('Size', 'N/A')} | "
            f"Risk Category: {project.get('RiskCategory', 'N/A')}"
        )

        # ── Dates ─────────────────────────────────────────────────────────────
        parts.append(
            f"Approval Date: {self._fmt_date(project.get('ApprovalDate'))} | "
            f"Implementation Start: {self._fmt_date(project.get('DateImplementationStart'))} | "
            f"Start: {self._fmt_date(project.get('StartDate'))} | "
            f"End: {self._fmt_date(project.get('EndDate'))} | "
            f"Closing: {self._fmt_date(project.get('DateClosing'))} | "
            f"Duration: {project.get('DurationMonths', 'N/A')} months"
        )

        # ── Funding summary ───────────────────────────────────────────────────
        parts.append(
            f"Total GCF Funding: USD {project.get('TotalGCFFunding', 0):,.0f} | "
            f"Co-Financing: USD {project.get('TotalCoFinancing', 0):,.0f} | "
            f"Total Project Value: USD {project.get('TotalValue', 0):,.0f}"
        )

        # ── Beneficiaries and CO2 ─────────────────────────────────────────────
        lifetime_co2 = project.get("LifeTimeCO2") or 0
        parts.append(
            f"Direct Beneficiaries: {project.get('DirectBeneficiaries', 0):,} | "
            f"Indirect Beneficiaries: {project.get('IndirectBeneficiaries', 0):,} | "
            f"Lifetime CO2 (tCO2eq): {lifetime_co2:,.0f}"
        )

        # ── Countries ─────────────────────────────────────────────────────────
        countries = project.get("Countries") or []
        if countries:
            country_lines: list[str] = []
            for c in countries:
                flags = []
                if c.get("LDCs"):
                    flags.append("LDC")
                if c.get("SIDS"):
                    flags.append("SIDS")
                flag_str = f" [{', '.join(flags)}]" if flags else ""
                financing = c.get("Financing") or []
                fin_str = ""
                if financing:
                    fin = financing[0]
                    fin_str = (
                        f" | GCF: {fin.get('Currency', 'USD')} "
                        f"{fin.get('GCF', 0):,.0f}"
                    )
                country_lines.append(
                    f"{c.get('CountryName', 'Unknown')} "
                    f"({c.get('ISO3', '?')}) – "
                    f"{c.get('Region', 'Unknown')}"
                    f"{flag_str}{fin_str}"
                )
            parts.append("Countries: " + "; ".join(country_lines))

        # ── Implementing entities ─────────────────────────────────────────────
        entities = project.get("Entities") or []
        if entities:
            entity_lines: list[str] = []
            for e in entities:
                entity_lines.append(
                    f"{e.get('Name', 'Unknown')} ({e.get('Acronym', '?')}) "
                    f"– Access: {e.get('Access', 'N/A')} / "
                    f"Type: {e.get('Type', 'N/A')} / "
                    f"Sector: {e.get('Sector', 'N/A')}"
                )
            parts.append("Implementing Entities: " + "; ".join(entity_lines))

        # ── Result areas (non-zero only) ──────────────────────────────────────
        result_areas = project.get("ResultAreas") or []
        non_zero_ra = [
            ra for ra in result_areas
            if ra.get("Value") not in ("0.00%", "0%", None, 0, "")
            and ra.get("Value", "0.00%") != "0.00%"
        ]
        if non_zero_ra:
            ra_lines = [
                f"{ra.get('Area')} ({ra.get('Type')}): {ra.get('Value')}"
                for ra in non_zero_ra
            ]
            parts.append("Result Areas: " + "; ".join(ra_lines))

        # ── Funding instruments ───────────────────────────────────────────────
        funding = project.get("Funding") or []
        if funding:
            fund_lines: list[str] = []
            for f in funding:
                fund_lines.append(
                    f"{f.get('Source')} – {f.get('Instrument')}: "
                    f"{f.get('Currency', 'USD')} {f.get('Budget', 0):,.0f}"
                )
            parts.append("Funding Instruments: " + "; ".join(fund_lines))

        # ── Disbursements summary ─────────────────────────────────────────────
        disbursements = project.get("Disbursements") or []
        if disbursements:
            total_disbursed = sum(
                d.get("AmountDisbursedUSDeq") or 0 for d in disbursements
            )
            last_date = max(
                (d.get("DateEffective", "") for d in disbursements),
                default="N/A",
            )
            parts.append(
                f"Disbursements: {len(disbursements)} tranches, "
                f"total USD {total_disbursed:,.0f} disbursed "
                f"(latest: {last_date})"
            )

        return "\n".join(parts)

    # ── Field extraction helpers ──────────────────────────────────────────────

    def _extract_countries(self, project: dict) -> list[str]:
        """Return ISO3 country codes from the Countries array."""
        result: list[str] = []
        for c in project.get("Countries") or []:
            if isinstance(c, dict):
                code = c.get("ISO3") or c.get("iso_code") or c.get("CountryName", "")
                if code:
                    result.append(code.strip())
            elif isinstance(c, str):
                result.append(c.strip())
        return result

    def _primary_entity_name(self, project: dict) -> str | None:
        """Return the name of the first implementing entity, or None."""
        entities = project.get("Entities") or []
        if entities and isinstance(entities[0], dict):
            return entities[0].get("Name")
        return None

    def _parse_date(self, value: str | None) -> datetime | None:
        if not value:
            return None
        clean = value.split("T")[0] if "T" in value else value
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y"):
            try:
                return datetime.strptime(clean, fmt)
            except ValueError:
                continue
        return None

    def _fmt_date(self, value: str | None) -> str:
        dt = self._parse_date(value)
        return dt.strftime("%Y-%m-%d") if dt else "N/A"
