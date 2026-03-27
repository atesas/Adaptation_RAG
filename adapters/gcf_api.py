# =============================================================================
# adapters/gcf_api.py
# GCF Project Browser API adapter — Phase 2
# Fetches structured project data from the Green Climate Fund public API.
# =============================================================================

import logging
from datetime import datetime
from typing import AsyncIterator

import requests

from adapters.base import AdapterAuthError, AdapterFetchError, BaseAdapter
from schemas.document import Document

logger = logging.getLogger(__name__)

_GCF_PROJECTS_URL = "https://www.greenclimate.fund/projects/api/projects"
_MAX_RETRIES = 3
_RETRY_BACKOFF = [2, 4, 8]


class GCFAPIAdapter(BaseAdapter):
    """
    Fetches GCF project records from the public GCF Project Browser API.
    No authentication required. Returns structured JSON.
    Each project record becomes one Document.
    """

    source_type: str = "gcf_api"

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self._api_base = config.get("api_base_url", _GCF_PROJECTS_URL)
        self._result_area_filters: list[str] = config.get("filters", {}).get("result_areas", [])
        self._status_filters: list[str] = config.get("filters", {}).get("status", [])
        self._sector_hint: list[str] = config.get("sector_hints", [])

    async def fetch(self, query_or_path: str) -> AsyncIterator[Document]:
        projects = await self._fetch_projects()
        for project in projects:
            doc = self._project_to_document(project)
            if doc is not None:
                yield doc

    async def _fetch_projects(self) -> list[dict]:
        import asyncio
        params: dict = {"format": "json", "limit": 500}
        if self._result_area_filters:
            params["result_areas"] = ",".join(self._result_area_filters)
        if self._status_filters:
            params["status"] = ",".join(self._status_filters)

        last_exc: Exception = AdapterFetchError("No attempts made")
        for attempt, wait in enumerate([0] + _RETRY_BACKOFF):
            if wait:
                await asyncio.sleep(wait)
            try:
                resp = requests.get(self._api_base, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                projects = data if isinstance(data, list) else data.get("results", data.get("projects", []))
                logger.info("GCF API returned %d projects", len(projects))
                return projects
            except Exception as exc:
                logger.warning("GCF API attempt %d failed: %s", attempt + 1, exc)
                last_exc = exc

        raise AdapterFetchError(f"GCF API failed after {_MAX_RETRIES} retries: {last_exc}") from last_exc

    def _project_to_document(self, project: dict) -> Document | None:
        project_id = project.get("id") or project.get("project_id") or project.get("gcf_id")
        if not project_id:
            logger.warning("GCF project missing id, skipping")
            return None

        title = project.get("title") or project.get("name") or f"GCF Project {project_id}"
        countries = self._extract_countries(project)
        raw_text = self._project_to_text(project, title)
        source_url = f"https://www.greenclimate.fund/projects/fp{project_id}"
        pub_date = self._parse_date(project.get("approved_date") or project.get("start_date"))

        import hashlib, uuid
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
            company_name=project.get("implementing_entity") or project.get("entity"),
            company_id=None,
            csrd_wave=None,
            country=countries,
            sector_hint=self._sector_hint,
            extraction_status="pending",
            extraction_error=None,
        )

    def _project_to_text(self, project: dict, title: str) -> str:
        parts = [f"GCF Project: {title}"]
        for field in ("description", "objective", "expected_results",
                      "implementing_entity", "result_areas",
                      "funding_amount", "status", "countries"):
            value = project.get(field)
            if value:
                label = field.replace("_", " ").title()
                parts.append(f"{label}: {value}")
        return "\n".join(parts)

    def _extract_countries(self, project: dict) -> list[str]:
        countries_raw = project.get("countries") or project.get("country") or []
        if isinstance(countries_raw, str):
            return [c.strip() for c in countries_raw.split(",") if c.strip()]
        if isinstance(countries_raw, list):
            result = []
            for item in countries_raw:
                if isinstance(item, str):
                    result.append(item.strip())
                elif isinstance(item, dict):
                    code = item.get("iso_code") or item.get("code") or item.get("name", "")
                    if code:
                        result.append(code.strip())
            return result
        return []

    def _parse_date(self, value: str | None) -> datetime | None:
        if not value:
            return None
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y", "%Y"):
            try:
                return datetime.strptime(value[:len(fmt)], fmt)
            except ValueError:
                continue
        return None
