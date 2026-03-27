# =============================================================================
# adapters/oecd_api.py
# OECD Creditor Reporting System (CRS) adapter — Phase 2
# Fetches structured climate finance flow data from the OECD Stats API.
# =============================================================================

import logging
from datetime import datetime
from typing import AsyncIterator

import requests

from adapters.base import AdapterFetchError, BaseAdapter
from schemas.document import Document

logger = logging.getLogger(__name__)

_OECD_CRS_URL = "https://stats.oecd.org/SDMX-JSON/data/CRS"
_MAX_RETRIES = 3
_RETRY_BACKOFF = [2, 4, 8]

# CRS purpose codes for agriculture/food/water
_DEFAULT_PURPOSE_PREFIXES = ["311", "140"]


class OECDAPIAdapter(BaseAdapter):
    """
    Fetches OECD CRS climate finance flow records from the public SDMX-JSON API.
    No authentication required. Returns structured JSON.
    Each recipient-country × sector record becomes one Document.
    """

    source_type: str = "oecd_api"

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self._api_base = config.get("api_base_url", _OECD_CRS_URL)
        filters_cfg = config.get("filters", {})
        self._climate_markers: list[int] = filters_cfg.get("climate_markers", [1, 2])
        self._purpose_prefixes: list[str] = filters_cfg.get(
            "purpose_code_prefixes", _DEFAULT_PURPOSE_PREFIXES
        )
        self._sector_hint: list[str] = config.get("sector_hints", [])

    async def fetch(self, query_or_path: str) -> AsyncIterator[Document]:
        records = await self._fetch_records()
        for record in records:
            doc = self._record_to_document(record)
            if doc is not None:
                yield doc

    async def _fetch_records(self) -> list[dict]:
        import asyncio

        # OECD SDMX-JSON query: CRS/all records, last 3 years
        current_year = datetime.utcnow().year
        year_range = f"{current_year - 3}:{current_year}"
        marker_str = "+".join(str(m) for m in self._climate_markers)
        params = {
            "startTime": str(current_year - 3),
            "endTime": str(current_year),
        }
        url = f"{self._api_base}/.{marker_str}..../all?{_build_query(params)}"

        last_exc: Exception = AdapterFetchError("No attempts made")
        for attempt, wait in enumerate([0] + _RETRY_BACKOFF):
            if wait:
                await asyncio.sleep(wait)
            try:
                resp = requests.get(url, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                records = self._parse_sdmx_response(data)
                logger.info("OECD CRS returned %d records", len(records))
                return records
            except Exception as exc:
                logger.warning("OECD API attempt %d failed: %s", attempt + 1, exc)
                last_exc = exc

        raise AdapterFetchError(f"OECD API failed after {_MAX_RETRIES} retries: {last_exc}") from last_exc

    def _parse_sdmx_response(self, data: dict) -> list[dict]:
        """Extract flat records from SDMX-JSON envelope."""
        try:
            dataset = data.get("dataSets", [{}])[0]
            structure = data.get("structure", {})
            dimensions = structure.get("dimensions", {}).get("observation", [])
            dim_names = [d.get("id", f"dim_{i}") for i, d in enumerate(dimensions)]
            dim_values = {
                d.get("id"): [v.get("name", v.get("id", "")) for v in d.get("values", [])]
                for d in dimensions
            }
            records = []
            for key, obs in dataset.get("observations", {}).items():
                parts = key.split(":")
                record: dict = {}
                for i, part in enumerate(parts):
                    if i < len(dim_names):
                        dim_id = dim_names[i]
                        vals = dim_values.get(dim_id, [])
                        idx = int(part)
                        record[dim_id] = vals[idx] if idx < len(vals) else part
                record["value"] = obs[0] if obs else None
                records.append(record)
            return records
        except Exception as exc:
            logger.warning("Failed to parse SDMX response: %s", exc)
            return []

    def _record_to_document(self, record: dict) -> Document | None:
        import hashlib, uuid

        raw_text = self._record_to_text(record)
        if not raw_text.strip():
            return None

        content_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
        year = record.get("TIME_PERIOD") or record.get("year")
        pub_date = None
        if year:
            try:
                pub_date = datetime(int(str(year)[:4]), 1, 1)
            except ValueError:
                pass

        recipient = record.get("RECIPIENT") or record.get("recipient", "unknown")
        purpose = record.get("PURPOSE") or record.get("purpose", "unknown")
        title = f"OECD CRS — {purpose} — {recipient}"

        return Document(
            doc_id=str(uuid.uuid4()),
            content_hash=content_hash,
            raw_text=raw_text,
            title=title,
            language="en",
            source_url=self._api_base,
            source_type=self.source_type,
            adapter=self.__class__.__name__,
            publication_date=pub_date,
            ingestion_date=datetime.utcnow(),
            reporting_year=pub_date.year if pub_date else None,
            document_type="project_db",
            company_name=None,
            company_id=None,
            csrd_wave=None,
            country=[_iso2(recipient)],
            sector_hint=self._sector_hint,
            extraction_status="pending",
            extraction_error=None,
        )

    def _record_to_text(self, record: dict) -> str:
        parts = ["OECD CRS Climate Finance Record"]
        label_map = {
            "DONOR": "Donor",
            "RECIPIENT": "Recipient country",
            "PURPOSE": "Purpose / sector",
            "CHANNEL": "Channel of delivery",
            "TIME_PERIOD": "Year",
            "value": "USD million (committed)",
        }
        for key, label in label_map.items():
            val = record.get(key)
            if val is not None:
                parts.append(f"{label}: {val}")
        return "\n".join(parts)


def _build_query(params: dict) -> str:
    return "&".join(f"{k}={v}" for k, v in params.items())


def _iso2(name: str) -> str:
    """Best-effort: return name as-is (already a country name from OECD)."""
    return name.strip()[:50] if name else ""
