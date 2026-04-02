"""PromQL skill — execute queries against Prometheus HTTP API."""

from __future__ import annotations

import json
from typing import Any

import httpx

from kubortex.investigator.skills.models import SkillInput, SkillResult
from kubortex.shared.config import InvestigatorSettings


class PromQLSkill:
    """Execute PromQL queries via the Prometheus HTTP API."""

    def __init__(self) -> None:
        self._settings = InvestigatorSettings()

    async def execute(self, inp: SkillInput) -> SkillResult:
        query = inp.query
        if not query:
            return SkillResult(success=False, error="empty PromQL query")

        base_url = self._settings.prometheus_url.rstrip("/")
        params: dict[str, str] = {"query": query}

        start = inp.parameters.get("start")
        end = inp.parameters.get("end")
        step = inp.parameters.get("step")

        # Use range query if start/end specified, else instant query
        if start and end:
            url = f"{base_url}/api/v1/query_range"
            params["start"] = str(start)
            params["end"] = str(end)
            params["step"] = str(step or "60s")
        else:
            url = f"{base_url}/api/v1/query"

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()

            result_data = data.get("data", {}).get("result", [])
            raw = json.dumps(result_data, default=str)
            summary = _summarise(query, result_data)

            return SkillResult(
                success=True,
                data=result_data,
                summary=summary,
                raw_size=len(raw),
            )
        except Exception as exc:
            return SkillResult(success=False, error=f"PromQL query failed: {exc}")


def _summarise(query: str, results: list[Any]) -> str:
    if not results:
        return f"PromQL query returned no results: {query}"
    if len(results) == 1:
        val = results[0].get("value", [None, None])
        return f"PromQL result: {val[1]} (query: {query})"
    return f"PromQL returned {len(results)} series for: {query}"


def create() -> PromQLSkill:
    """Factory function referenced by the skill entrypoint."""
    return PromQLSkill()
