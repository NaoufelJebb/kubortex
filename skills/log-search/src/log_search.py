"""Log-search skill — execute LogQL queries against Loki HTTP API."""

from __future__ import annotations

import json
from typing import Any

import httpx

from kubortex.investigator.skills.models import SkillInput, SkillResult
from kubortex.shared.config import KubortexSettings


class LogSearchSkill:
    """Search logs via the Loki HTTP API."""

    def __init__(self) -> None:
        self._settings = KubortexSettings()

    async def execute(self, inp: SkillInput) -> SkillResult:
        query = inp.query
        if not query:
            return SkillResult(success=False, error="empty LogQL query")

        base_url = self._settings.loki_url.rstrip("/")
        params: dict[str, str] = {
            "query": query,
            "limit": str(inp.parameters.get("limit", 100)),
        }

        start = inp.parameters.get("start")
        end = inp.parameters.get("end")
        if start:
            params["start"] = str(start)
        if end:
            params["end"] = str(end)

        url = f"{base_url}/loki/api/v1/query_range"

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()

            streams = data.get("data", {}).get("result", [])
            lines = _extract_lines(streams)
            raw = json.dumps(lines, default=str)

            summary = f"Found {len(lines)} log lines matching query"
            if lines:
                summary += f". First: {lines[0][:200]}"

            return SkillResult(
                success=True,
                data=lines,
                summary=summary,
                raw_size=len(raw),
            )
        except Exception as exc:
            return SkillResult(success=False, error=f"LogQL query failed: {exc}")


def _extract_lines(streams: list[Any]) -> list[str]:
    """Flatten Loki stream results into a list of log lines."""
    lines: list[str] = []
    for stream in streams:
        for _ts, line in stream.get("values", []):
            lines.append(line)
    return lines


def create() -> LogSearchSkill:
    """Factory function referenced by the skill entrypoint."""
    return LogSearchSkill()
