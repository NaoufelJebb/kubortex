"""PVC-backed payload store for full skill results.

Stores large payloads externally so the LangGraph context window only
carries compressed summaries.  Payloads > 10 KiB are gzip-compressed.
"""

from __future__ import annotations

import gzip
import json
import time
from pathlib import Path
from typing import Any

import structlog

from kubortex.shared.config import KubortexSettings

logger = structlog.get_logger(__name__)

_GZIP_THRESHOLD = 10 * 1024  # 10 KiB


class PayloadStore:
    """File-backed store keyed by ``{incident_name}/{investigation_name}/{seq}``."""

    def __init__(self, root: Path | None = None) -> None:
        settings = KubortexSettings()
        self._root = root or Path(settings.payload_store_path)
        self._max_size = settings.payload_max_size_bytes

    # -- write / read --------------------------------------------------------

    def write(
        self,
        incident_name: str,
        investigation_name: str,
        seq: int,
        payload: dict[str, Any],
    ) -> Path:
        """Persist *payload* and return the path on disk."""
        directory = self._root / incident_name / investigation_name
        directory.mkdir(parents=True, exist_ok=True)

        raw = json.dumps(payload, default=str).encode()

        if len(raw) > self._max_size:
            raw = raw[: self._max_size]
            logger.warning(
                "payload_truncated",
                incident=incident_name,
                investigation=investigation_name,
                seq=seq,
            )

        if len(raw) > _GZIP_THRESHOLD:
            path = directory / f"{seq}.json.gz"
            path.write_bytes(gzip.compress(raw))
        else:
            path = directory / f"{seq}.json"
            path.write_bytes(raw)

        logger.debug("payload_written", path=str(path), size=len(raw))
        return path

    def read(
        self,
        incident_name: str,
        investigation_name: str,
        seq: int,
    ) -> dict[str, Any] | None:
        """Read a previously stored payload.  Returns *None* if missing."""
        directory = self._root / incident_name / investigation_name

        gz_path = directory / f"{seq}.json.gz"
        if gz_path.exists():
            raw = gzip.decompress(gz_path.read_bytes())
            return json.loads(raw)

        plain_path = directory / f"{seq}.json"
        if plain_path.exists():
            return json.loads(plain_path.read_bytes())

        return None

    # -- garbage collection --------------------------------------------------

    def gc(self, max_age_seconds: int = 7 * 24 * 3600) -> int:
        """Delete payloads older than *max_age_seconds*.  Returns count removed."""
        cutoff = time.time() - max_age_seconds
        removed = 0

        if not self._root.exists():
            return 0

        for path in self._root.rglob("*.json*"):
            if path.stat().st_mtime < cutoff:
                path.unlink()
                removed += 1

        # Clean empty directories
        for dirpath in sorted(self._root.rglob("*"), reverse=True):
            if dirpath.is_dir() and not any(dirpath.iterdir()):
                dirpath.rmdir()

        if removed:
            logger.info("payload_gc_complete", removed=removed)
        return removed
