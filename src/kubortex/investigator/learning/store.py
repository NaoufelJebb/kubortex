"""PVC-backed JSON persistence for diagnostic learning scores."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class LearningStore:
    """Simple file-backed store for diagnostic scores, keyed by (category, targetKind)."""

    def __init__(self, store_path: str | Path) -> None:
        self._path = Path(store_path)
        self._path.mkdir(parents=True, exist_ok=True)

    def _key_path(self, category: str, target_kind: str) -> Path:
        safe_key = f"{category}__{target_kind}.json"
        return self._path / safe_key

    def load(self, category: str, target_kind: str) -> dict[str, Any]:
        """Load scores for a (category, targetKind) tuple."""
        path = self._key_path(category, target_kind)
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text())
        except Exception:
            logger.exception("learning_store_load_error", path=str(path))
            return {}

    def save(self, category: str, target_kind: str, data: dict[str, Any]) -> None:
        """Persist scores for a (category, targetKind) tuple."""
        path = self._key_path(category, target_kind)
        try:
            path.write_text(json.dumps(data, indent=2, default=str))
        except Exception:
            logger.exception("learning_store_save_error", path=str(path))
