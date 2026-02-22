# scripts/render_images_us/sector_blocks/payload_io.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def ymd_effective(payload: Dict[str, Any]) -> str:
    return str(payload.get("ymd_effective") or payload.get("ymd") or "").strip()


def slot(payload: Dict[str, Any]) -> str:
    return str(payload.get("slot") or payload.get("asof") or "").strip() or "close"


def yesterday_ymd(ymd: str) -> Optional[str]:
    try:
        d = datetime.strptime(ymd, "%Y-%m-%d").date()
        return (d - timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception:
        return None


def find_payload_for_ymd(*, repo_root: Path, market: str, ymd: str, slot: str) -> Optional[Path]:
    """
    data/cache/{market}/{ymd}/{slot}.payload.json
    """
    p = repo_root / "data" / "cache" / market / ymd / f"{slot}.payload.json"
    return p if p.exists() else None
