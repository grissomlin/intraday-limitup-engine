# scripts/render_images/sector_blocks/payload_io.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional


def ymd_effective(payload: Dict[str, Any]) -> str:
    y = str(payload.get("ymd_effective") or "").strip()
    if y:
        return y
    return str(payload.get("ymd") or "").strip()


def slot(payload: Dict[str, Any]) -> str:
    return str(payload.get("slot") or "midday").strip().lower() or "midday"


def read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        import json

        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def find_payload_for_ymd(*, repo_root: Path, market: str, ymd: str, slot: str) -> Optional[Path]:
    """
    Find:
      data/cache/<market>/<ymd>/<slot>.payload.agg.json
      else data/cache/<market>/<ymd>/<slot>.payload.json
    """
    if not ymd:
        return None
    mkt = (market or "").strip().lower() or "tw"
    base = repo_root / "data" / "cache" / mkt / ymd
    if not base.exists():
        return None

    p1 = base / f"{slot}.payload.agg.json"
    if p1.exists():
        return p1
    p2 = base / f"{slot}.payload.json"
    if p2.exists():
        return p2
    return None


def yesterday_ymd(ymd: str) -> str:
    try:
        import pandas as pd

        dt = pd.to_datetime(ymd, errors="coerce")
        if pd.isna(dt):
            return ""
        return (dt - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception:
        return ""
