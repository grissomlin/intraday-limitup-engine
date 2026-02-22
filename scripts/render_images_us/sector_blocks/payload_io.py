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
    # some payloads use ymd_effective, others use ymd
    return str(payload.get("ymd_effective") or payload.get("ymd") or "").strip()


def slot(payload: Dict[str, Any]) -> str:
    # in older payloads, asof == slot
    return str(payload.get("slot") or payload.get("asof") or "").strip() or "close"


def yesterday_ymd(ymd: str) -> Optional[str]:
    """Calendar-yesterday (kept for backward-compat)."""
    try:
        d = datetime.strptime(ymd, "%Y-%m-%d").date()
        return (d - timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception:
        return None


def find_payload_for_ymd(*, repo_root: Path, market: str, ymd: str, slot: str) -> Optional[Path]:
    """
    Expected cache layout:
      data/cache/{market}/{ymd}/{slot}.payload.json
    """
    p = repo_root / "data" / "cache" / market / ymd / f"{slot}.payload.json"
    return p if p.exists() else None


def prev_trading_ymd(
    ymd: str,
    *,
    repo_root: Path,
    market: str,
    slot: str,
    lookback_days: int = 10,
) -> Optional[str]:
    """
    Previous trading day (previous session) helper.

    We intentionally do NOT rely on an exchange calendar here.
    Instead, we walk back from ymd and return the most recent date
    that has a cached payload file for the same market+slot.

    This fixes:
      - Mon/holiday -> "yesterday" is non-trading day
      - multi-day holidays (CNY, Golden Week, etc.)

    lookback_days:
      - 10 is usually enough; increase if you want to cover longer closures.
    """
    try:
        d0 = datetime.strptime(ymd, "%Y-%m-%d").date()
    except Exception:
        return None

    for k in range(1, max(1, int(lookback_days)) + 1):
        y = (d0 - timedelta(days=k)).strftime("%Y-%m-%d")
        if find_payload_for_ymd(repo_root=repo_root, market=market, ymd=y, slot=slot):
            return y
    return None
