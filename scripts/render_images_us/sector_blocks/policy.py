# scripts/render_images_us/sector_blocks/policy.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

from .payload_io import (
    ymd_effective,
    slot,
    prev_trading_ymd,
    find_payload_for_ymd,
    read_json,
)


def build_yesterday_strong_set(
    today_payload: Dict[str, Any],
    *,
    repo_root: Path,
    market: str,
    strong_ret: float = 0.10,
    lookback_days: int = 10,
) -> set[str]:
    """
    Previous-session strong set (used for badges like: 前日/전일/prev. session).

    We define "previous session" as:
      - the most recent *cached* payload date before today_ymd (same market+slot)

    This avoids the common weekend/holiday bug:
      - Monday -> calendar yesterday is Sunday (no trading)

    strong = any symbol with ret>=strong_ret found in:
      - snapshot_open / snapshot_main
      - peers_not_limitup
    """
    ymd = ymd_effective(today_payload)
    slt = slot(today_payload)

    ymd_prev = prev_trading_ymd(
        ymd,
        repo_root=repo_root,
        market=market,
        slot=slt,
        lookback_days=lookback_days,
    )
    if not ymd_prev:
        return set()

    p = find_payload_for_ymd(repo_root=repo_root, market=market, ymd=ymd_prev, slot=slt)
    if not p:
        return set()

    py = read_json(p) or {}
    strong: set[str] = set()

    def feed(rows):
        if not rows:
            return
        for r in rows:
            if not isinstance(r, dict):
                continue
            sym = str(r.get("symbol") or "").strip()
            if not sym:
                continue
            try:
                ret = float(r.get("ret", 0.0) or 0.0)
            except Exception:
                ret = 0.0
            if ret >= strong_ret:
                strong.add(sym)

    feed(py.get("snapshot_open") or [])
    feed(py.get("snapshot_main") or [])
    feed(py.get("peers_not_limitup") or [])
    return strong


def collect_rows(
    payload: Dict[str, Any],
    sector: str,
    *,
    max_peers_per_page: int,
    num_pages_hint: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Keep the interface for future unification (common render path).
    The per-market CLI usually already prepares rows:
      - top rows (limitup / movers)
      - peer rows (peers_not_limitup in same sector)
    """
    L: List[Dict[str, Any]] = []
    P: List[Dict[str, Any]] = []
    return L, P
