# scripts/render_images_us/sector_blocks/policy.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

from .payload_io import ymd_effective, slot, yesterday_ymd, find_payload_for_ymd, read_json


def build_yesterday_strong_set(
    today_payload: Dict[str, Any],
    *,
    repo_root: Path,
    market: str,
    strong_ret: float = 0.10,
) -> set[str]:
    """
    Yesterday strong = any symbol with ret>=strong_ret in:
      - snapshot_open / snapshot_main
      - peers_not_limitup
    """
    ymd = ymd_effective(today_payload)
    slt = slot(today_payload)
    ymd_y = yesterday_ymd(ymd)
    if not ymd_y:
        return set()

    p = find_payload_for_ymd(repo_root=repo_root, market=market, ymd=ymd_y, slot=slt)
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
    US: top = movers >=10% (你 CLI 已做)
    peers: 從 payload peers_not_limitup group 回 sector (你 CLI 已做)
    這裡先保留介面，以後若要統一入口可搬過來。
    """
    L: List[Dict[str, Any]] = []
    P: List[Dict[str, Any]] = []
    return L, P
