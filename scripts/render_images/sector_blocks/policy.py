# scripts/render_images/sector_blocks/policy.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
      - emerging_watchlist
      - limitup
      - peers_not_limitup
      - snapshot_emerging (if exists)
      - snapshot_main (if exists)
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

    feed(py.get("emerging_watchlist") or [])
    feed(py.get("limitup") or [])
    feed(py.get("peers_not_limitup") or [])
    feed(py.get("snapshot_emerging") or [])
    feed(py.get("snapshot_main") or [])
    return strong


def inject_emerging_strong_into_peers(
    payload: Dict[str, Any],
    *,
    sector: str,
    peer_rows: List[Dict[str, Any]],
    yesterday_strong: set[str],
    cap: int,
    strong_ret: float = 0.10,
) -> List[Dict[str, Any]]:
    """
    Inject emerging strong movers (ret>=strong_ret) into peers block for the same sector.
    - Use payload["emerging_watchlist"] (already filtered in aggregator)
    - Mark peer_kind="emerging_strong"
    - Set streak_prev=1 if yesterday also strong
    """
    ew = payload.get("emerging_watchlist") or []
    if not isinstance(ew, list) or not ew:
        return (peer_rows or [])[:cap]

    lim_syms = {str(r.get("symbol", "")).strip() for r in (payload.get("limitup") or [])}
    peer_syms = {str(r.get("symbol", "")).strip() for r in (peer_rows or [])}

    injected: List[Dict[str, Any]] = []
    for r in ew:
        if not isinstance(r, dict):
            continue
        sec = (r.get("sector") or "未分類")
        if sec != sector:
            continue

        sym = str(r.get("symbol") or "").strip()
        if not sym or sym in lim_syms or sym in peer_syms:
            continue

        try:
            ret = float(r.get("ret", 0.0) or 0.0)
        except Exception:
            ret = 0.0
        if ret < strong_ret:
            continue

        rr = {
            "symbol": sym,
            "name": r.get("name") or "",
            "sector": sec,
            "market_detail": r.get("market_detail") or "emerging",
            "market_label": r.get("market_label") or "興櫃",
            "ret": ret,
            "peer_kind": "emerging_strong",
            "streak_prev": 1 if sym in yesterday_strong else 0,
        }
        injected.append(rr)

    injected.sort(key=lambda x: float(x.get("ret", 0.0) or 0.0), reverse=True)

    merged = injected + (peer_rows or [])

    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for r in merged:
        sym = str(r.get("symbol") or "").strip()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(r)

    return out[:cap]


def collect_rows(
    payload: Dict[str, Any],
    sector: str,
    *,
    yesterday_strong: set[str],
    max_peers_per_page: int,
    num_pages_hint: int,
    strong_ret: float = 0.10,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Collect limitup rows and peers rows for a sector.
    Also inject emerging strong into peers.
    """
    limitup = payload.get("limitup", []) or []
    L = [r for r in limitup if (r.get("sector") or "未分類") == sector]

    order = {"locked": 0, "touch_only": 1, "no_limit_theme": 2}

    def keyL(r):
        s = str(r.get("limitup_status", "")).lower()
        streak = int(r.get("streak", 0) or 0)
        try:
            ret = float(r.get("ret", 0.0) or 0.0)
        except Exception:
            ret = 0.0
        return (order.get(s, 9), -streak, -ret)

    L.sort(key=keyL)

    P: List[Dict[str, Any]] = []
    if isinstance(payload.get("peers_by_sector"), dict):
        P = list(payload["peers_by_sector"].get(sector, []) or [])

    lim_syms = {str(r.get("symbol", "")).strip() for r in L}
    P2: List[Dict[str, Any]] = []
    for r in P:
        sym = str(r.get("symbol", "")).strip()
        if not sym or sym in lim_syms:
            continue
        if str(r.get("limitup_status", "")).lower() in ("locked", "touch_only", "no_limit_theme"):
            continue
        P2.append(r)

    def keyP(r):
        kind = str(r.get("peer_kind", "")).lower()
        kind_i = 0 if kind == "emerging_strong" else 1
        try:
            retv = float(r.get("ret", 0.0) or 0.0)
        except Exception:
            retv = 0.0
        return (kind_i, -retv)

    cap = max(1, int(num_pages_hint)) * max(1, int(max_peers_per_page))
    P2 = inject_emerging_strong_into_peers(
        payload,
        sector=sector,
        peer_rows=P2,
        yesterday_strong=yesterday_strong,
        cap=cap,
        strong_ret=strong_ret,
    )

    P2.sort(key=keyP)
    return L, P2


def paginate_sector(
    limitup_rows: List[Dict[str, Any]],
    peer_rows: List[Dict[str, Any]],
    max_limitup_per_page: int,
    max_peers_per_page: int,
) -> List[Dict[str, Any]]:
    """
    ✅ 頁數以「漲停頁數」為主，peers 最多多 1 頁
    """
    pages: List[Dict[str, Any]] = []

    nL = max(1, (len(limitup_rows) + max_limitup_per_page - 1) // max_limitup_per_page)

    extra_peer_pages = 1 if len(peer_rows) > (nL * max_peers_per_page) else 0
    num_pages = nL + extra_peer_pages

    peer_cap = num_pages * max_peers_per_page
    peer_rows = peer_rows[:peer_cap]

    for page in range(num_pages):
        sL = page * max_limitup_per_page
        sP = page * max_peers_per_page
        pages.append(
            {
                "limitup_rows": limitup_rows[sL : sL + max_limitup_per_page],
                "peer_rows": peer_rows[sP : sP + max_peers_per_page],
            }
        )
    return pages
