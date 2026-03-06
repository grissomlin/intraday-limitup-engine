# markets/fr/aggregator.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List

from markets.common.open_movers_aggregator import (
    aggregate_no_limit_market,
)

from .builders_fr import (
    build_open_limit_watchlist_fr,
    build_sector_summary_open_limit_fr,
)


def _pct(x: Any) -> float:
    try:
        return float(x or 0.0)
    except Exception:
        return 0.0


def _bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    s = str(x or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _norm_sector(x: Any) -> str:
    s = str(x or "").strip()
    if (not s) or s.lower() in {"nan", "none", "-", "—", "--"}:
        return "Unknown"
    return s


def _safe_div(n: float, d: float) -> float:
    try:
        if d and d > 0:
            return float(n) / float(d)
    except Exception:
        pass
    return 0.0


def _build_fr_sector_summary_from_snapshot_open(
    snapshot_open: List[Dict[str, Any]],
    *,
    ret_th: float,
) -> List[Dict[str, Any]]:
    """
    FR 專用 overview sector_summary：
    直接從 snapshot_open 建，而不是只從 watchlist 建。
    這樣 sector page 與 overview 口徑更接近。

    規則：
    - touched_only / is_limitup_touch => touched_cnt
    - ret >= ret_th 且非 touched_only => bigmove10_cnt
    - sector_total_cnt = 該 sector 在 snapshot_open 的總數
    """
    buckets: Dict[str, Dict[str, int]] = {}

    for r in snapshot_open or []:
        sector = _norm_sector(r.get("sector") or r.get("industry"))
        ret = _pct(r.get("ret"))
        touched_only = _bool(r.get("touched_only"))
        is_touch_flag = _bool(r.get("is_limitup_touch"))

        is_touch = bool(touched_only or is_touch_flag)
        is_hit = (ret >= float(ret_th)) and (not is_touch)

        if sector not in buckets:
            buckets[sector] = {
                "sector_total_cnt": 0,
                "bigmove10_cnt": 0,
                "touched_cnt": 0,
            }

        buckets[sector]["sector_total_cnt"] += 1

        if is_touch:
            buckets[sector]["touched_cnt"] += 1
        elif is_hit:
            buckets[sector]["bigmove10_cnt"] += 1

    out: List[Dict[str, Any]] = []
    for sector, c in buckets.items():
        total_cnt = int(c["sector_total_cnt"])
        big_cnt = int(c["bigmove10_cnt"])
        tch_cnt = int(c["touched_cnt"])

        # 只保留有事件的 sector，避免 overview 爆太多無意義 rows
        if big_cnt <= 0 and tch_cnt <= 0:
            continue

        out.append(
            {
                "sector": sector,
                "locked_cnt": 0,
                "touched_cnt": tch_cnt,
                "bigmove10_cnt": big_cnt,
                "sector_total_cnt": total_cnt,
                "bigmove10_pct": _safe_div(big_cnt, total_cnt),
                "touched_pct": _safe_div(tch_cnt, total_cnt),
            }
        )

    out.sort(
        key=lambda x: (
            x.get("bigmove10_cnt", 0),
            x.get("touched_cnt", 0),
            x.get("sector_total_cnt", 0),
        ),
        reverse=True,
    )
    return out


def aggregate(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    FR aggregator (no daily limit):
    - snapshot_open -> open_limit_watchlist
    - keep open_limit_sector_summary
    - BUT overview sector_summary is rebuilt from snapshot_open
      so it matches sector pages more closely
    """
    out = aggregate_no_limit_market(
        payload,
        market_code="FR",
        watch_builder=build_open_limit_watchlist_fr,
        sector_summary_builder=build_sector_summary_open_limit_fr,
        snapshot_key_primary="snapshot_open",
        snapshot_key_fallback="snapshot_emerging",
        ret_th_default=0.10,
    )

    filters = out.get("filters") or {}
    ret_th = float(filters.get("ret_th", 0.10) or 0.10)

    snapshot_open = out.get("snapshot_open") or []
    if not isinstance(snapshot_open, list):
        snapshot_open = []

    # ✅ FR 專屬：overview 改用 snapshot_open 重建
    fr_sector_summary = _build_fr_sector_summary_from_snapshot_open(
        snapshot_open,
        ret_th=ret_th,
    )
    out["sector_summary"] = fr_sector_summary

    # stats refresh
    out.setdefault("stats", {})
    out["stats"]["sector_summary_count"] = int(len(fr_sector_summary))

    return out
