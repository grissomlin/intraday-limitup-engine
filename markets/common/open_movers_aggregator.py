# markets/common/open_movers_aggregator.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple
from datetime import datetime


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
    return s if s else "Unknown"


def _safe_div(n: float, d: float) -> float:
    try:
        if d and d > 0:
            return float(n) / float(d)
    except Exception:
        pass
    return 0.0


# ------------------------------------------------------------
# Move-word buckets (for non-limit markets)
# ------------------------------------------------------------
_BUCKETS: List[Tuple[float, float, str]] = [
    (0.00, 0.10, "mild"),
    (0.10, 0.20, "big"),
    (0.20, 0.30, "mover"),
    (0.30, 0.40, "surge"),
    (0.40, 0.50, "soar"),
    (0.50, 0.60, "jump"),
    (0.60, 0.70, "spike"),
    (0.70, 0.80, "blast"),
    (0.80, 0.90, "rocket"),
    (0.90, 1.00, "zoom"),
    (1.00, 9.99, "moon"),
]


def move_word(ret: float) -> str:
    """ret is ratio (0.12 = +12%). Only for UP movers; caller should ensure ret>=0."""
    r = float(ret or 0.0)
    for lo, hi, w in _BUCKETS:
        if (r >= lo) and (r < hi):
            return w
    return "mild"


def move_band(ret: float) -> str:
    """Human-friendly band label."""
    r = float(ret or 0.0)
    if r >= 1.0:
        return "100%+"
    lo = int((r // 0.10) * 10)
    hi = lo + 10
    # clamp to 0–100
    lo = max(0, min(lo, 90))
    hi = max(10, min(hi, 100))
    return f"{lo}–{hi}%"


# ------------------------------------------------------------
# Totals & sector_summary for overview_mpl
# ------------------------------------------------------------
def build_sector_totals_from_snapshot(snapshot_rows: List[Dict[str, Any]]) -> Dict[str, int]:
    totals: Dict[str, int] = {}
    for r in snapshot_rows or []:
        sector = _norm_sector(r.get("sector") or r.get("industry"))
        totals[sector] = totals.get(sector, 0) + 1
    return totals


def build_sector_summary_from_watchlist(
    watch: List[Dict[str, Any]],
    ret_th: float,
    sector_totals: Optional[Dict[str, int]] = None,
    *,
    sector_key1: str = "sector",
    sector_key2: str = "industry",
    ret_key: str = "ret",
    touched_only_key: str = "touched_only",
    touch_flag_key: str = "is_limitup_touch",
) -> List[Dict[str, Any]]:
    """
    Output schema that overview/render.py recognizes:
      - sector
      - locked_cnt (always 0)
      - touched_cnt
      - bigmove10_cnt

    Extra:
      - sector_total_cnt
      - bigmove10_pct
      - touched_pct
    """
    buckets: Dict[str, Dict[str, int]] = {}

    for r in watch or []:
        sector = _norm_sector(r.get(sector_key1) or r.get(sector_key2))
        ret = _pct(r.get(ret_key))
        touched_only = _bool(r.get(touched_only_key))
        is_touch_flag = _bool(r.get(touch_flag_key))

        is_touch = bool(touched_only or is_touch_flag)
        is_hit = (ret >= float(ret_th)) and (not is_touch)

        if sector not in buckets:
            buckets[sector] = {"bigmove10_cnt": 0, "touched_cnt": 0}

        if is_touch:
            buckets[sector]["touched_cnt"] += 1
        elif is_hit:
            buckets[sector]["bigmove10_cnt"] += 1

    totals = sector_totals or {}
    out: List[Dict[str, Any]] = []
    for sector, c in buckets.items():
        total_cnt = int(totals.get(sector, 0))
        big_cnt = int(c["bigmove10_cnt"])
        tch_cnt = int(c["touched_cnt"])

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


# ------------------------------------------------------------
# Public: aggregate for "no daily limit" markets
# ------------------------------------------------------------
WatchBuilder = Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]
SectorSummaryBuilder = Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]


def aggregate_no_limit_market(
    payload: Dict[str, Any],
    *,
    market_code: str,
    watch_builder: WatchBuilder,
    sector_summary_builder: SectorSummaryBuilder,
    snapshot_key_primary: str = "snapshot_open",
    snapshot_key_fallback: str = "snapshot_emerging",
    ret_th_default: float = 0.10,
) -> Dict[str, Any]:
    payload = dict(payload or {})

    snapshot_open = payload.get(snapshot_key_primary)
    if snapshot_open is None:
        snapshot_open = payload.get(snapshot_key_fallback) or []
    else:
        snapshot_open = snapshot_open or []

    payload[snapshot_key_primary] = snapshot_open
    payload.setdefault("snapshot_main", [])

    payload.setdefault("filters", {})
    ret_th = float(payload["filters"].get("ret_th", ret_th_default) or ret_th_default)

    sector_totals = build_sector_totals_from_snapshot(snapshot_open)

    watch = watch_builder(snapshot_open)

    # enrich watch rows with move_word/move_band for sector-pages usage
    for r in watch:
        try:
            rr = _pct(r.get("ret"))
        except Exception:
            rr = 0.0
        if rr >= 0:
            r["move_word"] = move_word(rr)
            r["move_band"] = move_band(rr)

    payload["open_limit_watchlist"] = watch
    payload["open_limit_sector_summary"] = sector_summary_builder(watch)

    # legacy alias
    payload["emerging_watchlist"] = payload["open_limit_watchlist"]
    payload["emerging_sector_summary"] = payload["open_limit_sector_summary"]

    # overview-compatible sector_summary
    payload["sector_summary"] = build_sector_summary_from_watchlist(watch, ret_th, sector_totals)

    # placeholders (keep schema consistent with limit-up markets)
    payload["limitup"] = payload.get("limitup") or []
    payload["peers_by_sector"] = payload.get("peers_by_sector") or {}
    payload["peers_not_limitup"] = payload.get("peers_not_limitup") or []

    payload.setdefault("stats", {})
    payload["stats"].update(
        {
            "snapshot_main_count": 0,
            "snapshot_open_count": int(len(snapshot_open)),
            "open_limit_watchlist_count": int(len(watch)),
            "sector_summary_count": int(len(payload.get("sector_summary") or [])),
            "peers_sectors": int(len(payload.get("peers_by_sector") or {})),
            "peers_flat_count": int(len(payload.get("peers_not_limitup") or [])),
            "sector_totals_count": int(len(sector_totals)),
        }
    )

    payload["filters"].setdefault("enable_open_watchlist", True)

    payload.setdefault("meta", {})
    payload["meta"].setdefault("aggregated_at", datetime.now().isoformat(timespec="seconds"))
    payload["meta"].setdefault("aggregator", f"markets.{market_code.lower()}.aggregator.aggregate")

    payload["market"] = payload.get("market") or market_code
    return payload
