# markets/tw/aggregator/meta.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict

from ..config import (
    TEST_MODE_DEFAULT,
    FORCE_RET_GE_10_AS_LIMITUP,
    NO_LIMIT_SYMBOLS,
    AUTO_INFER_NO_LIMIT_FROM_PRICE,
    EMERGING_STRONG_RET,
)
from .helpers import safe_int


def apply_filters(payload: Dict[str, Any], *, slot: str, surge_ret: float) -> None:
    payload.setdefault("filters", {})
    payload["filters"]["test_mode"] = (
        True
        if TEST_MODE_DEFAULT is True
        else False
        if TEST_MODE_DEFAULT is False
        else slot in ("midday", "am", "pm")
    )
    payload["filters"]["force_ret_ge_10_as_limitup"] = bool(FORCE_RET_GE_10_AS_LIMITUP)
    payload["filters"]["no_limit_symbols_cnt"] = int(len(NO_LIMIT_SYMBOLS))
    payload["filters"]["auto_infer_no_limit_from_price"] = int(1 if AUTO_INFER_NO_LIMIT_FROM_PRICE else 0)
    payload["filters"]["surge_ret_threshold"] = float(surge_ret)
    payload["filters"]["open_limit_ret_high_threshold"] = float(EMERGING_STRONG_RET)


def apply_stats(
    payload: Dict[str, Any],
    *,
    dfS_len: int,
    dfO_len: int,
    is_market_open: bool,
) -> None:
    payload.setdefault("stats", {})
    payload["stats"].update(
        {
            "snapshot_main_count": int(dfS_len),
            "snapshot_open_count": int(dfO_len),
            "limitup_count": int(len(payload.get("limitup") or [])),
            "peers_sectors": int(len(payload.get("peers_by_sector") or {})),
            "peers_flat_count": int(len(payload.get("peers_not_limitup") or [])),
            "open_limit_watchlist_count": int(len(payload.get("open_limit_watchlist") or [])),
            "open_limit_sector_cnt": int(len(payload.get("open_limit_sector_summary") or [])),
            "universe_total": int(payload.get("universe", {}).get("total", 0) or 0),
            "is_market_open": int(1 if is_market_open else 0),
        }
    )

    counts = payload.get("overview_counts") or {}
    payload["stats"].update(
        {
            "tw_locked_total": safe_int(counts.get("locked_total"), 0),
            "tw_touch_only_total": safe_int(counts.get("touch_only_total"), 0),
            "tw_bigmove10_total": safe_int(counts.get("bigmove10_total"), 0),
            "tw_bigmove10_inclusive_total": safe_int(counts.get("bigmove10_inclusive_total"), 0),
            "tw_bigmove10_ex_locked_total": safe_int(counts.get("bigmove10_ex_locked_total"), 0),
            "tw_open_limit_close_ge10_total": safe_int(counts.get("open_limit_close_ge10_total"), 0),
            "tw_no_limit_theme_total": safe_int(counts.get("no_limit_theme_total"), 0),
            "tw_open_limit_theme_total": safe_int(counts.get("open_limit_theme_total"), 0),
            "tw_mix_total": safe_int(counts.get("mix_total"), 0),
        }
    )


def apply_meta(
    payload: Dict[str, Any],
    *,
    requested_ymd: str,
    current_ymd: str,
    raw_ymd_effective: str,
    inferred_ymd_effective: str,
) -> None:
    payload.setdefault("meta", {})
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        meta = {}
        payload["meta"] = meta

    meta.update(
        {
            "requested_ymd": requested_ymd,
            "ymd_in_payload": current_ymd,
            "ymd_effective": payload.get("ymd_effective", ""),
            "ymd_effective_sources": {
                "raw_payload": raw_ymd_effective,
                "inferred_from_rows": inferred_ymd_effective,
            },
        }
    )

    totals = meta.get("totals")
    if not isinstance(totals, dict):
        totals = {}

    counts = payload.get("overview_counts") or {}

    totals["locked_total"] = safe_int(counts.get("locked_total"), 0)
    totals["touched_total"] = safe_int(counts.get("touch_only_total"), 0)

    totals["bigmove10_total"] = safe_int(counts.get("bigmove10_total"), 0)
    totals["bigmove10_inclusive_total"] = safe_int(counts.get("bigmove10_inclusive_total"), 0)

    totals["bigmove10_ex_locked_total"] = safe_int(counts.get("bigmove10_ex_locked_total"), 0)
    totals["open_limit_close_ge10_total"] = safe_int(counts.get("open_limit_close_ge10_total"), 0)

    totals["no_limit_theme_total"] = safe_int(counts.get("no_limit_theme_total"), 0)
    totals["mix_total"] = safe_int(counts.get("mix_total"), 0)

    totals["open_limit_theme_total"] = safe_int(counts.get("open_limit_theme_total"), 0)
    totals["open_limit_locked_total"] = safe_int(counts.get("open_limit_locked_total"), 0)
    totals["open_limit_opened_total"] = safe_int(counts.get("open_limit_opened_total"), 0)

    totals["touch_only_ret_ge10_total"] = safe_int(counts.get("touch_only_ret_ge10_total"), 0)
    totals["touch_only_ret_lt10_total"] = safe_int(counts.get("touch_only_ret_lt10_total"), 0)

    metrics = meta.get("metrics")
    if not isinstance(metrics, dict):
        metrics = {}

    metrics["bigmove10_total"] = safe_int(counts.get("bigmove10_total"), 0)
    metrics["bigmove10_inclusive_total"] = safe_int(counts.get("bigmove10_inclusive_total"), 0)
    metrics["bigmove10_ex_locked_total"] = safe_int(counts.get("bigmove10_ex_locked_total"), 0)
    metrics["open_limit_close_ge10_total"] = safe_int(counts.get("open_limit_close_ge10_total"), 0)
    metrics["mix_total"] = safe_int(counts.get("mix_total"), 0)

    meta["totals"] = totals
    meta["metrics"] = metrics
    payload["meta"] = meta
