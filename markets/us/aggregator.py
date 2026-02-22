# markets/us/aggregator.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict

from markets.common.open_movers_aggregator import aggregate_no_limit_market

from .builders_us import (
    build_open_limit_watchlist_us,
    build_sector_summary_open_limit_us,
)


def aggregate(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    US aggregator (no daily limit):
    - snapshot_open -> open_limit_watchlist
    - open_limit_sector_summary (kept)
    - sector_summary for overview_mpl (bigmove10/touched + Breadth%)
    - add move_word/move_band into watch rows (for sector pages / labels)
    """
    return aggregate_no_limit_market(
        payload,
        market_code="US",
        watch_builder=build_open_limit_watchlist_us,
        sector_summary_builder=build_sector_summary_open_limit_us,
        snapshot_key_primary="snapshot_open",
        snapshot_key_fallback="snapshot_emerging",
        ret_th_default=0.10,
    )
