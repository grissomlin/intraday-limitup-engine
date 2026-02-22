# markets/uk/aggregator.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict

from markets.common.open_movers_aggregator import aggregate_no_limit_market

from .builders_uk import (
    build_open_limit_watchlist_uk,
    build_sector_summary_open_limit_uk,
)


def aggregate(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    UK aggregator (no daily limit):
    - snapshot_open -> open_limit_watchlist
    - open_limit_sector_summary (kept)
    - sector_summary for overview_mpl (bigmove10/touched + Breadth% via common aggregator)
    - keep move_band/move_key in watch rows (for sector pages / labels)
    """
    return aggregate_no_limit_market(
        payload,
        market_code="UK",
        watch_builder=build_open_limit_watchlist_uk,
        sector_summary_builder=build_sector_summary_open_limit_uk,
        snapshot_key_primary="snapshot_open",
        snapshot_key_fallback="snapshot_emerging",
        ret_th_default=0.10,
    )
