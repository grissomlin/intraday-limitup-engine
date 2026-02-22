# markets/au/downloader_au.py
# -*- coding: utf-8 -*-
"""
AU (Australia / ASX) - thin entrypoint.

Public API used by main.py / runners.py:
  - run_sync(...)
  - run_intraday(...)

AU is treated as a "no daily limit" market (open movers), same contract as UK/CA/US.

Implementation split:
  - au_list.py     : official ASX list download + filtering + sector mapping (GICS industry group)
  - au_prices.py   : rolling-window prices sync into DB
  - au_snapshot.py : DB -> snapshot_open builder

Env (optional):
- AU_DB_PATH
- AU_LIST_URL / AU_LIST_CSV_PATH
- AU_OPEN_WATCHLIST_RET_TH (used by builders/aggregator side, not here)
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .au_prices import run_sync as _run_sync_prices
from .au_snapshot import run_intraday as _run_intraday


def run_sync(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,  # end_exclusive (kept for consistency with US/UK/CA)
    refresh_list: bool = True,
) -> Dict[str, Any]:
    """
    Sync AU rolling-window data (list + prices).

    Args:
        start_date: "YYYY-MM-DD" (inclusive). If None, backend decides default rolling window.
        end_date  : "YYYY-MM-DD" (exclusive). Kept consistent with US/UK/CA contract.
        refresh_list: If True, refresh AU_list.csv / stock_info (sector uses ASX "GICS industry group" raw string).

    Returns:
        Dict payload compatible with runners/guard:
        - should include at least: success/total/failed/db_path (depending on au_prices implementation)
    """
    return _run_sync_prices(start_date=start_date, end_date=end_date, refresh_list=refresh_list)


def run_intraday(*, slot: str, asof: str, ymd: str) -> Dict[str, Any]:
    """
    Build AU intraday snapshot (from DB) for render/aggregation.

    Args:
        slot: e.g. "open" / "mid" / "close" (used for cache path + images)
        asof: "HH:MM" (string displayed on images)
        ymd : "YYYY-MM-DD"

    Returns:
        raw_payload that contains:
        - snapshot_open (list[dict]) for open movers aggregator
        - meta/ymd_effective if your snapshot builder provides it
    """
    return _run_intraday(slot=slot, asof=asof, ymd=ymd)


if __name__ == "__main__":
    # quick smoke test (local)
    res = run_sync(refresh_list=True)
    keys = ["success", "total", "failed", "db_path", "list_rows", "kept_rows", "prices_rows"]
    print("run_sync:", {k: res.get(k) for k in keys if k in res})
