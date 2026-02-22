# markets/uk/downloader_uk.py
# -*- coding: utf-8 -*-
"""
Thin entrypoint for UK market.

This file keeps the public API used by main.py:
  - run_sync(...)
  - run_intraday(...)

UK is treated as a no-daily-limit market (open movers), similar to US.

Implementation is split into:
  - uk_list.py
  - uk_prices.py
  - uk_snapshot.py

Env:
- UK_DB_PATH
- UK_LIST_URL / UK_LIST_XLSX_PATH (optional)
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .uk_prices import run_sync as _run_sync_prices
from .uk_snapshot import run_intraday as _run_intraday


def run_sync(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,  # end_exclusive (kept for consistency with US)
    refresh_list: bool = True,
) -> Dict[str, Any]:
    """
    Run UK rolling-window sync (prices + list).

    - start_date/end_date: forwarded to uk_prices.run_sync
      NOTE: end_date is treated as end_exclusive in caller contract (same as US).
    - refresh_list=True: re-download instrument list and refresh stock_info.
    """
    return _run_sync_prices(start_date=start_date, end_date=end_date, refresh_list=refresh_list)


def run_intraday(*, slot: str, asof: str, ymd: str) -> Dict[str, Any]:
    """
    Build UK intraday snapshot (from DB) for render/aggregation.

    - slot: e.g. "open", "mid", "close"
    - asof: "HH:MM" string shown on images
    - ymd: "YYYY-MM-DD"
    """
    return _run_intraday(slot=slot, asof=asof, ymd=ymd)


if __name__ == "__main__":
    # quick smoke test
    res = run_sync(refresh_list=True)
    print("run_sync:", {k: res.get(k) for k in ["success", "total", "failed", "db_path"]})
