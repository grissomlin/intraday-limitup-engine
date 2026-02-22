# markets/us/downloader_us.py
# -*- coding: utf-8 -*-
"""Thin entrypoint for US market.

This file keeps the public API used by main.py:
  - run_sync(...)
  - run_intraday(...)

Implementation is split into:
  - us_config.py
  - us_calendar.py
  - us_db.py
  - us_list.py
  - us_prices.py
  - us_snapshot.py
  - sec_industry_sync.py
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from .us_prices import run_sync as _run_sync_prices
from .us_snapshot import run_intraday as _run_intraday
from .us_config import log
from .sec_industry_sync import sync_sec_industry


def run_sync(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,  # end_exclusive
    refresh_list: bool = True,
) -> Dict[str, Any]:
    """Run US rolling-window sync (prices + optional SEC SIC sync)."""
    res = _run_sync_prices(start_date=start_date, end_date=end_date, refresh_list=refresh_list)

    # ✅ NEW: SEC industry sync (after DB connection closed)
    sec_res: Dict[str, Any] = {"enabled": False}
    if os.getenv("US_SEC_SYNC", "0").strip().lower() in ("1", "true", "yes", "y", "on"):
        try:
            log("🏷️ SEC SIC sync ...")
            sec_res = sync_sec_industry(str(res.get("db_path", "")) or os.getenv("US_DB_PATH", ""))
            log(f"✅ SEC sync done: {sec_res}")
        except Exception as e:
            sec_res = {"enabled": True, "status": "failed", "error": str(e)}
            log(f"⚠️ SEC sync failed (continue): {e}")

    res["sec_sync"] = sec_res
    return res


def run_intraday(*, slot: str, asof: str, ymd: str) -> Dict[str, Any]:
    return _run_intraday(slot=slot, asof=asof, ymd=ymd)
