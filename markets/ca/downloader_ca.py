# markets/ca/downloader_ca.py
# -*- coding: utf-8 -*-
"""
Thin entrypoint for Canada market (TSX/TSXV).

Public API (used by main.py):
  - run_sync(...)
  - run_intraday(...)

Implementation is split into:
  - ca_list.py
  - ca_prices.py
  - ca_snapshot.py

Env:
- CA_DB_PATH
- CA_LIST_URL / CA_LIST_XLSX_PATH (optional)
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .ca_prices import run_sync as _run_sync_prices
from .ca_snapshot import run_intraday as _run_intraday


def run_sync(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,  # end_exclusive (kept for consistency with US/UK)
    refresh_list: bool = True,
) -> Dict[str, Any]:
    return _run_sync_prices(start_date=start_date, end_date=end_date, refresh_list=refresh_list)


def run_intraday(*, slot: str, asof: str, ymd: str) -> Dict[str, Any]:
    return _run_intraday(slot=slot, asof=asof, ymd=ymd)


if __name__ == "__main__":
    res = run_sync(refresh_list=True)
    print("run_sync:", {k: res.get(k) for k in ["success", "total", "failed", "db_path"]})
