# markets/us/us_config.py
# -*- coding: utf-8 -*-
"""US pipeline config + helpers (split from downloader_us.py)."""

from __future__ import annotations

import os
import re
from pathlib import Path

import pandas as pd

MARKET_CODE = "us"

BASE_DIR = Path(__file__).resolve().parent

NASDAQ_API = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=20000&download=true"
NASDAQ_REFERER = "https://www.nasdaq.com/market-activity/stocks/screener"

# 篩掉非普通股（粗略，但能少掉 ETF/權證/特殊票）
EXCLUDE_NAME_RE = re.compile(
    r"Warrant|Right|Preferred|Unit|ETF|Index|Index-linked|Trust|Fund|Notes|ETN|Depositary|ADR",
    re.I,
)

# 允許常見 ticker 格式（含 BRK.B / BF.B 這種點）
TICKER_RE = re.compile(r"^[A-Z0-9.\-]+$")


def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)


def _db_path() -> Path:
    p = os.getenv("US_DB_PATH", "").strip()
    if p:
        return Path(p)
    return BASE_DIR / "us_stock_warehouse.db"

