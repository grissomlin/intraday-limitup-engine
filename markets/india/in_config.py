# markets/india/india_config.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import pandas as pd


def _db_path() -> str:
    return os.getenv("INDIA_DB_PATH", os.path.join(os.path.dirname(__file__), "india_stock_warehouse.db"))


def _rolling_trading_days() -> int:
    return int(os.getenv("INDIA_ROLLING_TRADING_DAYS", "30"))


def _calendar_ticker() -> str:
    # India index example: ^NSEI (NIFTY 50)
    return os.getenv("INDIA_CALENDAR_TICKER", "^NSEI")


def _calendar_lookback_cal_days() -> int:
    return int(os.getenv("INDIA_CAL_LOOKBACK_CAL_DAYS", "240"))


def _fallback_rolling_cal_days() -> int:
    return int(os.getenv("INDIA_ROLLING_CAL_DAYS", "120"))


def _batch_size() -> int:
    return int(os.getenv("INDIA_DAILY_BATCH_SIZE", "180"))


def _batch_sleep_sec() -> float:
    return float(os.getenv("INDIA_BATCH_SLEEP_SEC", "0.15"))


def _fallback_single_enabled() -> bool:
    return str(os.getenv("INDIA_FALLBACK_SINGLE", "1")).strip() == "1"


def _yf_threads_enabled() -> bool:
    return str(os.getenv("INDIA_YF_THREADS", "1")).strip() == "1"


def _single_sleep_sec() -> float:
    return float(os.getenv("INDIA_SLEEP_SEC", "0.20"))


def _yf_suffix() -> str:
    return os.getenv("INDIA_YF_SUFFIX", ".NS").strip() or ".NS"


def _master_csv_path() -> str:
    """
    你的每日產出 master 檔：
      NSE_Stock_Master_Data.csv

    Env:
      INDIA_MASTER_CSV_PATH
    Default:
      data/nse/NSE_Stock_Master_Data.csv
    """
    return os.getenv("INDIA_MASTER_CSV_PATH", os.path.join("data", "nse", "NSE_Stock_Master_Data.csv"))


def _cal_cache_root() -> str:
    return os.getenv("CAL_CACHE_ROOT", os.path.join("data", "cache", "calendar"))


def log(msg: str) -> None:
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)
