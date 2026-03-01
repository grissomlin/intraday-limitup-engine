# markets/india/in_config.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import pandas as pd


def _db_path() -> str:
    return os.getenv("IN_DB_PATH", os.path.join(os.path.dirname(__file__), "in_stock_warehouse.db"))


def _rolling_trading_days() -> int:
    return int(os.getenv("IN_ROLLING_TRADING_DAYS", "30"))


def _calendar_ticker() -> str:
    # India index example: ^NSEI (NIFTY 50)
    return os.getenv("IN_CALENDAR_TICKER", "^NSEI")


def _calendar_lookback_cal_days() -> int:
    return int(os.getenv("IN_CAL_LOOKBACK_CAL_DAYS", "240"))


def _fallback_rolling_cal_days() -> int:
    return int(os.getenv("IN_ROLLING_CAL_DAYS", "120"))


def _batch_size() -> int:
    return int(os.getenv("IN_DAILY_BATCH_SIZE", "180"))


def _batch_sleep_sec() -> float:
    return float(os.getenv("IN_BATCH_SLEEP_SEC", "0.15"))


def _fallback_single_enabled() -> bool:
    return str(os.getenv("IN_FALLBACK_SINGLE", "1")).strip() == "1"


def _yf_threads_enabled() -> bool:
    return str(os.getenv("IN_YF_THREADS", "1")).strip() == "1"


def _single_sleep_sec() -> float:
    return float(os.getenv("IN_SLEEP_SEC", "0.20"))


def _yf_suffix() -> str:
    return os.getenv("IN_YF_SUFFIX", ".NS").strip() or ".NS"


def _master_csv_path() -> str:
    """
    你的每日產出 master 檔：
      NSE_Stock_Master_Data.csv
    建議 workflow 把它放 repo 的 data/ 或 cache 後再跑 run_sync。

    Env:
      IN_MASTER_CSV_PATH
    Default:
      data/nse/NSE_Stock_Master_Data.csv
    """
    return os.getenv("IN_MASTER_CSV_PATH", os.path.join("data", "nse", "NSE_Stock_Master_Data.csv"))


def _cal_cache_root() -> str:
    return os.getenv("CAL_CACHE_ROOT", os.path.join("data", "cache", "calendar"))


def log(msg: str) -> None:
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)
