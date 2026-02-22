# markets/th/th_config.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import glob
import os
from typing import Any

import pandas as pd


# =============================================================================
# Env getters
# =============================================================================
def _db_path() -> str:
    return os.getenv("TH_DB_PATH", os.path.join(os.path.dirname(__file__), "th_stock_warehouse.db"))


def _rolling_trading_days() -> int:
    return int(os.getenv("TH_ROLLING_TRADING_DAYS", "30"))


def _calendar_ticker() -> str:
    # SET index on Yahoo is typically "^SET.BK"
    return os.getenv("TH_CALENDAR_TICKER", "^SET.BK")


def _calendar_lookback_cal_days() -> int:
    return int(os.getenv("TH_CAL_LOOKBACK_CAL_DAYS", "240"))


def _fallback_rolling_cal_days() -> int:
    return int(os.getenv("TH_ROLLING_CAL_DAYS", "120"))


def _batch_size() -> int:
    return int(os.getenv("TH_DAILY_BATCH_SIZE", "200"))


def _batch_sleep_sec() -> float:
    return float(os.getenv("TH_BATCH_SLEEP_SEC", "0.05"))


def _fallback_single_enabled() -> bool:
    return str(os.getenv("TH_FALLBACK_SINGLE", "1")).strip() == "1"


def _yf_threads_enabled() -> bool:
    return str(os.getenv("TH_YF_THREADS", "1")).strip() == "1"


def _single_sleep_sec() -> float:
    return float(os.getenv("TH_SLEEP_SEC", "0.03"))


def _yf_suffix() -> str:
    # Most Thai equities use ".BK" on Yahoo
    return os.getenv("TH_YF_SUFFIX", ".BK").strip() or ".BK"


def _disable_thaifin() -> bool:
    return str(os.getenv("TH_DISABLE_THAIFIN", "0")).strip().lower() in ("1", "true", "yes", "y", "on")


def _list_xlsx_path() -> str:
    """
    1) If TH_LIST_XLSX_PATH is set -> use it (even if missing; caller will handle)
    2) Else default to same folder: thai_stocks_with_industry_sector.xlsx
    3) If default missing -> glob same folder: thai_stocks_with_industry_sector*.xlsx and pick latest mtime
    """
    envp = os.getenv("TH_LIST_XLSX_PATH", "").strip()
    if envp:
        return envp

    base_dir = os.path.dirname(__file__)
    default_path = os.path.join(base_dir, "thai_stocks_with_industry_sector.xlsx")
    if os.path.exists(default_path):
        return default_path

    pattern = os.path.join(base_dir, "thai_stocks_with_industry_sector*.xlsx")
    cands = [p for p in glob.glob(pattern) if os.path.isfile(p)]
    if not cands:
        return default_path  # return default (missing) as last resort

    # pick latest modified
    cands.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return cands[0]


def _cal_cache_root() -> str:
    return os.getenv("CAL_CACHE_ROOT", os.path.join("data", "cache", "calendar"))


def log(msg: str) -> None:
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)