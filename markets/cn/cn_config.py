# markets/cn/cn_config.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import pandas as pd

# -------------------------
# Config / Paths
# -------------------------
def default_db_path() -> str:
    return os.getenv("CN_DB_PATH", os.path.join(os.path.dirname(__file__), "cn_stock_warehouse.db"))

def rolling_trading_days() -> int:
    return int(os.getenv("CN_ROLLING_TRADING_DAYS", "30"))

def calendar_ticker() -> str:
    return os.getenv("CN_CALENDAR_TICKER", "000001.SS")

def calendar_lookback_cal_days() -> int:
    return int(os.getenv("CN_CAL_LOOKBACK_CAL_DAYS", "180"))

def fallback_rolling_cal_days() -> int:
    return int(os.getenv("CN_ROLLING_CAL_DAYS", "90"))

def intraday_trading_days() -> int:
    return int(os.getenv("CN_TRADING_DAYS", "30") or 30)

def use_requested_ymd() -> bool:
    return os.getenv("CN_USE_REQUESTED_YMD", "").strip().lower() in ("1", "true", "yes", "y", "on")

def debug_limit() -> int:
    return int(os.getenv("CN_DEBUG_LIMIT", "0") or 0)

def sleep_sec() -> float:
    return float(os.getenv("CN_SLEEP_SEC", "0.03"))

def db_vacuum() -> bool:
    return os.getenv("CN_DB_VACUUM", "0").strip().lower() in ("1", "true", "yes", "y", "on")

def fix_sector_flag() -> bool:
    # ✅ 改：預設開（把空/壞 sector 先清成「未分類」，讓 SW sync only-missing 能覆蓋）
    return os.getenv("CN_FIX_SECTOR_MISSING", "1").strip().lower() in ("1", "true", "yes", "y", "on")

# -------------------------
# SW industry sync toggles
# -------------------------
def sync_sw_industry_flag() -> bool:
    # ✅ 改：預設開（你要一條龍就能自動產業）
    return os.getenv("CN_SYNC_SW_INDUSTRY", "1").strip().lower() in ("1", "true", "yes", "y", "on")

def sw_only_missing() -> bool:
    # 預設只補缺失/壞值（不亂覆蓋你已經有的 sector）
    return os.getenv("CN_SW_ONLY_MISSING", "1").strip().lower() in ("1", "true", "yes", "y", "on")

def sw_max_industries() -> str:
    # 測試用：只跑前 N 個行業（空字串代表全部）
    return os.getenv("CN_SW_MAX_INDUSTRIES", "").strip()

def sw_sector_level() -> str:
    """申萬行業層級：l1 / l2 / l3（預設 l3）"""
    v = os.getenv("CN_SECTOR_LEVEL", "l3").strip().lower()
    return v if v in ("l1", "l2", "l3") else "l3"

def sw_sync_timeout() -> int:
    return int(os.getenv("CN_SW_SYNC_TIMEOUT", "600") or 600)

# -------------------------
# Logging
# -------------------------
def log(msg: str) -> None:
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)
