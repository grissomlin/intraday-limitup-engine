# markets/fr/fr_config.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from pathlib import Path
from typing import Tuple


def log(msg: str) -> None:
    # keep simple; fr_snapshot imports this
    import pandas as pd
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)


def _parse_bool_env(name: str, default: bool) -> bool:
    v = (os.getenv(name, "").strip() or ("1" if default else "0")).lower()
    return v in ("1", "true", "yes", "y", "on")


def _get_first_env(*names: str) -> Tuple[str, str]:
    for n in names:
        v = (os.getenv(n) or "").strip()
        if v:
            return v, n
    return "", ""


# ----------------------------
# Snapshot thresholds
# ----------------------------
FR_RET_TH = float(os.getenv("FR_RET_TH", "0.10"))
FR_TOUCH_TH = float(os.getenv("FR_TOUCH_TH", "0.10"))
FR_ROWS_PER_BOX = int(os.getenv("FR_ROWS_PER_BOX", "6"))
FR_PEER_EXTRA_PAGES = int(os.getenv("FR_PEER_EXTRA_PAGES", "1"))
FR_STREAK_LOOKBACK_ROWS = int(os.getenv("FR_STREAK_LOOKBACK_ROWS", "90"))
FR_BADGE_FALLBACK_LANG = (os.getenv("FR_BADGE_FALLBACK_LANG", "en") or "en").strip().lower()

# ----------------------------
# DB + Sync
# ----------------------------
def db_path() -> str:
    return os.getenv("FR_DB_PATH", os.path.join(os.path.dirname(__file__), "fr_stock_warehouse.db"))


def rolling_trading_days() -> int:
    return int(os.getenv("FR_ROLLING_TRADING_DAYS", "30"))


def calendar_ticker() -> str:
    return os.getenv("FR_CALENDAR_TICKER", "^FCHI")


def calendar_lookback_cal_days() -> int:
    return int(os.getenv("FR_CAL_LOOKBACK_CAL_DAYS", "180"))


def fallback_rolling_cal_days() -> int:
    return int(os.getenv("FR_ROLLING_CAL_DAYS", "90"))


def batch_size() -> int:
    return int(os.getenv("FR_DAILY_BATCH_SIZE", "200"))


def batch_sleep_sec() -> float:
    return float(os.getenv("FR_BATCH_SLEEP_SEC", "0.05"))


def fallback_single_enabled() -> bool:
    return str(os.getenv("FR_FALLBACK_SINGLE", "1")).strip() == "1"


def yf_threads_enabled() -> bool:
    return str(os.getenv("FR_YF_THREADS", "1")).strip() == "1"


def single_sleep_sec() -> float:
    return float(os.getenv("FR_SLEEP_SEC", "0.02"))


# ----------------------------
# Master CSV path (LOCAL)
# ----------------------------
def master_csv_local_path() -> str:
    """
    IMPORTANT:
    - FR_STOCKLIST is a Drive folder id (secret). Do NOT treat it as local path.
    - Local path is FR_MASTER_CSV_PATH (or fallback to repo default path).
    """
    p = (os.getenv("FR_MASTER_CSV_PATH") or "").strip()
    if p:
        return p

    repo_root = Path(__file__).resolve().parents[2]
    return str(repo_root / "data" / "stocklists" / "fr" / "FR_Stock_Master_Data.csv")
