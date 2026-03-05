# markets/fr/fr_config.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd


def log(msg: str) -> None:
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)


def _db_path() -> str:
    return os.getenv("FR_DB_PATH", os.path.join(os.path.dirname(__file__), "fr_stock_warehouse.db"))


def _yf_suffix() -> str:
    # Euronext Paris Yahoo suffix
    return (os.getenv("FR_YF_SUFFIX", ".PA") or ".PA").strip() or ".PA"


def _stocklist_path() -> str:
    """
    ✅ 你說你已經有 FR_STOCKLIST 這個 env，用來放 FR_Stock_Master_Data.csv
    - 沒設就預設放在 markets/fr/data/FR_Stock_Master_Data.csv
    """
    p = (os.getenv("FR_STOCKLIST") or "").strip()
    if p:
        return p
    return str(Path(__file__).resolve().parent / "data" / "FR_Stock_Master_Data.csv")


# ---------- filtering envs (你指定的) ----------
def _env_float(name: str, default: float) -> float:
    try:
        return float((os.getenv(name) or "").strip() or default)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int((os.getenv(name) or "").strip() or default)
    except Exception:
        return int(default)


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name) or "").strip()
    if not v:
        return bool(default)
    return v.lower() in ("1", "true", "yes", "y", "on")


def fr_min_price() -> float:
    return _env_float("FR_MIN_PRICE", 0.10)


def fr_min_volume() -> int:
    return _env_int("FR_MIN_VOLUME", 50000)


def fr_tick_eur() -> float:
    return _env_float("FR_TICK_EUR", 0.01)


def fr_exclude_one_tick_10pct() -> bool:
    return _env_bool("FR_EXCLUDE_ONE_TICK_10PCT", True)
