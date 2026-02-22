# dashboard/components/io_cache.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import glob
from typing import Dict, Any, List, Tuple, Optional

import pandas as pd
import streamlit as st

from .paths import TW_CACHE_DIR


def read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def safe_read_stocklist(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=["symbol", "name", "sector", "market_detail"])
    raw = json.loads(open(path, "r", encoding="utf-8").read())
    df = pd.DataFrame(raw)
    for c in ["symbol", "name", "sector", "market_detail"]:
        if c not in df.columns:
            df[c] = ""
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["name"] = df["name"].fillna("").astype(str)
    df["sector"] = df["sector"].fillna("").replace("", "未分類")
    df["market_detail"] = df["market_detail"].fillna("").astype(str)
    return df[["symbol", "name", "sector", "market_detail"]].drop_duplicates("symbol")


def find_latest_payloads() -> List[str]:
    paths = glob.glob(os.path.join(TW_CACHE_DIR, "*", "*.payload.json"))
    paths = [p for p in paths if os.path.isfile(p)]
    paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return paths


def list_days_slots() -> Tuple[List[str], Dict[str, List[str]]]:
    days: List[str] = []
    mapping: Dict[str, List[str]] = {}
    for day_dir in glob.glob(os.path.join(TW_CACHE_DIR, "20??-??-??")):
        day = os.path.basename(day_dir)
        payloads = glob.glob(os.path.join(day_dir, "*.payload.json"))
        if not payloads:
            continue
        slots = []
        for p in payloads:
            base = os.path.basename(p)
            slot = base.replace(".payload.json", "")
            slots.append(slot)
        slots = sorted(list(set(slots)))
        days.append(day)
        mapping[day] = slots
    days = sorted(list(set(days)), reverse=True)
    return days, mapping


def find_latest_daily_csv() -> Optional[str]:
    paths = glob.glob(os.path.join(TW_CACHE_DIR, "tw_prices_1d_*d_*.csv"))
    paths = [p for p in paths if os.path.isfile(p)]
    if not paths:
        return None
    paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return paths[0]


@st.cache_data(ttl=120)
def load_payload(day: str, slot: str) -> Dict[str, Any]:
    path = os.path.join(TW_CACHE_DIR, day, f"{slot}.payload.json")
    return read_json(path)


@st.cache_data(ttl=3600)
def load_daily_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    for c in ["symbol", "date"]:
        if c not in df.columns:
            df[c] = ""
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["date"] = df["date"].astype(str)
    return df


def latest_and_prev_daily(daily: pd.DataFrame) -> pd.DataFrame:
    """
    回傳每檔最新日K與前一日 close/high，並算 ret。
    columns:
      symbol, bar_date, last_close, last_high, prev_close, ret
    """
    if daily is None or daily.empty:
        return pd.DataFrame(columns=["symbol", "bar_date", "last_close", "last_high", "prev_close", "ret"])

    g = daily.sort_values(["symbol", "date"]).groupby("symbol", as_index=False)
    last_df = g.tail(1)[["symbol", "date", "close", "high"]].rename(
        columns={"date": "bar_date", "close": "last_close", "high": "last_high"}
    )
    prev_df = g.nth(-2)[["symbol", "close"]].rename(columns={"close": "prev_close"})

    out = last_df.merge(prev_df, on="symbol", how="left")
    out["ret"] = (
        pd.to_numeric(out["last_close"], errors="coerce") - pd.to_numeric(out["prev_close"], errors="coerce")
    ) / pd.to_numeric(out["prev_close"], errors="coerce")
    return out
