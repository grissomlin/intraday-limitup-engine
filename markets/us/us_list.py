# markets/us/us_list.py
# -*- coding: utf-8 -*-
"""US stock list fetch (Nasdaq first, fallback CSV) - split from downloader_us.py."""

from __future__ import annotations

import io
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import sqlite3
import requests

from .us_db import init_db
from .us_config import NASDAQ_API, NASDAQ_REFERER, EXCLUDE_NAME_RE, TICKER_RE, log

def _fetch_us_list_from_nasdaq_api() -> List[dict]:
    log("📡 Fetching US stock list from Nasdaq API ...")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Referer": NASDAQ_REFERER,
    }
    r = requests.get(NASDAQ_API, headers=headers, timeout=30)
    r.raise_for_status()
    j = r.json()
    rows = (j.get("data") or {}).get("rows") or []
    return rows


def _fetch_us_list_fallback_csv() -> List[dict]:
    """
    Fallback：Stooq 的 symbols list（通常穩，但可能含 ETF/基金，需要我們用 name/格式再濾一次）
    """
    log("📡 Nasdaq API failed; using fallback CSV list (Stooq) ...")
    url = "https://stooq.com/q/l/?s=us&i=1"
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    df = pd.read_csv(io.StringIO(r.text))
    sym_col = None
    for c in df.columns:
        if str(c).lower() in ("symbol", "s"):
            sym_col = c
            break
    if not sym_col:
        return []

    rows: List[dict] = []
    for x in df[sym_col].dropna().tolist():
        rows.append({"symbol": str(x).strip().upper(), "name": "Unknown", "sector": "Unknown", "exchange": "Unknown"})
    return rows


def get_us_stock_list(db_path: Path, refresh_list: bool = True) -> List[Tuple[str, str]]:
    """
    回傳 [(symbol, name), ...]，並寫入 stock_info
    refresh_list=False 代表不重新抓名單，直接從 DB stock_info 取回 symbols
    """
    init_db(db_path)

    if not refresh_list and db_path.exists():
        conn = sqlite3.connect(str(db_path))
        try:
            df = pd.read_sql_query("SELECT symbol, name FROM stock_info WHERE market='US'", conn)
            if not df.empty:
                items = [(str(r["symbol"]), str(r["name"])) for _, r in df.iterrows()]
                log(f"✅ 使用 DB stock_info 既有清單: {len(items)} 檔")
                return items
            log("⚠️ refresh_list=False but stock_info is empty; will try fetch list anyway.")
        finally:
            conn.close()

    rows: List[dict] = []
    source = "NASDAQ_API"
    try:
        rows = _fetch_us_list_from_nasdaq_api()
        source = "NASDAQ_API"
    except Exception as e:
        log(f"⚠️ Nasdaq API list failed: {e}")
        try:
            rows = _fetch_us_list_fallback_csv()
            source = "FALLBACK_CSV"
        except Exception as e2:
            log(f"❌ Fallback list also failed: {e2}")
            return []

    limit_n = int(os.getenv("US_LIMIT_SYMBOLS", "0") or "0")

    conn = sqlite3.connect(str(db_path))
    stock_list: List[Tuple[str, str]] = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        for row in rows:
            symbol = str(row.get("symbol", "")).strip().upper()
            if not symbol:
                continue
            if len(symbol) > 10:
                continue
            if not TICKER_RE.match(symbol):
                continue

            name = str(row.get("name", "Unknown")).strip() or symbol
            if EXCLUDE_NAME_RE.search(name or ""):
                continue

            sector = str(row.get("sector", "Unknown")).strip() or "Unknown"
            exchange = str(row.get("exchange", "Unknown")).strip() or "Unknown"

            conn.execute(
                """
                INSERT OR REPLACE INTO stock_info
                (symbol, name, sector, market, market_detail, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (symbol, name, sector, "US", exchange, now),
            )

            stock_list.append((symbol, name))

            if limit_n > 0 and len(stock_list) >= limit_n:
                break

        conn.commit()
    finally:
        conn.close()

    log(f"✅ US list imported: {len(stock_list)} (source={source}, limit={limit_n or 'ALL'})")
    return stock_list

