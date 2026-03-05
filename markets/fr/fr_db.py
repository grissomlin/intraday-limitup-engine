# markets/fr/fr_db.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def init_db(db_path: str) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_prices (
                symbol TEXT,
                date   TEXT,
                open   REAL,
                high   REAL,
                low    REAL,
                close  REAL,
                volume INTEGER,
                PRIMARY KEY (symbol, date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_info (
                symbol TEXT PRIMARY KEY,
                local_symbol TEXT,
                name   TEXT,
                sector TEXT,
                industry TEXT,
                market TEXT,
                market_detail TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS download_errors (
                symbol TEXT,
                name   TEXT,
                start_date TEXT,
                end_date   TEXT,
                error TEXT,
                created_at TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_symbol ON stock_prices(symbol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_date ON stock_prices(date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_info_market ON stock_info(market)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_err_symbol ON download_errors(symbol)")
        conn.commit()
    finally:
        conn.close()


def insert_prices(conn: sqlite3.Connection, df_long: pd.DataFrame) -> None:
    if df_long is None or df_long.empty:
        return

    dfw = df_long.copy()
    dfw["volume"] = pd.to_numeric(dfw["volume"], errors="coerce")
    for col in ["open", "high", "low", "close"]:
        dfw[col] = pd.to_numeric(dfw[col], errors="coerce")

    rows = [
        (
            str(r.symbol),
            str(r.date)[:10],
            None if pd.isna(r.open) else float(r.open),
            None if pd.isna(r.high) else float(r.high),
            None if pd.isna(r.low) else float(r.low),
            None if pd.isna(r.close) else float(r.close),
            None if pd.isna(r.volume) else int(r.volume),
        )
        for r in dfw.itertuples(index=False)
    ]

    conn.executemany(
        "INSERT OR REPLACE INTO stock_prices (symbol, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def pick_latest_leq(conn: sqlite3.Connection, ymd: str) -> Optional[str]:
    row = conn.execute("SELECT MAX(date) FROM stock_prices WHERE date <= ?", (ymd,)).fetchone()
    return row[0] if row and row[0] else None
