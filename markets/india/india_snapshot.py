# markets/india/india_snapshot.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from markets.common.time_builders import build_meta_time_asia
from .india_config import _db_path, log


def _pick_latest_leq(conn: sqlite3.Connection, ymd: str) -> Optional[str]:
    row = conn.execute(
        "SELECT MAX(date) FROM stock_prices WHERE date <= ? AND close IS NOT NULL",
        (ymd,),
    ).fetchone()
    return row[0] if row and row[0] else None


def run_intraday(*, slot: str, asof: str, ymd: str) -> Dict[str, Any]:
    db_path = _db_path()
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"INDIA DB not found: {db_path} (set INDIA_DB_PATH to override)")

    conn = sqlite3.connect(db_path)
    try:
        ymd_effective = _pick_latest_leq(conn, ymd) or ymd
        log(f"🕒 requested ymd={ymd} slot={slot} asof={asof}")
        log(f"📅 ymd_effective = {ymd_effective}")

        sql = """
        WITH p AS (
          SELECT
            symbol,
            date,
            open, high, low, close, volume,
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS last_close
          FROM stock_prices
        )
        SELECT
          p.symbol,
          p.date AS ymd,
          p.open, p.high, p.low, p.close, p.volume,
          p.last_close,
          i.local_symbol,
          i.name,
          i.industry,
          i.sector,
          i.market,
          i.market_detail
        FROM p
        LEFT JOIN stock_info i ON i.symbol = p.symbol
        WHERE p.date = ?
          AND p.close IS NOT NULL
        """
        df = pd.read_sql_query(sql, conn, params=(ymd_effective,))

        if df.empty:
            snapshot_main: List[Dict[str, Any]] = []
        else:
            df["name"] = df["name"].fillna("Unknown")
            df["industry"] = df["industry"].fillna("Unclassified")
            df["sector"] = df["sector"].fillna("Unclassified")

            df["last_close"] = pd.to_numeric(df["last_close"], errors="coerce")
            df["close"] = pd.to_numeric(df["close"], errors="coerce")

            df["ret"] = 0.0
            m = df["last_close"].notna() & (df["last_close"] > 0) & df["close"].notna()
            df.loc[m, "ret"] = (df.loc[m, "close"] / df.loc[m, "last_close"]) - 1.0

            df["streak"] = 1

            snapshot_main = df[
                [
                    "symbol",
                    "local_symbol",
                    "name",
                    "sector",
                    "industry",
                    "ymd",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "last_close",
                    "ret",
                    "streak",
                    "market",
                    "market_detail",
                ]
            ].to_dict(orient="records")

        meta_time = build_meta_time_asia(
            datetime.now(timezone.utc),
            tz_name="Asia/Kolkata",
            fallback_offset="+05:30",
        )

        return {
            "market": "india",
            "slot": slot,
            "asof": asof,
            "ymd": ymd,
            "ymd_effective": ymd_effective,
            "snapshot_main": snapshot_main,
            "snapshot_open": [],
            "stats": {"snapshot_main_count": int(len(snapshot_main)), "snapshot_open_count": 0},
            "meta": {"db_path": db_path, "ymd_effective": ymd_effective, "time": meta_time},
        }
    finally:
        conn.close()
