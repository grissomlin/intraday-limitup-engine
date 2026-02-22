# scripts/check_kr_move_stats.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import sqlite3
from typing import List, Optional, Tuple

import pandas as pd


# ----------------------------
# Helpers: schema discovery
# ----------------------------
def list_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    # PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
    return [r[1] for r in rows]


def find_price_table(conn: sqlite3.Connection) -> Tuple[str, str, str]:
    """
    Try to find a table that looks like OHLCV daily prices:
      must have columns: symbol, date, close
    Returns (table_name, symbol_col, date_col)
    """
    candidates = []
    for t in list_tables(conn):
        cols = [c.lower() for c in table_columns(conn, t)]
        if "symbol" in cols and "date" in cols and "close" in cols:
            candidates.append(t)

    if not candidates:
        raise RuntimeError(
            "找不到價格表：DB 內沒有任何 table 同時包含 symbol/date/close 欄位。\n"
            "請確認 KR downloader 寫入的價格表 schema。"
        )

    # Prefer common names if exist
    preferred = ["daily_prices", "prices", "ohlcv", "price_daily", "kline_daily"]
    for p in preferred:
        for t in candidates:
            if t.lower() == p:
                return t, "symbol", "date"

    # Otherwise pick the first candidate
    return candidates[0], "symbol", "date"


def has_sector_column(conn: sqlite3.Connection, stock_info_table: str = "stock_info") -> bool:
    cols = [c.lower() for c in table_columns(conn, stock_info_table)]
    return "sector" in cols


def has_name_column(conn: sqlite3.Connection, stock_info_table: str = "stock_info") -> bool:
    cols = [c.lower() for c in table_columns(conn, stock_info_table)]
    return "name" in cols


# ----------------------------
# Query builders
# ----------------------------
def get_latest_date(conn: sqlite3.Connection, price_table: str) -> str:
    row = conn.execute(f"SELECT MAX(date) FROM {price_table}").fetchone()
    if not row or not row[0]:
        raise RuntimeError(f"價格表 {price_table} 沒有任何 date 資料。")
    return str(row[0])


def load_snapshot_with_ret_and_streak10(
    conn: sqlite3.Connection,
    price_table: str,
    ymd: str,
    stock_info_table: str = "stock_info",
) -> pd.DataFrame:
    """
    Compute:
      - last_close (lag close)
      - ret = close/last_close - 1
      - up10 = ret >= 0.10
      - streak10: run length of consecutive up10 days (>=10%) within each symbol, for the row at ymd
    Join with stock_info (sector/name if exists).
    """

    # Detect availability of stock_info columns
    si_cols = [c.lower() for c in table_columns(conn, stock_info_table)] if stock_info_table in list_tables(conn) else []
    join_stock_info = stock_info_table in list_tables(conn) and "symbol" in si_cols

    select_si = ""
    join_si = ""
    if join_stock_info:
        # pick columns if exist
        parts = []
        if "name" in si_cols:
            parts.append("si.name AS name")
        else:
            parts.append("NULL AS name")
        if "sector" in si_cols:
            parts.append("si.sector AS sector")
        else:
            parts.append("NULL AS sector")
        select_si = ", " + ", ".join(parts)
        join_si = f"LEFT JOIN {stock_info_table} si ON si.symbol = r.symbol"

    # SQL:
    # 1) base with last_close
    # 2) ret + up10
    # 3) group id for streak computation: cumulative sum of breaks (up10==0)
    # 4) streak10 = count(*) over (symbol, group_id) when up10=1 else 0
    sql = f"""
    WITH base AS (
      SELECT
        symbol,
        date,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS last_close
      FROM {price_table}
    ),
    calc AS (
      SELECT
        symbol,
        date,
        close,
        last_close,
        CASE
          WHEN last_close IS NULL OR last_close = 0 THEN NULL
          ELSE (close / last_close) - 1.0
        END AS ret
      FROM base
    ),
    flagged AS (
      SELECT
        *,
        CASE WHEN ret IS NOT NULL AND ret >= 0.10 THEN 1 ELSE 0 END AS up10
      FROM calc
    ),
    grouped AS (
      SELECT
        *,
        SUM(CASE WHEN up10 = 0 THEN 1 ELSE 0 END)
          OVER (PARTITION BY symbol ORDER BY date ROWS UNBOUNDED PRECEDING) AS g
      FROM flagged
    ),
    runs AS (
      SELECT
        symbol,
        date,
        close,
        last_close,
        ret,
        up10,
        CASE
          WHEN up10 = 1 THEN COUNT(*) OVER (PARTITION BY symbol, g)
          ELSE 0
        END AS streak10
      FROM grouped
    )
    SELECT
      r.symbol,
      r.date,
      r.close,
      r.last_close,
      r.ret,
      r.streak10
      {select_si}
    FROM runs r
    {join_si}
    WHERE r.date = ?
      AND r.ret IS NOT NULL
    """

    df = pd.read_sql_query(sql, conn, params=(ymd,))
    # normalize sector nulls
    if "sector" in df.columns:
        df["sector"] = df["sector"].fillna("-")
    else:
        df["sector"] = "-"
    return df


# ----------------------------
# Reporting
# ----------------------------
def sector_dist(df: pd.DataFrame, top_n: int = 20) -> pd.Series:
    return df["sector"].value_counts().head(top_n)


def streak_dist(df: pd.DataFrame, top_n: int = 20) -> pd.Series:
    return df["streak10"].value_counts().sort_index().head(top_n)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="markets/kr/kr_stock_warehouse.db", help="Path to kr_stock_warehouse.db")
    ap.add_argument("--ymd", default=None, help="Target date YYYY-MM-DD. If omitted, use latest date in DB.")
    ap.add_argument("--top-n", type=int, default=20, help="Top N sectors to display for each bucket")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        # price table discovery
        price_table, _, _ = find_price_table(conn)

        # sector column check
        stock_info_exists = "stock_info" in list_tables(conn)
        sector_ok = stock_info_exists and has_sector_column(conn, "stock_info")

        # resolve ymd
        ymd = args.ymd or get_latest_date(conn, price_table)

        print(f"📦 DB = {args.db}")
        print(f"📈 price_table = {price_table}")
        print(f"📅 ymd_effective = {ymd}")
        print(f"🏷️ stock_info exists = {stock_info_exists}")
        print(f"🏷️ stock_info.sector exists = {sector_ok}")

        df = load_snapshot_with_ret_and_streak10(conn, price_table, ymd, "stock_info")

        total = len(df)
        print(f"\n✅ ret available = {total}")

        # buckets
        limit30 = df[df["ret"] >= 0.30].copy()
        b10_20 = df[(df["ret"] >= 0.10) & (df["ret"] < 0.20)].copy()
        b20_30 = df[(df["ret"] >= 0.20) & (df["ret"] < 0.30)].copy()

        print("\n📊 Buckets")
        print(f"🚀 30% 漲停 (ret>=30%) = {len(limit30)}")
        print(f"📈 10%~20% (10%<=ret<20%) = {len(b10_20)}")
        print(f"📈 20%~30% 非漲停 (20%<=ret<30%) = {len(b20_30)}")

        # sector distribution
        print("\n🏭 產業分布 Top (30% 漲停)")
        print(sector_dist(limit30, args.top_n).to_string())

        print("\n🏭 產業分布 Top (10%~20%)")
        print(sector_dist(b10_20, args.top_n).to_string())

        print("\n🏭 產業分布 Top (20%~30% 非漲停)")
        print(sector_dist(b20_30, args.top_n).to_string())

        # streak distribution (>=10% as streak day)
        print("\n🔁 連板(>=10%) streak10 分布（30% 漲停）")
        print(streak_dist(limit30, args.top_n).to_string())

        print("\n🔁 連板(>=10%) streak10 分布（10%~20%）")
        print(streak_dist(b10_20, args.top_n).to_string())

        print("\n🔁 連板(>=10%) streak10 分布（20%~30% 非漲停）")
        print(streak_dist(b20_30, args.top_n).to_string())

        # sanity notes
        print("\n📝 Notes")
        print("- 本腳本把「ret>=10%」當作『連板日』來算 streak10（連續幾天>=10%）。")
        print("- 連續 30% 多根在正常市場狀態幾乎是極少數極端事件，所以不拿 30% 當連板定義。")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
