# scripts/check_us_move_stats.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Tuple


DEFAULT_DB = r"markets\us\us_stock_warehouse.db"


BUCKETS = [
    ("10-20%", 0.10, 0.20),
    ("20-30%", 0.20, 0.30),
    ("30-40%", 0.30, 0.40),
    ("40-50%", 0.40, 0.50),
    (">=50%", 0.50, None),
]


def q1(conn: sqlite3.Connection, sql: str, params: Tuple = ()) -> int:
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def qall(conn: sqlite3.Connection, sql: str, params: Tuple = ()) -> List[Tuple]:
    return conn.execute(sql, params).fetchall()


def resolve_ymd_effective(conn: sqlite3.Connection, ymd: str) -> str:
    # 找 <= ymd 的最後一個交易日
    row = conn.execute("SELECT MAX(date) FROM stock_prices WHERE date <= ?", (ymd,)).fetchone()
    eff = row[0] if row and row[0] else None
    if not eff:
        raise RuntimeError("DB has no stock_prices rows.")
    return str(eff)


def print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB, help="path to us_stock_warehouse.db")
    ap.add_argument("--ymd", default=None, help="YYYY-MM-DD (default: today local)")
    ap.add_argument("--top", type=int, default=20, help="top N sectors to display")
    ap.add_argument("--by-sector-buckets", action="store_true", help="show bucket counts by sector (wide output)")

    # ✅ NEW: show streak
    ap.add_argument(
        "--show-streak",
        action="store_true",
        help="show movers list (ret>=10%) with streak/streak_prev (consecutive days ret>=10%)",
    )
    ap.add_argument(
        "--show-streak-top",
        type=int,
        default=120,
        help="top N movers to display when --show-streak (sorted by ret desc)",
    )

    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    ymd = args.ymd or datetime.now().strftime("%Y-%m-%d")

    conn = sqlite3.connect(str(db_path))
    try:
        # ---- basic DB sanity ----
        info_symbols = q1(conn, "SELECT COUNT(DISTINCT symbol) FROM stock_info WHERE market='US'")
        price_symbols = q1(conn, "SELECT COUNT(DISTINCT symbol) FROM stock_prices")
        max_date = conn.execute("SELECT MAX(date) FROM stock_prices").fetchone()[0]

        print_header("US DB Sanity")
        print(f"DB        = {db_path}")
        print(f"stock_info distinct(US) symbols = {info_symbols}")
        print(f"stock_prices distinct symbols   = {price_symbols}")
        print(f"stock_prices MAX(date)          = {max_date}")

        ymd_eff = resolve_ymd_effective(conn, ymd)
        print_header("Effective Trading Day")
        print(f"requested ymd      = {ymd}")
        print(f"ymd_effective      = {ymd_eff}")

        # ---- compute daily returns for ymd_eff ----
        sql_ret = """
        WITH px AS (
          SELECT
            symbol,
            date,
            close,
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
          FROM stock_prices
          WHERE date <= ?
        ),
        today AS (
          SELECT
            symbol,
            close,
            prev_close,
            (close / prev_close) - 1.0 AS ret
          FROM px
          WHERE date = ?
            AND prev_close IS NOT NULL
            AND prev_close > 0
            AND close IS NOT NULL
        )
        SELECT COUNT(*) FROM today
        """
        universe = q1(conn, sql_ret, (ymd_eff, ymd_eff))

        # count >=10%
        sql_ge10 = """
        WITH px AS (
          SELECT symbol, date, close,
                 LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
          FROM stock_prices
          WHERE date <= ?
        ),
        today AS (
          SELECT symbol, (close / prev_close) - 1.0 AS ret
          FROM px
          WHERE date = ?
            AND prev_close IS NOT NULL AND prev_close > 0 AND close IS NOT NULL
        )
        SELECT COUNT(*) FROM today WHERE ret >= 0.10
        """
        ge10 = q1(conn, sql_ge10, (ymd_eff, ymd_eff))

        print_header("Move Stats (Daily Return)")
        print(f"universe (has prev_close) = {universe}")
        print(f"ret >= 10%                = {ge10}")

        # buckets
        for name, lo, hi in BUCKETS:
            if hi is None:
                sql = """
                WITH px AS (
                  SELECT symbol, date, close,
                         LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
                  FROM stock_prices
                  WHERE date <= ?
                ),
                today AS (
                  SELECT symbol, (close / prev_close) - 1.0 AS ret
                  FROM px
                  WHERE date = ?
                    AND prev_close IS NOT NULL AND prev_close > 0 AND close IS NOT NULL
                )
                SELECT COUNT(*) FROM today WHERE ret >= ?
                """
                cnt = q1(conn, sql, (ymd_eff, ymd_eff, lo))
            else:
                sql = """
                WITH px AS (
                  SELECT symbol, date, close,
                         LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
                  FROM stock_prices
                  WHERE date <= ?
                ),
                today AS (
                  SELECT symbol, (close / prev_close) - 1.0 AS ret
                  FROM px
                  WHERE date = ?
                    AND prev_close IS NOT NULL AND prev_close > 0 AND close IS NOT NULL
                )
                SELECT COUNT(*) FROM today WHERE ret >= ? AND ret < ?
                """
                cnt = q1(conn, sql, (ymd_eff, ymd_eff, lo, hi))
            print(f"{name:<6} = {cnt}")

        # ---- sector distribution for ret >= 10% ----
        sql_sector_ge10 = """
        WITH px AS (
          SELECT
            p.symbol,
            p.date,
            p.close,
            LAG(p.close) OVER (PARTITION BY p.symbol ORDER BY p.date) AS prev_close
          FROM stock_prices p
          WHERE p.date <= ?
        ),
        today AS (
          SELECT
            symbol,
            (close / prev_close) - 1.0 AS ret
          FROM px
          WHERE date = ?
            AND prev_close IS NOT NULL AND prev_close > 0 AND close IS NOT NULL
        )
        SELECT
          COALESCE(NULLIF(TRIM(i.sector), ''), 'Unknown') AS sector,
          COUNT(*) AS cnt
        FROM today t
        LEFT JOIN stock_info i ON i.symbol = t.symbol
        WHERE t.ret >= 0.10
        GROUP BY sector
        ORDER BY cnt DESC
        LIMIT ?
        """
        rows = qall(conn, sql_sector_ge10, (ymd_eff, ymd_eff, args.top))

        print_header(f"Sectors with ret >= 10% (Top {args.top})")
        if not rows:
            print("(none)")
        else:
            for sector, cnt in rows:
                print(f"{sector:<30} {cnt}")

        # ---- optional: bucket counts by sector ----
        if args.by_sector_buckets:
            print_header("Bucket counts by sector (ret buckets)")

            sql_sector_buckets = """
            WITH px AS (
              SELECT
                p.symbol,
                p.date,
                p.close,
                LAG(p.close) OVER (PARTITION BY p.symbol ORDER BY p.date) AS prev_close
              FROM stock_prices p
              WHERE p.date <= ?
            ),
            today AS (
              SELECT
                symbol,
                (close / prev_close) - 1.0 AS ret
              FROM px
              WHERE date = ?
                AND prev_close IS NOT NULL AND prev_close > 0 AND close IS NOT NULL
            ),
            tagged AS (
              SELECT
                t.symbol,
                t.ret,
                CASE
                  WHEN t.ret >= 0.50 THEN '>=50%'
                  WHEN t.ret >= 0.40 THEN '40-50%'
                  WHEN t.ret >= 0.30 THEN '30-40%'
                  WHEN t.ret >= 0.20 THEN '20-30%'
                  WHEN t.ret >= 0.10 THEN '10-20%'
                  ELSE '<10%'
                END AS bucket
              FROM today t
            )
            SELECT
              COALESCE(NULLIF(TRIM(i.sector), ''), 'Unknown') AS sector,
              bucket,
              COUNT(*) AS cnt
            FROM tagged g
            LEFT JOIN stock_info i ON i.symbol = g.symbol
            WHERE g.bucket <> '<10%'
            GROUP BY sector, bucket
            ORDER BY sector ASC, bucket ASC
            """
            rows2 = qall(conn, sql_sector_buckets, (ymd_eff, ymd_eff))
            cur_sector = None
            for sector, bucket, cnt in rows2:
                if sector != cur_sector:
                    cur_sector = sector
                    print(f"\n[{sector}]")
                print(f"  {bucket:<6} {cnt}")

        # ---- optional: show movers with streak ----
        if args.show_streak:
            print_header(f"Movers with streak (ret>=10%) (Top {args.show_streak_top})")

            # ✅ FIX: streak 先算出來，再 LAG(streak)；避免 SQLite window nesting error
            sql_movers_streak = """
            WITH base AS (
              SELECT
                p.symbol,
                p.date,
                p.close,
                LAG(p.close) OVER (PARTITION BY p.symbol ORDER BY p.date) AS prev_close
              FROM stock_prices p
              WHERE p.date <= ?
            ),
            rets AS (
              SELECT
                symbol,
                date,
                (close / prev_close) - 1.0 AS ret,
                CASE
                  WHEN prev_close IS NOT NULL AND prev_close > 0 AND close IS NOT NULL
                       AND (close / prev_close) - 1.0 >= 0.10
                  THEN 1 ELSE 0
                END AS hit
              FROM base
              WHERE prev_close IS NOT NULL AND prev_close > 0 AND close IS NOT NULL
            ),
            grp AS (
              SELECT
                symbol,
                date,
                ret,
                hit,
                SUM(CASE WHEN hit = 0 THEN 1 ELSE 0 END)
                  OVER (PARTITION BY symbol ORDER BY date ROWS UNBOUNDED PRECEDING) AS g
              FROM rets
            ),
            streaked1 AS (
              SELECT
                symbol,
                date,
                ret,
                hit,
                CASE
                  WHEN hit = 1 THEN ROW_NUMBER() OVER (PARTITION BY symbol, g ORDER BY date)
                  ELSE 0
                END AS streak
              FROM grp
            ),
            streaked AS (
              SELECT
                symbol,
                date,
                ret,
                hit,
                streak,
                COALESCE(LAG(streak) OVER (PARTITION BY symbol ORDER BY date), 0) AS streak_prev
              FROM streaked1
            ),
            today AS (
              SELECT
                s.symbol,
                s.ret,
                s.streak,
                s.streak_prev
              FROM streaked s
              WHERE s.date = ?
                AND s.ret >= 0.10
            )
            SELECT
              t.symbol,
              ROUND(t.ret * 100.0, 2) AS ret_pct,
              t.streak,
              t.streak_prev,
              COALESCE(NULLIF(TRIM(i.sector), ''), 'Unknown') AS sector,
              COALESCE(NULLIF(TRIM(i.name), ''), '') AS name
            FROM today t
            LEFT JOIN stock_info i ON i.symbol = t.symbol
            ORDER BY t.ret DESC
            LIMIT ?
            """
            movers = qall(conn, sql_movers_streak, (ymd_eff, ymd_eff, args.show_streak_top))

            if not movers:
                print("(none)")
            else:
                for sym, ret_pct, streak, streak_prev, sector, name in movers:
                    print(f"{sym:<8} {ret_pct:>7}%  streak={streak:<2} prev={streak_prev:<2}  {sector:<22} {name}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
