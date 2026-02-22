# scripts/check_cn_move_stats.py
# -*- coding: utf-8 -*-
"""
Check CN move stats + "true limit-up" (practical rules) from CN SQLite DB.
...
（原文件头部注释保持不变）
"""

from __future__ import annotations

import os
import sqlite3
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Optional, Tuple

import pandas as pd


# -------------------------
# Config
# -------------------------
def _default_db_path() -> str:
    return os.getenv("CN_DB_PATH", os.path.join(os.path.dirname(__file__), "..", "markets", "cn", "cn_stock_warehouse.db"))


def _eps() -> float:
    # floating compare tolerance
    return float(os.getenv("CN_LIMIT_EPS", "0.0001"))


def _debug_top_n() -> int:
    return int(os.getenv("CN_DEBUG_TOP_N", "30"))


# -------------------------
# Helpers
# -------------------------
def _connect(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path, timeout=120)


def _pick_latest_date(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute("SELECT MAX(date) FROM stock_prices").fetchone()
    return row[0] if row and row[0] else None


def _round_price_2(x: float) -> float:
    # CN price tick is 0.01 (2 decimals). Use Decimal for stable rounding.
    d = Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(d)


def _is_st(name: str) -> bool:
    if not name:
        return False
    s = str(name).upper()
    # common A-share flags: "ST", "*ST"
    return ("ST" in s) or ("*ST" in s)


def _code_from_symbol(sym: str) -> str:
    return str(sym).split(".")[0].zfill(6)


def _limit_rate(symbol: str, name: str) -> float:
    """
    Practical CN limit:
      - ST/*ST => 5%
      - ChiNext (300/301) => 20%
      - STAR (688) => 20%
      - else => 10%
    """
    code = _code_from_symbol(symbol)

    if _is_st(name):
        return 0.05

    if code.startswith(("300", "301")):
        return 0.20
    if code.startswith("688"):
        return 0.20

    return 0.10


def _load_day_snapshot(conn: sqlite3.Connection, ymd: str) -> pd.DataFrame:
    """
    Load snapshot for ymd with last_close via window function.
    """
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
      i.name,
      i.sector,
      i.market,
      i.market_detail
    FROM p
    LEFT JOIN stock_info i ON i.symbol = p.symbol
    WHERE p.date = ?
    """
    df = pd.read_sql_query(sql, conn, params=(ymd,))
    return df


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    out["name"] = out.get("name", "Unknown").fillna("Unknown")
    out["sector"] = out.get("sector", "未分類").fillna("未分類")
    out["symbol"] = out["symbol"].astype(str)

    for col in ["open", "high", "low", "close", "last_close", "volume"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    out["ret"] = 0.0
    m = out["last_close"].notna() & (out["last_close"] > 0) & out["close"].notna()
    out.loc[m, "ret"] = (out.loc[m, "close"] / out.loc[m, "last_close"]) - 1.0

    return out


def _compute_limit_fields(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    eps = _eps()

    limit_rates = []
    limit_prices = []
    is_limitup = []
    hit_limit = []   # 新增：盘中是否触及涨停（最高价 ≥ 涨停价）

    for _, r in df.iterrows():
        sym = r.get("symbol", "")
        name = r.get("name", "Unknown")
        lc = r.get("last_close", None)
        close = r.get("close", None)
        high = r.get("high", None)

        rate = _limit_rate(sym, name)
        limit_rates.append(rate)

        if lc is None or pd.isna(lc) or lc <= 0:
            limit_prices.append(None)
            is_limitup.append(False)
            hit_limit.append(False)
            continue

        lp = _round_price_2(float(lc) * (1.0 + float(rate)))
        limit_prices.append(lp)

        # 是否收盘涨停
        if close is None or pd.isna(close):
            is_limitup.append(False)
        else:
            is_limitup.append(float(close) >= float(lp) - eps)

        # 是否盘中触及涨停
        if high is None or pd.isna(high) or lp is None:
            hit_limit.append(False)
        else:
            hit_limit.append(float(high) >= float(lp) - eps)

    out = df.copy()
    out["limit_rate"] = limit_rates
    out["limit_price"] = limit_prices
    out["is_limitup"] = is_limitup
    out["hit_limit"] = hit_limit   # 新增列
    return out


def _print_blast_stats_and_list(df: pd.DataFrame, topn: int) -> None:
    """
    打印创业板/科创板炸版股统计及清单。
    炸版定义：limit_rate=0.20，盘中触及涨停（hit_limit=True），收盘未涨停（is_limitup=False）。
    """
    if df.empty:
        print("\n💥 創業板/科創版 炸版股數 = 0")
        return

    blast = df[(df["limit_rate"] == 0.20) & (df["hit_limit"] == True) & (df["is_limitup"] == False)].copy()
    count = len(blast)
    print(f"\n💥 創業板/科創版 炸版股數 = {count}")

    if count > 0:
        print(f"\n📋 炸版股清單（最多 {topn} 檔）:")
        blast_sorted = blast.sort_values("ret", ascending=False).head(topn)
        for _, r in blast_sorted.iterrows():
            sym = r["symbol"]
            name = str(r.get("name", ""))
            sector = str(r.get("sector", "未分類"))
            high = r.get("high", None)
            close = r.get("close", None)
            limit_price = r.get("limit_price", None)
            ret = float(r.get("ret", 0.0))
            lr = float(r.get("limit_rate", 0.0))
            print(
                f"{sym:<10} | {name:<18} | {sector:<18} | "
                f"最高={high} | 收盤={close} | 漲停價={limit_price} | 漲幅={ret*100:6.2f}%"
            )


def main():
    db_path = os.path.abspath(_default_db_path())
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"CN DB not found: {db_path}")

    conn = _connect(db_path)
    try:
        ymd_effective = _pick_latest_date(conn)
        if not ymd_effective:
            print("❌ DB has no stock_prices date.")
            return

        df_raw = _load_day_snapshot(conn, ymd_effective)
        df = _normalize(df_raw)
        df = _compute_limit_fields(df)

    finally:
        conn.close()

    print(f"📅 ymd_effective = {ymd_effective}")
    print(f"snapshot_main (by DB) = {len(df)}")

    ret_ok = int(df["ret"].notna().sum()) if not df.empty else 0
    print(f"ret available          = {ret_ok}")

    if df.empty:
        return

    # basic move counts
    c5 = int((df["ret"] >= 0.05).sum())
    c10 = int((df["ret"] >= 0.10).sum())
    c20 = int((df["ret"] >= 0.20).sum())
    print(f"ret >=  5.0%           = {c5}")
    print(f"ret >= 10.0%           = {c10}")
    print(f"ret >= 20.0%           = {c20}")

    # limitup stats
    lim_df = df[df["is_limitup"] == True].copy()
    print("")
    print(f"🚀 真正『漲停』股票數 = {len(lim_df)}")

    # 新增：创业板/科创板炸版统计
    _print_blast_stats_and_list(df, _debug_top_n())

    # movers but not limit-up
    move10_not = df[(df["ret"] >= 0.10) & (df["is_limitup"] == False)].copy()
    print(f"\n📈 ret >= 10% 但『不是漲停』 = {len(move10_not)}")

    # sector distributions
    def _print_sector_dist(title: str, sub: pd.DataFrame, topn: int = 12):
        print("")
        print(title)
        if sub.empty:
            print("  (none)")
            return
        s = sub["sector"].fillna("未分類").value_counts().head(topn)
        for k, v in s.items():
            print(f"  {k:<30} {int(v)}")

    _print_sector_dist("📊 漲停股產業分佈（Top 12）:", lim_df)
    _print_sector_dist("📊 ret>=10%但非漲停 產業分佈（Top 12）:", move10_not)

    # top lists
    topn = _debug_top_n()

    print("")
    print(f"🔥 漲停股清單（最多 {topn} 檔）:")
    if lim_df.empty:
        print("  (none)")
    else:
        lim_df2 = lim_df.sort_values(["ret"], ascending=False).head(topn)
        for _, r in lim_df2.iterrows():
            sym = r["symbol"]
            name = str(r.get("name", ""))
            sector = str(r.get("sector", "未分類"))
            ret = float(r.get("ret", 0.0))
            close = r.get("close", None)
            lp = r.get("limit_price", None)
            lr = float(r.get("limit_rate", 0.0))
            print(
                f"{sym:<10} | {name:<18} | {sector:<18} | "
                f"ret={ret*100:6.2f}% | close={close} | limit={lp} | limit_rate={lr*100:.0f}%"
            )

    print("")
    print(f"⚠️ ret>=10% 但不是漲停（最多 {topn} 檔）:")
    if move10_not.empty:
        print("  (none)")
    else:
        m2 = move10_not.sort_values(["ret"], ascending=False).head(topn)
        for _, r in m2.iterrows():
            sym = r["symbol"]
            name = str(r.get("name", ""))
            sector = str(r.get("sector", "未分類"))
            ret = float(r.get("ret", 0.0))
            close = r.get("close", None)
            lp = r.get("limit_price", None)
            lr = float(r.get("limit_rate", 0.0))
            print(
                f"{sym:<10} | {name:<18} | {sector:<18} | "
                f"ret={ret*100:6.2f}% | close={close} | limit={lp} | limit_rate={lr*100:.0f}%"
            )


if __name__ == "__main__":
    main()