# scripts/check_cn_payload_vs_db.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


def _eps() -> float:
    return float(os.getenv("CN_LIMIT_EPS", "0.0001"))


def _debug_top_n() -> int:
    return int(os.getenv("CN_DEBUG_TOP_N", "30"))


def _connect(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path, timeout=120)


def _round_price_2(x: float) -> float:
    d = Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(d)


def _is_st(name: str) -> bool:
    if not name:
        return False
    s = str(name).upper()
    return ("*ST" in s) or ("ST" in s)


def _code_from_symbol(sym: str) -> str:
    return str(sym).split(".")[0].zfill(6)


def _limit_rate(symbol: str, name: str) -> float:
    code = _code_from_symbol(symbol)
    if _is_st(name):
        return 0.05
    if code.startswith(("300", "301")):
        return 0.20
    if code.startswith("688"):
        return 0.20
    return 0.10


def _load_day_snapshot(conn: sqlite3.Connection, ymd: str) -> pd.DataFrame:
    # include high for "touch"
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
    return pd.read_sql_query(sql, conn, params=(ymd,))


def _normalize_db_and_compute(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    out["symbol"] = out["symbol"].astype(str)
    out["name"] = out.get("name", "Unknown").fillna("Unknown").astype(str)

    for col in ["close", "last_close", "high"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    # ret
    out["ret_db"] = 0.0
    m = out["last_close"].notna() & (out["last_close"] > 0) & out["close"].notna()
    out.loc[m, "ret_db"] = (out.loc[m, "close"] / out.loc[m, "last_close"]) - 1.0

    eps = _eps()
    rates: List[float] = []
    prices: List[Optional[float]] = []
    locked: List[bool] = []
    touch: List[bool] = []

    for _, r in out.iterrows():
        sym = str(r.get("symbol", ""))
        nm = str(r.get("name", "Unknown"))
        lc = r.get("last_close", None)
        close = r.get("close", None)
        high = r.get("high", None)

        rate = _limit_rate(sym, nm)
        rates.append(rate)

        if lc is None or pd.isna(lc) or float(lc) <= 0:
            prices.append(None)
            locked.append(False)
            touch.append(False)
            continue

        lp = _round_price_2(float(lc) * (1.0 + float(rate)))
        prices.append(lp)

        locked.append(False if (close is None or pd.isna(close)) else (float(close) >= float(lp) - eps))
        touch.append(False if (high is None or pd.isna(high)) else (float(high) >= float(lp) - eps))

    out["limit_rate_db"] = rates
    out["limit_price_db"] = prices
    out["is_limitup_locked_db"] = locked
    out["is_limitup_touch_db"] = touch
    out["is_limitup_any_db"] = (out["is_limitup_locked_db"] | out["is_limitup_touch_db"])

    out = out.rename(
        columns={
            "close": "close_db",
            "high": "high_db",
            "last_close": "last_close_db",
            "name": "name_db",
        }
    )

    return out[
        [
            "symbol",
            "name_db",
            "close_db",
            "high_db",
            "last_close_db",
            "ret_db",
            "limit_rate_db",
            "limit_price_db",
            "is_limitup_locked_db",
            "is_limitup_touch_db",
            "is_limitup_any_db",
        ]
    ]


def _normalize_payload(payload: Dict[str, Any]) -> pd.DataFrame:
    rows = payload.get("snapshot_main") or []
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["symbol"] = df["symbol"].astype(str)

    need = {
        "name": "name_pl",
        "close": "close_pl",
        "high": "high_pl",
        "last_close": "last_close_pl",
        "ret": "ret_pl",
        "limit_rate": "limit_rate_pl",
        "limit_price": "limit_price_pl",
        "is_limitup_locked": "is_limitup_locked_pl",
        "is_limitup_touch": "is_limitup_touch_pl",
    }

    for k in list(need.keys()):
        if k not in df.columns:
            df[k] = False if k.startswith("is_") else pd.NA

    df = df[["symbol"] + list(need.keys())].copy()
    df = df.rename(columns=need)

    for col in ["close_pl", "high_pl", "last_close_pl", "ret_pl", "limit_rate_pl", "limit_price_pl"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["is_limitup_locked_pl"] = df["is_limitup_locked_pl"].fillna(False).astype(bool)
    df["is_limitup_touch_pl"] = df["is_limitup_touch_pl"].fillna(False).astype(bool)
    df["is_limitup_any_pl"] = df["is_limitup_locked_pl"] | df["is_limitup_touch_pl"]
    df["name_pl"] = df["name_pl"].fillna("").astype(str)

    return df


def _pick_ymd(payload: Dict[str, Any], ymd_arg: Optional[str]) -> str:
    if ymd_arg:
        return str(ymd_arg)[:10]
    ymd_eff = str(payload.get("ymd_effective") or "")[:10]
    if ymd_eff:
        return ymd_eff
    ymd = str(payload.get("ymd") or "")[:10]
    if ymd:
        return ymd
    rows = payload.get("snapshot_main") or []
    if rows:
        v = str(rows[0].get("ymd") or "")[:10]
        if v:
            return v
    raise ValueError("Cannot determine ymd. Provide --ymd.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--payload", required=True)
    ap.add_argument("--db", default=os.path.join("markets", "cn", "cn_stock_warehouse.db"))
    ap.add_argument("--ymd", default=None)
    ap.add_argument("--top", type=int, default=None)
    args = ap.parse_args()

    payload_path = Path(args.payload)
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    ymd = _pick_ymd(payload, args.ymd)

    dfP = _normalize_payload(payload)
    if dfP.empty:
        print("❌ payload snapshot_main empty")
        return

    db_path = os.path.abspath(args.db)
    conn = _connect(db_path)
    try:
        df_raw = _load_day_snapshot(conn, ymd)
    finally:
        conn.close()

    dfD = _normalize_db_and_compute(df_raw)
    df = dfD.merge(dfP, on="symbol", how="left")

    topn = args.top if args.top is not None else _debug_top_n()

    present = df["close_pl"].notna() & df["last_close_pl"].notna()
    missing_payload = int((~present).sum())

    tol_ret = 1e-6
    tol_price = 1e-4
    tol_rate = 1e-9

    ret_m = present & df["ret_pl"].notna() & ((df["ret_pl"] - df["ret_db"]).abs() > tol_ret)
    rate_m = present & df["limit_rate_pl"].notna() & ((df["limit_rate_pl"] - df["limit_rate_db"]).abs() > tol_rate)
    price_m = present & df["limit_price_pl"].notna() & ((df["limit_price_pl"] - df["limit_price_db"]).abs() > tol_price)

    locked_m = present & (df["is_limitup_locked_pl"].astype(bool) != df["is_limitup_locked_db"].astype(bool))
    touch_m = present & (df["is_limitup_touch_pl"].astype(bool) != df["is_limitup_touch_db"].astype(bool))
    any_m = present & (df["is_limitup_any_pl"].astype(bool) != df["is_limitup_any_db"].astype(bool))

    print(f"📄 payload = {payload_path}")
    print(f"📦 db      = {db_path}")
    print(f"📅 ymd     = {ymd}")
    print("")
    print(f"DB rows                  = {len(dfD)}")
    print(f"payload rows             = {len(dfP)}")
    print(f"missing in payload(join) = {missing_payload}")
    print("")
    print(f"ret mismatch             = {int(ret_m.sum())}")
    print(f"limit_rate mismatch      = {int(rate_m.sum())}")
    print(f"limit_price mismatch     = {int(price_m.sum())}")
    print(f"locked mismatch          = {int(locked_m.sum())}")
    print(f"touch mismatch           = {int(touch_m.sum())}")
    print(f"any(locked|touch) mismatch = {int(any_m.sum())}")

    def show(title: str, mask: pd.Series):
        n = int(mask.sum())
        print("")
        print(f"🔎 {title} (top {min(topn, n)} of {n})")
        if n <= 0:
            print("  (none)")
            return
        cols = [
            "symbol",
            "name_db", "name_pl",
            "close_db", "high_db", "last_close_db",
            "close_pl", "high_pl", "last_close_pl",
            "ret_db", "ret_pl",
            "limit_rate_db", "limit_rate_pl",
            "limit_price_db", "limit_price_pl",
            "is_limitup_locked_db", "is_limitup_locked_pl",
            "is_limitup_touch_db", "is_limitup_touch_pl",
            "is_limitup_any_db", "is_limitup_any_pl",
        ]
        sub = df.loc[mask, cols].copy()
        sub["ret_diff"] = (sub["ret_pl"] - sub["ret_db"]).abs()
        sub = sub.sort_values(["ret_diff"], ascending=False)
        print(sub.head(topn).to_string(index=False))

    show("RET mismatch", ret_m)
    show("LIMIT_RATE mismatch", rate_m)
    show("LIMIT_PRICE mismatch", price_m)
    show("LOCKED mismatch", locked_m)
    show("TOUCH mismatch", touch_m)
    show("ANY mismatch", any_m)

    # ------------------------------
    # 新增：双创(20%)炸版股对比
    # ------------------------------
    # 炸版定义：limit_rate≈20%，盘中触及涨停，收盘未封板
    blast_cond_db = (
        df["limit_rate_db"].notna()
        & ((df["limit_rate_db"] - 0.20).abs() < tol_rate)
        & df["is_limitup_touch_db"]
        & ~df["is_limitup_locked_db"]
    )
    blast_cond_pl = (
        df["limit_rate_pl"].notna()
        & ((df["limit_rate_pl"] - 0.20).abs() < tol_rate)
        & df["is_limitup_touch_pl"]
        & ~df["is_limitup_locked_pl"]
    )

    db_blast_cnt = blast_cond_db.sum()
    pl_blast_cnt = blast_cond_pl.sum()
    print("\n" + "=" * 60)
    print("📊 雙創(20%)炸版股對比")
    print("=" * 60)
    print(f"  DB 炸版股數       = {db_blast_cnt}")
    print(f"  Payload 炸版股數  = {pl_blast_cnt}")

    # 不一致
    db_only = blast_cond_db & ~blast_cond_pl
    pl_only = blast_cond_pl & ~blast_cond_db
    print(f"  DB 獨有炸版       = {db_only.sum()}")
    print(f"  Payload 獨有炸版  = {pl_only.sum()}")

    def show_blast_list(mask: pd.Series, title: str, limit: int):
        n = int(mask.sum())
        if n == 0:
            return
        print(f"\n{title} (top {min(limit, n)} of {n}):")
        cols = [
            "symbol",
            "name_db", "name_pl",
            "close_db", "high_db", "limit_price_db",
            "close_pl", "high_pl", "limit_price_pl",
        ]
        sub = df.loc[mask, cols].copy()
        # 按 symbol 排序，便于人工比对
        sub = sub.sort_values("symbol")
        print(sub.head(limit).to_string(index=False))

    show_blast_list(db_only, "🔴 DB獨有炸版股 (DB有炸版但Payload沒有)", topn)
    show_blast_list(pl_only, "🟢 Payload獨有炸版股 (Payload有炸版但DB沒有)", topn)


if __name__ == "__main__":
    main()