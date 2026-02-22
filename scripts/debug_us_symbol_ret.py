# scripts/debug_us_symbol_ret.py
# -*- coding: utf-8 -*-
"""
Debug US single symbol returns:
- Read from DB (sqlite) and compute ret_close / ret_high / touch_10 / streak_close10
- Read from payload JSON (snapshot_open/open_limit_watchlist...) and show what's inside
- Compare both sides for recent N trading days

Usage (PowerShell):
  python scripts/debug_us_symbol_ret.py --symbol MOD --ymd 2026-01-29 --days 12 ^
    --db markets/us/us_stock_warehouse.db ^
    --payload data/cache/us/2026-01-29/close.payload.json

If you omit --db, will try env US_DB_PATH or markets/us/us_stock_warehouse.db
If you omit --payload, it will only print DB side.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


RET_TH = 0.10


def _f(x: Any) -> float:
    try:
        if x is None:
            return float("nan")
        return float(x)
    except Exception:
        return float("nan")


def _i(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0


def _load_payload(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    return json.loads(p.read_text(encoding="utf-8"))


def _pick_universe(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    # 依你的 pipeline 慣例：可能在 snapshot_open / open_limit_watchlist / snapshot
    for k in ("open_limit_watchlist", "snapshot_open", "snapshot_main", "snapshot_all", "snapshot"):
        v = payload.get(k)
        if isinstance(v, list) and v:
            return v
    return []


def _find_symbol_in_payload_rows(rows: List[Dict[str, Any]], symbol: str) -> Dict[str, Any]:
    symu = (symbol or "").strip().upper()
    for r in rows:
        if str(r.get("symbol") or "").strip().upper() == symu:
            return r
    return {}


def _db_default_path() -> Path:
    env = (os.getenv("US_DB_PATH") or "").strip()
    if env:
        return Path(env)
    return Path("markets/us/us_stock_warehouse.db")


def _query_db_recent_days(
    db_path: Path,
    symbol: str,
    ymd: str,
    days: int,
) -> pd.DataFrame:
    """
    抓最近 N 個「交易日」：date <= ymd ORDER BY date desc LIMIT N
    然後在 python 端計算：
      prev_close (LAG close)
      ret_close = close/prev_close - 1
      ret_high  = high/prev_close - 1
      touch_10  = (ret_high>=10% and ret_close<10%)
      streak_close10: 以 ret_close>=10% 的連續天數（到當天為止）
    """
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    symu = (symbol or "").strip().upper()

    conn = sqlite3.connect(str(db_path))
    try:
        # 先抓最近 N 天 raw OHLC（交易日）
        df = pd.read_sql_query(
            """
            SELECT date, open, high, low, close, volume
            FROM stock_prices
            WHERE symbol = ? AND date <= ?
            ORDER BY date DESC
            LIMIT ?
            """,
            conn,
            params=(symu, ymd, int(days)),
        )
    finally:
        conn.close()

    if df.empty:
        return df

    # 轉成時間正序方便算 LAG / streak
    df = df.sort_values("date").reset_index(drop=True)

    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["prev_close"] = df["close"].shift(1)

    df["ret_close"] = (df["close"] / df["prev_close"]) - 1.0
    df["ret_high"] = (df["high"] / df["prev_close"]) - 1.0

    df["hit_close_10"] = ((df["ret_close"] >= RET_TH) & df["ret_close"].notna()).astype(int)
    df["touch_10"] = ((df["ret_high"] >= RET_TH) & (df["hit_close_10"] == 0) & df["ret_high"].notna()).astype(int)

    # streak：以 close>=10% 連續
    streak = []
    cur = 0
    for v in df["hit_close_10"].tolist():
        if v == 1:
            cur += 1
        else:
            cur = 0
        streak.append(cur)
    df["streak_close10"] = streak
    df["streak_prev"] = df["streak_close10"].apply(lambda x: max(0, int(x) - 1))

    # 只保留你關心欄位
    keep = [
        "date",
        "prev_close",
        "open",
        "high",
        "low",
        "close",
        "ret_close",
        "ret_high",
        "hit_close_10",
        "touch_10",
        "streak_close10",
        "streak_prev",
        "volume",
    ]
    df = df[keep]

    return df


def _fmt_pct(x: Any, digits: int = 2) -> str:
    v = _f(x)
    if pd.isna(v):
        return "NA"
    return f"{v*100:+.{digits}f}%"


def _print_payload_snapshot(payload_row: Dict[str, Any], symbol: str) -> None:
    if not payload_row:
        print(f"[JSON] symbol={symbol}: NOT FOUND in payload rows")
        return

    # 盡量把你 pipeline 會用到的欄位都列出來
    fields = [
        "bar_date",
        "symbol",
        "name",
        "sector",
        "prev_close",
        "open",
        "high",
        "low",
        "close",
        "ret",
        "ret_high",
        "hit_10_close",
        "touch_10",
        "streak",
        "streak_prev",
    ]
    print("\n[JSON] payload row (key fields)")
    for k in fields:
        if k in payload_row:
            v = payload_row.get(k)
            if k in ("ret", "ret_high"):
                print(f"  {k:12s} = {_fmt_pct(v)}")
            else:
                print(f"  {k:12s} = {v}")
    print("")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="MOD")
    ap.add_argument("--ymd", required=True, help="as-of date, e.g. 2026-01-31")
    ap.add_argument("--days", type=int, default=12, help="recent trading days from DB")
    ap.add_argument("--db", default=None, help="sqlite db path (default: US_DB_PATH or markets/us/us_stock_warehouse.db)")
    ap.add_argument("--payload", default=None, help="payload json path (optional)")
    args = ap.parse_args()

    symbol = args.symbol.strip().upper()
    ymd = args.ymd.strip()
    days = int(args.days)

    db_path = Path(args.db) if args.db else _db_default_path()

    # ---- DB side ----
    df = _query_db_recent_days(db_path=db_path, symbol=symbol, ymd=ymd, days=days)
    print(f"[DB] db={db_path}")
    if df.empty:
        print(f"[DB] symbol={symbol} <= {ymd}: NO ROWS")
    else:
        # 顯示表格
        df_show = df.copy()
        df_show["ret_close"] = df_show["ret_close"].apply(lambda x: _fmt_pct(x, 2))
        df_show["ret_high"] = df_show["ret_high"].apply(lambda x: _fmt_pct(x, 2))
        df_show["prev_close"] = df_show["prev_close"].map(lambda x: "NA" if pd.isna(x) else f"{x:.4f}")
        for c in ["open", "high", "low", "close"]:
            df_show[c] = df_show[c].map(lambda x: "NA" if pd.isna(x) else f"{x:.4f}")
        print("\n[DB] recent trading days (computed)")
        print(df_show.to_string(index=False))

        # 印出最後一天的「你 us_snapshot.py 會輸出的」核心結論
        last = df.iloc[-1].to_dict()
        print("\n[DB] last-day summary")
        print(f"  date        = {last['date']}")
        print(f"  ret_close   = {_fmt_pct(last['ret_close'])}  (close/prev_close-1)")
        print(f"  ret_high    = {_fmt_pct(last['ret_high'])}  (high/prev_close-1)")
        print(f"  hit_close_10= {int(last['hit_close_10'])}")
        print(f"  touch_10    = {int(last['touch_10'])}")
        print(f"  streak      = {int(last['streak_close10'])}  (close>=10% consecutive)")
        print(f"  streak_prev = {int(last['streak_prev'])}")

    # ---- JSON side ----
    payload = _load_payload(args.payload)
    if payload:
        rows = _pick_universe(payload)
        payload_row = _find_symbol_in_payload_rows(rows, symbol)
        print(f"\n[JSON] payload={args.payload}")
        _print_payload_snapshot(payload_row, symbol)

        # 對照最後一天（如果 DB 有）
        if not df.empty and payload_row:
            print("[COMPARE] DB last day vs JSON (same-date check)")
            db_last = df.iloc[-1]
            db_date = str(db_last["date"])
            js_date = str(payload_row.get("bar_date") or payload.get("ymd_effective") or payload.get("ymd") or "")
            print(f"  DB date     = {db_date}")
            print(f"  JSON bar_date/ymd = {js_date}")

            # JSON ret 欄位（你 pipeline 通常用 ret 當 close ret）
            js_ret = _f(payload_row.get("ret"))
            js_ret_high = _f(payload_row.get("ret_high"))
            js_touch = _i(payload_row.get("touch_10"))
            js_streak = _i(payload_row.get("streak"))
            js_streak_prev = _i(payload_row.get("streak_prev"))

            print(f"  DB  ret_close = {_fmt_pct(db_last['ret_close'])} | JSON ret      = {_fmt_pct(js_ret)}")
            print(f"  DB  ret_high  = {_fmt_pct(db_last['ret_high'])} | JSON ret_high = {_fmt_pct(js_ret_high)}")
            print(f"  DB  touch_10  = {int(db_last['touch_10'])} | JSON touch_10 = {js_touch}")
            print(f"  DB  streak    = {int(db_last['streak_close10'])} | JSON streak    = {js_streak}")
            print(f"  DB  streak_prev={int(db_last['streak_prev'])} | JSON streak_prev={js_streak_prev}")

            # 常見差異原因提示
            print("\n[HINT] If mismatch:")
            print("  - JSON ret 不是用 close/prev_close？或 prev_close 對不上交易日序列（缺日/資料缺洞）")
            print("  - JSON 的 ymd_effective 跟你拿來比的 ymd 不同")
            print("  - DB 裡 high/close 有缺值或被 0/NULL 清掉")
    else:
        if args.payload:
            print(f"[JSON] payload path provided but empty? path={args.payload}")

    print("\nDone.")


if __name__ == "__main__":
    main()
