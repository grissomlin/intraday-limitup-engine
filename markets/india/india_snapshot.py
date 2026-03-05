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

EPS = 1e-6
INDIA_SURGE_RET = float(os.getenv("INDIA_SURGE_RET", "0.10"))  # >=10%


def _pick_latest_leq(conn: sqlite3.Connection, ymd: str) -> Optional[str]:
    row = conn.execute(
        "SELECT MAX(date) FROM stock_prices WHERE date <= ? AND close IS NOT NULL",
        (ymd,),
    ).fetchone()
    return row[0] if row and row[0] else None


def _parse_band_pct_from_market_detail(md: Any) -> Optional[float]:
    s = ("" if md is None else str(md)).strip()
    if not s:
        return None
    parts = s.split("|")
    band_raw = None
    for p in parts:
        if p.startswith("band="):
            band_raw = p.split("=", 1)[1].strip()
            break
    if not band_raw:
        return None
    br = band_raw.strip()
    if not br or br.lower() in {"-", "no band", "none", "nan"}:
        return None
    try:
        v = float(br)
        if v <= 0:
            return None
        return v / 100.0
    except Exception:
        return None


def _limit_price(last_close: float, band_pct: float) -> float:
    return float(last_close) * (1.0 + float(band_pct))


def _day_status(
    *,
    close: float,
    high: float,
    last_close: float,
    band_pct: Optional[float],
    ret: float,
    surge_ret: float,
) -> str:
    """
    return one of: "hit", "touch", "big", ""
    Priority: hit > touch > big > none
    """
    if (band_pct is not None) and (last_close > 0):
        lp = _limit_price(last_close, float(band_pct))
        touch = (high > 0) and (high >= lp - EPS)
        locked = (close > 0) and (close >= lp - EPS)
        if locked:
            return "hit"
        if touch:
            return "touch"
    # big10 (exclude touch/hit already handled above)
    if ret >= float(surge_ret):
        return "big"
    return ""


def _compute_streaks_for_symbol(rows_desc: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    rows_desc: list of day dicts sorted by date DESC (today first)
    Each row should have: close, high, last_close, band_pct, ret
    """
    statuses: List[str] = []
    events: List[bool] = []
    for r in rows_desc:
        st = _day_status(
            close=float(r.get("close") or 0.0),
            high=float(r.get("high") or 0.0),
            last_close=float(r.get("last_close") or 0.0),
            band_pct=r.get("band_pct"),
            ret=float(r.get("ret") or 0.0),
            surge_ret=INDIA_SURGE_RET,
        )
        statuses.append(st)
        events.append(bool(st))  # hit/touch/big 都算事件

    def _count_from(idx: int) -> int:
        c = 0
        for j in range(idx, len(events)):
            if events[j]:
                c += 1
            else:
                break
        return c

    today_status = statuses[0] if len(statuses) >= 1 else ""
    prev_status = statuses[1] if len(statuses) >= 2 else ""

    streak_today = _count_from(0) if len(events) >= 1 else 0
    streak_prev = _count_from(1) if len(events) >= 2 else 0

    return {
        "today_status": today_status,
        "prev_status": prev_status,
        "streak_today": int(streak_today),
        "streak_prev": int(streak_prev),
    }


def run_intraday(*, slot: str, asof: str, ymd: str) -> Dict[str, Any]:
    db_path = _db_path()
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"INDIA DB not found: {db_path} (set INDIA_DB_PATH to override)")

    conn = sqlite3.connect(db_path)
    try:
        ymd_effective = _pick_latest_leq(conn, ymd) or ymd
        log(f"🕒 requested ymd={ymd} slot={slot} asof={asof}")
        log(f"📅 ymd_effective = {ymd_effective}")

        # 抓每個 symbol 最近 N 天（含今天），用於算 streak/prev_status
        N_DAYS = 12

        sql_hist = f"""
        WITH p AS (
          SELECT
            symbol,
            date,
            open, high, low, close, volume,
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS last_close,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
          FROM stock_prices
          WHERE date <= ?
            AND close IS NOT NULL
        )
        SELECT
          p.symbol, p.date AS ymd,
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
        WHERE p.rn <= {int(N_DAYS)}
        ORDER BY p.symbol, p.ymd DESC
        """
        dfh = pd.read_sql_query(sql_hist, conn, params=(ymd_effective,))
        if dfh.empty:
            snapshot_main: List[Dict[str, Any]] = []
        else:
            # clean / types
            dfh["name"] = dfh["name"].fillna("Unknown")
            dfh["industry"] = dfh["industry"].fillna("Unclassified")
            dfh["sector"] = dfh["sector"].fillna("Unclassified")

            for c in ["open", "high", "low", "close", "volume", "last_close"]:
                dfh[c] = pd.to_numeric(dfh[c], errors="coerce")

            # band pct per row (same for symbol, but safe)
            dfh["band_pct"] = dfh["market_detail"].apply(_parse_band_pct_from_market_detail)

            # ret per row (close / last_close - 1)
            dfh["ret"] = 0.0
            m = dfh["last_close"].notna() & (dfh["last_close"] > 0) & dfh["close"].notna()
            dfh.loc[m, "ret"] = (dfh.loc[m, "close"] / dfh.loc[m, "last_close"]) - 1.0
            dfh["ret_pct"] = dfh["ret"] * 100.0

            # per-symbol compute status + streak
            extra_map: Dict[str, Dict[str, Any]] = {}
            for sym, g in dfh.groupby("symbol", sort=False):
                rows_desc = g.sort_values("ymd", ascending=False, kind="mergesort").to_dict(orient="records")
                extra_map[sym] = _compute_streaks_for_symbol(rows_desc)

            # build TODAY snapshot (ymd_effective rows only)
            dft = dfh[dfh["ymd"] == ymd_effective].copy()
            if dft.empty:
                snapshot_main = []
            else:
                # attach extra fields
                dft["today_status"] = dft["symbol"].map(lambda s: (extra_map.get(s) or {}).get("today_status", ""))
                dft["prev_status"] = dft["symbol"].map(lambda s: (extra_map.get(s) or {}).get("prev_status", ""))
                dft["streak_today"] = dft["symbol"].map(lambda s: int((extra_map.get(s) or {}).get("streak_today", 0)))
                dft["streak_prev"] = dft["symbol"].map(lambda s: int((extra_map.get(s) or {}).get("streak_prev", 0)))

                # also keep prev session ret (from dfh: prev day row)
                # easiest: compute prev_ret from dfh's prev row close/last_close
                # but for renderer "Prev session" we want yesterday ret_pct, not today.
                # So map: prev_ret_pct = dfh[ymd_prev].ret_pct
                prev_ret_pct_map: Dict[str, float] = {}
                prev_close_map: Dict[str, float] = {}
                # pick prev row per symbol (rn==2 in DESC)
                for sym, g in dfh.groupby("symbol", sort=False):
                    g2 = g.sort_values("ymd", ascending=False, kind="mergesort")
                    if len(g2) >= 2:
                        prev_row = g2.iloc[1]
                        prev_ret_pct_map[sym] = float(prev_row.get("ret_pct") or 0.0)
                        prev_close_map[sym] = float(prev_row.get("close") or 0.0)
                    else:
                        prev_ret_pct_map[sym] = 0.0
                        prev_close_map[sym] = float("nan")

                dft["prev_ret_pct"] = dft["symbol"].map(lambda s: float(prev_ret_pct_map.get(s, 0.0)))
                dft["prev_close"] = dft["symbol"].map(lambda s: prev_close_map.get(s))

                # unified naming in your payload:
                # last_close already = yesterday close
                dft["streak"] = dft["streak_today"]  # keep old key for compatibility

                snapshot_main = dft[
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
                        "ret_pct",
                        "market",
                        "market_detail",
                        "band_pct",

                        # ✅ new fields
                        "today_status",
                        "prev_status",
                        "streak_today",
                        "streak_prev",

                        # ✅ prev session display helpers
                        "prev_ret_pct",
                        "prev_close",
                        "streak",
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
