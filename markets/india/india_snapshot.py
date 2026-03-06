# markets/india/india_snapshot.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import math
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from markets.common.time_builders import build_meta_time_asia
from .india_config import _db_path, log

EPS = 1e-6
INDIA_SURGE_RET = float(os.getenv("INDIA_SURGE_RET", "0.10"))  # >=10%
INDIA_TICK_SIZE = float(os.getenv("INDIA_TICK_SIZE", "0.01"))  # conservative default


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


def _round_to_tick(x: float, tick: float = INDIA_TICK_SIZE) -> float:
    if tick <= 0:
        return float(x)
    return round(round(float(x) / tick) * tick, 6)


def _limit_price(last_close: float, band_pct: float) -> float:
    """
    Use rounded circuit price instead of raw theoretical value.
    This greatly reduces false negatives like 4.98% on 5% band names.
    """
    raw = float(last_close) * (1.0 + float(band_pct))
    return _round_to_tick(raw, INDIA_TICK_SIZE)


def _touch_locked_flags(
    *,
    close: float,
    high: float,
    last_close: float,
    band_pct: Optional[float],
) -> Dict[str, Any]:
    if band_pct is None or last_close <= 0:
        return {
            "limit_price": None,
            "is_limitup_touch": False,
            "is_limitup_locked": False,
            "is_limitup_opened": False,
        }

    lp = _limit_price(last_close, float(band_pct))
    touch = (high > 0) and (high >= lp - EPS)
    locked = (close > 0) and (close >= lp - EPS)

    return {
        "limit_price": float(lp),
        "is_limitup_touch": bool(touch),
        "is_limitup_locked": bool(locked),
        "is_limitup_opened": bool(touch and not locked),
    }


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
    flags = _touch_locked_flags(
        close=close,
        high=high,
        last_close=last_close,
        band_pct=band_pct,
    )

    if flags["is_limitup_locked"]:
        return "hit"
    if flags["is_limitup_touch"]:
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
        events.append(bool(st))

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
          p.symbol,
          p.date AS ymd,
          p.open,
          p.high,
          p.low,
          p.close,
          p.volume,
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
        ORDER BY p.symbol, p.date DESC
        """
        dfh = pd.read_sql_query(sql_hist, conn, params=(ymd_effective,))

        if dfh.empty:
            snapshot_main: List[Dict[str, Any]] = []
        else:
            dfh["name"] = dfh["name"].fillna("Unknown")
            dfh["industry"] = dfh["industry"].fillna("Unclassified")
            dfh["sector"] = dfh["sector"].fillna("Unclassified")

            for c in ["open", "high", "low", "close", "volume", "last_close"]:
                dfh[c] = pd.to_numeric(dfh[c], errors="coerce")

            dfh["band_pct"] = dfh["market_detail"].apply(_parse_band_pct_from_market_detail)

            dfh["ret"] = 0.0
            m = dfh["last_close"].notna() & (dfh["last_close"] > 0) & dfh["close"].notna()
            dfh.loc[m, "ret"] = (dfh.loc[m, "close"] / dfh.loc[m, "last_close"]) - 1.0
            dfh["ret_pct"] = dfh["ret"] * 100.0

            # today's row-level limit flags
            flag_rows: List[Dict[str, Any]] = []
            for _, r in dfh.iterrows():
                flags = _touch_locked_flags(
                    close=float(r.get("close") or 0.0),
                    high=float(r.get("high") or 0.0),
                    last_close=float(r.get("last_close") or 0.0),
                    band_pct=r.get("band_pct"),
                )
                flag_rows.append(flags)

            dff = pd.DataFrame(flag_rows, index=dfh.index)
            for c in dff.columns:
                dfh[c] = dff[c]

            # normalized limit labels
            dfh["limit_rate"] = dfh["band_pct"]
            dfh["limit_rate_pct"] = dfh["band_pct"].apply(
                lambda x: (float(x) * 100.0) if pd.notna(x) else None
            )

            # per-symbol compute today/prev status + streak
            extra_map: Dict[str, Dict[str, Any]] = {}
            for sym, g in dfh.groupby("symbol", sort=False):
                rows_desc = g.sort_values("ymd", ascending=False, kind="mergesort").to_dict(orient="records")
                extra_map[sym] = _compute_streaks_for_symbol(rows_desc)

            dft = dfh[dfh["ymd"] == ymd_effective].copy()
            if dft.empty:
                snapshot_main = []
            else:
                dft["today_status"] = dft["symbol"].map(lambda s: (extra_map.get(s) or {}).get("today_status", ""))
                dft["prev_status"] = dft["symbol"].map(lambda s: (extra_map.get(s) or {}).get("prev_status", ""))
                dft["streak_today"] = dft["symbol"].map(lambda s: int((extra_map.get(s) or {}).get("streak_today", 0)))
                dft["streak_prev"] = dft["symbol"].map(lambda s: int((extra_map.get(s) or {}).get("streak_prev", 0)))

                prev_ret_pct_map: Dict[str, float] = {}
                prev_close_map: Dict[str, float] = {}
                prev_status_map: Dict[str, str] = {}

                for sym, g in dfh.groupby("symbol", sort=False):
                    g2 = g.sort_values("ymd", ascending=False, kind="mergesort")
                    if len(g2) >= 2:
                        prev_row = g2.iloc[1]
                        prev_ret_pct_map[sym] = float(prev_row.get("ret_pct") or 0.0)
                        prev_close_map[sym] = float(prev_row.get("close") or 0.0)
                        prev_status_map[sym] = _day_status(
                            close=float(prev_row.get("close") or 0.0),
                            high=float(prev_row.get("high") or 0.0),
                            last_close=float(prev_row.get("last_close") or 0.0),
                            band_pct=prev_row.get("band_pct"),
                            ret=float(prev_row.get("ret") or 0.0),
                            surge_ret=INDIA_SURGE_RET,
                        )
                    else:
                        prev_ret_pct_map[sym] = 0.0
                        prev_close_map[sym] = float("nan")
                        prev_status_map[sym] = ""

                dft["prev_ret_pct"] = dft["symbol"].map(lambda s: float(prev_ret_pct_map.get(s, 0.0)))
                dft["prev_close"] = dft["symbol"].map(lambda s: prev_close_map.get(s))
                dft["prev_limitup_status"] = dft["symbol"].map(lambda s: prev_status_map.get(s, ""))

                dft["limitup_status"] = dft["today_status"]
                dft["is_display_limitup"] = dft["limitup_status"].apply(lambda x: bool(str(x).strip()))
                dft["is_surge_ge10"] = dft["ret"].apply(lambda x: bool(float(x) >= INDIA_SURGE_RET))
                dft["is_bigmove10_ex_locked"] = dft.apply(
                    lambda r: bool(
                        float(r.get("ret") or 0.0) >= INDIA_SURGE_RET
                        and not bool(r.get("is_limitup_locked"))
                    ),
                    axis=1,
                )

                dft["ret_high"] = 0.0
                mh = dft["last_close"].notna() & (dft["last_close"] > 0) & dft["high"].notna()
                dft.loc[mh, "ret_high"] = (dft.loc[mh, "high"] / dft.loc[mh, "last_close"]) - 1.0
                dft["ret_high_pct"] = dft["ret_high"] * 100.0

                dft["abs_move"] = (dft["close"] - dft["last_close"]).abs()
                dft["is_penny_20inr"] = dft["last_close"].apply(lambda x: bool(pd.notna(x) and float(x) < 20.0))
                dft["ticks_needed_for_10pct"] = dft["last_close"].apply(
                    lambda x: (float(x) * 0.10 / INDIA_TICK_SIZE) if pd.notna(x) and INDIA_TICK_SIZE > 0 else None
                )
                dft["is_tick_danger"] = False
                dft["streak"] = dft["streak_today"]

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
                        "today_status",
                        "prev_status",
                        "streak_today",
                        "streak_prev",
                        "prev_ret_pct",
                        "prev_close",
                        "streak",
                        "ret_high",
                        "ret_high_pct",
                        "limit_price",
                        "limit_rate",
                        "limit_rate_pct",
                        "is_limitup_touch",
                        "is_limitup_locked",
                        "is_limitup_opened",
                        "is_penny_20inr",
                        "ticks_needed_for_10pct",
                        "is_tick_danger",
                        "is_surge_ge10",
                        "abs_move",
                        "is_bigmove10_ex_locked",
                        "is_display_limitup",
                        "limitup_status",
                        "prev_limitup_status",
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
