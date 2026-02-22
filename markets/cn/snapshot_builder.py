# markets/cn/snapshot_builder.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd


# =============================================================================
# Config
# =============================================================================
def _eps() -> float:
    return float(os.getenv("CN_LIMIT_EPS", "0.0001"))


def _db_path() -> str:
    return os.getenv(
        "CN_DB_PATH",
        os.path.join(os.path.dirname(__file__), "cn_stock_warehouse.db"),
    )


def _connect(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path, timeout=120)


def _round_price_2(x: float) -> float:
    d = Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(d)


def _code_from_symbol(sym: str) -> str:
    return str(sym).split(".")[0].zfill(6)


def _is_st(name: str) -> bool:
    if not name:
        return False
    s = str(name).upper().strip()
    import re

    return re.search(r"(^|\W)\*?ST", s) is not None


def _limit_rate(symbol: str, name: str) -> float:
    """
    CN 基本漲跌幅：
      - ST: 5%
      - 创业(300/301), 科创(688): 20%
      - 北交(4xxxx/8xxxx): 30%
      - 其餘主板：10%
    """
    code = _code_from_symbol(symbol)
    if _is_st(name):
        return 0.05
    if code.startswith(("8", "4")):
        return 0.30
    if code.startswith(("300", "301", "688")):
        return 0.20
    return 0.10


# =============================================================================
# Load from DB
# =============================================================================
def _load_day(conn: sqlite3.Connection, ymd: str) -> pd.DataFrame:
    sql = """
    WITH p AS (
      SELECT
        stock_prices.symbol AS symbol,
        stock_prices.date   AS date,
        stock_prices.open   AS open,
        stock_prices.high   AS high,
        stock_prices.low    AS low,
        stock_prices.close  AS close,
        stock_prices.volume AS volume,
        LAG(stock_prices.close) OVER (PARTITION BY stock_prices.symbol ORDER BY stock_prices.date) AS last_close
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


def _pick_latest_trading_day(conn: sqlite3.Connection, ymd: str) -> Optional[str]:
    """
    找 <= ymd 的最後交易日（DB 有 stock_prices.date 的那一天）。
    用於放假/休市時仍能產出最新一份 snapshot。
    """
    sql = """
    SELECT MAX(date) AS last_date
    FROM stock_prices
    WHERE date <= ?
    """
    df = pd.read_sql_query(sql, conn, params=(str(ymd)[:10],))
    if df.empty:
        return None
    v = df.loc[0, "last_date"]
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v)[:10].strip()
    return s or None


def _load_recent_prices(conn: sqlite3.Connection, ymd: str, limit_days: int = 60) -> pd.DataFrame:
    """
    取最近 N 個交易日的 close/high + last_close，供 streak 計算用
    """
    sql_dates = """
    SELECT date
    FROM (
      SELECT DISTINCT date
      FROM stock_prices
      WHERE date <= ?
      ORDER BY date DESC
      LIMIT ?
    )
    ORDER BY date ASC
    """
    df_dates = pd.read_sql_query(sql_dates, conn, params=(ymd, int(limit_days)))
    if df_dates.empty:
        return pd.DataFrame()

    dates = df_dates["date"].astype(str).tolist()
    placeholders = ",".join(["?"] * len(dates))

    # ✅ 修正 ambiguous column name: symbol（全部加表名前綴 + alias）
    sql = f"""
    WITH p AS (
      SELECT
        stock_prices.symbol AS symbol,
        stock_prices.date   AS date,
        stock_prices.high   AS high,
        stock_prices.close  AS close,
        LAG(stock_prices.close) OVER (PARTITION BY stock_prices.symbol ORDER BY stock_prices.date) AS last_close,
        i.name AS name
      FROM stock_prices
      LEFT JOIN stock_info i ON i.symbol = stock_prices.symbol
      WHERE stock_prices.date IN ({placeholders})
    )
    SELECT symbol, date, high, close, last_close, name
    FROM p
    ORDER BY symbol, date
    """
    return pd.read_sql_query(sql, conn, params=dates)


# =============================================================================
# Compute fields
# =============================================================================
def _compute_limit_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["symbol"] = out["symbol"].astype(str)
    out["name"] = out.get("name", "").fillna("").astype(str)

    for c in ["open", "high", "low", "close", "volume", "last_close"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    eps = _eps()

    rates: List[float] = []
    limit_prices: List[Optional[float]] = []
    locked: List[bool] = []
    touch_any: List[bool] = []
    ret: List[float] = []
    touch_ret: List[float] = []

    for _, r in out.iterrows():
        sym = str(r.get("symbol", ""))
        nm = str(r.get("name", ""))
        lc = r.get("last_close", None)
        c = r.get("close", None)
        h = r.get("high", None)

        rate = _limit_rate(sym, nm)
        rates.append(rate)

        if lc is not None and pd.notna(lc) and float(lc) > 0 and c is not None and pd.notna(c):
            ret.append((float(c) / float(lc)) - 1.0)
        else:
            ret.append(0.0)

        if lc is not None and pd.notna(lc) and float(lc) > 0 and h is not None and pd.notna(h):
            touch_ret.append((float(h) / float(lc)) - 1.0)
        else:
            touch_ret.append(0.0)

        if lc is None or pd.isna(lc) or float(lc) <= 0:
            limit_prices.append(None)
            locked.append(False)
            touch_any.append(False)
            continue

        lp = _round_price_2(float(lc) * (1.0 + float(rate)))
        limit_prices.append(lp)

        locked.append(False if (c is None or pd.isna(c)) else (float(c) >= float(lp) - eps))
        touch_any.append(False if (h is None or pd.isna(h)) else (float(h) >= float(lp) - eps))

    out["limit_rate"] = rates
    out["limit_price"] = limit_prices
    out["is_limitup_locked"] = locked

    # 這裡仍維持 raw touch_any（包含封板者），後面 CN aggregator 會把「炸板」重算出來
    out["is_limitup_touch"] = touch_any
    out["touched_only"] = out["is_limitup_touch"].astype(bool) & (~out["is_limitup_locked"].astype(bool))

    out["ret"] = ret
    out["touch_ret"] = touch_ret

    out["market_label"] = out.get("market_label", "Unknown")
    out["badge_text"] = out.get("badge_text", "")
    out["badge_level"] = out.get("badge_level", 0)
    out["limit_type"] = out.get("limit_type", "standard")

    return out


def _attach_streaks(df_day: pd.DataFrame, df_recent: pd.DataFrame, ymd: str) -> pd.DataFrame:
    out = df_day.copy()
    out["streak"] = 0
    out["streak_prev"] = 0
    out["prev_was_limitup_locked"] = False
    out["prev_was_limitup_touch"] = False
    out["hit_prev"] = 0

    if df_recent is None or df_recent.empty:
        return out

    dfR = df_recent.copy()
    dfR["symbol"] = dfR["symbol"].astype(str)
    dfR["name"] = dfR.get("name", "").fillna("").astype(str)
    for c in ["close", "high", "last_close"]:
        dfR[c] = pd.to_numeric(dfR[c], errors="coerce")

    eps = _eps()
    dfR["limit_rate"] = dfR.apply(lambda r: _limit_rate(r["symbol"], r["name"]), axis=1)

    def lp(row) -> Optional[float]:
        lc = row["last_close"]
        if lc is None or pd.isna(lc) or float(lc) <= 0:
            return None
        return _round_price_2(float(lc) * (1.0 + float(row["limit_rate"])))

    dfR["limit_price"] = dfR.apply(lp, axis=1)
    dfR["is_locked"] = dfR.apply(
        lambda r: False
        if r["limit_price"] is None or pd.isna(r["close"])
        else (float(r["close"]) >= float(r["limit_price"]) - eps),
        axis=1,
    )
    dfR["is_touch"] = dfR.apply(
        lambda r: False
        if r["limit_price"] is None or pd.isna(r["high"])
        else (float(r["high"]) >= float(r["limit_price"]) - eps),
        axis=1,
    )

    streak_map: Dict[str, int] = {}
    streak_prev_map: Dict[str, int] = {}
    prev_locked_map: Dict[str, bool] = {}
    prev_touch_map: Dict[str, bool] = {}

    for sym, g in dfR.groupby("symbol", sort=False):
        g = g.sort_values("date")
        dates = g["date"].astype(str).tolist()
        locked = g["is_locked"].astype(bool).tolist()
        touch = g["is_touch"].astype(bool).tolist()

        try:
            idx = dates.index(str(ymd))
        except ValueError:
            continue

        idx_y = idx - 1
        if idx_y >= 0:
            prev_locked_map[sym] = bool(locked[idx_y])
            prev_touch_map[sym] = bool(touch[idx_y])
        else:
            prev_locked_map[sym] = False
            prev_touch_map[sym] = False

        sp = 0
        j = idx_y
        while j >= 0 and bool(locked[j]):
            sp += 1
            j -= 1
        streak_prev_map[sym] = sp

        s0 = 0
        j = idx
        while j >= 0 and bool(locked[j]):
            s0 += 1
            j -= 1
        streak_map[sym] = s0

    out["symbol"] = out["symbol"].astype(str)
    out["streak"] = out["symbol"].map(lambda s: int(streak_map.get(s, 0)))
    out["streak_prev"] = out["symbol"].map(lambda s: int(streak_prev_map.get(s, 0)))
    out["prev_was_limitup_locked"] = out["symbol"].map(lambda s: bool(prev_locked_map.get(s, False)))
    out["prev_was_limitup_touch"] = out["symbol"].map(lambda s: bool(prev_touch_map.get(s, False)))
    out["hit_prev"] = out["symbol"].map(
        lambda s: 1 if (prev_locked_map.get(s, False) or prev_touch_map.get(s, False)) else 0
    )

    return out


# =============================================================================
# Public API
# =============================================================================
def run_intraday(
    *,
    slot: str = "midday",
    asof: str = "midday",
    ymd: Optional[str] = None,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    db = db_path or _db_path()
    if ymd is None:
        ymd = datetime.now().strftime("%Y-%m-%d")

    req_ymd = str(ymd)[:10]

    payload: Dict[str, Any] = {
        "market": "CN",
        "slot": slot,
        "asof": asof,
        "ymd": req_ymd,              # 使用者要求的日期（可能是休市日）
        "ymd_effective": req_ymd,    # 實際抓資料的日期（若休市會回退）
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "snapshot_main": [],
    }

    if not os.path.exists(db):
        payload["error"] = f"DB not found: {db}"
        return payload

    conn = _connect(db)
    try:
        df_day = _load_day(conn, req_ymd)

        # ✅ 放假/休市：改用 <= req_ymd 的最後交易日
        if df_day is None or df_day.empty:
            eff = _pick_latest_trading_day(conn, req_ymd)
            if not eff:
                return payload
            payload["ymd_effective"] = eff
            df_day = _load_day(conn, eff)
            if df_day is None or df_day.empty:
                return payload
        else:
            payload["ymd_effective"] = req_ymd

        # streak 用 effective 日去算（避免 idx 找不到）
        df_recent = _load_recent_prices(conn, payload["ymd_effective"], limit_days=60)
    finally:
        conn.close()

    df_day = _compute_limit_fields(df_day)
    df_day = _attach_streaks(df_day, df_recent, payload["ymd_effective"])

    if "sector" not in df_day.columns:
        df_day["sector"] = "未分類"
    df_day["sector"] = df_day["sector"].fillna("未分類").astype(str)
    df_day.loc[df_day["sector"].isin(["", "A-Share", "—", "-", "--", "－", "–"]), "sector"] = "未分類"

    payload["snapshot_main"] = df_day.to_dict(orient="records")
    return payload