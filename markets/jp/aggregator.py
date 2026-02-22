# markets/jp/aggregator.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
import math
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd

from .jp_limit_rules import jp_calc_limit, is_true_limitup
from .jp_labels import surge_label


# =============================================================================
# Env knobs
# =============================================================================
JP_SURGE_RET = float(os.getenv("JP_SURGE_RET", "0.10"))  # >=10% treated as "surge"
PEERS_BY_SECTOR_CAP = int(os.getenv("JP_PEERS_BY_SECTOR_CAP", "50"))

JP_ENABLE_TRUE_LIMITUP = os.getenv("JP_ENABLE_TRUE_LIMITUP", "1").strip().lower() in (
    "1", "true", "yes", "y", "on"
)

JP_ENABLE_STREAK = os.getenv("JP_ENABLE_STREAK", "1").strip().lower() in (
    "1", "true", "yes", "y", "on"
)

JP_STREAK_LOOKBACK_DAYS = int(os.getenv("JP_STREAK_LOOKBACK_DAYS", "20"))

JP_STREAK_ONLY_LOCKED = os.getenv("JP_STREAK_ONLY_LOCKED", "1").strip().lower() in (
    "1", "true", "yes", "y", "on"
)

EPS = 1e-6


# =============================================================================
# Helpers
# =============================================================================
def _to_float(x: Any, default: float = 0.0) -> float:
    """Robust float caster."""
    try:
        if x is None:
            return default
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except Exception:
        return default


def _norm_sector(x: Any) -> str:
    s = (str(x).strip() if x is not None else "")
    return s if s else "未分類"


def _pick_db_path(raw_payload: Dict[str, Any]) -> Optional[str]:
    meta = raw_payload.get("meta") or {}
    dbp = meta.get("db_path")
    if isinstance(dbp, str) and dbp.strip():
        return dbp.strip()

    env_db = os.getenv("JP_DB_PATH", "").strip()
    if env_db:
        return env_db

    try:
        here = os.path.dirname(__file__)
        return os.path.join(here, "jp_stock_warehouse.db")
    except Exception:
        return None


def _get_prev_trade_date(conn: sqlite3.Connection, ymd_effective: str) -> Optional[str]:
    """Ignore empty rows where close is NULL."""
    try:
        row = conn.execute(
            "SELECT MAX(date) FROM stock_prices "
            "WHERE date < ? AND close IS NOT NULL",
            (ymd_effective,),
        ).fetchone()
        if row and row[0]:
            return str(row[0])
    except Exception:
        pass
    return None


def _fetch_prev_day_rows(
    conn: sqlite3.Connection,
    *,
    ymd_prev: str,
    symbols: List[str],
) -> Dict[str, Dict[str, float]]:
    """
    Returns:
      {sym: {"high":..., "close":..., "last_close":...}}

    last_close here = close of the day before ymd_prev.
    Exclude rows where close is NULL.
    """
    if not symbols:
        return {}

    out: Dict[str, Dict[str, float]] = {}
    chunk_size = 800

    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i : i + chunk_size]
        qs = ",".join(["?"] * len(chunk))

        sql = f"""
        WITH p AS (
          SELECT
            symbol, date, high, close,
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS last_close
          FROM stock_prices
          WHERE symbol IN ({qs})
            AND date <= ?
        )
        SELECT symbol, high, close, last_close
        FROM p
        WHERE date = ?
          AND close IS NOT NULL
        """

        params = list(chunk) + [ymd_prev, ymd_prev]

        try:
            rows = conn.execute(sql, params).fetchall()
        except Exception:
            rows = []

        for sym, high, close, last_close in rows:
            out[str(sym)] = {
                "high": _to_float(high, 0.0),
                "close": _to_float(close, 0.0),
                "last_close": _to_float(last_close, 0.0),
            }

    return out


def _compute_locked_streaks(
    *,
    db_path: str,
    ymd_effective: str,
    symbols: List[str],
    lookback_days: int,
) -> Dict[str, int]:
    """
    consecutive TRUE limitup (locked close at limit) days ending at ymd_effective
    """
    if not symbols:
        return {}
    if not db_path or not os.path.exists(db_path):
        return {}

    out: Dict[str, int] = {}
    conn = sqlite3.connect(db_path)

    try:
        for sym in symbols:
            sql = """
            WITH p AS (
              SELECT
                symbol, date, close,
                LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS last_close
              FROM stock_prices
              WHERE symbol = ?
                AND date <= ?
              ORDER BY date DESC
              LIMIT ?
            )
            SELECT symbol, date, close, last_close
            FROM p
            ORDER BY date DESC
            """

            rows = conn.execute(
                sql,
                (sym, ymd_effective, int(max(5, lookback_days))),
            ).fetchall()

            if not rows:
                continue

            streak = 0
            for (_s, _d, close, last_close) in rows:
                c = _to_float(close, 0.0)
                lc = _to_float(last_close, 0.0)

                if lc <= 0 or c <= 0:
                    break

                if is_true_limitup(c, lc):
                    streak += 1
                else:
                    break

            if streak >= 1:
                out[sym] = int(streak)

    finally:
        conn.close()

    return out


# =============================================================================
# Main API
# =============================================================================
def aggregate(raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    JP aggregator:

    - enrich snapshot_main rows with:
        limit_price, is_limitup_touch/locked/opened
        prev_is_limitup_*
        streak, streak_prev

    - build display list (mix):
        touch (incl locked) + surge>=10%

    - build sector_summary + peers

    - meta.totals:
        locked_total
        touched_total (touch-only)
        bigmove10_total (10%+ including stop-high)
        bigmove10_ex_locked_total (pure 10%+ excluding any touch)
        mix_total = locked + touch-only + pure10
    """

    snap: List[Dict[str, Any]] = raw_payload.get("snapshot_main") or []
    if not snap:
        raw_payload.setdefault("limitup", [])
        raw_payload.setdefault("sector_summary", [])
        raw_payload.setdefault("peers_by_sector", {})
        raw_payload.setdefault("peers_not_limitup", [])
        raw_payload.setdefault("stats", {})
        raw_payload["stats"]["limitup_count"] = 0
        raw_payload.setdefault("meta", {})
        raw_payload["meta"].setdefault("totals", {})
        return raw_payload

    df = pd.DataFrame(snap).copy()

    for c in ["symbol", "name", "sector", "open", "high", "low", "close", "last_close", "ret"]:
        if c not in df.columns:
            df[c] = None

    df = df.dropna(subset=["symbol"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df[df["close"].notna()].copy()

    df["sector"] = df["sector"].apply(_norm_sector)
    df["open"] = df["open"].apply(_to_float)
    df["high"] = df["high"].apply(_to_float)
    df["low"] = df["low"].apply(_to_float)
    df["close"] = df["close"].apply(_to_float)
    df["last_close"] = df["last_close"].apply(_to_float)
    df["ret"] = df["ret"].apply(_to_float)

    df["streak"] = 1
    df["streak_prev"] = 0

    # ------------------------------------------------------------
    # Today: limit price + touch/locked
    # ------------------------------------------------------------
    limit_price: List[Optional[float]] = []
    limit_pct: List[Optional[float]] = []
    limit_amt: List[Optional[float]] = []
    is_touch: List[bool] = []
    is_locked: List[bool] = []

    if JP_ENABLE_TRUE_LIMITUP:
        for _, r in df.iterrows():
            lc = float(r["last_close"] or 0.0)
            c = float(r["close"] or 0.0)
            h = float(r["high"] or 0.0)

            if lc > 0:
                res = jp_calc_limit(lc)
                lp = float(res.limit_price)

                limit_price.append(lp)
                limit_pct.append(float(res.limit_pct))
                limit_amt.append(float(res.limit_amount))

                touch = (h > 0) and (h >= lp - EPS)
                locked = (c > 0) and (c >= lp - EPS)

                is_touch.append(bool(touch))
                is_locked.append(bool(locked))
            else:
                limit_price.append(None)
                limit_pct.append(None)
                limit_amt.append(None)
                is_touch.append(False)
                is_locked.append(False)

    else:
        limit_price = [None] * len(df)
        limit_pct = [None] * len(df)
        limit_amt = [None] * len(df)
        is_touch = [False] * len(df)
        is_locked = [False] * len(df)

    df["limit_price"] = limit_price
    df["jp_limit_price"] = limit_price
    df["jp_limit_pct"] = limit_pct
    df["jp_limit_amount"] = limit_amt

    df["is_limitup_touch"] = is_touch
    df["is_limitup_locked"] = is_locked
    df["is_limitup_opened"] = df["is_limitup_touch"] & (~df["is_limitup_locked"])
    df["is_limitup"] = df["is_limitup_touch"]
    df["is_true_limitup"] = df["is_limitup_locked"]

    # ------------------------------------------------------------
    # Yesterday: prev touch/locked + streak_prev
    # ------------------------------------------------------------
    df["prev_is_limitup_touch"] = False
    df["prev_is_limitup_locked"] = False
    df["prev_is_limitup_opened"] = False
    df["prev_is_true_limitup"] = False

    ymd_effective = str(raw_payload.get("ymd_effective") or raw_payload.get("ymd") or "").strip()
    db_path = _pick_db_path(raw_payload)

    ymd_prev: Optional[str] = None
    prev_map: Dict[str, Dict[str, float]] = {}

    if JP_ENABLE_TRUE_LIMITUP and ymd_effective and db_path and os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        try:
            ymd_prev = _get_prev_trade_date(conn, ymd_effective)
            if ymd_prev:
                syms = df["symbol"].dropna().astype(str).unique().tolist()
                prev_map = _fetch_prev_day_rows(conn, ymd_prev=ymd_prev, symbols=syms)
        finally:
            conn.close()

    if ymd_prev and prev_map:
        prev_touch: List[bool] = []
        prev_locked: List[bool] = []

        for _, r in df.iterrows():
            sym = str(r.get("symbol") or "").strip()
            info = prev_map.get(sym)

            if not sym or not info:
                prev_touch.append(False)
                prev_locked.append(False)
                continue

            h_prev = float(info.get("high") or 0.0)
            c_prev = float(info.get("close") or 0.0)
            lc_prev = float(info.get("last_close") or 0.0)

            if lc_prev > 0:
                res_prev = jp_calc_limit(lc_prev)
                lp_prev = float(res_prev.limit_price)

                t = (h_prev > 0) and (h_prev >= lp_prev - EPS)
                l = (c_prev > 0) and (c_prev >= lp_prev - EPS)

                prev_touch.append(bool(t))
                prev_locked.append(bool(l))
            else:
                prev_touch.append(False)
                prev_locked.append(False)

        df["prev_is_limitup_touch"] = prev_touch
        df["prev_is_limitup_locked"] = prev_locked
        df["prev_is_limitup_opened"] = df["prev_is_limitup_touch"] & (~df["prev_is_limitup_locked"])
        df["prev_is_true_limitup"] = df["prev_is_limitup_locked"]

    # ------------------------------------------------------------
    # Surge >=10%
    # ------------------------------------------------------------
    df["is_surge_ge10"] = df["ret"] >= float(JP_SURGE_RET)

    df["is_touch_only"] = (df["is_limitup_touch"] == True) & (df["is_limitup_locked"] == False)
    df["is_stop_high"] = (df["is_limitup_locked"] == True)

    df["is_surge_ge10_excl_limitup"] = (df["is_surge_ge10"] == True) & (df["is_limitup_touch"] == False)

    df["is_display_limitup"] = (df["is_limitup_touch"] == True) | (df["is_surge_ge10"] == True)

    # ------------------------------------------------------------
    # Streak locked-only
    # ------------------------------------------------------------
    streak_applied = False
    streak_prev_applied = False

    if (
        JP_ENABLE_STREAK
        and JP_ENABLE_TRUE_LIMITUP
        and JP_STREAK_ONLY_LOCKED
        and ymd_effective
        and db_path
        and os.path.exists(db_path)
    ):
        syms_locked = df.loc[df["is_limitup_locked"] == True, "symbol"].dropna().astype(str).unique().tolist()
        if syms_locked:
            m = _compute_locked_streaks(
                db_path=db_path,
                ymd_effective=ymd_effective,
                symbols=syms_locked,
                lookback_days=JP_STREAK_LOOKBACK_DAYS,
            )
            if m:
                mask = df["symbol"].astype(str).isin(m.keys())
                df.loc[mask, "streak"] = df.loc[mask, "symbol"].astype(str).map(m).fillna(1).astype(int).values
                streak_applied = True

        if ymd_prev:
            syms_prev_locked = df.loc[df["prev_is_limitup_locked"] == True, "symbol"].dropna().astype(str).unique().tolist()
            if syms_prev_locked:
                m2 = _compute_locked_streaks(
                    db_path=db_path,
                    ymd_effective=ymd_prev,
                    symbols=syms_prev_locked,
                    lookback_days=JP_STREAK_LOOKBACK_DAYS,
                )
                if m2:
                    mask2 = df["symbol"].astype(str).isin(m2.keys())
                    df.loc[mask2, "streak_prev"] = (
                        df.loc[mask2, "symbol"].astype(str).map(m2).fillna(0).astype(int).values
                    )
                    streak_prev_applied = True

    # ------------------------------------------------------------
    # Tags
    # ------------------------------------------------------------
    def _tag_row(r) -> Tuple[str, Any]:
        if bool(r["is_true_limitup"]):
            pct = r.get("jp_limit_pct")
            return "ストップ高", float(pct) if pct is not None else None

        if bool(r["is_surge_ge10"]) and (not bool(r["is_limitup_touch"])):
            return surge_label(float(r["ret"])), None

        return "", None

    tags: List[str] = []
    limit_pct_show: List[Any] = []

    for _, r in df.iterrows():
        t, p = _tag_row(r)
        tags.append(t)
        limit_pct_show.append(p)

    df["tag"] = tags
    df["limit_pct"] = limit_pct_show

    # ------------------------------------------------------------
    # Build display list
    # ------------------------------------------------------------
    df_limit = df[df["is_display_limitup"]].copy()
    df_limit = df_limit.sort_values(["ret"], ascending=False, kind="mergesort")

    limitup_records: List[Dict[str, Any]] = []
    for _, r in df_limit.iterrows():
        rec = r.to_dict()
        rec["sector"] = _norm_sector(rec.get("sector"))
        rec["ret"] = float(rec.get("ret") or 0.0)
        rec["streak"] = int(rec.get("streak") or 1)
        rec["streak_prev"] = int(rec.get("streak_prev") or 0)

        for k in [
            "is_limitup_touch",
            "is_limitup_locked",
            "is_limitup_opened",
            "is_limitup",
            "is_true_limitup",
            "prev_is_limitup_touch",
            "prev_is_limitup_locked",
            "prev_is_limitup_opened",
            "prev_is_true_limitup",
            "is_surge_ge10",
            "is_surge_ge10_excl_limitup",
            "is_touch_only",
            "is_stop_high",
            "is_display_limitup",
        ]:
            rec[k] = bool(rec.get(k))

        limitup_records.append(rec)

    # ------------------------------------------------------------
    # Peers
    # ------------------------------------------------------------
    df_peers = df[~df["is_display_limitup"]].copy()
    df_peers = df_peers[df_peers["ret"] < float(JP_SURGE_RET)]
    df_peers = df_peers.sort_values(["ret"], ascending=False, kind="mergesort")

    peers_by_sector: Dict[str, List[Dict[str, Any]]] = {}
    peers_flat: List[Dict[str, Any]] = []

    for sec, g in df_peers.groupby("sector", sort=False):
        g2 = g.head(int(PEERS_BY_SECTOR_CAP)).copy()
        recs = g2.to_dict(orient="records")
        peers_by_sector[sec] = recs
        peers_flat.extend(recs)

    # ------------------------------------------------------------
    # sector_summary
    # ------------------------------------------------------------
    summary_rows: List[Dict[str, Any]] = []

    for sec, g in df.groupby("sector", sort=False):
        sector_total = int(len(g))

        stop_high_cnt = int((g["is_limitup_locked"] == True).sum())
        touch_only_cnt = int(((g["is_limitup_touch"] == True) & (g["is_limitup_locked"] == False)).sum())
        opened_cnt = int((g["is_limitup_opened"] == True).sum())

        surge10_total_cnt = int((g["is_surge_ge10"] == True).sum())
        surge10_ex_limitup_cnt = int(((g["is_surge_ge10"] == True) & (g["is_limitup_touch"] == False)).sum())

        display_cnt = int((g["is_display_limitup"] == True).sum())
        peer_cnt = int(len(df_peers[df_peers["sector"] == sec]))

        denom = float(sector_total) if sector_total > 0 else 0.0

        summary_rows.append(
            {
                "sector": sec,
                "display_limitup_count": display_cnt,
                "true_limitup_count": stop_high_cnt,
                "limitup_touch_only_count": touch_only_cnt,
                "limitup_opened_count": opened_cnt,
                "surge_ge10_total_count": surge10_total_cnt,
                "surge_ge10_ex_limitup_count": surge10_ex_limitup_cnt,
                "peers_count": peer_cnt,
                "locked_cnt": stop_high_cnt,
                "touched_cnt": touch_only_cnt,
                "bigmove10_cnt": surge10_ex_limitup_cnt,
                "sector_total": sector_total,
                "locked_pct": (stop_high_cnt / denom) if denom else None,
                "touched_pct": (touch_only_cnt / denom) if denom else None,
                "bigmove10_pct": (surge10_ex_limitup_cnt / denom) if denom else None,
                "mix_pct": (display_cnt / denom) if denom else None,
            }
        )

    summary_rows.sort(
        key=lambda x: (
            x.get("locked_cnt", 0),
            x.get("touched_cnt", 0),
            x.get("bigmove10_cnt", 0),
        ),
        reverse=True,
    )

    # ------------------------------------------------------------
    # stats + totals
    # ------------------------------------------------------------
    stats = raw_payload.get("stats", {}) or {}

    stats["limitup_count"] = int(len(limitup_records))
    stats["jp_true_limitup_count"] = int((df["is_stop_high"] == True).sum())
    stats["jp_limitup_touch_only_count"] = int((df["is_touch_only"] == True).sum())

    stats["jp_surge_ge10_total_count"] = int((df["is_surge_ge10"] == True).sum())
    stats["jp_surge_ge10_ex_limitup_count"] = int((df["is_surge_ge10_excl_limitup"] == True).sum())

    stats["jp_prev_trade_date"] = ymd_prev
    stats["jp_streak_applied"] = bool(streak_applied)
    stats["jp_streak_prev_applied"] = bool(streak_prev_applied)

    raw_payload["stats"] = stats

    raw_payload.setdefault("meta", {})
    meta = raw_payload.get("meta") or {}
    totals = meta.get("totals") or {}

    locked_total = int(stats["jp_true_limitup_count"])
    touched_only_total = int(stats["jp_limitup_touch_only_count"])
    surge10_ex_limitup_total = int(stats["jp_surge_ge10_ex_limitup_count"])
    surge10_total = int(stats["jp_surge_ge10_total_count"])

    totals["locked_total"] = locked_total
    totals["touched_total"] = touched_only_total
    totals["bigmove10_total"] = surge10_total
    totals["bigmove10_ex_locked_total"] = surge10_ex_limitup_total
    totals["mix_total"] = locked_total + touched_only_total + surge10_ex_limitup_total

    meta["totals"] = totals
    raw_payload["meta"] = meta

    # ------------------------------------------------------------
    # attach outputs
    # ------------------------------------------------------------
    raw_payload["snapshot_main"] = df.to_dict(orient="records")
    raw_payload["limitup"] = limitup_records
    raw_payload["sector_summary"] = summary_rows
    raw_payload["peers_by_sector"] = peers_by_sector
    raw_payload["peers_not_limitup"] = peers_flat

    return raw_payload
