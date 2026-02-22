# markets/th/aggregator.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
import math
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd


# =============================================================================
# Env knobs (Thailand)
# =============================================================================
TH_LIMIT_PCT = float(os.getenv("TH_LIMIT_PCT", "0.30"))   # ceiling +30% (general case)
TH_SURGE_RET = float(os.getenv("TH_SURGE_RET", "0.10"))   # >=10% treated as "big mover"

TH_BIN20_RET = float(os.getenv("TH_BIN20_RET", "0.20"))   # 20%
TH_BIN30_RET = float(os.getenv("TH_BIN30_RET", "0.30"))   # 30% (same as ceiling for general case)

PEERS_BY_SECTOR_CAP = int(os.getenv("TH_PEERS_BY_SECTOR_CAP", "50"))

# Penny filter knobs
TH_FILTER_PENNY = (os.getenv("TH_FILTER_PENNY", "1") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
TH_PENNY_PRICE_MAX = float(os.getenv("TH_PENNY_PRICE_MAX", "0.15"))  # THB; close < this is penny

# Debug knobs
TH_DEBUG_PENNY = (os.getenv("TH_DEBUG_PENNY", "0") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
TH_DEBUG_TOUCH_VS_10 = (os.getenv("TH_DEBUG_TOUCH_VS_10", "0") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
TH_DEBUG_TOUCH_VS_10_MAX = int(os.getenv("TH_DEBUG_TOUCH_VS_10_MAX", "80"))  # print first N rows

EPS = 1e-6


# =============================================================================
# Helpers
# =============================================================================
def _to_float(x: Any, default: float = 0.0) -> float:
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
    if not s or s in ("—", "-", "--", "－", "–"):
        return "Unclassified"
    return s


def _pick_db_path(raw_payload: Dict[str, Any]) -> Optional[str]:
    meta = raw_payload.get("meta") or {}
    dbp = meta.get("db_path")
    if isinstance(dbp, str) and dbp.strip():
        return dbp.strip()

    env_db = os.getenv("TH_DB_PATH", "").strip()
    if env_db:
        return env_db

    try:
        here = os.path.dirname(__file__)
        return os.path.join(here, "th_stock_warehouse.db")
    except Exception:
        return None


def _get_prev_trade_date(conn: sqlite3.Connection, ymd_effective: str) -> Optional[str]:
    """
    Ignore empty rows where close is NULL.
    """
    try:
        row = conn.execute(
            "SELECT MAX(date) FROM stock_prices WHERE date < ? AND close IS NOT NULL",
            (ymd_effective,),
        ).fetchone()
        if row and row[0]:
            return str(row[0])
    except Exception:
        pass
    return None


def _fetch_prev_day_rows(
    conn: sqlite3.Connection, *, ymd_prev: str, symbols: List[str]
) -> Dict[str, Dict[str, float]]:
    """
    Returns: {sym: {"high":..., "close":..., "last_close":...}}
    last_close here = close of the day before ymd_prev.
    Excludes rows where close is NULL to avoid polluted prev_map.
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
            symbol,
            date,
            high,
            close,
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS last_close
          FROM stock_prices
          WHERE symbol IN ({qs}) AND date <= ?
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


# =============================================================================
# ✅ Fetch TODAY rows (DB truth) to override last_close / high / close
# =============================================================================
def _fetch_today_rows(
    conn: sqlite3.Connection, *, ymd: str, symbols: List[str]
) -> Dict[str, Dict[str, float]]:
    """
    Returns: {sym: {"high":..., "close":..., "last_close":...}}
    last_close here = previous trading day's close (via LAG).
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
            symbol,
            date,
            high,
            close,
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS last_close
          FROM stock_prices
          WHERE symbol IN ({qs}) AND date <= ?
        )
        SELECT symbol, high, close, last_close
        FROM p
        WHERE date = ?
          AND close IS NOT NULL
        """
        params = list(chunk) + [ymd, ymd]

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


def _limit_price(last_close: float, limit_pct: float) -> float:
    return float(last_close) * (1.0 + float(limit_pct))


def th_surge_label(ret: float) -> str:
    """
    Simple label for >10% movers (non-touch only).
    Keep short for UI. Example: '+15%'
    """
    try:
        p = int(round(float(ret) * 100.0))
        sign = "+" if p >= 0 else ""
        return f"{sign}{p}%"
    except Exception:
        return ""


def _sanitize_nan(obj: Any) -> Any:
    """
    Ensure JSON-safe payload:
    - Convert NaN/Inf floats to None (JSON standard; avoids `NaN` tokens).
    - Recurse dict/list/tuple.
    """
    try:
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
            return obj
    except Exception:
        pass

    try:
        if obj is pd.NA:
            return None
    except Exception:
        pass

    if obj is None:
        return None

    if isinstance(obj, dict):
        return {k: _sanitize_nan(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_sanitize_nan(v) for v in obj]

    if isinstance(obj, tuple):
        return tuple(_sanitize_nan(v) for v in obj)

    return obj


def _add_intraday_ret_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    ✅ Ensure we always have BOTH:
      - ret       : close-to-last_close return
      - ret_high  : high-to-last_close return (intraday peak)
    plus pct fields.
    """
    if df is None or df.empty:
        for c in ["ret", "ret_pct", "ret_high", "ret_high_pct"]:
            if c not in df.columns:
                df[c] = 0.0
        return df

    c = pd.to_numeric(df.get("close"), errors="coerce").fillna(0.0)
    h = pd.to_numeric(df.get("high"), errors="coerce").fillna(0.0)
    lc = pd.to_numeric(df.get("last_close"), errors="coerce").fillna(0.0)

    can = (lc > 0)

    ret_close = pd.Series(0.0, index=df.index, dtype="float64")
    ret_high = pd.Series(0.0, index=df.index, dtype="float64")

    ret_close.loc[can] = (c.loc[can] / lc.loc[can]) - 1.0
    ret_high.loc[can] = (h.loc[can] / lc.loc[can]) - 1.0

    df["ret"] = ret_close.astype(float)
    df["ret_pct"] = (df["ret"] * 100.0).astype(float)

    df["ret_high"] = ret_high.astype(float)
    df["ret_high_pct"] = (df["ret_high"] * 100.0).astype(float)
    return df


def _force_ret_fields(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    ✅ Guarantee renderer-safe numeric fields:
      - ret / ret_pct
      - ret_high / ret_high_pct
    """
    r = _to_float(rec.get("ret"), 0.0)
    rec["ret"] = float(r)
    rec["ret_pct"] = float(r * 100.0)

    rh = _to_float(rec.get("ret_high"), 0.0)
    rec["ret_high"] = float(rh)
    rec["ret_high_pct"] = float(rh * 100.0)
    return rec


def _env_on(name: str) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


# =============================================================================
# Main API
# =============================================================================
def aggregate(raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    TH aggregator (ceiling model, addable totals + inclusive metrics)

    ✅ Outputs for overview/footer compatibility (meta.totals):
      - locked_total                      : locked only
      - touched_total                     : touch-only (exclude locked)
      - bigmove10_ex_locked_total         : 10%+ movers excluding ANY touch/locked  (what footer wants)
      - bigmove10_total                   : inclusive 10%+ (can include touch/locked)  (gainbins / debug)
      - mix_total                         : locked + touched + bigmove10_ex_locked_total (+ other bins if you keep them)

    Notes:
      - bigmove10_ex_locked_total MUST NOT include locked/touch.
      - Thailand has no snapshot_builder in your repo; aggregator is the right place to fill meta.totals/meta.metrics.
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
        raw_payload["meta"].setdefault("metrics", {})
        return raw_payload

    df = pd.DataFrame(snap).copy()

    # ensure columns exist
    for c in ["symbol", "name", "sector", "open", "high", "low", "close", "last_close"]:
        if c not in df.columns:
            df[c] = None

    # guard: drop rows without symbol or close
    df = df.dropna(subset=["symbol"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df[df["close"].notna()].copy()

    df["name"] = df["name"].fillna("Unknown")
    df["sector"] = df["sector"].apply(_norm_sector)

    for col in ["open", "high", "low", "close", "last_close"]:
        df[col] = df[col].apply(_to_float)

    # ------------------------------------------------------------
    # ✅ Override TODAY last_close/high/close from DB truth (fix ret=0)
    # ------------------------------------------------------------
    ymd_effective = str(raw_payload.get("ymd_effective") or raw_payload.get("ymd") or "").strip()
    db_path = _pick_db_path(raw_payload)

    if ymd_effective and db_path and os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            try:
                syms = df["symbol"].dropna().astype(str).unique().tolist()
                today_map = _fetch_today_rows(conn, ymd=ymd_effective, symbols=syms)
            finally:
                conn.close()

            if today_map:
                def _pick(sym: str, key: str, fallback: float) -> float:
                    info = today_map.get(sym)
                    if not info:
                        return fallback
                    v = _to_float(info.get(key), fallback)
                    if key == "last_close":
                        return v
                    return v if v > 0 else fallback

                df["symbol"] = df["symbol"].astype(str)
                df["last_close"] = df.apply(
                    lambda r: _pick(r["symbol"], "last_close", _to_float(r.get("last_close"), 0.0)),
                    axis=1,
                )
                df["high"] = df.apply(
                    lambda r: _pick(r["symbol"], "high", _to_float(r.get("high"), 0.0)),
                    axis=1,
                )
                df["close"] = df.apply(
                    lambda r: _pick(r["symbol"], "close", _to_float(r.get("close"), 0.0)),
                    axis=1,
                )
        except Exception:
            pass

    # ✅ Compute ret + ret_high
    df = _add_intraday_ret_fields(df)

    # ------------------------------------------------------------
    # ✅ Penny mark (keep in snapshot_main; optionally exclude from stats/lists)
    # ------------------------------------------------------------
    close_px = pd.to_numeric(df.get("close"), errors="coerce").fillna(0.0).astype(float)
    df["is_penny"] = (close_px < float(TH_PENNY_PRICE_MAX)).astype(bool)

    if TH_DEBUG_PENNY:
        try:
            penny_cnt = int(df["is_penny"].sum())
            print("[TH_PENNY_DEBUG]")
            print("  TH_FILTER_PENNY =", bool(TH_FILTER_PENNY))
            print("  TH_PENNY_PRICE_MAX =", float(TH_PENNY_PRICE_MAX))
            print("  penny_count =", penny_cnt, "/", int(len(df)))
        except Exception:
            pass

    # df_calc = used for stats/lists (optionally exclude penny)
    if TH_FILTER_PENNY:
        df_calc = df[df["is_penny"] == False].copy()
    else:
        df_calc = df.copy()

    # ------------------------------------------------------------
    # Today: ceiling limit + touch/locked
    # ------------------------------------------------------------
    limit_price_list: List[Optional[float]] = []
    limit_pct_list: List[Optional[float]] = []
    is_touch: List[bool] = []
    is_locked: List[bool] = []

    for _, r in df_calc.iterrows():
        lc = float(r["last_close"] or 0.0)
        c = float(r["close"] or 0.0)
        h = float(r["high"] or 0.0)

        if lc > 0:
            lp = _limit_price(lc, TH_LIMIT_PCT)
            limit_price_list.append(lp)
            limit_pct_list.append(float(TH_LIMIT_PCT))

            touch = (h > 0) and (h >= lp - EPS)
            locked = (c > 0) and (c >= lp - EPS)

            is_touch.append(bool(touch))
            is_locked.append(bool(locked))
        else:
            limit_price_list.append(None)
            limit_pct_list.append(None)
            is_touch.append(False)
            is_locked.append(False)

    df_calc["limit_price"] = limit_price_list
    df_calc["th_limit_price"] = limit_price_list
    df_calc["th_limit_pct"] = limit_pct_list

    df_calc["is_limitup_touch"] = is_touch                 # touched ceiling intraday (includes locked)
    df_calc["is_limitup_locked"] = is_locked               # closed at ceiling (locked)
    df_calc["is_limitup_opened"] = df_calc["is_limitup_touch"] & (~df_calc["is_limitup_locked"])

    df_calc["is_limitup"] = df_calc["is_limitup_touch"]
    df_calc["is_true_limitup"] = df_calc["is_limitup_locked"]   # compatibility naming

    # ✅ Explicit independence:
    df_calc["is_touch_only"] = (df_calc["is_limitup_touch"] == True) & (df_calc["is_limitup_locked"] == False)
    df_calc["is_stop_high"] = (df_calc["is_limitup_locked"] == True)

    # ------------------------------------------------------------
    # Yesterday: prev touch/locked/opened (optional; requires DB)
    # ------------------------------------------------------------
    df_calc["prev_is_limitup_touch"] = False
    df_calc["prev_is_limitup_locked"] = False
    df_calc["prev_is_limitup_opened"] = False
    df_calc["prev_is_true_limitup"] = False

    ymd_prev: Optional[str] = None
    prev_map: Dict[str, Dict[str, float]] = {}

    if ymd_effective and db_path and os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            try:
                ymd_prev = _get_prev_trade_date(conn, ymd_effective)
                if ymd_prev:
                    syms = df_calc["symbol"].dropna().astype(str).unique().tolist()
                    prev_map = _fetch_prev_day_rows(conn, ymd_prev=ymd_prev, symbols=syms)
            finally:
                conn.close()
        except Exception:
            ymd_prev = None
            prev_map = {}

    if ymd_prev and prev_map:
        prev_touch: List[bool] = []
        prev_locked: List[bool] = []

        for _, r in df_calc.iterrows():
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
                lp_prev = _limit_price(lc_prev, TH_LIMIT_PCT)
                t = (h_prev > 0) and (h_prev >= lp_prev - EPS)
                l = (c_prev > 0) and (c_prev >= lp_prev - EPS)
                prev_touch.append(bool(t))
                prev_locked.append(bool(l))
            else:
                prev_touch.append(False)
                prev_locked.append(False)

        df_calc["prev_is_limitup_touch"] = prev_touch
        df_calc["prev_is_limitup_locked"] = prev_locked
        df_calc["prev_is_limitup_opened"] = df_calc["prev_is_limitup_touch"] & (~df_calc["prev_is_limitup_locked"])
        df_calc["prev_is_true_limitup"] = df_calc["prev_is_limitup_locked"]

    # ------------------------------------------------------------
    # Movers & bins
    # ------------------------------------------------------------
    # Inclusive concept: >=10% (close-based) — may overlap with touch/locked
    df_calc["is_surge_ge10"] = df_calc["ret"] >= float(TH_SURGE_RET)
    df_calc["is_surge_ge20"] = df_calc["ret"] >= float(TH_BIN20_RET)

    # Exclusive movers: exclude ANY touch (incl locked)  ✅ footer wants this
    df_calc["is_bigmove10_ex_locked"] = (df_calc["is_surge_ge10"] == True) & (df_calc["is_limitup_touch"] == False)

    # Optional bins (exclusive + addable style)
    df_calc["is_move10_20_ex_touch"] = (
        (df_calc["ret"] >= float(TH_SURGE_RET))
        & (df_calc["ret"] < float(TH_BIN20_RET))
        & (df_calc["is_limitup_touch"] == False)
    )
    df_calc["is_move20_30_ex_touch"] = (
        (df_calc["ret"] >= float(TH_BIN20_RET))
        & (df_calc["ret"] < float(TH_BIN30_RET))
        & (df_calc["is_limitup_touch"] == False)
    )

    # Display list: touch (incl locked) OR ret>=10 (inclusive)
    df_calc["is_display_limitup"] = (df_calc["is_limitup_touch"] == True) | (df_calc["is_surge_ge10"] == True)

    # ------------------------------------------------------------
    # ✅ Debug flags: touch but ret < 10% (helps validate overview page 3)
    # ------------------------------------------------------------
    df_calc["dbg_touch_and_ret_ge10"] = (df_calc["is_limitup_touch"] == True) & (df_calc["ret"] >= float(TH_SURGE_RET))
    df_calc["dbg_touch_but_ret_lt10"] = (df_calc["is_limitup_touch"] == True) & (df_calc["ret"] < float(TH_SURGE_RET))

    if TH_DEBUG_TOUCH_VS_10:
        try:
            a = int((df_calc["dbg_touch_and_ret_ge10"] == True).sum())
            b = int((df_calc["dbg_touch_but_ret_lt10"] == True).sum())
            print("[TH_TOUCH_VS_10_DEBUG]")
            print("  touch_and_ret_ge10 =", a)
            print("  touch_but_ret_lt10 =", b)
            # print first N offenders (touch but ret < 10)
            bad = df_calc[df_calc["dbg_touch_but_ret_lt10"] == True].copy()
            bad = bad.sort_values(["ret"], ascending=True, kind="mergesort")
            if not bad.empty:
                cols = ["symbol", "name", "sector", "last_close", "high", "close", "ret_pct", "ret_high_pct"]
                cols = [c for c in cols if c in bad.columns]
                print("  offenders (first %d):" % int(TH_DEBUG_TOUCH_VS_10_MAX))
                print(bad[cols].head(int(TH_DEBUG_TOUCH_VS_10_MAX)).to_string(index=False))
        except Exception:
            pass

    # ------------------------------------------------------------
    # Display labels for render (tag)
    # ------------------------------------------------------------
    def _tag_row(r) -> Tuple[str, Any]:
        if bool(r["is_true_limitup"]):
            return "ติดซิลลิ่ง", float(r.get("th_limit_pct") or TH_LIMIT_PCT)
        if bool(r["is_surge_ge10"]) and (not bool(r["is_limitup_touch"])):
            return th_surge_label(float(r["ret"])), None
        return "", None

    tags: List[str] = []
    limit_pct_show: List[Any] = []
    for _, r in df_calc.iterrows():
        t, p = _tag_row(r)
        tags.append(t)
        limit_pct_show.append(p)
    df_calc["tag"] = tags
    df_calc["limit_pct"] = limit_pct_show

    # ------------------------------------------------------------
    # Build display list (mix) for top section usage
    # ------------------------------------------------------------
    df_limit = df_calc[df_calc["is_display_limitup"]].copy()
    df_limit = df_limit.sort_values(["ret"], ascending=False, kind="mergesort")

    limitup_records: List[Dict[str, Any]] = []
    for _, r in df_limit.iterrows():
        rec = r.to_dict()
        rec["sector"] = _norm_sector(rec.get("sector"))
        rec = _force_ret_fields(rec)

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
            "is_surge_ge20",
            "is_touch_only",
            "is_stop_high",
            "is_bigmove10_ex_locked",
            "is_move10_20_ex_touch",
            "is_move20_30_ex_touch",
            "is_display_limitup",
            # debug flags
            "dbg_touch_and_ret_ge10",
            "dbg_touch_but_ret_lt10",
        ]:
            if k in rec:
                rec[k] = bool(rec.get(k))

        # penny flag (useful for debugging even though excluded)
        rec["is_penny"] = bool(rec.get("is_penny", False))

        limitup_records.append(_sanitize_nan(rec))

    # ------------------------------------------------------------
    # Peers (not display_limitup, and ret < 10%)
    # ------------------------------------------------------------
    df_peers = df_calc[~df_calc["is_display_limitup"]].copy()
    df_peers = df_peers[df_peers["ret"] < float(TH_SURGE_RET)]
    df_peers = df_peers.sort_values(["ret"], ascending=False, kind="mergesort")

    peers_by_sector: Dict[str, List[Dict[str, Any]]] = {}
    peers_flat: List[Dict[str, Any]] = []
    for sec, g in df_peers.groupby("sector", sort=False):
        g2 = g.head(int(PEERS_BY_SECTOR_CAP)).copy()
        recs_raw = g2.to_dict(orient="records")

        recs: List[Dict[str, Any]] = []
        for rr in recs_raw:
            rr = _force_ret_fields(rr)
            rr["is_penny"] = bool(rr.get("is_penny", False))
            recs.append(_sanitize_nan(rr))

        peers_by_sector[sec] = recs
        peers_flat.extend(recs)

    # ------------------------------------------------------------
    # sector_summary (for overview + pages)
    # ------------------------------------------------------------
    summary_rows: List[Dict[str, Any]] = []
    for sec, g in df_calc.groupby("sector", sort=False):
        sector_total = int(len(g))

        locked_cnt = int((g["is_limitup_locked"] == True).sum())
        touch_total_cnt = int((g["is_limitup_touch"] == True).sum())  # includes locked
        touch_only_cnt = int(((g["is_limitup_touch"] == True) & (g["is_limitup_locked"] == False)).sum())
        opened_cnt = int((g["is_limitup_opened"] == True).sum())

        surge10_total_cnt = int((g["is_surge_ge10"] == True).sum())  # inclusive
        surge20_total_cnt = int((g["is_surge_ge20"] == True).sum())  # inclusive

        # ✅ exclusive (exclude any touch/locked)
        big10_ex_locked_cnt = int((g["is_bigmove10_ex_locked"] == True).sum())

        move10_20_cnt = int((g["is_move10_20_ex_touch"] == True).sum())
        move20_30_cnt = int((g["is_move20_30_ex_touch"] == True).sum())

        display_cnt = int((g["is_display_limitup"] == True).sum())
        peer_cnt = int(len(df_peers[df_peers["sector"] == sec]))

        denom = float(sector_total) if sector_total > 0 else 0.0
        locked_pct = (locked_cnt / denom) if denom else None
        touched_pct = (touch_only_cnt / denom) if denom else None
        bigmove10_pct = (big10_ex_locked_cnt / denom) if denom else None
        mix_pct = (display_cnt / denom) if denom else None

        summary_rows.append(
            _sanitize_nan(
                {
                    "sector": sec,

                    "display_limitup_count": display_cnt,
                    "true_limitup_count": locked_cnt,

                    "limitup_touch_count": touch_total_cnt,
                    "limitup_touch_only_count": touch_only_cnt,
                    "limitup_opened_count": opened_cnt,

                    "surge_ge10_total_count": surge10_total_cnt,
                    "surge_ge20_total_count": surge20_total_cnt,

                    # ✅ exclusive movers for overview/footer consistency
                    "surge_ge10_ex_locked_count": big10_ex_locked_cnt,

                    "move10_20_count": move10_20_cnt,
                    "move20_30_count": move20_30_cnt,

                    "peers_count": peer_cnt,

                    # legacy fields
                    "locked_cnt": locked_cnt,
                    "touched_cnt": touch_only_cnt,
                    "bigmove10_cnt": big10_ex_locked_cnt,

                    "sector_total": sector_total,
                    "locked_pct": locked_pct,
                    "touched_pct": touched_pct,
                    "bigmove10_pct": bigmove10_pct,
                    "mix_pct": mix_pct,
                }
            )
        )

    summary_rows.sort(
        key=lambda x: (
            x.get("locked_cnt", 0),
            x.get("touched_cnt", 0),
            x.get("move20_30_count", 0),
            x.get("move10_20_count", 0),
            x.get("bigmove10_cnt", 0),
        ),
        reverse=True,
    )

    # ------------------------------------------------------------
    # stats (market-level)
    # ------------------------------------------------------------
    stats = raw_payload.get("stats", {}) or {}

    stats["limitup_count"] = int(len(limitup_records))
    stats["th_display_limitup_count"] = int((df_calc["is_display_limitup"] == True).sum())

    # independent limit family
    stats["th_true_limitup_count"] = int((df_calc["is_stop_high"] == True).sum())               # locked only
    stats["th_limitup_touch_count"] = int((df_calc["is_limitup_touch"] == True).sum())         # touch total (incl locked)
    stats["th_limitup_touch_only_count"] = int((df_calc["is_touch_only"] == True).sum())       # touch-only
    stats["th_limitup_opened_count"] = int((df_calc["is_limitup_opened"] == True).sum())

    # inclusive movers (may overlap with touch/locked)
    stats["th_surge_ge10_total_count"] = int((df_calc["is_surge_ge10"] == True).sum())
    stats["th_surge_ge20_total_count"] = int((df_calc["is_surge_ge20"] == True).sum())

    # ✅ exclusive 10%+ (exclude touch/locked) — footer uses this
    stats["th_bigmove10_ex_locked_count"] = int((df_calc["is_bigmove10_ex_locked"] == True).sum())

    # bins (exclusive, addable style)
    stats["th_move10_20_ex_touch_count"] = int((df_calc["is_move10_20_ex_touch"] == True).sum())
    stats["th_move20_30_ex_touch_count"] = int((df_calc["is_move20_30_ex_touch"] == True).sum())

    # prev day
    stats["th_prev_trade_date"] = ymd_prev
    stats["th_prev_true_limitup_count"] = int((df_calc["prev_is_limitup_locked"] == True).sum())
    stats["th_prev_limitup_touch_count"] = int((df_calc["prev_is_limitup_touch"] == True).sum())
    stats["th_prev_limitup_opened_count"] = int((df_calc["prev_is_limitup_opened"] == True).sum())

    # penny debug totals (even when filtered)
    stats["th_penny_count"] = int(df["is_penny"].sum()) if "is_penny" in df.columns else 0
    stats["th_calc_universe"] = int(len(df_calc))

    # touch-vs-10 debug totals
    stats["th_dbg_touch_and_ret_ge10_count"] = int((df_calc["dbg_touch_and_ret_ge10"] == True).sum())
    stats["th_dbg_touch_but_ret_lt10_count"] = int((df_calc["dbg_touch_but_ret_lt10"] == True).sum())

    raw_payload["stats"] = _sanitize_nan(stats)

    # ------------------------------------------------------------
    # meta.totals (STRICT addable / mutually-exclusive)
    # ------------------------------------------------------------
    raw_payload.setdefault("meta", {})
    meta = raw_payload.get("meta") or {}
    if not isinstance(meta, dict):
        meta = {}
        raw_payload["meta"] = meta

    totals = meta.get("totals")
    if not isinstance(totals, dict):
        totals = {}
        meta["totals"] = totals

    locked_total = int(raw_payload["stats"].get("th_true_limitup_count") or 0)
    touched_total = int(raw_payload["stats"].get("th_limitup_touch_only_count") or 0)

    # ✅ REQUIRED BY footer.py:
    bigmove10_ex_locked_total = int(raw_payload["stats"].get("th_bigmove10_ex_locked_count") or 0)

    # bins (optional)
    move20_30_total = int(raw_payload["stats"].get("th_move20_30_ex_touch_count") or 0)
    move10_20_total = int(raw_payload["stats"].get("th_move10_20_ex_touch_count") or 0)

    # mix_total should align with your overview metric "mix"
    mix_total = int(locked_total + touched_total + bigmove10_ex_locked_total)

    totals["locked_total"] = locked_total
    totals["touched_total"] = touched_total

    # ✅ footer expects either bigmove10_ex_locked_total OR bigmove10_ex_limitup_total
    totals["bigmove10_ex_locked_total"] = bigmove10_ex_locked_total

    # keep bins if you want (doesn't hurt; useful debug)
    totals["move20_30_total"] = move20_30_total
    totals["move10_20_total"] = move10_20_total

    totals["mix_total"] = mix_total

    # ------------------------------------------------------------
    # meta.metrics (inclusive concept counters; can overlap with totals)
    # ------------------------------------------------------------
    metrics = meta.get("metrics")
    if not isinstance(metrics, dict):
        metrics = {}
        meta["metrics"] = metrics

    # inclusive
    metrics["bigmove10_total"] = int(raw_payload["stats"].get("th_surge_ge10_total_count") or 0)
    metrics["bigmove20_total"] = int(raw_payload["stats"].get("th_surge_ge20_total_count") or 0)

    # exclusive (touch excluded) — kept for debug/analysis
    metrics["bigmove10_ex_locked_total"] = bigmove10_ex_locked_total

    # ------------------------------------------------------------
    # optional debug print
    # ------------------------------------------------------------
    if _env_on("TH_DEBUG_TOTALS"):
        print("[TH_TOTALS_DEBUG]")
        print("  TH_FILTER_PENNY =", bool(TH_FILTER_PENNY))
        print("  TH_PENNY_PRICE_MAX =", float(TH_PENNY_PRICE_MAX))
        print("  penny_count =", int(df["is_penny"].sum()) if "is_penny" in df.columns else 0)
        print("  calc_universe =", int(len(df_calc)))
        print("  locked_total =", locked_total)
        print("  touched_total =", touched_total)
        print("  bigmove10_ex_locked_total =", bigmove10_ex_locked_total)
        print("  move10_20_total =", move10_20_total)
        print("  move20_30_total =", move20_30_total)
        print("  mix_total =", mix_total)
        print("  metrics.bigmove10_total =", metrics.get("bigmove10_total"))
        print("  metrics.bigmove20_total =", metrics.get("bigmove20_total"))
        print("  dbg.touch_and_ret_ge10 =", raw_payload["stats"].get("th_dbg_touch_and_ret_ge10_count"))
        print("  dbg.touch_but_ret_lt10 =", raw_payload["stats"].get("th_dbg_touch_but_ret_lt10_count"))

    meta["totals"] = _sanitize_nan(totals)
    meta["metrics"] = _sanitize_nan(metrics)
    raw_payload["meta"] = _sanitize_nan(meta)

    # ------------------------------------------------------------
    # attach outputs + overwrite snapshot_main with enriched rows
    # snapshot_main keeps ALL stocks (including penny) with is_penny flag
    # ------------------------------------------------------------
    df_safe = df.where(pd.notna(df), None)

    for c in ["ret", "ret_pct", "ret_high", "ret_high_pct", "is_penny"]:
        if c not in df_safe.columns:
            df_safe[c] = 0.0 if c != "is_penny" else False

    raw_payload["snapshot_main"] = _sanitize_nan(df_safe.to_dict(orient="records"))
    raw_payload["limitup"] = _sanitize_nan(limitup_records)
    raw_payload["sector_summary"] = _sanitize_nan(summary_rows)
    raw_payload["peers_by_sector"] = _sanitize_nan(peers_by_sector)
    raw_payload["peers_not_limitup"] = _sanitize_nan(peers_flat)

    raw_payload.setdefault("filters", {})
    raw_payload["filters"]["th_limit_pct"] = float(TH_LIMIT_PCT)
    raw_payload["filters"]["th_surge_ret_threshold"] = float(TH_SURGE_RET)
    raw_payload["filters"]["th_bin20_ret"] = float(TH_BIN20_RET)
    raw_payload["filters"]["th_bin30_ret"] = float(TH_BIN30_RET)
    raw_payload["filters"]["th_prev_trade_date"] = ymd_prev

    # penny filter settings (so you can see in payload)
    raw_payload["filters"]["th_filter_penny"] = bool(TH_FILTER_PENNY)
    raw_payload["filters"]["th_penny_price_max"] = float(TH_PENNY_PRICE_MAX)

    raw_payload["filters"] = _sanitize_nan(raw_payload["filters"])

    return raw_payload