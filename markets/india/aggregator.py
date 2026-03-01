# markets/india/aggregator.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import math
import os
from typing import Any, Dict, List, Optional

import pandas as pd

EPS = 1e-6


# =============================================================================
# Env knobs (India)
# =============================================================================
INDIA_SURGE_RET = float(os.getenv("INDIA_SURGE_RET", "0.10"))  # >=10%

# Tick / penny knobs
INDIA_TICK_SIZE = float(os.getenv("INDIA_TICK_SIZE", "0.05"))  # NSE typical
INDIA_PENNY_PRICE_MAX = float(os.getenv("INDIA_PENNY_PRICE_MAX", "20.0"))  # informational

INDIA_FILTER_TICK_DANGER = (
    os.getenv("INDIA_FILTER_TICK_DANGER", "1").strip().lower() in {"1", "true", "yes", "y", "on"}
)
INDIA_TICK_DANGER_MAX_TICKS = float(os.getenv("INDIA_TICK_DANGER_MAX_TICKS", "3"))  # <=3 ticks for +10%

# Optional abs-move gate for movers (INR)
INDIA_ABS_MOVE_GATE = float(os.getenv("INDIA_ABS_MOVE_GATE", "0.0"))  # 0/0.5/1/2 ...

PEERS_BY_SECTOR_CAP = int(os.getenv("INDIA_PEERS_BY_SECTOR_CAP", "50"))


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


def _sanitize_nan(obj: Any) -> Any:
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


def _parse_band_pct_from_market_detail(md: Any) -> Optional[float]:
    """
    market_detail example:
      "NSE|band=20|remarks=-|src=master_csv"
      "NSE|band=No Band|remarks=-|src=master_csv"
    Return ratio (0.20), not percent.
    """
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


def _limit_price(last_close: float, limit_pct: float) -> float:
    return float(last_close) * (1.0 + float(limit_pct))


def _add_ret_fields(df: pd.DataFrame) -> pd.DataFrame:
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


# =============================================================================
# Main
# =============================================================================
def aggregate(raw_payload: Dict[str, Any]) -> Dict[str, Any]:
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

    for c in ["symbol", "name", "sector", "open", "high", "low", "close", "last_close", "market_detail"]:
        if c not in df.columns:
            df[c] = None

    df = df.dropna(subset=["symbol"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df[df["close"].notna()].copy()

    df["name"] = df["name"].fillna("Unknown")
    df["sector"] = df["sector"].apply(_norm_sector)

    for col in ["open", "high", "low", "close", "last_close"]:
        df[col] = df[col].apply(_to_float)

    # ret fields
    df = _add_ret_fields(df)

    # parse band pct per symbol (ratio: 0.20)
    df["band_pct"] = df["market_detail"].apply(_parse_band_pct_from_market_detail)
    df["limit_price"] = None

    # ✅ NEW: unify with CN payload field name
    # CN renderer uses limit_rate (ratio). India band_pct is same meaning.
    df["limit_rate"] = df["band_pct"]
    df["limit_rate_pct"] = None
    m_lr = df["limit_rate"].notna()
    df.loc[m_lr, "limit_rate_pct"] = (pd.to_numeric(df.loc[m_lr, "limit_rate"], errors="coerce") * 100.0)

    # compute limit touch/locked (only when band_pct exists & last_close>0)
    is_touch: List[bool] = []
    is_locked: List[bool] = []
    for _, r in df.iterrows():
        lc = float(r["last_close"] or 0.0)
        h = float(r["high"] or 0.0)
        c = float(r["close"] or 0.0)
        bp = r.get("band_pct", None)

        if (bp is not None) and (lc > 0):
            lp = _limit_price(lc, float(bp))
            touch = (h > 0) and (h >= lp - EPS)
            locked = (c > 0) and (c >= lp - EPS)
            is_touch.append(bool(touch))
            is_locked.append(bool(locked))
        else:
            is_touch.append(False)
            is_locked.append(False)

    df["is_limitup_touch"] = is_touch
    df["is_limitup_locked"] = is_locked
    df["is_limitup_opened"] = df["is_limitup_touch"] & (~df["is_limitup_locked"])

    # penny / tick danger
    prev_close = pd.to_numeric(df["last_close"], errors="coerce").fillna(0.0).astype(float)
    df["is_penny_20inr"] = (prev_close > 0) & (prev_close < float(INDIA_PENNY_PRICE_MAX))

    # ticks needed for +10% ≈ prev_close*0.10 / tick
    df["ticks_needed_for_10pct"] = None
    can = prev_close > 0
    df.loc[can, "ticks_needed_for_10pct"] = (prev_close.loc[can] * 0.10) / float(INDIA_TICK_SIZE)

    df["is_tick_danger"] = False
    if INDIA_FILTER_TICK_DANGER:
        df["is_tick_danger"] = (
            pd.to_numeric(df["ticks_needed_for_10pct"], errors="coerce").fillna(1e9) <= float(INDIA_TICK_DANGER_MAX_TICKS)
        )

    # df_calc: exclude tick danger (default)
    df_calc = df[~df["is_tick_danger"]].copy() if INDIA_FILTER_TICK_DANGER else df.copy()

    # movers
    df_calc["is_surge_ge10"] = df_calc["ret"] >= float(INDIA_SURGE_RET)

    # optional abs-move gate for movers
    abs_move = (
        pd.to_numeric(df_calc["close"], errors="coerce").fillna(0.0)
        - pd.to_numeric(df_calc["last_close"], errors="coerce").fillna(0.0)
    ).abs()
    df_calc["abs_move"] = abs_move.astype(float)
    if float(INDIA_ABS_MOVE_GATE) > 0:
        df_calc["is_surge_ge10"] = df_calc["is_surge_ge10"] & (df_calc["abs_move"] >= float(INDIA_ABS_MOVE_GATE))

    # exclusive movers for footer: exclude touch/locked
    df_calc["is_bigmove10_ex_locked"] = (df_calc["is_surge_ge10"] == True) & (df_calc["is_limitup_touch"] == False)

    # display list = touch OR 10%+
    df_calc["is_display_limitup"] = (df_calc["is_limitup_touch"] == True) | (df_calc["is_surge_ge10"] == True)

    # build limit list
    df_limit = df_calc[df_calc["is_display_limitup"]].copy()
    df_limit = df_limit.sort_values(["ret"], ascending=False, kind="mergesort")
    limitup_records = [_sanitize_nan(r) for r in df_limit.to_dict(orient="records")]

    # peers
    df_peers = df_calc[~df_calc["is_display_limitup"]].copy()
    df_peers = df_peers[df_peers["ret"] < float(INDIA_SURGE_RET)]
    df_peers = df_peers.sort_values(["ret"], ascending=False, kind="mergesort")

    peers_by_sector: Dict[str, List[Dict[str, Any]]] = {}
    peers_flat: List[Dict[str, Any]] = []
    for sec, g in df_peers.groupby("sector", sort=False):
        g2 = g.head(int(PEERS_BY_SECTOR_CAP)).copy()
        recs = [_sanitize_nan(x) for x in g2.to_dict(orient="records")]
        peers_by_sector[sec] = recs
        peers_flat.extend(recs)

    # sector_summary (compact)
    summary_rows: List[Dict[str, Any]] = []
    for sec, g in df_calc.groupby("sector", sort=False):
        sector_total = int(len(g))
        locked_cnt = int((g["is_limitup_locked"] == True).sum())
        touch_only_cnt = int(((g["is_limitup_touch"] == True) & (g["is_limitup_locked"] == False)).sum())
        big10_ex = int((g["is_bigmove10_ex_locked"] == True).sum())
        display_cnt = int((g["is_display_limitup"] == True).sum())

        denom = float(sector_total) if sector_total > 0 else 0.0
        summary_rows.append(
            _sanitize_nan(
                {
                    "sector": sec,
                    "locked_cnt": locked_cnt,
                    "touched_cnt": touch_only_cnt,
                    "bigmove10_cnt": big10_ex,
                    "display_limitup_count": display_cnt,
                    "sector_total": sector_total,
                    "locked_pct": (locked_cnt / denom) if denom else None,
                    "touched_pct": (touch_only_cnt / denom) if denom else None,
                    "bigmove10_pct": (big10_ex / denom) if denom else None,
                    "mix_pct": (display_cnt / denom) if denom else None,
                }
            )
        )

    # stats/meta.totals
    raw_payload.setdefault("stats", {})
    stats = raw_payload["stats"] or {}

    stats["india_display_limitup_count"] = int((df_calc["is_display_limitup"] == True).sum())
    stats["india_true_limitup_count"] = int((df_calc["is_limitup_locked"] == True).sum())
    stats["india_limitup_touch_only_count"] = int(((df_calc["is_limitup_touch"] == True) & (df_calc["is_limitup_locked"] == False)).sum())
    stats["india_bigmove10_ex_locked_count"] = int((df_calc["is_bigmove10_ex_locked"] == True).sum())
    stats["india_surge_ge10_total_count"] = int((df_calc["is_surge_ge10"] == True).sum())

    # penny/tick debug
    stats["india_penny_20inr_count"] = int((df["is_penny_20inr"] == True).sum())
    stats["india_tick_danger_count"] = int((df["is_tick_danger"] == True).sum())
    stats["india_calc_universe"] = int(len(df_calc))

    raw_payload["stats"] = _sanitize_nan(stats)

    raw_payload.setdefault("meta", {})
    meta = raw_payload["meta"] or {}
    meta.setdefault("totals", {})
    meta.setdefault("metrics", {})

    locked_total = int(stats.get("india_true_limitup_count") or 0)
    touched_total = int(stats.get("india_limitup_touch_only_count") or 0)
    bigmove10_ex_locked_total = int(stats.get("india_bigmove10_ex_locked_count") or 0)

    meta["totals"]["locked_total"] = locked_total
    meta["totals"]["touched_total"] = touched_total
    meta["totals"]["bigmove10_ex_locked_total"] = bigmove10_ex_locked_total
    meta["totals"]["mix_total"] = int(locked_total + touched_total + bigmove10_ex_locked_total)

    meta["metrics"]["bigmove10_total"] = int(stats.get("india_surge_ge10_total_count") or 0)
    meta["metrics"]["bigmove10_ex_locked_total"] = bigmove10_ex_locked_total

    meta["filters"] = _sanitize_nan(
        {
            "india_surge_ret_threshold": float(INDIA_SURGE_RET),
            "india_tick_size": float(INDIA_TICK_SIZE),
            "india_filter_tick_danger": bool(INDIA_FILTER_TICK_DANGER),
            "india_tick_danger_max_ticks": float(INDIA_TICK_DANGER_MAX_TICKS),
            "india_abs_move_gate": float(INDIA_ABS_MOVE_GATE),
            "india_penny_price_max": float(INDIA_PENNY_PRICE_MAX),
        }
    )
    raw_payload["meta"] = _sanitize_nan(meta)

    # attach outputs
    raw_payload["snapshot_main"] = _sanitize_nan(df.to_dict(orient="records"))  # include band_pct/limit_rate/tick flags
    raw_payload["limitup"] = _sanitize_nan(limitup_records)
    raw_payload["sector_summary"] = _sanitize_nan(summary_rows)
    raw_payload["peers_by_sector"] = _sanitize_nan(peers_by_sector)
    raw_payload["peers_not_limitup"] = _sanitize_nan(peers_flat)

    return raw_payload
