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
# NOTE:
# use 0.01 as safer default for circuit rounding.
INDIA_TICK_SIZE = float(os.getenv("INDIA_TICK_SIZE", "0.01"))
INDIA_PENNY_PRICE_MAX = float(os.getenv("INDIA_PENNY_PRICE_MAX", "20.0"))

INDIA_FILTER_TICK_DANGER = (
    os.getenv("INDIA_FILTER_TICK_DANGER", "1").strip().lower() in {"1", "true", "yes", "y", "on"}
)
INDIA_TICK_DANGER_MAX_TICKS = float(os.getenv("INDIA_TICK_DANGER_MAX_TICKS", "3"))

INDIA_ABS_MOVE_GATE = float(os.getenv("INDIA_ABS_MOVE_GATE", "0.0"))

PEERS_BY_SECTOR_CAP = int(os.getenv("INDIA_PEERS_BY_SECTOR_CAP", "50"))


# =============================================================================
# Helpers
# =============================================================================
def _is_valid_num(x: Any) -> bool:
    try:
        v = float(x)
        return not (math.isnan(v) or math.isinf(v))
    except Exception:
        return False


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
        if math.isnan(v) or math.isinf(v) or v <= 0:
            return None
        return v / 100.0
    except Exception:
        return None


def _round_to_tick(x: float, tick: float) -> Optional[float]:
    if not _is_valid_num(x):
        return None
    if not _is_valid_num(tick):
        return float(x)

    tick_f = float(tick)
    if tick_f <= 0:
        return float(x)

    return round(round(float(x) / tick_f) * tick_f, 6)


def _limit_price(last_close: float, limit_pct: float) -> Optional[float]:
    if not _is_valid_num(last_close) or not _is_valid_num(limit_pct):
        return None

    lc = float(last_close)
    lpct = float(limit_pct)
    if lc <= 0 or lpct <= 0:
        return None

    raw = lc * (1.0 + lpct)
    return _round_to_tick(raw, INDIA_TICK_SIZE)


def _add_ret_fields(df: pd.DataFrame) -> pd.DataFrame:
    c = pd.to_numeric(df.get("close"), errors="coerce").fillna(0.0)
    h = pd.to_numeric(df.get("high"), errors="coerce").fillna(0.0)
    lc = pd.to_numeric(df.get("last_close"), errors="coerce").fillna(0.0)
    can = lc > 0

    ret_close = pd.Series(0.0, index=df.index, dtype="float64")
    ret_high = pd.Series(0.0, index=df.index, dtype="float64")

    ret_close.loc[can] = (c.loc[can] / lc.loc[can]) - 1.0
    ret_high.loc[can] = (h.loc[can] / lc.loc[can]) - 1.0

    df["ret"] = ret_close.astype(float)
    df["ret_pct"] = (df["ret"] * 100.0).astype(float)
    df["ret_high"] = ret_high.astype(float)
    df["ret_high_pct"] = (df["ret_high"] * 100.0).astype(float)
    return df


def _normalize_status(x: Any) -> str:
    s = ("" if x is None else str(x)).strip().lower()
    if s in {"hit", "locked", "limit_hit"}:
        return "hit"
    if s in {"touch", "touched", "opened", "bomb"}:
        return "touch"
    if s in {"big", "big10", "big10+", "surge"}:
        return "big"
    return ""


def _status_from_row(
    *,
    close: float,
    high: float,
    last_close: float,
    band_pct: Optional[float],
    ret: float,
) -> Dict[str, Any]:
    close_f = _to_float(close, 0.0)
    high_f = _to_float(high, 0.0)
    last_close_f = _to_float(last_close, 0.0)
    ret_f = _to_float(ret, 0.0)

    limit_price = None
    is_touch = False
    is_locked = False

    if band_pct is not None and _is_valid_num(band_pct) and last_close_f > 0:
        lp = _limit_price(last_close_f, float(band_pct))
        if lp is not None and _is_valid_num(lp):
            limit_price = float(lp)
            is_touch = bool((high_f > 0) and (high_f >= limit_price - EPS))
            is_locked = bool((close_f > 0) and (close_f >= limit_price - EPS))

    is_opened = bool(is_touch and not is_locked)
    is_surge_ge10 = bool(ret_f >= float(INDIA_SURGE_RET))

    if is_locked:
        today_status = "hit"
    elif is_touch:
        today_status = "touch"
    elif is_surge_ge10:
        today_status = "big"
    else:
        today_status = ""

    return {
        "limit_price": limit_price,
        "is_limitup_touch": is_touch,
        "is_limitup_locked": is_locked,
        "is_limitup_opened": is_opened,
        "is_surge_ge10": is_surge_ge10,
        "today_status": today_status,
        "limitup_status": today_status,
        "is_display_limitup": bool(today_status),
        "is_bigmove10_ex_locked": bool(today_status == "big"),
    }


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

    for c in [
        "symbol", "name", "sector", "open", "high", "low", "close",
        "last_close", "market_detail", "today_status", "prev_status",
        "streak_today", "streak_prev", "prev_limitup_status", "band_pct"
    ]:
        if c not in df.columns:
            df[c] = None

    df = df.dropna(subset=["symbol"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df[df["close"].notna()].copy()

    df["name"] = df["name"].fillna("Unknown")
    df["sector"] = df["sector"].apply(_norm_sector)

    for col in ["open", "high", "low", "close", "last_close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = _add_ret_fields(df)

    # -----------------------------------------------------------------------------
    # band_pct: avoid assigning None array into float64 column via masked loc
    # -----------------------------------------------------------------------------
    band_existing = pd.to_numeric(df["band_pct"], errors="coerce")
    band_from_detail = df["market_detail"].apply(_parse_band_pct_from_market_detail)
    band_from_detail = pd.to_numeric(band_from_detail, errors="coerce")
    df["band_pct"] = band_existing.where(band_existing.notna(), band_from_detail)

    df["limit_rate"] = df["band_pct"]
    df["limit_rate_pct"] = pd.to_numeric(df["limit_rate"], errors="coerce") * 100.0

    # preserve previous snapshot-provided prev/today status if present, but recompute today_status
    df["prev_status"] = df["prev_status"].apply(_normalize_status)
    df["prev_limitup_status"] = df["prev_limitup_status"].apply(_normalize_status)
    df.loc[df["prev_limitup_status"] == "", "prev_limitup_status"] = df.loc[
        df["prev_limitup_status"] == "", "prev_status"
    ]

    df["streak_today"] = pd.to_numeric(df["streak_today"], errors="coerce").fillna(0).astype(int)
    df["streak_prev"] = pd.to_numeric(df["streak_prev"], errors="coerce").fillna(0).astype(int)

    # recompute today's band-aware status
    calc_rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        calc_rows.append(
            _status_from_row(
                close=_to_float(r.get("close"), 0.0),
                high=_to_float(r.get("high"), 0.0),
                last_close=_to_float(r.get("last_close"), 0.0),
                band_pct=(None if pd.isna(r.get("band_pct")) else r.get("band_pct")),
                ret=_to_float(r.get("ret"), 0.0),
            )
        )

    dcalc = pd.DataFrame(calc_rows, index=df.index)
    for c in dcalc.columns:
        df[c] = dcalc[c]

    df["today_status"] = df["today_status"].apply(_normalize_status)
    df["limitup_status"] = df["limitup_status"].apply(_normalize_status)
    df["prev_status"] = df["prev_status"].apply(_normalize_status)
    df["prev_limitup_status"] = df["prev_limitup_status"].apply(_normalize_status)

    # penny / tick danger
    prev_close = pd.to_numeric(df["last_close"], errors="coerce").fillna(0.0).astype(float)
    df["is_penny_20inr"] = (prev_close > 0) & (prev_close < float(INDIA_PENNY_PRICE_MAX))
    df["ticks_needed_for_10pct"] = None
    can = prev_close > 0
    df.loc[can, "ticks_needed_for_10pct"] = (prev_close.loc[can] * 0.10) / float(INDIA_TICK_SIZE)

    df["is_tick_danger"] = False
    if INDIA_FILTER_TICK_DANGER:
        df["is_tick_danger"] = (
            pd.to_numeric(df["ticks_needed_for_10pct"], errors="coerce").fillna(1e9)
            <= float(INDIA_TICK_DANGER_MAX_TICKS)
        )

    # abs-move
    df["abs_move"] = (
        pd.to_numeric(df["close"], errors="coerce").fillna(0.0)
        - pd.to_numeric(df["last_close"], errors="coerce").fillna(0.0)
    ).abs().astype(float)

    # optional abs-move gate only for BIG 10%+, not for true touch/hit
    if float(INDIA_ABS_MOVE_GATE) > 0:
        gate_mask = df["today_status"] == "big"
        keep_big = gate_mask & (df["abs_move"] >= float(INDIA_ABS_MOVE_GATE))
        df.loc[gate_mask & (~keep_big), "today_status"] = ""
        df.loc[gate_mask & (~keep_big), "limitup_status"] = ""
        df.loc[gate_mask & (~keep_big), "is_display_limitup"] = False
        df.loc[gate_mask & (~keep_big), "is_bigmove10_ex_locked"] = False
        df.loc[gate_mask & (~keep_big), "is_surge_ge10"] = False

    # tick-danger filter only removes BIG10 display, not touch/hit
    if INDIA_FILTER_TICK_DANGER:
        danger_mask = (df["is_tick_danger"] == True) & (df["today_status"] == "big")
        df.loc[danger_mask, "today_status"] = ""
        df.loc[danger_mask, "limitup_status"] = ""
        df.loc[danger_mask, "is_display_limitup"] = False
        df.loc[danger_mask, "is_bigmove10_ex_locked"] = False
        df.loc[danger_mask, "is_surge_ge10"] = False

    # final display logic
    df["is_display_limitup"] = df["today_status"].apply(lambda x: bool(_normalize_status(x)))
    df["limitup_status"] = df["today_status"]
    df["is_bigmove10_ex_locked"] = df["today_status"] == "big"
    df["is_surge_ge10"] = df["ret"] >= float(INDIA_SURGE_RET)

    # build limit list
    df_limit = df[df["is_display_limitup"]].copy()
    df_limit = df_limit.sort_values(["ret"], ascending=False, kind="mergesort")
    limitup_records = [_sanitize_nan(r) for r in df_limit.to_dict(orient="records")]

    # peers
    df_peers = df[~df["is_display_limitup"]].copy()
    df_peers = df_peers[df_peers["ret"] < float(INDIA_SURGE_RET)]
    df_peers = df_peers.sort_values(["ret"], ascending=False, kind="mergesort")

    peers_by_sector: Dict[str, List[Dict[str, Any]]] = {}
    peers_flat: List[Dict[str, Any]] = []
    for sec, g in df_peers.groupby("sector", sort=False):
        g2 = g.head(int(PEERS_BY_SECTOR_CAP)).copy()
        recs = [_sanitize_nan(x) for x in g2.to_dict(orient="records")]
        peers_by_sector[sec] = recs
        peers_flat.extend(recs)

    # sector summary
    summary_rows: List[Dict[str, Any]] = []
    for sec, g in df.groupby("sector", sort=False):
        sector_total = int(len(g))
        locked_cnt = int((g["today_status"] == "hit").sum())
        touch_only_cnt = int((g["today_status"] == "touch").sum())
        big10_ex = int((g["today_status"] == "big").sum())
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

    # stats/meta
    raw_payload.setdefault("stats", {})
    stats = raw_payload["stats"] or {}

    stats["india_display_limitup_count"] = int((df["is_display_limitup"] == True).sum())
    stats["india_true_limitup_count"] = int((df["today_status"] == "hit").sum())
    stats["india_limitup_touch_only_count"] = int((df["today_status"] == "touch").sum())
    stats["india_bigmove10_ex_locked_count"] = int((df["today_status"] == "big").sum())
    stats["india_surge_ge10_total_count"] = int((df["ret"] >= float(INDIA_SURGE_RET)).sum())

    stats["india_penny_20inr_count"] = int((df["is_penny_20inr"] == True).sum())
    stats["india_tick_danger_count"] = int((df["is_tick_danger"] == True).sum())
    stats["india_calc_universe"] = int(len(df))

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

    raw_payload["snapshot_main"] = _sanitize_nan(df.to_dict(orient="records"))
    raw_payload["limitup"] = _sanitize_nan(limitup_records)
    raw_payload["sector_summary"] = _sanitize_nan(summary_rows)
    raw_payload["peers_by_sector"] = _sanitize_nan(peers_by_sector)
    raw_payload["peers_not_limitup"] = _sanitize_nan(peers_flat)

    return raw_payload
