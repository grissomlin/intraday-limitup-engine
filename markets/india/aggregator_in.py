# markets/india/aggregator_in.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

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


def _is_blank_sector(x: Any) -> bool:
    s = (str(x).strip() if x is not None else "")
    return (not s) or (s in ("—", "-", "--", "－", "–", "nan", "None"))


def _norm_sector(x: Any) -> str:
    if _is_blank_sector(x):
        return "Unclassified"
    return str(x).strip()


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


def _force_ret_fields(rec: Dict[str, Any]) -> Dict[str, Any]:
    r = _to_float(rec.get("ret"), 0.0)
    rec["ret"] = float(r)
    rec["ret_pct"] = float(r * 100.0)

    rh = _to_float(rec.get("ret_high"), 0.0)
    rec["ret_high"] = float(rh)
    rec["ret_high_pct"] = float(rh * 100.0)
    return rec


def _add_ret_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure:
      - ret      : close/prev_close - 1
      - ret_high : high/prev_close - 1
    """
    if df is None or df.empty:
        for c in ["ret", "ret_pct", "ret_high", "ret_high_pct"]:
            if c not in df.columns:
                df[c] = 0.0
        return df

    c = pd.to_numeric(df.get("close"), errors="coerce").fillna(0.0)
    h = pd.to_numeric(df.get("high"), errors="coerce").fillna(0.0)
    pc = pd.to_numeric(df.get("prev_close"), errors="coerce").fillna(0.0)

    can = (pc > 0)

    ret_close = pd.Series(0.0, index=df.index, dtype="float64")
    ret_high = pd.Series(0.0, index=df.index, dtype="float64")

    ret_close.loc[can] = (c.loc[can] / pc.loc[can]) - 1.0
    ret_high.loc[can] = (h.loc[can] / pc.loc[can]) - 1.0

    df["ret"] = ret_close.astype(float)
    df["ret_pct"] = (df["ret"] * 100.0).astype(float)
    df["ret_high"] = ret_high.astype(float)
    df["ret_high_pct"] = (df["ret_high"] * 100.0).astype(float)
    return df


def _limit_price(prev_close: float, limit_pct: float) -> float:
    return float(prev_close) * (1.0 + float(limit_pct))


def _safe_limit_pct(x: Any) -> Optional[float]:
    """
    If band/limit_pct missing => None (no limit logic)
    """
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s or s.lower() in ("nan", "none"):
            return None
        v = float(s)
        if v <= 0:
            return None
        return float(v)
    except Exception:
        return None


# =============================================================================
# Main
# =============================================================================
def aggregate(raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    India aggregator (TH-like):

    Produces:
      - snapshot_main enriched with:
          ret/ret_high fields,
          limit_price, limit_pct_effective,
          is_limitup_touch/locked/opened, touch-only flags,
          touch-only>=10% and <10% flags,
          is_bigmove10_ex_locked
      - limitup list (display_limitup)
      - sector_summary (same schema as TH, but likely 1 sector until you add sector)
      - peers_by_sector / peers_not_limitup
      - meta.totals + meta.metrics (addable totals for footer/overview)
    """
    snap: List[Dict[str, Any]] = raw_payload.get("snapshot_main") or []
    if not snap:
        raw_payload.setdefault("limitup", [])
        raw_payload.setdefault("sector_summary", [])
        raw_payload.setdefault("peers_by_sector", {})
        raw_payload.setdefault("peers_not_limitup", [])
        raw_payload.setdefault("stats", {})
        raw_payload.setdefault("meta", {})
        raw_payload["meta"].setdefault("totals", {})
        raw_payload["meta"].setdefault("metrics", {})
        return raw_payload

    df = pd.DataFrame(snap).copy()

    # ensure basic columns
    for c in ["symbol", "name", "sector", "industry", "prev_close", "open", "high", "low", "close", "volume", "limit_pct", "band", "streak"]:
        if c not in df.columns:
            df[c] = None

    # Normalize + drop invalid
    df = df.dropna(subset=["symbol"])
    df["symbol"] = df["symbol"].astype(str)

    df["name"] = df["name"].fillna("Unknown")
    df["industry"] = df["industry"].fillna("Unclassified")
    df["sector"] = df["sector"].fillna("Unclassified")
    df["sector"] = df["sector"].apply(_norm_sector)

    for col in ["prev_close", "open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # if close missing => drop (renderer + aggregation assumes tradable)
    df = df[df["close"].notna()].copy()

    # streak integer
    try:
        df["streak"] = pd.to_numeric(df["streak"], errors="coerce").fillna(0).astype(int)
    except Exception:
        df["streak"] = 0

    # ret fields
    df = _add_ret_fields(df)

    # -------------------------------------------------------------------------
    # Limit logic (per-symbol limit_pct)
    # -------------------------------------------------------------------------
    limit_price_list: List[Optional[float]] = []
    limit_pct_eff: List[Optional[float]] = []

    is_touch: List[bool] = []
    is_locked: List[bool] = []

    for _, r in df.iterrows():
        pc = _to_float(r.get("prev_close"), 0.0)
        c = _to_float(r.get("close"), 0.0)
        h = _to_float(r.get("high"), 0.0)

        lpct = _safe_limit_pct(r.get("limit_pct"))
        if pc > 0 and lpct is not None:
            lp = _limit_price(pc, lpct)
            limit_price_list.append(lp)
            limit_pct_eff.append(lpct)

            touch = (h > 0) and (h >= lp - EPS)
            locked = (c > 0) and (c >= lp - EPS)

            is_touch.append(bool(touch))
            is_locked.append(bool(locked))
        else:
            limit_price_list.append(None)
            limit_pct_eff.append(None)
            is_touch.append(False)
            is_locked.append(False)

    df["limit_price"] = limit_price_list
    df["limit_pct_effective"] = limit_pct_eff

    df["is_limitup_touch"] = is_touch              # includes locked
    df["is_limitup_locked"] = is_locked            # locked now (close at/above limit)
    df["is_limitup_opened"] = df["is_limitup_touch"] & (~df["is_limitup_locked"])

    # TH compatibility naming
    df["is_true_limitup"] = df["is_limitup_locked"]
    df["is_touch_only"] = (df["is_limitup_touch"] == True) & (df["is_limitup_locked"] == False)
    df["is_stop_high"] = (df["is_limitup_locked"] == True)

    # -------------------------------------------------------------------------
    # Movers / bins (India: define "bigmove10" as >=10% close-based)
    # -------------------------------------------------------------------------
    SURGE_RET = 0.10  # keep consistent with open-movers; can env later if you want

    df["is_surge_ge10"] = (df["ret"] >= float(SURGE_RET))
    df["is_bigmove10_ex_locked"] = (df["is_surge_ge10"] == True) & (df["is_limitup_touch"] == False)

    # -------------------------------------------------------------------------
    # You asked: "touch after drop: still >=10% or <10%" (avoid future recalcs)
    # - close-based
    # - also provide high-based variants if you want to analyze peaks
    # -------------------------------------------------------------------------
    df["is_touch_only_ret_ge10"] = (df["is_touch_only"] == True) & (df["ret"] >= float(SURGE_RET))
    df["is_touch_only_ret_lt10"] = (df["is_touch_only"] == True) & (df["ret"] < float(SURGE_RET))

    df["is_touch_only_ret_high_ge10"] = (df["is_touch_only"] == True) & (df["ret_high"] >= float(SURGE_RET))
    df["is_touch_only_ret_high_lt10"] = (df["is_touch_only"] == True) & (df["ret_high"] < float(SURGE_RET))

    # Display list rule (like TH): touch OR >=10% (inclusive)
    df["is_display_limitup"] = (df["is_limitup_touch"] == True) | (df["is_surge_ge10"] == True)

    # -------------------------------------------------------------------------
    # Build limitup list
    # -------------------------------------------------------------------------
    df_limit = df[df["is_display_limitup"]].copy()
    df_limit = df_limit.sort_values(["ret"], ascending=False, kind="mergesort")

    limitup_records: List[Dict[str, Any]] = []
    for _, r in df_limit.iterrows():
        rec = r.to_dict()
        rec["sector"] = _norm_sector(rec.get("sector"))
        rec = _force_ret_fields(rec)

        # bool normalize for json + renderer stability
        for k in [
            "is_limitup_touch",
            "is_limitup_locked",
            "is_limitup_opened",
            "is_true_limitup",
            "is_touch_only",
            "is_stop_high",
            "is_surge_ge10",
            "is_bigmove10_ex_locked",
            "is_display_limitup",
            "is_touch_only_ret_ge10",
            "is_touch_only_ret_lt10",
            "is_touch_only_ret_high_ge10",
            "is_touch_only_ret_high_lt10",
        ]:
            if k in rec:
                rec[k] = bool(rec.get(k))

        limitup_records.append(_sanitize_nan(rec))

    # -------------------------------------------------------------------------
    # Peers (not display_limitup AND ret < 10)
    # -------------------------------------------------------------------------
    df_peers = df[~df["is_display_limitup"]].copy()
    df_peers = df_peers[df_peers["ret"] < float(SURGE_RET)]
    df_peers = df_peers.sort_values(["ret"], ascending=False, kind="mergesort")

    peers_by_sector: Dict[str, List[Dict[str, Any]]] = {}
    peers_flat: List[Dict[str, Any]] = []

    for sec, g in df_peers.groupby("sector", sort=False):
        # cap later if needed; India currently 1 sector
        g2 = g.head(80).copy()
        recs_raw = g2.to_dict(orient="records")

        recs: List[Dict[str, Any]] = []
        for rr in recs_raw:
            rr = _force_ret_fields(rr)
            recs.append(_sanitize_nan(rr))

        peers_by_sector[sec] = recs
        peers_flat.extend(recs)

    # -------------------------------------------------------------------------
    # sector_summary (TH-compatible schema)
    # -------------------------------------------------------------------------
    summary_rows: List[Dict[str, Any]] = []
    for sec, g in df.groupby("sector", sort=False):
        sector_total = int(len(g))

        locked_cnt = int((g["is_limitup_locked"] == True).sum())
        touch_total_cnt = int((g["is_limitup_touch"] == True).sum())  # includes locked
        touch_only_cnt = int(((g["is_limitup_touch"] == True) & (g["is_limitup_locked"] == False)).sum())
        opened_cnt = int((g["is_limitup_opened"] == True).sum())

        surge10_total_cnt = int((g["is_surge_ge10"] == True).sum())  # inclusive
        big10_ex_locked_cnt = int((g["is_bigmove10_ex_locked"] == True).sum())  # exclusive

        # your requested touch-only split
        touch_only_ge10 = int((g["is_touch_only_ret_ge10"] == True).sum())
        touch_only_lt10 = int((g["is_touch_only_ret_lt10"] == True).sum())

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
                    "surge_ge10_ex_locked_count": big10_ex_locked_cnt,

                    # ✅ extra splits you asked for
                    "touch_only_ret_ge10_count": touch_only_ge10,
                    "touch_only_ret_lt10_count": touch_only_lt10,

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
            x.get("bigmove10_cnt", 0),
        ),
        reverse=True,
    )

    # -------------------------------------------------------------------------
    # stats + totals
    # -------------------------------------------------------------------------
    stats = raw_payload.get("stats", {}) or {}
    stats["limitup_count"] = int(len(limitup_records))

    stats["in_true_limitup_count"] = int((df["is_stop_high"] == True).sum())
    stats["in_limitup_touch_count"] = int((df["is_limitup_touch"] == True).sum())  # includes locked
    stats["in_limitup_touch_only_count"] = int((df["is_touch_only"] == True).sum())
    stats["in_limitup_opened_count"] = int((df["is_limitup_opened"] == True).sum())

    stats["in_surge_ge10_total_count"] = int((df["is_surge_ge10"] == True).sum())
    stats["in_bigmove10_ex_locked_count"] = int((df["is_bigmove10_ex_locked"] == True).sum())

    stats["in_touch_only_ret_ge10_count"] = int((df["is_touch_only_ret_ge10"] == True).sum())
    stats["in_touch_only_ret_lt10_count"] = int((df["is_touch_only_ret_lt10"] == True).sum())
    stats["in_touch_only_ret_high_ge10_count"] = int((df["is_touch_only_ret_high_ge10"] == True).sum())
    stats["in_touch_only_ret_high_lt10_count"] = int((df["is_touch_only_ret_high_lt10"] == True).sum())

    raw_payload["stats"] = _sanitize_nan(stats)

    raw_payload.setdefault("meta", {})
    meta = raw_payload.get("meta") or {}
    if not isinstance(meta, dict):
        meta = {}
        raw_payload["meta"] = meta

    totals = meta.get("totals")
    if not isinstance(totals, dict):
        totals = {}
        meta["totals"] = totals

    locked_total = int(raw_payload["stats"].get("in_true_limitup_count") or 0)
    touched_total = int(raw_payload["stats"].get("in_limitup_touch_only_count") or 0)
    bigmove10_ex_locked_total = int(raw_payload["stats"].get("in_bigmove10_ex_locked_count") or 0)

    mix_total = int(locked_total + touched_total + bigmove10_ex_locked_total)

    totals["locked_total"] = locked_total
    totals["touched_total"] = touched_total
    totals["bigmove10_ex_locked_total"] = bigmove10_ex_locked_total
    totals["mix_total"] = mix_total

    # extra totals (handy)
    totals["touch_only_ret_ge10_total"] = int(raw_payload["stats"].get("in_touch_only_ret_ge10_count") or 0)
    totals["touch_only_ret_lt10_total"] = int(raw_payload["stats"].get("in_touch_only_ret_lt10_count") or 0)

    metrics = meta.get("metrics")
    if not isinstance(metrics, dict):
        metrics = {}
        meta["metrics"] = metrics

    metrics["bigmove10_total"] = int(raw_payload["stats"].get("in_surge_ge10_total_count") or 0)
    metrics["bigmove10_ex_locked_total"] = bigmove10_ex_locked_total

    meta["totals"] = _sanitize_nan(totals)
    meta["metrics"] = _sanitize_nan(metrics)
    raw_payload["meta"] = _sanitize_nan(meta)

    # -------------------------------------------------------------------------
    # snapshot_main enrichment (keep all rows with new fields)
    # -------------------------------------------------------------------------
    df_safe = df.where(pd.notna(df), None)

    for c in [
        "ret", "ret_pct", "ret_high", "ret_high_pct",
        "limit_price", "limit_pct_effective",
        "is_limitup_touch", "is_limitup_locked", "is_limitup_opened",
        "is_true_limitup", "is_touch_only", "is_stop_high",
        "is_surge_ge10", "is_bigmove10_ex_locked",
        "is_touch_only_ret_ge10", "is_touch_only_ret_lt10",
        "is_touch_only_ret_high_ge10", "is_touch_only_ret_high_lt10",
        "is_display_limitup",
    ]:
        if c not in df_safe.columns:
            df_safe[c] = None

    raw_payload["snapshot_main"] = _sanitize_nan(df_safe.to_dict(orient="records"))
    raw_payload["limitup"] = _sanitize_nan(limitup_records)
    raw_payload["sector_summary"] = _sanitize_nan(summary_rows)
    raw_payload["peers_by_sector"] = _sanitize_nan(peers_by_sector)
    raw_payload["peers_not_limitup"] = _sanitize_nan(peers_flat)

    raw_payload.setdefault("filters", {})
    raw_payload["filters"]["in_surge_ret_threshold"] = float(SURGE_RET)

    return raw_payload


if __name__ == "__main__":
    print("aggregator_in.py loaded (module test)")
