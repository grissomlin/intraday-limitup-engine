# markets/au/builders_au.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Dict, List
import os
import pandas as pd
import numpy as np

AU_OPEN_WATCHLIST_RET_TH = float(os.getenv("AU_OPEN_WATCHLIST_RET_TH", "0.10"))

# -----------------------------------------------------------------------------
# ✅ AU Penny / Tick gates (env-tunable)
# -----------------------------------------------------------------------------
# "penny" threshold in AUD
AU_PENNY_TH = float(os.getenv("AU_PENNY_TH", "0.20"))

# Layered rule for penny:
# - For penny stocks only, require either:
#     (tick_moves >= AU_PENNY_MIN_TICK_MOVES) OR (abs_move >= AU_PENNY_MIN_ABS_MOVE)
# Default aligns with your stats: kick 1~2 tick 10% movers.
AU_PENNY_MIN_TICK_MOVES = float(os.getenv("AU_PENNY_MIN_TICK_MOVES", "3.0"))
AU_PENNY_MIN_ABS_MOVE = float(os.getenv("AU_PENNY_MIN_ABS_MOVE", "0.002"))

# Optional global abs_move gate for all movers (0 disables)
AU_MIN_ABS_MOVE = float(os.getenv("AU_MIN_ABS_MOVE", "0.0"))

# Optional: if you want to gate by tick_moves for all movers (0 disables)
AU_MIN_TICK_MOVES_ALL = float(os.getenv("AU_MIN_TICK_MOVES_ALL", "0.0"))


def _infer_is_reit(sector: Any) -> bool:
    s = str(sector or "").strip().lower()
    if not s:
        return False
    # e.g. "Equity Real Estate Investment Trusts (REITs)"
    return ("reit" in s) or ("real estate investment trust" in s)


def _asx_tick_size(price: float) -> float:
    """
    ASX equities typical price steps (AUD):
      <= 0.10 : 0.001
      <  2.00 : 0.005
      >= 2.00 : 0.01
    """
    try:
        p = float(price)
    except Exception:
        return float("nan")
    if not np.isfinite(p) or p <= 0:
        return float("nan")
    if p <= 0.10:
        return 0.001
    if p < 2.00:
        return 0.005
    return 0.01


def build_open_limit_watchlist_au(snapshot_open_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    AU open_limit_watchlist：
    - 從 snapshot_open 過濾 (ret >= 門檻)
    - ✅ NEW: AU 便士股 / tick gate / abs_move gate 去除「一兩檔就 10%」的噪音
    - 不過度裁切：保留 sector pages / overview 會用到的欄位
    - sector 使用 ASX 的 GICS industry group 原文（直接沿用 snapshot_open）
    """
    if not snapshot_open_rows:
        return []

    df = pd.DataFrame(snapshot_open_rows)
    if df.empty:
        return []

    # --- normalize numeric fields we need ---
    df["ret"] = pd.to_numeric(df.get("ret", 0.0), errors="coerce").fillna(0.0)
    df["prev_close"] = pd.to_numeric(df.get("prev_close", 0.0), errors="coerce").fillna(0.0)
    df["close"] = pd.to_numeric(df.get("close", 0.0), errors="coerce").fillna(0.0)

    # --- base ret filter ---
    th = float(AU_OPEN_WATCHLIST_RET_TH)
    df = df[df["ret"] >= th].copy()
    if df.empty:
        return []

    # --- compute abs_move / tick / tick_moves ---
    df["abs_move"] = (df["close"] - df["prev_close"]).abs()
    df["tick"] = df["prev_close"].apply(_asx_tick_size)
    df["tick_moves"] = df["abs_move"] / df["tick"]
    df["is_penny"] = df["prev_close"] < float(AU_PENNY_TH)

    # --- optional global gates (apply to all movers) ---
    if float(AU_MIN_ABS_MOVE) > 0:
        df = df[df["abs_move"] >= float(AU_MIN_ABS_MOVE)].copy()
        if df.empty:
            return []

    if float(AU_MIN_TICK_MOVES_ALL) > 0:
        df = df[df["tick_moves"] >= float(AU_MIN_TICK_MOVES_ALL)].copy()
        if df.empty:
            return []

    # --- penny-specific layered gates (recommended) ---
    # keep if NOT penny, or (tick_moves >= N) or (abs_move >= M)
    # This kills the "0.0035->0.0040" and similar 1-tick 10% illusions.
    penny_min_ticks = float(AU_PENNY_MIN_TICK_MOVES)
    penny_min_abs = float(AU_PENNY_MIN_ABS_MOVE)

    if penny_min_ticks > 0 or penny_min_abs > 0:
        keep_non_penny = ~df["is_penny"]
        keep_by_ticks = df["tick_moves"] >= penny_min_ticks if penny_min_ticks > 0 else False
        keep_by_abs = df["abs_move"] >= penny_min_abs if penny_min_abs > 0 else False
        df = df[keep_non_penny | keep_by_ticks | keep_by_abs].copy()
        if df.empty:
            return []

    # sort
    df = df.sort_values("ret", ascending=False)

    # limit_type
    if "limit_type" not in df.columns:
        df["limit_type"] = "open_limit"

    # is_reit：如果 snapshot 已算好就尊重；沒有才推斷
    if "is_reit" not in df.columns:
        df["is_reit"] = df.get("sector", "").apply(_infer_is_reit)
    else:
        df["is_reit"] = df["is_reit"].fillna(False).astype(bool)

    # 補齊必要 meta 欄位（不覆蓋你原本算好的 move/status）
    for c, dv in [
        ("is_limitup_touch", False),
        ("is_limitup_locked", False),
        ("market_detail", "ASX"),
        ("market_label", "AU"),
        ("sector", "Unknown"),
        ("name", "Unknown"),
        ("symbol", ""),
        ("bar_date", ""),
        ("status_text", ""),
        # preferred fields
        ("move_band", -1),
        ("move_key", ""),
        # backward compatible
        ("badge_text", ""),
        ("badge_level", 0),
        ("streak", 0),
        ("streak_prev", 0),
        ("hit_prev", 0),
        ("touch_ret", 0.0),
        ("touched_only", False),
        ("open", 0.0),
        ("high", 0.0),
        ("low", 0.0),
        ("volume", 0),
    ]:
        if c not in df.columns:
            df[c] = dv

    keep = [
        "symbol",
        "name",
        "sector",
        "is_reit",
        "market_detail",
        "market_label",
        "bar_date",
        "prev_close",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "ret",
        "touch_ret",
        "touched_only",
        "streak",
        "streak_prev",
        "hit_prev",
        # ✅ new
        "move_band",
        "move_key",
        # ✅ old
        "badge_text",
        "badge_level",
        "limit_type",
        "is_limitup_touch",
        "is_limitup_locked",
        "status_text",
    ]

    for c in keep:
        if c not in df.columns:
            df[c] = ""  # 兜底

    # types cleanup (safe)
    df["move_band"] = pd.to_numeric(df.get("move_band", -1), errors="coerce").fillna(-1).astype(int)
    df["badge_level"] = pd.to_numeric(df.get("badge_level", 0), errors="coerce").fillna(0).astype(int)
    df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).astype(int)
    df["is_reit"] = df["is_reit"].fillna(False).astype(bool)

    return df[keep].to_dict(orient="records")


def build_sector_summary_open_limit_au(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    sector summary（AU open_limit）：
    - sector: count / avg_ret / max_ret
    """
    if not rows:
        return []

    df = pd.DataFrame(rows)
    if df.empty:
        return []

    df["sector"] = df.get("sector", "").fillna("").replace("", "Unknown")
    df["ret"] = pd.to_numeric(df.get("ret", 0.0), errors="coerce").fillna(0.0)

    agg = (
        df.groupby("sector", as_index=False)
        .agg(count=("symbol", "count"), avg_ret=("ret", "mean"), max_ret=("ret", "max"))
        .sort_values(["count", "avg_ret"], ascending=False)
    )
    return agg.to_dict(orient="records")
