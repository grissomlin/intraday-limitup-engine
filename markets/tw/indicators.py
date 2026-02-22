# markets/tw/indicators.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Tuple, Optional

import pandas as pd

EPS = 1e-9


# =============================================================================
# TW streak definition (limit-up 10%)
# =============================================================================
def _mode10() -> str:
    """
    TW 漲停連板定義：
    - touch : 盤中觸及漲停（ret_high >= 10%）
    - locked: 收盤鎖死漲停（is_limitup_locked）
    """
    v = str(os.getenv("TW_STREAK_MODE", "touch")).strip().lower()
    return "locked" if v == "locked" else "touch"


def _mode_surge10() -> str:
    """
    興櫃/開放池「大漲10%」連續定義：
    - close : 收盤 >=10% 才算（預設）
    - touch : 盤中高點 >=10% 就算
    """
    v = str(os.getenv("TW_SURGE_STREAK_MODE", "close")).strip().lower()
    return "touch" if v == "touch" else "close"


def _coerce_bool_series(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s.fillna(False)
    return (
        s.astype(str)
        .str.strip()
        .str.lower()
        .isin(["1", "true", "yes", "y", "t", "on"])
        .fillna(False)
    )


def _normalize_daily_for_streak(daily_df: pd.DataFrame, *, th10: float) -> pd.DataFrame:
    """
    Accepts either:
      A) normalized df: symbol, ymd, ret, ret_high, is_limitup_locked
      B) yfinance long df: symbol, date, open, high, low, close, volume
    Returns a df containing at least:
      symbol, ymd, ret, ret_high, is_limitup_locked
    """
    df = daily_df.copy()
    if df.empty:
        return df

    # symbol
    if "symbol" not in df.columns:
        df["symbol"] = ""
    df["symbol"] = df["symbol"].astype(str).str.strip()

    # ymd
    if "ymd" not in df.columns:
        if "date" in df.columns:
            df["ymd"] = df["date"]
        else:
            df["ymd"] = ""
    df["ymd"] = df["ymd"].astype(str).str.slice(0, 10)

    # compute ret/ret_high if missing
    need_compute_ret = ("ret" not in df.columns) or ("ret_high" not in df.columns)
    if need_compute_ret:
        for c in ["close", "high"]:
            if c not in df.columns:
                df[c] = pd.NA
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["high"] = pd.to_numeric(df["high"], errors="coerce")

        df = df.sort_values(["symbol", "ymd"]).reset_index(drop=True)
        df["last_close"] = df.groupby("symbol")["close"].shift(1)
        denom = df["last_close"]

        df["ret"] = (df["close"] / denom) - 1.0
        df["ret_high"] = (df["high"] / denom) - 1.0

    # is_limitup_locked
    if "is_limitup_locked" not in df.columns:
        df["is_limitup_locked"] = (
            pd.to_numeric(df["ret"], errors="coerce") >= (th10 - EPS)
        )
    else:
        df["is_limitup_locked"] = _coerce_bool_series(df["is_limitup_locked"])

    # numeric ret/ret_high
    df["ret"] = pd.to_numeric(df.get("ret", 0), errors="coerce").fillna(0.0)
    df["ret_high"] = pd.to_numeric(df.get("ret_high", 0), errors="coerce").fillna(0.0)

    return df


def compute_streak_maps(
    daily_df: pd.DataFrame,
    *,
    ymd_effective: str,
    th10: float = 0.10,
) -> Tuple[
    Dict[str, int],   # streak_map[today]
    Dict[str, int],   # streak_prev_map[yesterday]
    Dict[str, bool],  # prev_was_locked_map[yesterday locked?]
    Dict[str, bool],  # prev_was_touch_map[yesterday touch?]
]:
    """
    漲停連板（10% limit-up 語意）
    """
    mode = _mode10()

    ymd_effective = str(ymd_effective or "").strip()[:10]
    if not ymd_effective:
        return {}, {}, {}, {}

    df = _normalize_daily_for_streak(daily_df, th10=th10)
    if df is None or df.empty:
        return {}, {}, {}, {}

    df = df.sort_values(["symbol", "ymd"]).reset_index(drop=True)

    streak_map: Dict[str, int] = {}
    streak_prev_map: Dict[str, int] = {}
    prev_was_locked_map: Dict[str, bool] = {}
    prev_was_touch_map: Dict[str, bool] = {}

    current_symbol: Optional[str] = None
    streak = 0

    for _, row in df.iterrows():
        sym = str(row.get("symbol") or "").strip()
        ymd = str(row.get("ymd") or "").strip()[:10]
        if not sym or not ymd:
            continue

        if sym != current_symbol:
            current_symbol = sym
            streak = 0

        ret_high = float(row.get("ret_high", 0.0) or 0.0)
        locked = bool(row.get("is_limitup_locked", False))

        if mode == "touch":
            hit = ret_high >= (th10 - EPS)
        else:
            hit = locked

        streak = (streak + 1) if hit else 0

        if ymd == ymd_effective:
            streak_map[sym] = streak

        if ymd < ymd_effective:
            streak_prev_map[sym] = streak
            prev_was_locked_map[sym] = locked
            prev_was_touch_map[sym] = (ret_high >= (th10 - EPS))

    return streak_map, streak_prev_map, prev_was_locked_map, prev_was_touch_map


def compute_surge_streak_maps(
    daily_df: pd.DataFrame,
    *,
    ymd_effective: str,
    th10: float = 0.10,
) -> Tuple[
    Dict[str, int],   # surge_streak_map[today]
    Dict[str, int],   # surge_streak_prev_map[yesterday]
    Dict[str, bool],  # prev_was_surge_map[yesterday close>=10?]
    Dict[str, bool],  # prev_was_surge_touch_map[yesterday high>=10?]
]:
    """
    興櫃/開放池「大漲10%」連續（獨立於漲停）
    - mode=close: ret >= 10%
    - mode=touch: ret_high >= 10%
    """
    mode = _mode_surge10()

    ymd_effective = str(ymd_effective or "").strip()[:10]
    if not ymd_effective:
        return {}, {}, {}, {}

    df = _normalize_daily_for_streak(daily_df, th10=th10)
    if df is None or df.empty:
        return {}, {}, {}, {}

    df = df.sort_values(["symbol", "ymd"]).reset_index(drop=True)

    streak_map: Dict[str, int] = {}
    streak_prev_map: Dict[str, int] = {}
    prev_was_map: Dict[str, bool] = {}
    prev_was_touch_map: Dict[str, bool] = {}

    current_symbol: Optional[str] = None
    streak = 0

    for _, row in df.iterrows():
        sym = str(row.get("symbol") or "").strip()
        ymd = str(row.get("ymd") or "").strip()[:10]
        if not sym or not ymd:
            continue

        if sym != current_symbol:
            current_symbol = sym
            streak = 0

        ret = float(row.get("ret", 0.0) or 0.0)
        ret_high = float(row.get("ret_high", 0.0) or 0.0)

        if mode == "touch":
            hit = ret_high >= (th10 - EPS)
        else:
            hit = ret >= (th10 - EPS)

        streak = (streak + 1) if hit else 0

        if ymd == ymd_effective:
            streak_map[sym] = streak

        if ymd < ymd_effective:
            streak_prev_map[sym] = streak
            prev_was_map[sym] = (ret >= (th10 - EPS))
            prev_was_touch_map[sym] = (ret_high >= (th10 - EPS))

    return streak_map, streak_prev_map, prev_was_map, prev_was_touch_map


# =============================================================================
# Enrich helpers
# =============================================================================
def _iter_snapshot_rows(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    for k in ("snapshot_main", "snapshot_open"):
        v = payload.get(k)
        if isinstance(v, list):
            for r in v:
                if isinstance(r, dict):
                    yield r


def _set_if_missing(r: Dict[str, Any], key: str, val: Any) -> None:
    if key not in r:
        r[key] = val


def enrich_payload_streaks(
    payload: Dict[str, Any],
    *,
    daily_df: pd.DataFrame,
    ymd_effective: str,
    th10: float = 0.10,
) -> Dict[str, Any]:
    """
    把 streak 資訊寫回 payload 的每一筆 snapshot row。

    Limit-up (漲停語意):
      - streak, streak_prev
      - prev_is_limitup_locked, prev_is_limitup_touch

    Surge (興櫃大漲語意):
      - surge_streak, surge_streak_prev
      - prev_is_surge10, prev_is_surge10_touch
    """
    if not isinstance(payload, dict):
        return payload

    if not ymd_effective:
        ymd_effective = str(payload.get("ymd_effective") or payload.get("ymd") or "").strip()
    ymd_effective = str(ymd_effective or "").strip()[:10]
    if not ymd_effective:
        return payload

    if daily_df is None or getattr(daily_df, "empty", True):
        for r in _iter_snapshot_rows(payload):
            # limit-up
            _set_if_missing(r, "streak", 0)
            _set_if_missing(r, "streak_prev", 0)
            _set_if_missing(r, "prev_is_limitup_locked", False)
            _set_if_missing(r, "prev_is_limitup_touch", False)
            # surge
            _set_if_missing(r, "surge_streak", 0)
            _set_if_missing(r, "surge_streak_prev", 0)
            _set_if_missing(r, "prev_is_surge10", False)
            _set_if_missing(r, "prev_is_surge10_touch", False)
        return payload

    streak_map, prev_map, prev_locked_map, prev_touch_map = compute_streak_maps(
        daily_df, ymd_effective=ymd_effective, th10=th10
    )
    surge_map, surge_prev_map, prev_surge_map, prev_surge_touch_map = compute_surge_streak_maps(
        daily_df, ymd_effective=ymd_effective, th10=th10
    )

    for r in _iter_snapshot_rows(payload):
        sym = str(r.get("symbol") or "").strip()

        # limit-up
        r["streak"] = int(streak_map.get(sym, 0) or 0)
        r["streak_prev"] = int(prev_map.get(sym, 0) or 0)
        r["prev_is_limitup_locked"] = bool(prev_locked_map.get(sym, False))
        r["prev_is_limitup_touch"] = bool(prev_touch_map.get(sym, False))

        # surge (open-limit pool)
        r["surge_streak"] = int(surge_map.get(sym, 0) or 0)
        r["surge_streak_prev"] = int(surge_prev_map.get(sym, 0) or 0)
        r["prev_is_surge10"] = bool(prev_surge_map.get(sym, False))
        r["prev_is_surge10_touch"] = bool(prev_surge_touch_map.get(sym, False))

    return payload


def enrich_snapshot_main(
    snapshot_main: List[Dict[str, Any]],
    *,
    daily_df: pd.DataFrame,
    ymd_effective: str,
    snapshot_open: Optional[List[Dict[str, Any]]] = None,
    th10: float = 0.10,
) -> List[Dict[str, Any]]:
    payload_like: Dict[str, Any] = {
        "ymd_effective": ymd_effective,
        "snapshot_main": snapshot_main,
        "snapshot_open": snapshot_open or [],
    }
    enrich_payload_streaks(payload_like, daily_df=daily_df, ymd_effective=ymd_effective, th10=th10)
    return snapshot_main
