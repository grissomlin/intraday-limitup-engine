# markets/kr/indicators_kr.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Any, Dict, List, Tuple

import pandas as pd


def _mode30() -> str:
    """
    30% streak 用什麼當「連板日」？
    - touch: 盤中觸及 30% 就算（預設，較符合題材延續）
    - locked: 收盤 >=30% 才算（更嚴格）
    """
    v = str(os.getenv("KR_STREAK30_MODE", "touch")).strip().lower()
    return "locked" if v == "locked" else "touch"


def compute_streak_maps(
    daily_df: pd.DataFrame,
    *,
    ymd_effective: str,
    th30: float = 0.30,
    th10: float = 0.10,
) -> Tuple[
    Dict[str, int], Dict[str, int], Dict[str, bool], Dict[str, bool],  # 30%
    Dict[str, int], Dict[str, int], Dict[str, bool],                  # 10%
]:
    """
    daily_df 需要欄位：symbol, ymd, close, high, prev_close

    回傳：
      - streak30_map, streak30_prev_map, prev_locked30_map, prev_touch30_map
      - streak10_map, streak10_prev_map, prev_big10_map
    """
    if daily_df is None or daily_df.empty:
        return {}, {}, {}, {}, {}, {}, {}

    df = daily_df.copy()

    # normalize columns
    if "date" in df.columns and "ymd" not in df.columns:
        df["ymd"] = df["date"]

    need = {"symbol", "ymd", "close", "high", "prev_close"}
    if not need.issubset(set(df.columns)):
        return {}, {}, {}, {}, {}, {}, {}

    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["ymd"] = df["ymd"].astype(str).str.strip()

    for c in ["close", "high", "prev_close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # keep <= ymd_effective
    df = df[df["ymd"].notna() & (df["ymd"] <= ymd_effective)].copy()
    if df.empty:
        return {}, {}, {}, {}, {}, {}, {}

    # need prev_close for returns
    m = df["prev_close"].notna() & (df["prev_close"] > 0) & df["close"].notna()
    df = df[m].copy()
    if df.empty:
        return {}, {}, {}, {}, {}, {}, {}

    df = df.sort_values(["symbol", "ymd"], kind="mergesort")

    df["ret_close"] = (df["close"] / df["prev_close"]) - 1.0
    df["ret_high"] = pd.NA
    mh = df["high"].notna() & (df["high"] > 0)
    df.loc[mh, "ret_high"] = (df.loc[mh, "high"] / df.loc[mh, "prev_close"]) - 1.0

    # === flags ===
    df["is_limitup30_locked"] = df["ret_close"].notna() & (df["ret_close"] >= float(th30))

    # ✅ touch-only：盤中高點達到 30% 且 收盤未達 30%（排除 locked）
    df["is_limitup30_touch"] = (
        df["ret_high"].notna()
        & (df["ret_high"] >= float(th30))
        & df["ret_close"].notna()
        & (df["ret_close"] < float(th30))
    )

    df["is_bigup10"] = df["ret_close"].notna() & (df["ret_close"] >= float(th10))

    mode30 = _mode30()
    flag30 = "is_limitup30_locked" if mode30 == "locked" else "is_limitup30_touch"

    streak30_map: Dict[str, int] = {}
    streak30_prev_map: Dict[str, int] = {}
    prev_locked30_map: Dict[str, bool] = {}
    prev_touch30_map: Dict[str, bool] = {}

    streak10_map: Dict[str, int] = {}
    streak10_prev_map: Dict[str, int] = {}
    prev_big10_map: Dict[str, bool] = {}

    for sym, g in df.groupby("symbol", sort=False):
        ymds = g["ymd"].tolist()
        if not ymds:
            continue
        last_i = len(ymds) - 1

        # 必須有 ymd_effective 當天資料，不然 streak 會偏
        if ymds[last_i] != ymd_effective:
            continue

        f30 = g[flag30].tolist()
        locked30 = g["is_limitup30_locked"].tolist()
        touch30 = g["is_limitup30_touch"].tolist()
        f10 = g["is_bigup10"].tolist()

        # today streak30
        s30 = 0
        i = last_i
        while i >= 0 and bool(f30[i]):
            s30 += 1
            i -= 1
        streak30_map[sym] = s30

        # today streak10
        s10 = 0
        i = last_i
        while i >= 0 and bool(f10[i]):
            s10 += 1
            i -= 1
        streak10_map[sym] = s10

        # yesterday
        if last_i - 1 >= 0:
            prev_locked30_map[sym] = bool(locked30[last_i - 1])
            prev_touch30_map[sym] = bool(touch30[last_i - 1])
            prev_big10_map[sym] = bool(f10[last_i - 1])

            s30p = 0
            j = last_i - 1
            while j >= 0 and bool(f30[j]):
                s30p += 1
                j -= 1
            streak30_prev_map[sym] = s30p

            s10p = 0
            j = last_i - 1
            while j >= 0 and bool(f10[j]):
                s10p += 1
                j -= 1
            streak10_prev_map[sym] = s10p
        else:
            prev_locked30_map[sym] = False
            prev_touch30_map[sym] = False
            prev_big10_map[sym] = False
            streak30_prev_map[sym] = 0
            streak10_prev_map[sym] = 0

    return (
        streak30_map, streak30_prev_map, prev_locked30_map, prev_touch30_map,
        streak10_map, streak10_prev_map, prev_big10_map,
    )


def apply_maps(snapshot_main: List[Dict[str, Any]], maps: Tuple) -> List[Dict[str, Any]]:
    (
        streak30_map, streak30_prev_map, prev_locked30_map, prev_touch30_map,
        streak10_map, streak10_prev_map, prev_big10_map,
    ) = maps

    out: List[Dict[str, Any]] = []
    for r in snapshot_main or []:
        sym = str(r.get("symbol", "")).strip()
        rr = dict(r)

        rr["streak30"] = int(streak30_map.get(sym, 0))
        rr["streak30_prev"] = int(streak30_prev_map.get(sym, 0))
        rr["prev_was_limitup30_locked"] = bool(prev_locked30_map.get(sym, False))
        rr["prev_was_limitup30_touch"] = bool(prev_touch30_map.get(sym, False))

        rr["streak10"] = int(streak10_map.get(sym, 0))
        rr["streak10_prev"] = int(streak10_prev_map.get(sym, 0))
        rr["prev_was_bigup10"] = bool(prev_big10_map.get(sym, False))

        out.append(rr)
    return out
