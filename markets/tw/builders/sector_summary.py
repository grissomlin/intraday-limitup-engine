# markets/tw/builders/sector_summary.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from ._common import _ensure_cols, _normalize_sector
from .open_limit import normalize_open_limit_rows


def merge_open_limit_into_limitup_df(
    limitup_df: pd.DataFrame,
    open_limit_rows: Optional[List[Dict[str, Any]]] = None,
) -> pd.DataFrame:
    """
    ✅ 讓 sector page 主榜也能顯示興櫃：
    - 把 open_limit_watchlist rows 轉成 df 並 append 到 limitup_df。
    - 不會把 is_limitup_* 變成 True（避免誤當漲停鎖死/失敗）
    - status_text 會有 10%+ / 觸及10% 可供 UI 顯示
    """
    base = pd.DataFrame() if (limitup_df is None or limitup_df.empty) else limitup_df.copy()
    if not open_limit_rows:
        return base

    dfE = normalize_open_limit_rows(open_limit_rows)
    if dfE.empty:
        return base

    if base.empty:
        return dfE

    # align columns
    for c in dfE.columns:
        if c not in base.columns:
            base[c] = None
    for c in base.columns:
        if c not in dfE.columns:
            dfE[c] = None

    out = pd.concat([base, dfE[base.columns]], ignore_index=True)
    return out


def build_sector_summary_main(
    limitup_df: pd.DataFrame,
    *,
    open_limit_rows: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    sector_summary（主榜）：
    - locked_cnt: 鎖死數
    - touch_cnt : 觸及數（包含 locked）
    - no_limit_cnt: no_limit 題材數（輸出 key 維持舊名，避免 UI 爆）
    - open_limit_cnt: 興櫃/開放式門檻數（新增）
    - total_cnt/count: 合計

    ✅ 若傳入 open_limit_rows，會把興櫃也算進 total_cnt/count 與排序。
    """
    base = pd.DataFrame() if (limitup_df is None or limitup_df.empty) else limitup_df.copy()
    if open_limit_rows:
        base = merge_open_limit_into_limitup_df(base, open_limit_rows=open_limit_rows)

    if base is None or base.empty:
        return []

    df = base.copy()
    _ensure_cols(
        df,
        [
            ("sector", "未分類"),
            ("is_limitup_locked", False),
            ("is_limitup_touch", False),
            ("limit_type", "standard"),
        ],
    )

    df["sector"] = _normalize_sector(df["sector"])

    df["locked_cnt"] = df["is_limitup_locked"].fillna(False).astype(bool).astype(int)
    df["touch_cnt"] = df["is_limitup_touch"].fillna(False).astype(bool).astype(int)  # 含 locked

    df["_theme_cnt"] = (df["limit_type"].fillna("standard").astype(str).eq("no_limit")).astype(int)
    df["_open_cnt"] = (df["limit_type"].fillna("standard").astype(str).eq("open_limit")).astype(int)

    df["total_cnt"] = 1

    agg = (
        df.groupby("sector", as_index=False)[["locked_cnt", "touch_cnt", "_theme_cnt", "_open_cnt", "total_cnt"]]
        .sum()
        .sort_values(["locked_cnt", "touch_cnt", "_theme_cnt", "_open_cnt", "total_cnt"], ascending=False)
        .rename(columns={"_theme_cnt": "no_limit_cnt", "_open_cnt": "open_limit_cnt"})
    )
    agg["count"] = agg["total_cnt"]
    return agg.to_dict(orient="records")
