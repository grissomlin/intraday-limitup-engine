# dashboard/components/tw_candidates.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Set
import pandas as pd

from .tw_links import add_link_columns, tw_market_label


def build_sector_candidates(
    meta_df: pd.DataFrame,
    daily_lastprev: pd.DataFrame,
    sector: str,
    limitup_symbols: Set[str],
    *,
    candidate_ret_floor: float = 0.03,
    exclude_near_limit: float = 0.095,  # 9.5% 以上視為接近漲停，從候補排除
) -> pd.DataFrame:
    """
    產業候補：同產業、但不在主榜 limitup 的股票
    - 以 ret 排序
    - 篩 ret >= candidate_ret_floor
    - 排除接近漲停（exclude_near_limit）
    """
    if meta_df is None or meta_df.empty or daily_lastprev is None or daily_lastprev.empty:
        return pd.DataFrame()

    sector_syms = meta_df.loc[meta_df["sector"] == sector, "symbol"].astype(str).tolist()
    df = daily_lastprev[daily_lastprev["symbol"].isin(sector_syms)].copy()
    if df.empty:
        return pd.DataFrame()

    # 排除主榜已入選
    df = df[~df["symbol"].astype(str).isin(set(map(str, limitup_symbols)))].copy()

    df["ret"] = pd.to_numeric(df.get("ret"), errors="coerce")
    df = df.dropna(subset=["ret"])

    # 候補門檻
    df = df[df["ret"] >= float(candidate_ret_floor)].copy()

    # 排除接近漲停
    df = df[df["ret"] < float(exclude_near_limit)].copy()
    if df.empty:
        return pd.DataFrame()

    df = df.merge(meta_df[["symbol", "name", "market_detail"]], on="symbol", how="left")

    df["漲幅%"] = (df["ret"] * 100).round(2)
    df["市場別"] = df["market_detail"].astype(str).apply(tw_market_label)

    df = df.rename(columns={"name": "名稱", "bar_date": "日K日期"})

    # 加連結欄位
    df = add_link_columns(df)

    want_cols = ["代碼", "名稱", "市場別", "日K日期", "漲幅%", "Yahoo", "財報狗", "鉅亨", "Wantgoo", "HiStock"]
    for c in want_cols:
        if c not in df.columns:
            df[c] = ""

    df = df.sort_values(["漲幅%"], ascending=False)
    return df[want_cols].reset_index(drop=True)
