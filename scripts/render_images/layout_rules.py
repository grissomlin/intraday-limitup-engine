# scripts/render_images/layout_rules.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Dict, List, Tuple
import pandas as pd


# =============================================================================
# Sector statistics
# =============================================================================
def sector_counts(limitup_sector_df: pd.DataFrame) -> Tuple[int, int, int]:
    """
    回傳某產業內的：
      (locked_cnt, touch_only_cnt, theme_cnt)
    """
    if limitup_sector_df is None or limitup_sector_df.empty:
        return 0, 0, 0

    s = limitup_sector_df["limitup_status"].astype(str).str.lower()
    locked = int((s == "locked").sum())
    touch = int((s == "touch_only").sum())
    theme = int((s == "no_limit_theme").sum())

    return locked, touch, theme


# =============================================================================
# Sector ranking
# =============================================================================
def rank_sectors(limitup_df: pd.DataFrame) -> List[str]:
    """
    產業排序規則（回傳 sector list）：
      1. 鎖死多的在前
      2. 打開多的在前
      3. 題材多的在前
      4. sector 名稱（穩定排序）
    """
    if limitup_df is None or limitup_df.empty:
        return []

    sectors = sorted(limitup_df["sector"].unique().tolist())

    def _rank_key(sec: str) -> Tuple[int, int, int, str]:
        sdf = limitup_df.loc[limitup_df["sector"] == sec]
        locked, touch, theme = sector_counts(sdf)
        # 多的要排前面 → 用負號
        return (-locked, -touch, -theme, sec)

    return sorted(sectors, key=_rank_key)


# =============================================================================
# Page building (core logic)
# =============================================================================
def build_sector_pages(
    limitup_sector_df: pd.DataFrame,
    peers_sector_df: pd.DataFrame,
    *,
    rows_per_page: int,
    peers_max_per_page: int,
) -> List[Dict[str, List[Dict]]]:
    """
    對單一產業進行分頁，回傳 pages：

    每一頁結構：
    {
        "limitup_rows": [dict, ...],
        "peer_rows":    [dict, ...],
    }

    規則：
    - 每頁最多 rows_per_page 行（limitup + peers）
    - limitup 永遠優先放
    - peers 只有在還有空位才補
    - peers 每頁最多 peers_max_per_page
    """
    L = limitup_sector_df.copy() if limitup_sector_df is not None else pd.DataFrame()
    P = peers_sector_df.copy() if peers_sector_df is not None else pd.DataFrame()

    # -------------------------
    # 排序（這裡只做「必要排序」）
    # -------------------------
    # limitup：鎖死 → 打開 → 題材，再來連板數高
    if not L.empty:
        order_map = {
            "locked": 0,
            "touch_only": 1,
            "no_limit_theme": 2,
        }
        L["_ord"] = (
            L["limitup_status"]
            .astype(str)
            .str.lower()
            .map(order_map)
            .fillna(9)
            .astype(int)
        )
        if "streak" in L.columns:
            L = L.sort_values(
                ["_ord", "streak"],
                ascending=[True, False],
            )
        else:
            L = L.sort_values(["_ord"])
        L = L.drop(columns=["_ord"]).reset_index(drop=True)

    # peers：只做報酬排序（高到低）
    if not P.empty and "ret" in P.columns:
        P["ret"] = pd.to_numeric(P["ret"], errors="coerce").fillna(0.0)
        P = P.sort_values(["ret"], ascending=False).reset_index(drop=True)

    # -------------------------
    # 分頁
    # -------------------------
    pages: List[Dict[str, List[Dict]]] = []

    li = 0
    pi = 0
    L_total = len(L)
    P_total = len(P)

    # 至少產生一頁（避免整個產業被吞掉）
    while True:
        if li >= L_total and pi >= P_total:
            if pages:
                break
            pages.append({"limitup_rows": [], "peer_rows": []})
            break

        remaining = rows_per_page

        # 先塞 limitup
        limit_rows: List[Dict] = []
        if li < L_total and remaining > 0:
            takeL = min(remaining, L_total - li)
            limit_rows = L.iloc[li : li + takeL].to_dict(orient="records")
            li += takeL
            remaining -= takeL

        # 再塞 peers（若有空間）
        peer_rows: List[Dict] = []
        if remaining > 0 and pi < P_total:
            takeP = min(remaining, peers_max_per_page, P_total - pi)
            peer_rows = P.iloc[pi : pi + takeP].to_dict(orient="records")
            pi += takeP
            remaining -= takeP

        pages.append(
            {
                "limitup_rows": limit_rows,
                "peer_rows": peer_rows,
            }
        )

        if li >= L_total and pi >= P_total:
            break

    return pages
