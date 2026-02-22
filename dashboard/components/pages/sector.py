# dashboard/components/pages/sector.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Set
import pandas as pd
import streamlit as st

from ..tw_candidates import build_sector_candidates


def render_links_table(df: pd.DataFrame, height: int = 520):
    if df is None or df.empty:
        st.info("(empty)")
        return

    colcfg = {
        "Yahoo": st.column_config.LinkColumn("Yahoo", display_text="Yahoo"),
        "財報狗": st.column_config.LinkColumn("財報狗", display_text="財報狗"),
        "鉅亨": st.column_config.LinkColumn("鉅亨", display_text="鉅亨"),
        "Wantgoo": st.column_config.LinkColumn("Wantgoo", display_text="Wantgoo"),
        "HiStock": st.column_config.LinkColumn("HiStock", display_text="HiStock"),
    }
    st.data_editor(
        df,
        use_container_width=True,
        height=height,
        disabled=True,
        column_config=colcfg,
    )


def render_page(
    *,
    sector_df: pd.DataFrame,
    main_df: pd.DataFrame,
    meta_df: pd.DataFrame,
    daily_lastprev: pd.DataFrame,
    candidate_ret_floor: float,
    candidate_ret_floor_pct: int,
):
    st.subheader("產業鑽研：主產業 + 產業候補")

    if sector_df is None or sector_df.empty or "產業" not in sector_df.columns:
        st.info("沒有 sector_summary（或缺少『產業』欄），請先確認 payload 有輸出 sector_summary。")
        st.stop()

    # ✅ 保證下拉選單依「家數」排序（兼容 count / 家數）
    sdf = sector_df.copy()
    sort_col = None
    if "count" in sdf.columns:
        sort_col = "count"
    elif "家數" in sdf.columns:
        sort_col = "家數"

    if sort_col is not None:
        sdf[sort_col] = pd.to_numeric(sdf[sort_col], errors="coerce").fillna(0)
        sdf = sdf.sort_values(sort_col, ascending=False).reset_index(drop=True)

    sector_list = sdf["產業"].astype(str).tolist()
    pick_sector = st.selectbox("選擇產業（依家數排序）", sector_list, index=0 if sector_list else 0)

    st.markdown("### 主榜：本產業漲停/觸及（主榜）")
    if main_df is None or main_df.empty:
        st.info("主榜為空")
    else:
        sec_main = main_df[main_df["產業"].astype(str) == pick_sector].copy()
        if sec_main.empty:
            st.warning("主榜沒有這個產業的股票")
        else:
            want = ["代碼", "名稱", "產業", "漲幅%", "streak", "bar_date", "Yahoo", "財報狗", "鉅亨", "Wantgoo", "HiStock"]
            for c in want:
                if c not in sec_main.columns:
                    sec_main[c] = ""
            render_links_table(sec_main[want], height=420)

    st.markdown("### 產業候補：同產業未漲停（門檻用 %，且排除接近漲停）")
    if meta_df is None or meta_df.empty or daily_lastprev is None or daily_lastprev.empty:
        st.info("缺少 meta 或 daily CSV，無法產生候補（請確認 data/tw_stock_list.json 與 data/cache/tw/tw_prices_1d_*.csv 存在）")
        return

    limit_syms: Set[str] = set(main_df["symbol"].astype(str)) if (main_df is not None and not main_df.empty and "symbol" in main_df.columns) else set()
    candidates = build_sector_candidates(
        meta_df,
        daily_lastprev,
        pick_sector,
        limit_syms,
        candidate_ret_floor=candidate_ret_floor,
    )

    if candidates is None or candidates.empty:
        st.warning("沒有符合門檻的產業候補（或該產業今天都很弱 / 接近漲停者被排除）")
    else:
        render_links_table(candidates, height=520)

    st.markdown("### 產業摘要（給影片/文案用）")
    sec_row = sdf[sdf["產業"].astype(str) == pick_sector].head(1)
    if not sec_row.empty:
        r = sec_row.iloc[0].to_dict()
        st.write(f"- 產業：**{pick_sector}**")
        st.write(f"- 鎖漲停：**{r.get('limitup_locked', 0)}**，觸及：**{r.get('limitup_touch', 0)}**，總數：**{r.get('count', r.get('家數', 0))}**")
        st.write(f"- 候補門檻：**{candidate_ret_floor_pct}%**（且排除 ≥ 9.5% 接近漲停）")
