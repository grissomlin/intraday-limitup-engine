# dashboard/components/pages/overview.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Dict, Any, List
import pandas as pd
import streamlit as st

from ..charts import build_sector_bar_fig_plotly


def render_links_table(df: pd.DataFrame, height: int = 520):
    """用 data_editor + LinkColumn 讓 URL 可點"""
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
    payload: Dict[str, Any],
    stats: Dict[str, Any],
    sector_df: pd.DataFrame,
    main_df: pd.DataFrame,
    emerging_df: pd.DataFrame,
    errors_rows: List[Dict[str, Any]],
    sector_main_bar: pd.DataFrame,
    sector_emg_bar: pd.DataFrame,
):
    st.subheader("總覽")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("抓取標的數", stats.get("total_symbols_fetch", stats.get("total_symbols", 0)))
    c2.metric("主榜數", stats.get("main_count", stats.get("limitup_count", 0)))
    c3.metric("興櫃榜數", stats.get("emerging_count", 0))
    c4.metric("errors", stats.get("errors_count", len(errors_rows)))

    st.caption(
        f"payload: {payload.get('ymd')} {payload.get('slot')} "
        f"asof={payload.get('asof')} generated_at={payload.get('generated_at')}"
    )

    st.markdown("### 產業柱狀圖（主榜 / 興櫃分開）")
    colA, colB = st.columns(2)

    # ✅ 圖的標題加上時間（影片更可信）
    asof = payload.get("asof") or payload.get("generated_at") or ""
    title_main = f"主榜｜各產業漲停/觸及家數（{asof}）" if asof else "主榜｜各產業漲停/觸及家數"
    title_emg  = f"興櫃｜各產業漲停/觸及家數（{asof}）" if asof else "興櫃｜各產業漲停/觸及家數"

    with colA:
        st.markdown("**主榜：各產業漲停/觸及家數**")
        if sector_main_bar is None or sector_main_bar.empty:
            st.info("(empty)")
        else:
            # 保險：再排序一次，避免上游 df 沒排序
            dfm = sector_main_bar.copy()
            dfm["家數"] = pd.to_numeric(dfm["家數"], errors="coerce").fillna(0)
            dfm = dfm.sort_values("家數", ascending=False).reset_index(drop=True)

            fig = build_sector_bar_fig_plotly(
                dfm,
                title_main,
                top_n=None,
                show_values=True,
                tick_angle=-45,
                height=420,
            )
            st.plotly_chart(fig, use_container_width=True)

    with colB:
        st.markdown("**興櫃：各產業漲停/觸及家數**")
        if sector_emg_bar is None or sector_emg_bar.empty:
            st.info("(empty)")
        else:
            dfe = sector_emg_bar.copy()
            dfe["家數"] = pd.to_numeric(dfe["家數"], errors="coerce").fillna(0)
            dfe = dfe.sort_values("家數", ascending=False).reset_index(drop=True)

            fig = build_sector_bar_fig_plotly(
                dfe,
                title_emg,
                top_n=None,
                show_values=True,
                tick_angle=-45,
                height=420,
            )
            st.plotly_chart(fig, use_container_width=True)

    left, right = st.columns([1, 2])

    with left:
        st.markdown("### 產業排行榜（payload sector_summary）")
        if sector_df is None or sector_df.empty:
            st.info("sector_summary 為空")
        else:
            st.dataframe(sector_df, use_container_width=True, height=520)

    with right:
        st.markdown("### 主榜（可點連結）")
        if main_df is None or main_df.empty:
            st.info("主榜為空")
        else:
            want = ["代碼", "名稱", "產業", "漲幅%", "streak", "bar_date", "Yahoo", "財報狗", "鉅亨", "Wantgoo", "HiStock"]
            for c in want:
                if c not in main_df.columns:
                    main_df[c] = ""
            render_links_table(main_df[want].copy(), height=520)

        st.markdown("### 興櫃榜（可點連結）")
        if emerging_df is None or emerging_df.empty:
            st.info("興櫃榜為空")
        else:
            want = ["代碼", "名稱", "產業", "漲幅%", "streak", "bar_date", "Yahoo", "財報狗", "鉅亨", "Wantgoo", "HiStock"]
            for c in want:
                if c not in emerging_df.columns:
                    emerging_df[c] = ""
            render_links_table(emerging_df[want].copy(), height=360)
