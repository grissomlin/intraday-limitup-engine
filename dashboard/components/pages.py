# dashboard/components/pages.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Dict, Any, List, Set, Tuple, Optional

import pandas as pd
import streamlit as st

from .formatters import df_to_md_table, safe_format
from .tw_candidates import build_sector_candidates


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


def render_tab_overview(
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
        f"payload: {payload.get('ymd')} {payload.get('slot')} asof={payload.get('asof')} generated_at={payload.get('generated_at')}"
    )

    st.markdown("### 產業柱狀圖（主榜 / 興櫃分開）")
    colA, colB = st.columns(2)

    with colA:
        st.markdown("**主榜：各產業漲停/觸及家數**")
        if sector_main_bar.empty:
            st.info("(empty)")
        else:
            st.bar_chart(sector_main_bar.set_index("產業")["家數"])

    with colB:
        st.markdown("**興櫃：各產業漲停/觸及家數**")
        if sector_emg_bar.empty:
            st.info("(empty)")
        else:
            st.bar_chart(sector_emg_bar.set_index("產業")["家數"])

    left, right = st.columns([1, 2])
    with left:
        st.markdown("### 產業排行榜（payload sector_summary）")
        if sector_df.empty:
            st.info("sector_summary 為空")
        else:
            st.dataframe(sector_df, use_container_width=True, height=520)

    with right:
        st.markdown("### 主榜（可點連結）")
        if main_df.empty:
            st.info("主榜為空")
        else:
            show = main_df[["代碼", "名稱", "產業", "漲幅%", "streak", "bar_date", "Yahoo", "財報狗", "鉅亨", "Wantgoo", "HiStock"]].copy()
            render_links_table(show, height=520)

        st.markdown("### 興櫃榜（可點連結）")
        if emerging_df.empty:
            st.info("興櫃榜為空")
        else:
            show2 = emerging_df[["代碼", "名稱", "產業", "漲幅%", "streak", "bar_date", "Yahoo", "財報狗", "鉅亨", "Wantgoo", "HiStock"]].copy()
            render_links_table(show2, height=360)


def render_tab_sector(
    *,
    sector_df: pd.DataFrame,
    main_df: pd.DataFrame,
    meta_df: pd.DataFrame,
    daily_lastprev: pd.DataFrame,
    candidate_ret_floor: float,
    candidate_ret_floor_pct: int,
):
    st.subheader("產業鑽研：主產業 + 產業候補")

    if sector_df.empty or "產業" not in sector_df.columns:
        st.info("沒有 sector_summary（或缺少『產業』欄），請先確認 payload 有輸出 sector_summary。")
        st.stop()

    sector_list = sector_df["產業"].astype(str).tolist()
    pick_sector = st.selectbox("選擇產業", sector_list, index=0 if sector_list else 0)

    st.markdown("### 主榜：本產業漲停/觸及（主榜）")
    if main_df.empty:
        st.info("主榜為空")
    else:
        sec_main = main_df[main_df["產業"].astype(str) == pick_sector].copy()
        if sec_main.empty:
            st.warning("主榜沒有這個產業的股票")
        else:
            render_links_table(
                sec_main[["代碼", "名稱", "產業", "漲幅%", "streak", "bar_date", "Yahoo", "財報狗", "鉅亨", "Wantgoo", "HiStock"]],
                height=420
            )

    st.markdown("### 產業候補：同產業未漲停（門檻用 %，且排除接近漲停）")
    if meta_df.empty or daily_lastprev.empty:
        st.info("缺少 meta 或 daily CSV，無法產生候補（請確認 data/tw_stock_list.json 與 data/cache/tw/tw_prices_1d_*.csv 存在）")
    else:
        limit_syms: Set[str] = set(main_df["symbol"].astype(str)) if (not main_df.empty and "symbol" in main_df.columns) else set()
        candidates = build_sector_candidates(
            meta_df,
            daily_lastprev,
            pick_sector,
            limit_syms,
            candidate_ret_floor=candidate_ret_floor,
        )
        if candidates.empty:
            st.warning("沒有符合門檻的產業候補（或該產業今天都很弱 / 接近漲停者被排除）")
        else:
            render_links_table(candidates, height=520)

    st.markdown("### 產業摘要（給影片/文案用）")
    sec_row = sector_df[sector_df["產業"].astype(str) == pick_sector].head(1)
    if not sec_row.empty:
        r = sec_row.iloc[0].to_dict()
        st.write(f"- 產業：**{pick_sector}**")
        st.write(f"- 鎖漲停：**{r.get('limitup_locked', 0)}**，觸及：**{r.get('limitup_touch', 0)}**，總數：**{r.get('count', 0)}**")
        st.write(f"- 候補門檻：**{candidate_ret_floor_pct}%**（且排除 ≥ 9.5% 接近漲停）")


def render_tab_prompts(
    *,
    payload: Dict[str, Any],
    sector_df: pd.DataFrame,
    main_df: pd.DataFrame,
    meta_df: pd.DataFrame,
    daily_lastprev: pd.DataFrame,
    prompts_dir: str,
    prompt_rows: int,
    candidate_ret_floor: float,
    candidate_ret_floor_pct: int,
):
    st.subheader("Prompt Studio（用檔案模板避免 token/字串爆炸）")

    tpl_name = st.selectbox("選擇模板", ["tw_market.md", "tw_sector.md", "tw_stock.md"], index=1)
    path = os.path.join(prompts_dir, tpl_name)
    if not os.path.exists(path):
        st.error(f"找不到模板檔：{path}")
        st.stop()

    template = open(path, "r", encoding="utf-8").read()

    top_sectors_md = df_to_md_table(sector_df.head(20), max_rows=min(prompt_rows, 50)) if not sector_df.empty else "(empty)"

    top_limitup_md = "(empty)"
    if not main_df.empty:
        df_tmp = main_df.copy()
        keep = [c for c in ["symbol", "代碼", "名稱", "產業", "漲幅%", "streak", "market_detail"] if c in df_tmp.columns]
        top_limitup_md = df_to_md_table(df_tmp[keep], max_rows=prompt_rows)

    pick_sector2 = st.selectbox("（可選）產業", sector_df["產業"].tolist(), index=0)
    limit_syms2: Set[str] = set(main_df["symbol"].astype(str)) if (not main_df.empty and "symbol" in main_df.columns) else set()

    candidates_md = "(empty)"
    if not meta_df.empty and not daily_lastprev.empty:
        candidates2 = build_sector_candidates(meta_df, daily_lastprev, pick_sector2, limit_syms2, candidate_ret_floor=candidate_ret_floor)
        if not candidates2.empty:
            candidates_md = df_to_md_table(candidates2, max_rows=prompt_rows)

    mapping = dict(
        ymd=payload.get("ymd", ""),
        slot=payload.get("slot", ""),
        asof=payload.get("asof", ""),
        generated_at=payload.get("generated_at", ""),
        sector=pick_sector2,
        sector_summary_table=top_sectors_md,
        limitup_table=top_limitup_md,
        candidates_table=candidates_md,
        candidate_ret_floor=candidate_ret_floor,
        candidate_ret_floor_pct=candidate_ret_floor_pct,
    )

    filled = safe_format(template, mapping)

    st.markdown("### 生成的 Prompt（可直接複製貼到 GPT/Claude）")
    st.code(filled, language="markdown")

    st.markdown("### 模板檔內容（可在 prompts/ 直接改，不用動程式）")
    st.code(template, language="markdown")

    with st.expander("🔎 目前可用的模板變數（mapping keys）"):
        st.write(sorted(list(mapping.keys())))


def render_tab_errors(errors_df: pd.DataFrame, payload: Dict[str, Any]):
    st.subheader("Errors / Debug")

    st.markdown("### errors（抓不到/缺資料/制度疑似日）")
    if errors_df is None or errors_df.empty:
        st.info("errors 為空")
    else:
        st.dataframe(errors_df, use_container_width=True, height=520)

    st.markdown("### payload 原始資訊")
    st.json({k: payload.get(k) for k in ["market", "ymd", "slot", "asof", "generated_at", "rules", "filters", "stats"]})
