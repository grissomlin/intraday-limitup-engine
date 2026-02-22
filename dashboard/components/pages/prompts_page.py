# dashboard/components/pages/prompts_page.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Dict, Any, Set
import pandas as pd
import streamlit as st

from ..formatters import df_to_md_table, safe_format
from ..tw_candidates import build_sector_candidates


def render_page(
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

    top_sectors_md = df_to_md_table(
        sector_df.head(20),
        max_rows=min(prompt_rows, 50),
    ) if (sector_df is not None and not sector_df.empty) else "(empty)"

    top_limitup_md = "(empty)"
    if main_df is not None and not main_df.empty:
        df_tmp = main_df.copy()
        keep = [c for c in ["symbol", "代碼", "名稱", "產業", "漲幅%", "streak", "market_detail"] if c in df_tmp.columns]
        top_limitup_md = df_to_md_table(df_tmp[keep], max_rows=prompt_rows)

    pick_sector2 = st.selectbox("（可選）產業", sector_df["產業"].tolist() if sector_df is not None and not sector_df.empty else [], index=0)
    limit_syms2: Set[str] = set(main_df["symbol"].astype(str)) if (main_df is not None and not main_df.empty and "symbol" in main_df.columns) else set()

    candidates_md = "(empty)"
    if meta_df is not None and not meta_df.empty and daily_lastprev is not None and not daily_lastprev.empty and pick_sector2:
        candidates2 = build_sector_candidates(meta_df, daily_lastprev, pick_sector2, limit_syms2, candidate_ret_floor=candidate_ret_floor)
        if candidates2 is not None and not candidates2.empty:
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
