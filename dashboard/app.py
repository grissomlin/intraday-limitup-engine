# dashboard/app.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import pandas as pd
import streamlit as st

# ✅ 重要：components/__init__.py 必須是「極簡」，不能亂 import
from components.paths import ROOT_DIR, DATA_DIR, TW_CACHE_DIR, PROMPTS_DIR, STOCKLIST_FILE
from components.io_cache import (
    list_days_slots,
    find_latest_payloads,
    find_latest_daily_csv,
    load_payload,
    safe_read_stocklist,
    load_daily_csv,
    latest_and_prev_daily,
)
from components.tw_links import add_link_columns
from components.charts import build_sector_bar_df

# ✅ pages：用 render_page（不是 render_tab_xxx）
from components.pages.overview import render_page as render_tab_overview
from components.pages.sector import render_page as render_tab_sector
from components.pages.prompts_page import render_page as render_tab_prompts
from components.pages.errors import render_page as render_tab_errors


st.set_page_config(page_title="盤中漲停板儀表板 - TW", layout="wide")

st.sidebar.title("設定")
st.sidebar.caption(f"ROOT_DIR: {ROOT_DIR}")
st.sidebar.caption(f"TW_CACHE_DIR: {TW_CACHE_DIR}")

days, day2slots = list_days_slots()
latest_payloads = find_latest_payloads()

if (not days) and latest_payloads:
    p = latest_payloads[0]
    day = os.path.basename(os.path.dirname(p))
    slot = os.path.basename(p).replace(".payload.json", "")
    days = [day]
    day2slots = {day: [slot]}

if not days:
    st.error(
        "找不到任何 payload.json。\n\n"
        "請在專案根目錄先跑：\n"
        "python main.py --market tw --slot midday --asof 11:00\n\n"
        f"目前掃描路徑：{TW_CACHE_DIR}"
    )
    st.stop()

day = st.sidebar.selectbox("選擇日期", days, index=0)
slot = st.sidebar.selectbox("選擇 slot", day2slots.get(day, ["midday"]), index=0)

candidate_ret_floor_pct = st.sidebar.slider(
    "產業候補門檻（同產業未漲停）",
    0, 20, 3, 1,
    help="用『漲幅%』篩選產業候補，例如 3% 代表 ret>=0.03；候補會排除接近漲停(>=9.5%)",
)
candidate_ret_floor = candidate_ret_floor_pct / 100.0

prompt_rows = st.sidebar.slider("Prompt 表格最大列數", 5, 80, 30, 5)

payload = load_payload(day, slot)

st.sidebar.markdown("---")
daily_csv_path = find_latest_daily_csv()
st.sidebar.caption(f"daily CSV: {os.path.basename(daily_csv_path) if daily_csv_path else '(missing)'}")

meta_df = safe_read_stocklist(STOCKLIST_FILE)

daily_lastprev = None
if daily_csv_path:
    daily_df = load_daily_csv(daily_csv_path)
    daily_lastprev = latest_and_prev_daily(daily_df)

stats = payload.get("stats", {}) or {}
limitup_rows = payload.get("limitup", []) or []
sector_rows = payload.get("sector_summary", []) or []
errors_rows = payload.get("errors", []) or []

limitup_df = pd.DataFrame(limitup_rows)
sector_df = pd.DataFrame(sector_rows)
errors_df = pd.DataFrame(errors_rows)

if (not sector_df.empty) and ("sector" in sector_df.columns) and ("產業" not in sector_df.columns):
    sector_df = sector_df.rename(columns={"sector": "產業"})
if (not limitup_df.empty) and ("sector" in limitup_df.columns) and ("產業" not in limitup_df.columns):
    limitup_df = limitup_df.rename(columns={"sector": "產業"})

main_df = pd.DataFrame()
emerging_df = pd.DataFrame()
if not limitup_df.empty:
    if "market_detail" not in limitup_df.columns:
        limitup_df["market_detail"] = ""
    md = limitup_df["market_detail"].astype(str).str.lower()
    main_df = limitup_df[~md.eq("emerging")].copy()
    emerging_df = limitup_df[md.eq("emerging")].copy()

def with_ret_pct(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    if "ret" in out.columns:
        out["漲幅%"] = (pd.to_numeric(out["ret"], errors="coerce") * 100).round(2)
    return out

main_df = with_ret_pct(main_df)
emerging_df = with_ret_pct(emerging_df)

if not main_df.empty:
    main_df = add_link_columns(main_df)
if not emerging_df.empty:
    emerging_df = add_link_columns(emerging_df)

sector_main_bar, sector_emg_bar = build_sector_bar_df(limitup_df)

tab_overview, tab_sector, tab_prompts, tab_errors = st.tabs(["總覽", "產業鑽研", "Prompt Studio", "Errors/Debug"])

with tab_overview:
    render_tab_overview(
        payload=payload,
        stats=stats,
        sector_df=sector_df,
        main_df=main_df,
        emerging_df=emerging_df,
        errors_rows=errors_rows,
        sector_main_bar=sector_main_bar,
        sector_emg_bar=sector_emg_bar,
    )

with tab_sector:
    render_tab_sector(
        sector_df=sector_df,
        main_df=main_df,
        meta_df=meta_df,
        daily_lastprev=daily_lastprev,
        candidate_ret_floor=candidate_ret_floor,
        candidate_ret_floor_pct=candidate_ret_floor_pct,
    )

with tab_prompts:
    render_tab_prompts(
        payload=payload,
        sector_df=sector_df,
        main_df=main_df,
        meta_df=meta_df,
        daily_lastprev=daily_lastprev,
        prompts_dir=PROMPTS_DIR,
        prompt_rows=prompt_rows,
        candidate_ret_floor=candidate_ret_floor,
        candidate_ret_floor_pct=candidate_ret_floor_pct,
    )

with tab_errors:
    render_tab_errors(errors_df=errors_df, payload=payload)
