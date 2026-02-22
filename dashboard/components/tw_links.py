# dashboard/components/tw_links.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd


def tw_market_label(market_detail: str) -> str:
    md = (market_detail or "").strip().lower()
    mapping = {
        "listed": "上市",
        "otc": "上櫃",
        "emerging": "興櫃",
        "etf": "ETF",
        "dr": "DR",
        "innovation": "創新板",
        "innovation_a": "創新板A",
        "innovation_c": "創新板C",
        "tw_innovation": "創新板C",
        "otc_innovation": "創新板A",
    }
    return mapping.get(md, (market_detail or ""))


def tw_symbol_to_code(symbol: str) -> str:
    s = (symbol or "").strip()
    if "." in s:
        return s.split(".")[0]
    return s


def add_link_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    產出可點的 URL 欄位（搭配 st.data_editor LinkColumn）
    需要 df 內有 symbol 欄（或代碼欄）。
    """
    if df is None or df.empty:
        return df

    df = df.copy()

    if "symbol" not in df.columns:
        if "代碼" in df.columns:
            df["symbol"] = df["代碼"].astype(str)
        else:
            df["symbol"] = ""

    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["代碼"] = df["symbol"].apply(tw_symbol_to_code)

    # 台灣常用站台（用代碼）
    df["Yahoo"] = df["代碼"].apply(lambda c: f"https://tw.stock.yahoo.com/quote/{c}")
    df["財報狗"] = df["代碼"].apply(lambda c: f"https://statementdog.com/analysis/{c}")
    df["鉅亨"] = df["代碼"].apply(lambda c: f"https://www.cnyes.com/twstock/{c}")
    df["Wantgoo"] = df["代碼"].apply(lambda c: f"https://www.wantgoo.com/stock/{c}/technical-chart")
    df["HiStock"] = df["代碼"].apply(lambda c: f"https://histock.tw/stock/{c}")

    return df
