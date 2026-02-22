# markets/tw/builders/open_limit.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from ._common import (
    _add_ret_fields,
    _ensure_cols,
    _norm_md,
    _normalize_open_limit_identity_df,
    _normalize_sector,
    _sanitize_nan,
)

from ..config import EMERGING_STRONG_RET


# =============================================================================
# Open-limit merge helpers (for sector pages / summaries)
# =============================================================================
def normalize_open_limit_rows(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    把 open_limit_watchlist 的 rows 轉成 DataFrame，並補齊：
    - limit_type=open_limit
    - market_detail/market_label 正規化
    - ret/ret_high/ret_pct...
    - flags: is_surge10_touch/locked/opened
    - status_text: 10%+ / 觸及10%
    """
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).copy()

    _ensure_cols(
        df,
        [
            ("symbol", ""),
            ("name", ""),
            ("sector", "未分類"),
            ("market_detail", "emerging"),
            ("market_label", "興櫃"),
            ("bar_date", ""),
            ("prev_close", None),
            ("open", None),
            ("high", None),
            ("low", None),
            ("close", None),
            ("volume", None),
            ("ret", 0.0),
            ("ret_high", 0.0),
            ("ret_pct", 0.0),
            ("ret_high_pct", 0.0),
            ("limit_type", "open_limit"),
            ("sub_limit_type", None),
            ("is_open_limit_board", True),
            ("is_limitup_touch", False),
            ("is_limitup_locked", False),
            ("is_surge10_touch", False),
            ("is_surge10_locked", False),
            ("is_surge10_opened", False),
            ("status_text", ""),
        ],
    )

    df["sector"] = _normalize_sector(df["sector"])
    df["market_detail"] = df["market_detail"].apply(_norm_md)
    df["market_label"] = df["market_label"].fillna("興櫃").astype(str).str.strip()
    df.loc[df["market_label"].eq(""), "market_label"] = "興櫃"

    # force open_limit identity
    df = _normalize_open_limit_identity_df(df)

    # ret fields
    df["ret"] = pd.to_numeric(df.get("ret", 0.0), errors="coerce").fillna(0.0)
    df["ret_high"] = pd.to_numeric(df.get("ret_high", 0.0), errors="coerce").fillna(0.0)
    df["ret_pct"] = pd.to_numeric(df.get("ret_pct", df["ret"] * 100.0), errors="coerce").fillna(df["ret"] * 100.0)
    df["ret_high_pct"] = pd.to_numeric(
        df.get("ret_high_pct", df["ret_high"] * 100.0), errors="coerce"
    ).fillna(df["ret_high"] * 100.0)

    for c in ["is_surge10_touch", "is_surge10_locked", "is_surge10_opened"]:
        if c not in df.columns:
            df[c] = False
        df[c] = df[c].fillna(False).astype(bool)

    # ✅ status_text (bugfix): df.get(...,"") 會回傳 str，不能 fillna
    if "status_text" not in df.columns:
        df["status_text"] = ""
    df["status_text"] = df["status_text"].fillna("").astype(str)

    m_empty = df["status_text"].eq("")
    df.loc[m_empty & (df["is_surge10_locked"] == True), "status_text"] = "10%+"
    df.loc[m_empty & (df["is_surge10_opened"] == True), "status_text"] = "觸及10%"

    # sort: locked first, then opened, then ret desc
    df["locked_i"] = df["is_surge10_locked"].astype(int)
    df["opened_i"] = df["is_surge10_opened"].astype(int)
    df = df.sort_values(["locked_i", "opened_i", "ret"], ascending=[False, False, False]).drop(
        columns=["locked_i", "opened_i"]
    )

    return df


# =============================================================================
# Open-limit watchlist (ex: Emerging) + sector summary
# =============================================================================
def build_open_limit_watchlist(dfE: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    open_limit_watchlist（原 emerging_watchlist）：

    "touched" 不是漲停價 touched（主榜），而是「10%門檻」touched（美國 movers 風格）：
      - is_surge10_touch  : ret_high >= threshold  （盤中曾 >=10%）
      - is_surge10_locked : ret >= threshold       （收盤仍 >=10%）
      - is_surge10_opened : touch & ~locked        （觸及10%但沒守住）

    收錄規則：
      - 只取「觸及10%」：ret_high >= EMERGING_STRONG_RET
        （可抓到 "上去又掉下來" 的興櫃）

    欄位：
      - limit_type 固定 open_limit
      - is_limitup_* 固定 False（避免跟主榜漲停邏輯混用）
      - is_open_limit_board 固定 True
      - status_text:
          - "10%+"    (locked)
          - "觸及10%" (touch only)
    """
    if dfE is None or dfE.empty:
        return []

    df = dfE.copy()

    # enforce open-limit identity + compute ret fields
    df = _normalize_open_limit_identity_df(df)
    df = _add_ret_fields(df)

    th = float(EMERGING_STRONG_RET)

    r_high = pd.to_numeric(df.get("ret_high", 0.0), errors="coerce").fillna(0.0)
    r_close = pd.to_numeric(df.get("ret", 0.0), errors="coerce").fillna(0.0)

    df["is_surge10_touch"] = (r_high >= th)
    df["is_surge10_locked"] = (r_close >= th)
    df["is_surge10_opened"] = (df["is_surge10_touch"] == True) & (df["is_surge10_locked"] == False)

    # include: touch (includes locked)
    df = df[df["is_surge10_touch"] == True].copy()
    if df.empty:
        return []

    # sort: locked first, then opened, then ret desc
    df["locked_i"] = df["is_surge10_locked"].astype(int)
    df["opened_i"] = df["is_surge10_opened"].astype(int)
    df = df.sort_values(["locked_i", "opened_i", "ret"], ascending=[False, False, False]).drop(
        columns=["locked_i", "opened_i"]
    )

    df["is_limitup_touch"] = False
    df["is_limitup_locked"] = False

    # ✅ status_text (bugfix)
    if "status_text" not in df.columns:
        df["status_text"] = ""
    df["status_text"] = df["status_text"].fillna("").astype(str)

    m_empty = df["status_text"].eq("")
    df.loc[m_empty & (df["is_surge10_locked"] == True), "status_text"] = "10%+"
    df.loc[m_empty & (df["is_surge10_opened"] == True), "status_text"] = "觸及10%"

    keep = [
        "symbol",
        "name",
        "sector",
        "market_detail",
        "market_label",
        "bar_date",
        "prev_close",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "ret",
        "ret_pct",
        "ret_high",
        "ret_high_pct",
        "limit_type",
        "sub_limit_type",
        "is_open_limit_board",
        "is_limitup_touch",
        "is_limitup_locked",
        "is_surge10_touch",
        "is_surge10_locked",
        "is_surge10_opened",
        "status_text",
    ]
    for c in keep:
        if c not in df.columns:
            df[c] = None

    rows = df[keep].to_dict(orient="records")
    return _sanitize_nan(rows)


# Backward-compat aliases
def build_emerging_watchlist(dfE: pd.DataFrame) -> List[Dict[str, Any]]:
    return build_open_limit_watchlist(dfE)


def build_sector_summary_open_limit(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    open_limit_sector_summary（原 emerging_sector_summary）：
    - 基本欄位（向下相容）：count / avg_ret / max_ret
    - 追加：touch_cnt / locked_cnt / opened_cnt
      （這裡 touch/locked/opened 都是「10%門檻」語意，不是漲停價）
    """
    if not rows:
        return []

    df = pd.DataFrame(rows)
    _ensure_cols(
        df,
        [
            ("sector", "未分類"),
            ("ret", 0.0),
            ("is_surge10_touch", False),
            ("is_surge10_locked", False),
            ("is_surge10_opened", False),
        ],
    )

    df["sector"] = _normalize_sector(df["sector"])
    df["ret"] = pd.to_numeric(df.get("ret", 0.0), errors="coerce").fillna(0.0)

    for c in ["is_surge10_touch", "is_surge10_locked", "is_surge10_opened"]:
        df[c] = df.get(c, False)
        df[c] = df[c].fillna(False).astype(bool)

    agg = (
        df.groupby("sector", as_index=False)
        .agg(
            count=("symbol", "count"),
            avg_ret=("ret", "mean"),
            max_ret=("ret", "max"),
            touch_cnt=("is_surge10_touch", "sum"),
            locked_cnt=("is_surge10_locked", "sum"),
            opened_cnt=("is_surge10_opened", "sum"),
        )
        .sort_values(["count", "avg_ret"], ascending=False)
    )
    out = agg.to_dict(orient="records")
    return _sanitize_nan(out)


def build_sector_summary_emerging(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return build_sector_summary_open_limit(rows)
