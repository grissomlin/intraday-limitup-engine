# markets/tw/snapshot.py
# -*- coding: utf-8 -*-
"""
TW snapshot normalize
---------------------
把 downloader 產出的 RAW snapshot(list[dict]) 正規化成 DataFrame，提供 aggregator glue 使用。

本檔提供：
- normalize_snapshot_main   : 主榜快照 -> dfS
- normalize_snapshot_open   : 開放制度快照(原 emerging) -> dfO
- extract_effective_ymd     : 從 dfS/dfO 推導有效交易日
- is_snapshot_effectively_empty : 判斷快照是否等同於空盤/未開盤/無資料

向下相容：
- normalize_snapshot_emerging  : alias -> normalize_snapshot_open
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, List

import pandas as pd


# =============================================================================
# Small helpers
# =============================================================================
def _safe_str(x: Any, default: str = "") -> str:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return default
    except Exception:
        pass
    s = str(x).strip()
    return s if s else default


def _safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        if pd.isna(x):
            return default
    except Exception:
        pass
    try:
        return float(x)
    except Exception:
        return default


def _coerce_ymd(s: Any) -> str:
    """取 YYYY-MM-DD（吃 bar_date/date 這種欄）"""
    ss = _safe_str(s, "")
    return ss[:10] if len(ss) >= 10 else ""


def _ensure_cols(df: pd.DataFrame, defaults: List[Tuple[str, Any]]) -> pd.DataFrame:
    for c, dv in defaults:
        if c not in df.columns:
            df[c] = dv
    return df


def _normalize_common(df: pd.DataFrame) -> pd.DataFrame:
    """
    共通欄位清理：型別/缺欄/空值
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # 欄位保護（你 downstream 會用到的）
    df = _ensure_cols(
        df,
        defaults=[
            ("symbol", ""),
            ("name", ""),
            ("sector", "未分類"),
            ("market", ""),
            ("market_detail", ""),
            ("market_label", ""),
            ("bar_date", ""),
            ("prev_close", None),
            ("open", None),
            ("high", None),
            ("low", None),
            ("close", None),
            ("volume", None),
            ("ret", 0.0),
            ("limit_type", "standard"),
            ("status_text", ""),
            ("streak", 0),
            ("streak_prev", 0),
            ("prev_was_limitup_locked", False),
            # tick-based flags (aggregator 之後會算，但先保護欄位)
            ("is_limitup_touch", False),
            ("is_limitup_locked", False),
            ("limit_up_price", None),
        ],
    )

    # string normalize
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).fillna("")
    df["sector"] = df["sector"].astype(str).fillna("").replace("", "未分類")
    df["market_detail"] = df["market_detail"].astype(str).fillna("").str.strip()
    df["market_label"] = df["market_label"].astype(str).fillna("").str.strip()
    df["limit_type"] = df["limit_type"].astype(str).fillna("standard").str.strip()
    df["status_text"] = df["status_text"].astype(str).fillna("")

    # dates
    df["bar_date"] = df["bar_date"].apply(_coerce_ymd)

    # numeric normalize
    for c in ["prev_close", "open", "high", "low", "close", "volume", "ret"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # bool normalize
    for c in ["is_limitup_touch", "is_limitup_locked", "prev_was_limitup_locked"]:
        df[c] = df[c].fillna(False).astype(bool)

    # streak ints
    for c in ["streak", "streak_prev"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    # drop empty symbols
    df = df[df["symbol"].astype(str).str.strip().ne("")]
    df = df.reset_index(drop=True)
    return df


# =============================================================================
# Public normalize APIs
# =============================================================================
def normalize_snapshot_main(snapshot_main: Any) -> pd.DataFrame:
    """
    主榜快照（listed/otc/innovation/dr...） -> dfS
    """
    if not snapshot_main:
        return pd.DataFrame()

    df = pd.DataFrame(snapshot_main if isinstance(snapshot_main, list) else [])
    if df.empty:
        return pd.DataFrame()

    df = _normalize_common(df)

    # 主榜 limit_type 預設 standard（downloader 可能已填 no_limit，保留）
    df["limit_type"] = df["limit_type"].fillna("standard").astype(str).str.strip()
    df.loc[df["limit_type"].eq(""), "limit_type"] = "standard"
    return df


def normalize_snapshot_open(snapshot_open: Any) -> pd.DataFrame:
    """
    開放制度快照（原 emerging） -> dfO
    這裡不做漲停價判斷，只讓 builders 做「題材門檻」的 watchlist。
    """
    if not snapshot_open:
        return pd.DataFrame()

    df = pd.DataFrame(snapshot_open if isinstance(snapshot_open, list) else [])
    if df.empty:
        return pd.DataFrame()

    df = _normalize_common(df)

    # open 的 limit_type 統一成 open_limit（即便 downloader 給 emerging_no_limit 也統一）
    # 這樣跨市場概念一致：open_limit = 不做漲停價、只看門檻
    df["limit_type"] = "open_limit"

    # open 不該有 touch/locked（避免誤用）
    df["is_limitup_touch"] = False
    df["is_limitup_locked"] = False
    df["limit_up_price"] = None
    return df


# 向下相容：舊函數名
def normalize_snapshot_emerging(snapshot_emerging: Any) -> pd.DataFrame:
    return normalize_snapshot_open(snapshot_emerging)


# =============================================================================
# Meta helpers for aggregator
# =============================================================================
def extract_effective_ymd(dfS: pd.DataFrame, dfO: pd.DataFrame) -> str:
    """
    優先取主榜 dfS 的 bar_date（因為主榜最能代表「是否開盤」）
    取不到才退到 dfO。
    """
    # dfS first
    if dfS is not None and not dfS.empty and "bar_date" in dfS.columns:
        s = dfS["bar_date"].dropna().astype(str)
        s = s[s.str.len() >= 10]
        if not s.empty:
            # 取眾數/最大值都可；用最大值較直覺
            return str(s.max())[:10]

    # dfO fallback
    if dfO is not None and not dfO.empty and "bar_date" in dfO.columns:
        s = dfO["bar_date"].dropna().astype(str)
        s = s[s.str.len() >= 10]
        if not s.empty:
            return str(s.max())[:10]

    return ""


def is_snapshot_effectively_empty(dfS: pd.DataFrame) -> bool:
    """
    判斷主榜快照是否「等同於空」：
    - dfS 空
    - 或 symbol 幾乎沒有有效 close/prev_close
    """
    if dfS is None or dfS.empty:
        return True

    if "close" not in dfS.columns or "prev_close" not in dfS.columns:
        return True

    close = pd.to_numeric(dfS["close"], errors="coerce")
    prev = pd.to_numeric(dfS["prev_close"], errors="coerce")

    # 有效定義：close 與 prev_close 皆 > 0
    m_valid = (close.notna() & prev.notna() & (close > 0) & (prev > 0))
    return int(m_valid.sum()) == 0
