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

✅ 2026-02 修正（跨市場相容）：
- 接受 rows 使用 ymd/date 取代 bar_date（KR/CN/JP 常見）
- 接受 rows 使用 last_close 取代 prev_close（KR/US 常見）
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


def _coerce_ymd(s: Any) -> str:
    """取 YYYY-MM-DD（吃 bar_date/ymd/date/index 這種欄）"""
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
    # ✅ 加入 ymd / date / last_close 讓跨市場資料能順利映射到 bar_date / prev_close
    df = _ensure_cols(
        df,
        defaults=[
            ("symbol", ""),
            ("name", ""),
            ("sector", "未分類"),
            ("market", ""),
            ("market_detail", ""),
            ("market_label", ""),
            # date aliases
            ("bar_date", ""),
            ("ymd", ""),
            ("date", ""),
            # prev close aliases
            ("prev_close", None),
            ("last_close", None),
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

    # -------------------------
    # string normalize
    # -------------------------
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).fillna("")
    df["sector"] = df["sector"].astype(str).fillna("").replace("", "未分類")
    df["market_detail"] = df["market_detail"].astype(str).fillna("").str.strip()
    df["market_label"] = df["market_label"].astype(str).fillna("").str.strip()
    df["limit_type"] = df["limit_type"].astype(str).fillna("standard").str.strip()
    df["status_text"] = df["status_text"].astype(str).fillna("")

    # -------------------------
    # dates normalize
    # -------------------------
    df["bar_date"] = df["bar_date"].apply(_coerce_ymd)
    df["ymd"] = df["ymd"].apply(_coerce_ymd)
    df["date"] = df["date"].apply(_coerce_ymd)

    # ✅ 如果 bar_date 缺失，用 ymd/date 補
    # （KR payload 通常是 ymd）
    m_empty_bd = df["bar_date"].astype(str).str.len().lt(10)
    if "ymd" in df.columns:
        m_has_ymd = df["ymd"].astype(str).str.len().ge(10)
        df.loc[m_empty_bd & m_has_ymd, "bar_date"] = df.loc[m_empty_bd & m_has_ymd, "ymd"]
        m_empty_bd = df["bar_date"].astype(str).str.len().lt(10)
    if "date" in df.columns:
        m_has_date = df["date"].astype(str).str.len().ge(10)
        df.loc[m_empty_bd & m_has_date, "bar_date"] = df.loc[m_empty_bd & m_has_date, "date"]

    # -------------------------
    # numeric normalize
    # -------------------------
    for c in ["prev_close", "last_close", "open", "high", "low", "close", "volume", "ret"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # ✅ 如果 prev_close 缺失，用 last_close 補
    # （KR payload 通常是 last_close）
    if "last_close" in df.columns:
        m_prev_nan = df["prev_close"].isna()
        m_last_ok = df["last_close"].notna()
        df.loc[m_prev_nan & m_last_ok, "prev_close"] = df.loc[m_prev_nan & m_last_ok, "last_close"]

    # -------------------------
    # bool normalize
    # -------------------------
    for c in ["is_limitup_touch", "is_limitup_locked", "prev_was_limitup_locked"]:
        df[c] = df[c].fillna(False).astype(bool)

    # -------------------------
    # streak ints
    # -------------------------
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
    優先取主榜 dfS 的 bar_date（主榜最能代表「是否開盤」）
    取不到才退到 dfO。
    """
    # dfS first
    if dfS is not None and not dfS.empty and "bar_date" in dfS.columns:
        s = dfS["bar_date"].dropna().astype(str)
        s = s[s.str.len() >= 10]
        if not s.empty:
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

    if "close" not in dfS.columns:
        return True

    # ✅ prev_close 若不存在，也允許用 last_close（因為 normalize 會補到 prev_close）
    if "prev_close" not in dfS.columns and "last_close" not in dfS.columns:
        return True

    close = pd.to_numeric(dfS.get("close"), errors="coerce")
    prev = pd.to_numeric(dfS.get("prev_close"), errors="coerce")

    m_valid = (close.notna() & prev.notna() & (close > 0) & (prev > 0))
    return int(m_valid.sum()) == 0
