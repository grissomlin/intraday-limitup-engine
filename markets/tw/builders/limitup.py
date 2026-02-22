# markets/tw/builders/limitup.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd

from ._common import _coalesce_int, _ensure_cols

from ..config import NO_LIMIT_THEME_RET


def build_limitup(dfS: pd.DataFrame) -> pd.DataFrame:
    """
    建立 limitup 清單：
    - standard: is_limitup_touch or is_limitup_locked
    - no_limit: ret >= NO_LIMIT_THEME_RET
    - open_limit: 不進主榜 limitup（只走 watchlist/題材池）

    並依照 locked > touch > no_limit > ret 排序
    同時產出：
    - limitup_status: locked / touch_only / no_limit_theme
    - status_text:
        - locked: X連漲停(>=2) / 漲停鎖死
        - touch_only: 昨無 / 昨X
        - no_limit: 題材（只在原本為空時補）
    """
    if dfS is None or dfS.empty:
        return pd.DataFrame()

    _ensure_cols(
        dfS,
        [
            ("limit_type", "standard"),
            ("is_limitup_touch", False),
            ("is_limitup_locked", False),
            ("ret", 0.0),
            ("status_text", ""),
            ("streak_prev", 0),
            ("streak", 0),
            ("sector", "未分類"),
            ("symbol", ""),
            ("name", ""),
            ("market_detail", ""),
            ("market_label", ""),
        ],
    )

    dfS = dfS.copy()

    dfS["limit_type"] = dfS["limit_type"].fillna("standard").astype(str).str.strip()
    dfS["is_limitup_touch"] = dfS["is_limitup_touch"].fillna(False).astype(bool)
    dfS["is_limitup_locked"] = dfS["is_limitup_locked"].fillna(False).astype(bool)
    dfS["ret"] = pd.to_numeric(dfS["ret"], errors="coerce").fillna(0.0)

    # streak: allow alias columns
    dfS["streak"] = _coalesce_int(dfS, ["streak", "streak_today", "streak_now"], default=0)
    dfS["streak_prev"] = _coalesce_int(dfS, ["streak_prev", "streak_yesterday"], default=0)

    lt = dfS["limit_type"]

    in_standard = (lt == "standard") & (dfS["is_limitup_touch"] | dfS["is_limitup_locked"])
    in_no_limit = (lt == "no_limit") & (dfS["ret"] >= float(NO_LIMIT_THEME_RET))

    df = dfS[in_standard | in_no_limit].copy()
    if df.empty:
        return df

    def _status(row) -> str:
        _lt = row.get("limit_type")
        if _lt == "no_limit":
            return "no_limit_theme"
        if bool(row.get("is_limitup_locked")):
            return "locked"
        if bool(row.get("is_limitup_touch")):
            return "touch_only"
        return ""

    df["limitup_status"] = df.apply(_status, axis=1)

    # status_text fill only if empty
    df["status_text"] = df["status_text"].fillna("").astype(str)

    # locked
    m_locked = df["status_text"].eq("") & df["is_limitup_locked"]
    st = pd.to_numeric(df.get("streak", 0), errors="coerce").fillna(0).astype(int)
    df.loc[m_locked & (st >= 2), "status_text"] = st[m_locked & (st >= 2)].apply(lambda x: f"{x}連漲停")
    df.loc[m_locked & (st < 2), "status_text"] = "漲停鎖死"

    # no_limit
    m_nolimit = df["status_text"].eq("") & (df["limit_type"] == "no_limit")
    df.loc[m_nolimit, "status_text"] = "題材"

    # touch_only
    m_touch_only = (
        df["status_text"].eq("")
        & df["is_limitup_touch"]
        & (~df["is_limitup_locked"])
        & (df["limit_type"] == "standard")
    )
    sp = pd.to_numeric(df.get("streak_prev", 0), errors="coerce").fillna(0).astype(int)
    df.loc[m_touch_only & (sp <= 0), "status_text"] = "昨無"
    df.loc[m_touch_only & (sp > 0), "status_text"] = sp[m_touch_only & (sp > 0)].apply(lambda x: f"昨{x}")

    # sort
    df["locked_i"] = df["is_limitup_locked"].astype(int)
    df["touch_i"] = df["is_limitup_touch"].astype(int)
    df["no_limit_i"] = (df["limit_type"] == "no_limit").astype(int)

    df = (
        df.sort_values(["locked_i", "touch_i", "no_limit_i", "ret"], ascending=[False, False, False, False])
        .drop(columns=["locked_i", "touch_i", "no_limit_i"])
    )
    return df
