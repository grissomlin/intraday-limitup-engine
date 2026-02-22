# markets/tw/builders.py
# -*- coding: utf-8 -*-
"""
TW builders
-----------
從 normalize 後的 dfS/dfE 與 limitup_df 建立：
- limitup (rows)
- sector_summary (main)
- peers_by_sector / peers_not_limitup
- open_limit_watchlist / open_limit_sector_summary

注意：
- builders 只做「組表/排序/統計」，不做 tick/limitup 判斷（那在 limitup_flags.py）
- 參數門檻統一從 config.py 讀取，避免散落 getenv

limit_type 命名（跨市場可共用概念）：
- standard   : 有明確漲跌幅限制（可算漲停價）
- no_limit   : 無漲跌幅限制（新上市/特殊制度/手動清單）
- open_limit : 開放式（例如興櫃：不做漲停價判斷，只做題材門檻）
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from .config import (
    NO_LIMIT_THEME_RET,
    EMERGING_STRONG_RET,
    PEERS_BY_SECTOR_CAP,
)


# =============================================================================
# Limitup rows
# =============================================================================
def build_limitup(dfS: pd.DataFrame) -> pd.DataFrame:
    """
    建立 limitup 清單：
    - standard: is_limitup_touch or is_limitup_locked
    - no_limit: ret >= NO_LIMIT_THEME_RET
    - open_limit: 不進主榜 limitup（只走 watchlist/題材池）

    並依照 locked > touch > no_limit > ret 排序
    同時產出：
    - limitup_status: locked / touch_only / no_limit_theme
    - status_text: 鎖死 / 昨無 / 昨X / 題材（只在原本為空時補）
    """
    if dfS is None or dfS.empty:
        return pd.DataFrame()

    # 確保欄位存在（避免上游漏欄）
    for c, dv in [
        ("limit_type", "standard"),
        ("is_limitup_touch", False),
        ("is_limitup_locked", False),
        ("ret", 0.0),
        ("status_text", ""),
        ("streak_prev", 0),
        ("sector", "未分類"),
        ("symbol", ""),
        ("name", ""),
        ("market_detail", ""),
        ("market_label", ""),
    ]:
        if c not in dfS.columns:
            dfS[c] = dv

    # normalize basic types
    dfS["limit_type"] = dfS["limit_type"].fillna("standard").astype(str).str.strip()
    dfS["is_limitup_touch"] = dfS["is_limitup_touch"].fillna(False).astype(bool)
    dfS["is_limitup_locked"] = dfS["is_limitup_locked"].fillna(False).astype(bool)
    dfS["ret"] = pd.to_numeric(dfS["ret"], errors="coerce").fillna(0.0)

    lt = dfS["limit_type"]

    # ✅ standard：觸及或鎖死
    in_standard = (lt == "standard") & (dfS["is_limitup_touch"] | dfS["is_limitup_locked"])

    # ✅ no_limit：用題材門檻（ret）
    in_no_limit = (lt == "no_limit") & (dfS["ret"] >= float(NO_LIMIT_THEME_RET))

    # ✅ open_limit：不進 limitup（只走 watchlist）
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

    # status_text 規則（只補空字串）
    df["status_text"] = df["status_text"].fillna("").astype(str)

    m_locked = df["status_text"].eq("") & df["is_limitup_locked"]
    df.loc[m_locked, "status_text"] = "鎖死"

    m_nolimit = df["status_text"].eq("") & (df["limit_type"] == "no_limit")
    df.loc[m_nolimit, "status_text"] = "題材"

    m_touch_only = (
        df["status_text"].eq("")
        & df["is_limitup_touch"]
        & (~df["is_limitup_locked"])
        & (df["limit_type"] == "standard")
    )

    sp = pd.to_numeric(df.get("streak_prev", 0), errors="coerce").fillna(0).astype(int)
    df.loc[m_touch_only & (sp <= 0), "status_text"] = "昨無"
    df.loc[m_touch_only & (sp > 0), "status_text"] = sp[m_touch_only & (sp > 0)].apply(lambda x: f"昨{x}")

    # 排序：locked > touch > no_limit > ret
    df["locked_i"] = df["is_limitup_locked"].astype(int)
    df["touch_i"] = df["is_limitup_touch"].astype(int)
    df["no_limit_i"] = (df["limit_type"] == "no_limit").astype(int)

    df = (
        df.sort_values(["locked_i", "touch_i", "no_limit_i", "ret"], ascending=[False, False, False, False])
        .drop(columns=["locked_i", "touch_i", "no_limit_i"])
    )
    return df


# =============================================================================
# Sector summary (main)
# =============================================================================
def build_sector_summary_main(limitup_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    sector_summary（主榜）：
    - locked_cnt: 鎖死數
    - touch_cnt : 觸及數（包含 locked，與你原本邏輯一致）
    - no_limit_cnt: no_limit 題材數（輸出 key 維持舊名，避免 UI 爆）
    - total_cnt/count: 合計
    排序：locked_cnt, touch_cnt, no_limit_cnt, total_cnt desc
    """
    if limitup_df is None or limitup_df.empty:
        return []

    df = limitup_df.copy()

    if "sector" not in df.columns:
        df["sector"] = "未分類"
    df["sector"] = df["sector"].fillna("").replace("", "未分類")

    if "is_limitup_locked" not in df.columns:
        df["is_limitup_locked"] = False
    if "is_limitup_touch" not in df.columns:
        df["is_limitup_touch"] = False
    if "limit_type" not in df.columns:
        df["limit_type"] = "standard"

    df["locked_cnt"] = df["is_limitup_locked"].fillna(False).astype(bool).astype(int)
    df["touch_cnt"] = df["is_limitup_touch"].fillna(False).astype(bool).astype(int)  # 含 locked

    # ✅ 內部先用「題材型」計數，最後輸出仍叫 no_limit_cnt（向下相容）
    df["_theme_cnt"] = (df["limit_type"].fillna("standard").astype(str).eq("no_limit")).astype(int)
    df["total_cnt"] = 1

    agg = (
        df.groupby("sector", as_index=False)[["locked_cnt", "touch_cnt", "_theme_cnt", "total_cnt"]]
        .sum()
        .sort_values(["locked_cnt", "touch_cnt", "_theme_cnt", "total_cnt"], ascending=False)
        .rename(columns={"_theme_cnt": "no_limit_cnt"})
    )
    agg["count"] = agg["total_cnt"]
    return agg.to_dict(orient="records")


# =============================================================================
# Peers by sector / flatten peers
# =============================================================================
def build_peers_by_sector(dfS: pd.DataFrame, limitup_df: pd.DataFrame) -> Dict[str, List[Dict[str, Any]]]:
    """
    peers_by_sector：只抓「有上榜產業」的同業未漲停（standard 且非 touch/locked）
    - 每產業最多 PEERS_BY_SECTOR_CAP 筆（依 ret desc）
    """
    out: Dict[str, List[Dict[str, Any]]] = {}
    if dfS is None or dfS.empty or limitup_df is None or limitup_df.empty:
        return out

    # sectors: 以 limitup_df 中出現的產業為準
    if "sector" not in limitup_df.columns:
        return out
    sectors = set(limitup_df["sector"].fillna("").replace("", "未分類").tolist())

    # 欄位保護
    for c, dv in [
        ("limit_type", "standard"),
        ("is_limitup_touch", False),
        ("is_limitup_locked", False),
        ("sector", "未分類"),
        ("ret", 0.0),
        ("symbol", ""),
        ("name", ""),
        ("market_detail", ""),
        ("market_label", ""),
    ]:
        if c not in dfS.columns:
            dfS[c] = dv

    lt = dfS["limit_type"].fillna("standard").astype(str)
    touch = dfS["is_limitup_touch"].fillna(False).astype(bool)
    locked = dfS["is_limitup_locked"].fillna(False).astype(bool)
    sector = dfS["sector"].fillna("").replace("", "未分類")

    # 只挑 standard 且未觸及/未鎖
    dfP = dfS[(lt == "standard") & (~touch) & (~locked) & (sector.isin(sectors))].copy()
    if dfP.empty:
        return out

    dfP["ret"] = pd.to_numeric(dfP.get("ret", 0.0), errors="coerce").fillna(-999.0)

    keep = ["symbol", "name", "sector", "market_detail", "market_label", "ret"]
    for c in keep:
        if c not in dfP.columns:
            dfP[c] = "" if c != "ret" else -999.0

    for sec, g in dfP.groupby("sector", sort=False):
        out[sec] = g.sort_values("ret", ascending=False)[keep].to_dict(orient="records")[: int(PEERS_BY_SECTOR_CAP)]

    return out


def flatten_peers(peers_by_sector: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    peers_not_limitup：把 peers_by_sector 打平成單一 list 並依 ret desc 排序
    """
    flat: List[Dict[str, Any]] = []
    if not isinstance(peers_by_sector, dict):
        return flat

    for _, rows in peers_by_sector.items():
        if not rows:
            continue
        for r in rows:
            if isinstance(r, dict):
                flat.append(r)

    def _k(x):
        try:
            return float(x.get("ret", -999.0) or -999.0)
        except Exception:
            return -999.0

    flat.sort(key=_k, reverse=True)
    return flat


# =============================================================================
# Open-limit watchlist (ex: Emerging) + sector summary
# =============================================================================
def build_open_limit_watchlist(dfE: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    open_limit_watchlist（原 emerging_watchlist）：
    - 只取 dfE ret >= EMERGING_STRONG_RET
    - limit_type 固定 open_limit
    - status_text 固定 題材
    """
    if dfE is None or dfE.empty:
        return []

    df = dfE.copy()
    df["ret"] = pd.to_numeric(df.get("ret", 0.0), errors="coerce").fillna(0.0)
    df = df[df["ret"] >= float(EMERGING_STRONG_RET)].sort_values("ret", ascending=False)
    if df.empty:
        return []

    df["limit_type"] = "open_limit"
    df["is_limitup_touch"] = False
    df["is_limitup_locked"] = False
    df["status_text"] = "題材"

    keep = [
        "symbol",
        "name",
        "sector",
        "market_detail",
        "market_label",
        "bar_date",
        "ret",
        "limit_type",
        "is_limitup_touch",
        "is_limitup_locked",
        "status_text",
    ]
    for c in keep:
        if c not in df.columns:
            df[c] = ""

    return df[keep].to_dict(orient="records")


# 向下相容：舊函數名（不想立刻改 aggregator 的話，先保留 alias）
def build_emerging_watchlist(dfE: pd.DataFrame) -> List[Dict[str, Any]]:
    return build_open_limit_watchlist(dfE)


def build_sector_summary_open_limit(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    open_limit_sector_summary（原 emerging_sector_summary）：
    - sector: count / avg_ret / max_ret
    """
    if not rows:
        return []

    df = pd.DataFrame(rows)
    df["sector"] = df.get("sector", "").fillna("").replace("", "未分類")
    df["ret"] = pd.to_numeric(df.get("ret", 0.0), errors="coerce").fillna(0.0)

    agg = (
        df.groupby("sector", as_index=False)
        .agg(count=("symbol", "count"), avg_ret=("ret", "mean"), max_ret=("ret", "max"))
        .sort_values(["count", "avg_ret"], ascending=False)
    )
    return agg.to_dict(orient="records")


# 向下相容：舊函數名 alias
def build_sector_summary_emerging(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return build_sector_summary_open_limit(rows)
