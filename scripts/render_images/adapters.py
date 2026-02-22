# scripts/render_images/adapters.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Dict, Any
import pandas as pd


# =============================================================================
# Market / status label helpers
# =============================================================================
def market_label(market_detail: str) -> str:
    """
    將 payload 的 market_detail 轉成人類可讀文字
    """
    md = (market_detail or "").strip().lower()
    mapping = {
        "listed": "上市",
        "otc": "上櫃",
        "emerging": "興櫃",
        "innovation_a": "創新板",
        "innovation_c": "創新板",
        "dr": "DR",
    }
    return mapping.get(md, "上市/上櫃")


def status_label(limitup_status: str) -> str:
    """
    將內部狀態轉為顯示文字
    """
    s = (limitup_status or "").strip().lower()
    if s == "locked":
        return "鎖死"
    if s == "touch_only":
        return "打開"
    if s == "no_limit_theme":
        return "題材"
    return "—"


# =============================================================================
# Limit-up adapter
# =============================================================================
def limitup_df_from_payload(payload: Dict[str, Any]) -> pd.DataFrame:
    """
    從 payload 取出「漲停股」並正規化成 DataFrame

    期望欄位（最終）：
    - sector
    - symbol
    - name
    - market_detail
    - limitup_status
    - streak
    """
    rows = payload.get("limitup", [])
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # -------- 欄位相容處理 --------
    if "sector" not in df.columns:
        df["sector"] = df.get("產業", "未分類")

    if "symbol" not in df.columns:
        df["symbol"] = df.get("代碼", "")

    if "name" not in df.columns:
        df["name"] = df.get("名稱", "")

    if "market_detail" not in df.columns:
        df["market_detail"] = ""

    if "streak" not in df.columns:
        df["streak"] = 0

    # -------- 狀態推斷（保底） --------
    if "limitup_status" not in df.columns:
        locked = df.get("is_limitup_locked", False)
        touch = df.get("is_limitup_touch", False)
        limit_type = df.get("limit_type", "")

        def _infer_status(i: int) -> str:
            if str(limit_type.iloc[i]).lower() == "no_limit":
                return "no_limit_theme"
            if bool(locked.iloc[i]):
                return "locked"
            if bool(touch.iloc[i]):
                return "touch_only"
            return "other"

        df["limitup_status"] = [_infer_status(i) for i in range(len(df))]

    # -------- 正規化型別 --------
    df["sector"] = df["sector"].fillna("").replace("", "未分類").astype(str)
    df["symbol"] = df["symbol"].fillna("").astype(str)
    df["name"] = df["name"].fillna("").astype(str)
    df["market_detail"] = df["market_detail"].fillna("").astype(str)

    df["streak"] = pd.to_numeric(df["streak"], errors="coerce").fillna(0).astype(int)

    return df[
        [
            "sector",
            "symbol",
            "name",
            "market_detail",
            "limitup_status",
            "streak",
        ]
    ]


# =============================================================================
# Peers (non-limit-up) adapter
# =============================================================================
def peers_df_from_payload(payload: Dict[str, Any]) -> pd.DataFrame:
    """
    支援兩種來源：
    1) payload["peers_not_limitup"] : list[dict]
    2) payload["peers_by_sector"]   : dict[sector] -> list[dict]
    """
    if isinstance(payload.get("peers_not_limitup"), list):
        df = pd.DataFrame(payload["peers_not_limitup"])
        return _normalize_peers_df(df)

    if isinstance(payload.get("peers_by_sector"), dict):
        rows = []
        for sector, items in payload["peers_by_sector"].items():
            if not isinstance(items, list):
                continue
            for r in items:
                if isinstance(r, dict):
                    rr = dict(r)
                    rr["sector"] = rr.get("sector") or sector
                    rows.append(rr)
        df = pd.DataFrame(rows) if rows else pd.DataFrame()
        return _normalize_peers_df(df)

    return pd.DataFrame()


def _normalize_peers_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    d = df.copy()

    if "sector" not in d.columns:
        d["sector"] = d.get("產業", "未分類")

    if "symbol" not in d.columns:
        d["symbol"] = d.get("代碼", "")

    if "name" not in d.columns:
        d["name"] = d.get("名稱", "")

    if "market_detail" not in d.columns:
        d["market_detail"] = ""

    # ret 用於排序（layout 用），沒有就補 0
    if "ret" not in d.columns:
        d["ret"] = 0.0
    d["ret"] = pd.to_numeric(d["ret"], errors="coerce").fillna(0.0)

    d["sector"] = d["sector"].fillna("").replace("", "未分類").astype(str)
    d["symbol"] = d["symbol"].fillna("").astype(str)
    d["name"] = d["name"].fillna("").astype(str)
    d["market_detail"] = d["market_detail"].fillna("").astype(str)

    return d[
        [
            "sector",
            "symbol",
            "name",
            "market_detail",
            "ret",
        ]
    ]
