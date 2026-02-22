# markets/us/builders_us.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Dict, List
import os
import pandas as pd

US_OPEN_WATCHLIST_RET_TH = float(os.getenv("US_OPEN_WATCHLIST_RET_TH", "0.10"))


def build_open_limit_watchlist_us(snapshot_open_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    US open_limit_watchlist：
    - 直接從「原始 snapshot_open」過濾 (ret >= 門檻)
    - 重要：保留 streak / streak_prev / hit_prev / badge_text / status_text ... 等欄位
    - 不要像 TW build_open_limit_watchlist 那樣裁欄位
    """
    if not snapshot_open_rows:
        return []

    df = pd.DataFrame(snapshot_open_rows)
    if df.empty:
        return []

    df["ret"] = pd.to_numeric(df.get("ret", 0.0), errors="coerce").fillna(0.0)
    df = df[df["ret"] >= float(US_OPEN_WATCHLIST_RET_TH)].sort_values("ret", ascending=False)
    if df.empty:
        return []

    # 補齊必要的 meta 欄位（不覆蓋你原本算好的 streak/status）
    if "limit_type" not in df.columns:
        df["limit_type"] = "open_limit"

    for c, dv in [
        ("is_limitup_touch", False),
        ("is_limitup_locked", False),
        ("market_detail", "Unknown"),
        ("market_label", "Unknown"),
        ("sector", "Unknown"),
        ("name", "Unknown"),
        ("symbol", ""),
        ("bar_date", ""),
        ("status_text", ""),
        ("badge_text", ""),
        ("badge_level", 0),
        ("streak", 0),
        ("streak_prev", 0),
        ("hit_prev", 0),
        ("touch_ret", 0.0),
        ("touched_only", False),
        ("prev_close", 0.0),
        ("open", 0.0),
        ("high", 0.0),
        ("low", 0.0),
        ("close", 0.0),
        ("volume", 0),
    ]:
        if c not in df.columns:
            df[c] = dv

    # 你 UI/渲染端要用哪些欄位，就在 keep 裡列哪些；這裡「全都保留」
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
        "touch_ret",
        "touched_only",
        "streak",
        "streak_prev",
        "hit_prev",
        "badge_text",
        "badge_level",
        "limit_type",
        "is_limitup_touch",
        "is_limitup_locked",
        "status_text",
    ]

    for c in keep:
        if c not in df.columns:
            df[c] = ""  # 兜底

    return df[keep].to_dict(orient="records")


def build_sector_summary_open_limit_us(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    sector summary（US open_limit）：
    - sector: count / avg_ret / max_ret
    """
    if not rows:
        return []

    df = pd.DataFrame(rows)
    if df.empty:
        return []

    df["sector"] = df.get("sector", "").fillna("").replace("", "Unknown")
    df["ret"] = pd.to_numeric(df.get("ret", 0.0), errors="coerce").fillna(0.0)

    agg = (
        df.groupby("sector", as_index=False)
        .agg(count=("symbol", "count"), avg_ret=("ret", "mean"), max_ret=("ret", "max"))
        .sort_values(["count", "avg_ret"], ascending=False)
    )
    return agg.to_dict(orient="records")
