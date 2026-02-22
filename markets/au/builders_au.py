# markets/au/builders_au.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Dict, List
import os
import pandas as pd

AU_OPEN_WATCHLIST_RET_TH = float(os.getenv("AU_OPEN_WATCHLIST_RET_TH", "0.10"))


def _infer_is_reit(sector: Any) -> bool:
    s = str(sector or "").strip().lower()
    if not s:
        return False
    # e.g. "Equity Real Estate Investment Trusts (REITs)"
    return ("reit" in s) or ("real estate investment trust" in s)


def build_open_limit_watchlist_au(snapshot_open_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    AU open_limit_watchlist：
    - 從 snapshot_open 過濾 (ret >= 門檻)
    - 不過度裁切：保留 sector pages / overview 會用到的欄位
    - sector 使用 ASX 的 GICS industry group 原文（直接沿用 snapshot_open）
    - is_reit：若 snapshot_open 已算好就尊重；沒有才推斷
    """
    if not snapshot_open_rows:
        return []

    df = pd.DataFrame(snapshot_open_rows)
    if df.empty:
        return []

    df["ret"] = pd.to_numeric(df.get("ret", 0.0), errors="coerce").fillna(0.0)
    df = df[df["ret"] >= float(AU_OPEN_WATCHLIST_RET_TH)].sort_values("ret", ascending=False)
    if df.empty:
        return []

    # limit_type
    if "limit_type" not in df.columns:
        df["limit_type"] = "open_limit"

    # is_reit：如果 downloader/snapshot 已算好就尊重；沒有才推斷
    if "is_reit" not in df.columns:
        df["is_reit"] = df.get("sector", "").apply(_infer_is_reit)
    else:
        df["is_reit"] = df["is_reit"].fillna(False).astype(bool)

    # 補齊必要 meta 欄位（不覆蓋你原本算好的 move/status）
    for c, dv in [
        ("is_limitup_touch", False),
        ("is_limitup_locked", False),
        ("market_detail", "ASX"),
        ("market_label", "AU"),
        ("sector", "Unknown"),
        ("name", "Unknown"),
        ("symbol", ""),
        ("bar_date", ""),
        ("status_text", ""),
        # preferred fields
        ("move_band", -1),
        ("move_key", ""),
        # backward compatible
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

    keep = [
        "symbol",
        "name",
        "sector",
        "is_reit",
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
        # ✅ new
        "move_band",
        "move_key",
        # ✅ old
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

    # types cleanup (safe)
    df["move_band"] = pd.to_numeric(df.get("move_band", -1), errors="coerce").fillna(-1).astype(int)
    df["badge_level"] = pd.to_numeric(df.get("badge_level", 0), errors="coerce").fillna(0).astype(int)
    df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).astype(int)
    df["is_reit"] = df["is_reit"].fillna(False).astype(bool)

    return df[keep].to_dict(orient="records")


def build_sector_summary_open_limit_au(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    sector summary（AU open_limit）：
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
