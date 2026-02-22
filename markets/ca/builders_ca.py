# markets/ca/builders_ca.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Dict, List
import os
import pandas as pd

CA_OPEN_WATCHLIST_RET_TH = float(os.getenv("CA_OPEN_WATCHLIST_RET_TH", "0.10"))


def build_open_limit_watchlist_ca(snapshot_open_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not snapshot_open_rows:
        return []
    df = pd.DataFrame(snapshot_open_rows)
    if df.empty:
        return []

    df["ret"] = pd.to_numeric(df.get("ret", 0.0), errors="coerce").fillna(0.0)
    df = df[df["ret"] >= float(CA_OPEN_WATCHLIST_RET_TH)].sort_values("ret", ascending=False)
    if df.empty:
        return []

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
        ("move_band", -1),
        ("move_key", ""),
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
        "symbol","name","sector","market_detail","market_label","bar_date",
        "prev_close","open","high","low","close","volume",
        "ret","touch_ret","touched_only",
        "streak","streak_prev","hit_prev",
        "move_band","move_key",
        "badge_text","badge_level",
        "limit_type","is_limitup_touch","is_limitup_locked","status_text",
    ]
    for c in keep:
        if c not in df.columns:
            df[c] = ""

    df["move_band"] = pd.to_numeric(df.get("move_band", -1), errors="coerce").fillna(-1).astype(int)
    df["badge_level"] = pd.to_numeric(df.get("badge_level", 0), errors="coerce").fillna(0).astype(int)
    df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).astype(int)

    return df[keep].to_dict(orient="records")


def build_sector_summary_open_limit_ca(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
