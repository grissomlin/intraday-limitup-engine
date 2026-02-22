# markets/tw/aggregator/open_limit.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Any, Dict, List

import pandas as pd

from .normalize import _add_ret_fields


# =============================================================================
# Env
# =============================================================================
def _env_bool(name: str, default: str = "1") -> bool:
    """
    Default ON for TW open-limit watchlist.
    - 你抱怨的點在這：以前需要手動 set ENABLE_OPEN_WATCHLIST=1
    - 這裡直接把 default 設為 "1" => 預設就開
    """
    v = str(os.getenv(name, default)).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def open_watchlist_enabled() -> bool:
    """Whether to include/merge OPEN_LIMIT watchlist rows into snapshot_open."""
    return _env_bool("ENABLE_OPEN_WATCHLIST", "1")


# =============================================================================
# Open-limit helpers (興櫃/開放漲跌幅/無漲跌幅池)
# =============================================================================
def normalize_open_limit_watchlist_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Force watchlist rows to have stable open-limit identifiers.

    Notes:
    - limit_type 统一成 "open_limit"
    - market_detail 做 normalization（otc -> rotc；open-limit/openlimit -> open_limit）
    - 增加 is_open_limit_board=True
    """
    if not rows:
        return []

    out: List[Dict[str, Any]] = []
    for r in rows:
        rr = dict(r or {})
        rr["limit_type"] = "open_limit"

        md = str(rr.get("market_detail") or "emerging").strip().lower()
        if md in ("emerging_no_limit", "emerging-nolimit"):
            md = "emerging"
        if md in ("openlimit", "open-limit"):
            md = "open_limit"
        if md == "otc":
            md = "rotc"

        rr["market_detail"] = md or "emerging"
        rr["is_open_limit_board"] = True
        out.append(rr)

    return out


def enrich_open_limit_df(dfO: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize OPEN_LIMIT pool snapshot (興櫃/開放漲跌幅/無漲跌幅池).

    Expected columns (best effort):
    - limit_type / sub_limit_type
    - market_detail
    - market_label (default: 興櫃)
    - prev_close/high/close... (for ret fields)
    """
    if dfO is None or dfO.empty:
        return dfO

    # 统一 limit_type，并保留原始到 sub_limit_type（如果原本有）
    if "limit_type" not in dfO.columns:
        dfO["limit_type"] = "open_limit"
    else:
        if "sub_limit_type" not in dfO.columns:
            dfO["sub_limit_type"] = dfO["limit_type"]
        dfO["limit_type"] = "open_limit"

    # market_detail normalize
    if "market_detail" not in dfO.columns:
        dfO["market_detail"] = "emerging"
    dfO["market_detail"] = (
        dfO["market_detail"]
        .fillna("emerging")
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({"": "emerging"})
    )
    dfO["market_detail"] = dfO["market_detail"].replace(
        {
            "emerging_no_limit": "emerging",
            "emerging-nolimit": "emerging",
            "open-limit": "open_limit",
            "openlimit": "open_limit",
            "otc": "rotc",
        }
    )

    # label default = 興櫃
    if "market_label" not in dfO.columns:
        dfO["market_label"] = "興櫃"
    dfO["market_label"] = dfO["market_label"].fillna("興櫃").astype(str).str.strip()
    dfO.loc[dfO["market_label"].eq(""), "market_label"] = "興櫃"

    # compute ret / ret_high etc.
    _add_ret_fields(dfO)

    dfO["is_open_limit_board"] = True
    return dfO
