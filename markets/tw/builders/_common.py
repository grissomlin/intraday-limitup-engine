# markets/tw/builders/_common.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, List

import pandas as pd

EPS = 1e-9


# =============================================================================
# Helpers (open-limit threshold touch/locked)
# =============================================================================
def _add_ret_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure ret and ret_high exist (open-limit uses high-based touch)."""
    if df is None or df.empty:
        # keep schema stable even if caller passes empty df
        for c in ["ret", "ret_high", "ret_pct", "ret_high_pct"]:
            if c not in getattr(df, "columns", []):
                df[c] = 0.0
        return df

    c = pd.to_numeric(df.get("close", 0.0), errors="coerce").fillna(0.0)
    h = pd.to_numeric(df.get("high", 0.0), errors="coerce").fillna(0.0)
    pc = pd.to_numeric(df.get("prev_close", 0.0), errors="coerce").fillna(0.0)

    can = pc > 0

    ret_close = pd.Series(0.0, index=df.index, dtype="float64")
    ret_high = pd.Series(0.0, index=df.index, dtype="float64")

    ret_close.loc[can] = (c.loc[can] / pc.loc[can]) - 1.0
    ret_high.loc[can] = (h.loc[can] / pc.loc[can]) - 1.0

    df["ret"] = ret_close.astype(float)
    df["ret_high"] = ret_high.astype(float)

    df["ret_pct"] = (df["ret"] * 100.0).astype(float)
    df["ret_high_pct"] = (df["ret_high"] * 100.0).astype(float)
    return df


def _sanitize_nan(obj: Any) -> Any:
    """Convert NaN/Inf/pd.NA to None (json-safe)."""
    try:
        if isinstance(obj, float):
            if pd.isna(obj):
                return None
            return obj
    except Exception:
        pass

    try:
        if obj is pd.NA:
            return None
    except Exception:
        pass

    if obj is None:
        return None

    if isinstance(obj, dict):
        return {k: _sanitize_nan(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_sanitize_nan(v) for v in obj]

    if isinstance(obj, tuple):
        return tuple(_sanitize_nan(v) for v in obj)

    return obj


def _coalesce_int(df: pd.DataFrame, cols: List[str], default: int = 0) -> pd.Series:
    """Pick first existing column as int series."""
    for c in cols:
        if c in df.columns:
            return pd.to_numeric(df[c], errors="coerce").fillna(default).astype(int)
    return pd.Series(default, index=df.index, dtype="int64")


def _ensure_cols(df: pd.DataFrame, specs: List[tuple[str, Any]]) -> None:
    for c, dv in specs:
        if c not in df.columns:
            df[c] = dv


def _normalize_sector(s: pd.Series) -> pd.Series:
    return s.fillna("").replace("", "未分類")


def _norm_md(x: Any) -> str:
    md = str(x or "").strip().lower()
    if md in ("emerging_no_limit", "emerging-nolimit"):
        return "emerging"
    if md in ("openlimit", "open-limit"):
        return "open_limit"
    if md in ("otc",):
        return "rotc"
    return md or "emerging"


def _normalize_open_limit_identity_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Force open-limit identity fields consistent:
    - limit_type = "open_limit"
    - market_detail normalized
    - market_label default "興櫃"
    - is_open_limit_board = True
    """
    if df is None or df.empty:
        return df
    df = df.copy()

    _ensure_cols(
        df,
        [
            ("limit_type", "open_limit"),
            ("market_detail", "emerging"),
            ("market_label", "興櫃"),
            ("is_open_limit_board", True),
        ],
    )

    # keep original in sub_limit_type for debugging if it existed and differs
    if "sub_limit_type" not in df.columns and "limit_type" in df.columns:
        df["sub_limit_type"] = df["limit_type"]

    df["limit_type"] = "open_limit"
    df["market_detail"] = df["market_detail"].apply(_norm_md)
    df["market_label"] = df["market_label"].fillna("興櫃").astype(str).str.strip()
    df.loc[df["market_label"].eq(""), "market_label"] = "興櫃"
    df["is_open_limit_board"] = True
    return df
