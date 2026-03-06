# markets/fr/builders_fr.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List
import os
import pandas as pd

FR_OPEN_WATCHLIST_RET_TH = float(os.getenv("FR_OPEN_WATCHLIST_RET_TH", "0.10"))

# Noise filters
FR_MIN_PRICE = float(os.getenv("FR_MIN_PRICE", "0.10"))
FR_MIN_VOLUME = int(float(os.getenv("FR_MIN_VOLUME", "50000")))
FR_TICK_EUR = float(os.getenv("FR_TICK_EUR", "0.01"))
FR_EXCLUDE_ONE_TICK_10PCT = str(os.getenv("FR_EXCLUDE_ONE_TICK_10PCT", "1")).strip().lower() in (
    "1", "true", "yes", "y", "on"
)


def _one_tick_10pct_price_ceiling(tick: float) -> float:
    # if tick/price >= 0.10 => price <= tick/0.10 = 10*tick
    try:
        t = float(tick)
    except Exception:
        t = 0.01
    if t <= 0:
        t = 0.01
    return t / 0.10


def _clean_text_series(s: pd.Series, default: str = "Unknown") -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .str.strip()
        .replace(
            {
                "": default,
                "nan": default,
                "None": default,
                "none": default,
                "null": default,
                "-": default,
                "—": default,
                "--": default,
                "N/A": default,
                "n/a": default,
                "NA": default,
                "na": default,
            }
        )
    )


def _clean_sector_series(s: pd.Series) -> pd.Series:
    return _clean_text_series(s, default="Unknown")


def build_open_limit_watchlist_fr(snapshot_open_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    FR open_limit_watchlist:
    - include close >= threshold OR intraday touch >= threshold
    - keep streak/move_band/move_key/badge/status fields
    - apply noise filters (min price/volume, exclude one-tick 10% tiny-price)

    IMPORTANT:
    This makes FR overview closer to sector pages by including touched-only rows too.
    """
    if not snapshot_open_rows:
        return []

    df = pd.DataFrame(snapshot_open_rows)
    if df.empty:
        return []

    df["ret"] = pd.to_numeric(df.get("ret", 0.0), errors="coerce").fillna(0.0)
    df["touch_ret"] = pd.to_numeric(df.get("touch_ret", 0.0), errors="coerce").fillna(0.0)
    df["close"] = pd.to_numeric(df.get("close", 0.0), errors="coerce").fillna(0.0)
    df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).astype(int)

    if "touched_only" not in df.columns:
        df["touched_only"] = False
    df["touched_only"] = df["touched_only"].fillna(False).astype(bool)

    # mover filter:
    # - close >= 10%
    # - OR intraday touch >= 10%
    df = df[
        (df["ret"] >= float(FR_OPEN_WATCHLIST_RET_TH))
        | (df["touch_ret"] >= float(FR_OPEN_WATCHLIST_RET_TH))
    ].copy()

    if df.empty:
        return []

    # sort:
    # close 10%+ first, then touched-only by touch_ret
    df["is_close_hit"] = (df["ret"] >= float(FR_OPEN_WATCHLIST_RET_TH)).astype(int)
    df = df.sort_values(["is_close_hit", "ret", "touch_ret"], ascending=[False, False, False])

    # noise filters
    if FR_MIN_PRICE > 0:
        df = df[df["close"] >= float(FR_MIN_PRICE)]
    if FR_MIN_VOLUME > 0:
        df = df[df["volume"] >= int(FR_MIN_VOLUME)]

    if FR_EXCLUDE_ONE_TICK_10PCT:
        ceil_price = _one_tick_10pct_price_ceiling(FR_TICK_EUR)
        df = df[df["close"] > float(ceil_price)]

    if df.empty:
        return []

    # meta defaults (do not overwrite computed fields)
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

    df["sector"] = _clean_sector_series(df.get("sector", pd.Series(["Unknown"] * len(df))))
    df["name"] = _clean_text_series(df.get("name", pd.Series(["Unknown"] * len(df))), default="Unknown")
    df["market_detail"] = _clean_text_series(
        df.get("market_detail", pd.Series(["Unknown"] * len(df))), default="Unknown"
    )
    df["market_label"] = _clean_text_series(
        df.get("market_label", pd.Series(["Unknown"] * len(df))), default="Unknown"
    )

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
        "move_band",
        "move_key",
        "badge_text",
        "badge_level",
        "limit_type",
        "is_limitup_touch",
        "is_limitup_locked",
        "status_text",
    ]

    for c in keep:
        if c not in df.columns:
            df[c] = ""

    # types cleanup
    df["move_band"] = pd.to_numeric(df.get("move_band", -1), errors="coerce").fillna(-1).astype(int)
    df["badge_level"] = pd.to_numeric(df.get("badge_level", 0), errors="coerce").fillna(0).astype(int)
    df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).astype(int)

    return df[keep].to_dict(orient="records")


def build_sector_summary_open_limit_fr(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    sector summary (FR open_limit):
    - sector: count / avg_ret / max_ret / max_touch_ret
    """
    if not rows:
        return []

    df = pd.DataFrame(rows)
    if df.empty:
        return []

    df["sector"] = _clean_sector_series(df.get("sector", pd.Series(["Unknown"] * len(df))))
    df["ret"] = pd.to_numeric(df.get("ret", 0.0), errors="coerce").fillna(0.0)
    df["touch_ret"] = pd.to_numeric(df.get("touch_ret", 0.0), errors="coerce").fillna(0.0)

    agg = (
        df.groupby("sector", as_index=False)
        .agg(
            count=("symbol", "count"),
            avg_ret=("ret", "mean"),
            max_ret=("ret", "max"),
            max_touch_ret=("touch_ret", "max"),
        )
        .sort_values(["count", "avg_ret", "max_touch_ret"], ascending=False)
    )
    return agg.to_dict(orient="records")
