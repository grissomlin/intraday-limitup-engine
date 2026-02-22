# markets/tw/aggregator/universe.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd


# =============================================================================
# Small helpers
# =============================================================================
def safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def norm_sector(x: Any) -> str:
    """
    Sector normalization:
    - empty / nan / dash → 未分類
    """
    s = safe_str(x)
    if not s or s.lower() in ("nan", "none") or s in ("—", "-", "--", "－", "–"):
        return "未分類"
    return s


def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


# =============================================================================
# Universe builder
# =============================================================================
def build_universe(
    *,
    # preferred names
    df_main: Optional[pd.DataFrame] = None,
    df_open: Optional[pd.DataFrame] = None,
    # backward-compat aliases (some aggregators call dfS/dfO)
    dfS: Optional[pd.DataFrame] = None,
    dfO: Optional[pd.DataFrame] = None,
    include_open_limit: bool = True,
) -> Dict[str, Any]:
    """
    Build TW universe denominators.

    Universe definition (TW):
    - df_main (上市櫃主板) always included
    - df_open (興櫃/開放市場池) included if include_open_limit=True

    Backward-compat:
    - accept dfS as alias of df_main
    - accept dfO as alias of df_open

    Output format:

    {
      "total": 2451,
      "by_sector": [
        {"sector": "半導體業", "count": 300},
        ...
      ]
    }
    """

    # ✅ accept alias kwargs
    if df_main is None and dfS is not None:
        df_main = dfS
    if df_open is None and dfO is not None:
        df_open = dfO

    frames: List[pd.DataFrame] = []

    # --- main board always counted
    if df_main is not None and not df_main.empty and "sector" in df_main.columns:
        frames.append(df_main[["sector"]].copy())

    # --- open-limit pool optional
    if (
        include_open_limit
        and df_open is not None
        and not df_open.empty
        and "sector" in df_open.columns
    ):
        frames.append(df_open[["sector"]].copy())

    if not frames:
        return {"total": 0, "by_sector": []}

    uni_df = pd.concat(frames, ignore_index=True)

    # normalize sector
    uni_df["sector"] = uni_df["sector"].apply(norm_sector)

    # group counts
    agg = (
        uni_df.groupby("sector", as_index=False)
        .size()
        .rename(columns={"size": "count"})
        .sort_values("count", ascending=False)
    )

    return {
        "total": int(len(uni_df)),
        "by_sector": agg.to_dict(orient="records"),
    }


# =============================================================================
# Merge denominators into sector summary
# =============================================================================
def merge_sector_with_universe(
    *,
    sector_rows: List[Dict[str, Any]],
    universe_by_sector: List[Dict[str, Any]],
    key_count: str = "count",
) -> List[Dict[str, Any]]:
    """
    Attach denominator fields into sector summary rows.

    Each sector row will gain:

    - sector_total / universe_cnt
    - share_of_universe = strong_cnt / sector_total
    - locked_pct / touched_pct

    This is what renderer needs for:
      Sector X% / Market X%
    """

    if not sector_rows:
        return []

    # build map: sector -> total count
    uni_map: Dict[str, int] = {}
    for r in universe_by_sector or []:
        sec = norm_sector(r.get("sector"))
        uni_map[sec] = safe_int(r.get("count"), 0)

    out: List[Dict[str, Any]] = []

    for r in sector_rows:
        rr = dict(r or {})

        sec = norm_sector(rr.get("sector"))
        sector_total = uni_map.get(sec, 0)

        rr["sector_total"] = sector_total
        rr["universe_cnt"] = sector_total

        # numerator = strong movers count
        strong_cnt = rr.get(key_count, rr.get("total_cnt", 0))
        try:
            strong_cnt_f = float(strong_cnt)
        except Exception:
            strong_cnt_f = 0.0

        rr["share_of_universe"] = (
            strong_cnt_f / sector_total if sector_total > 0 else 0.0
        )

        # locked/touched pct
        locked_cnt = safe_int(rr.get("locked_cnt"), 0)
        touch_cnt = safe_int(rr.get("touch_cnt"), 0)

        rr["locked_pct"] = locked_cnt / sector_total if sector_total > 0 else 0.0
        rr["touched_pct"] = touch_cnt / sector_total if sector_total > 0 else 0.0

        out.append(rr)

    return out
