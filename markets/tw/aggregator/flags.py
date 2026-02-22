# markets/tw/aggregator/flags.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Optional

import pandas as pd

from ..config import NO_LIMIT_THEME_RET


def enrich_overview_flags(dfS: Optional[pd.DataFrame], *, surge_ret: float = 0.10) -> None:
    """
    Add overview flags used by TW mix logic (in-place).
    Keep here because it couples with TW limitup_flags / limit_type semantics.
    """
    if dfS is None or dfS.empty:
        return

    for c, dv in [
        ("limit_type", "standard"),
        ("is_limitup_touch", False),
        ("is_limitup_locked", False),
    ]:
        if c not in dfS.columns:
            dfS[c] = dv

    dfS["limit_type"] = dfS["limit_type"].fillna("standard").astype(str).str.strip()
    dfS["is_limitup_touch"] = dfS["is_limitup_touch"].fillna(False).astype(bool)
    dfS["is_limitup_locked"] = dfS["is_limitup_locked"].fillna(False).astype(bool)

    # ret fields are expected from normalize_snapshot_main, but keep safe
    from .normalize import _add_ret_fields  # local import to avoid circular
    _add_ret_fields(dfS)

    dfS["is_limitup_opened"] = dfS["is_limitup_touch"] & (~dfS["is_limitup_locked"])
    dfS["is_true_limitup"] = dfS["is_limitup_locked"]
    dfS["is_touch_only"] = dfS["is_limitup_touch"] & (~dfS["is_limitup_locked"])
    dfS["is_stop_high"] = dfS["is_limitup_locked"]

    dfS["is_surge_ge10"] = (
        pd.to_numeric(dfS.get("ret"), errors="coerce").fillna(0.0) >= float(surge_ret)
    )

    # >=10% AND did NOT touch limitup at all (standard only)
    dfS["is_bigmove10_ex_locked"] = (
        dfS["is_surge_ge10"]
        & (~dfS["is_limitup_touch"])
        & (dfS["limit_type"].eq("standard"))
    )

    dfS["is_touch_only_ret_ge10"] = dfS["is_touch_only"] & (dfS["ret"] >= float(surge_ret))
    dfS["is_touch_only_ret_lt10"] = dfS["is_touch_only"] & (dfS["ret"] < float(surge_ret))

    dfS["is_touch_only_ret_high_ge10"] = dfS["is_touch_only"] & (dfS["ret_high"] >= float(surge_ret))
    dfS["is_touch_only_ret_high_lt10"] = dfS["is_touch_only"] & (dfS["ret_high"] < float(surge_ret))

    dfS["is_no_limit_theme"] = dfS["limit_type"].eq("no_limit") & (dfS["ret"] >= float(NO_LIMIT_THEME_RET))

    dfS["is_display_limitup"] = dfS["is_limitup_touch"] | dfS["is_surge_ge10"] | dfS["is_no_limit_theme"]
