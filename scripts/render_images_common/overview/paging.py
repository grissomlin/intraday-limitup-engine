# scripts/render_images_common/overview/paging.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from .gain_bins import build_gain_bins_rows

# English labels for gain-bins page (same rule as original render.py)
EN_BINS_MARKETS = {"US", "CA", "AU", "UK", "EU"}

# ✅ Markets that must NEVER render gainbins page(s)
NO_GAINBINS_MARKETS = {"CN", "TW"}


def get_gainbins_rows_and_lang(
    payload: Dict[str, Any],
    *,
    market: str,
    lang: str,
) -> Tuple[List[Dict[str, Any]], str]:
    """
    Returns:
      (gain_rows, lang_bins)

    - gain_rows == [] => do NOT render gainbins pages.
    - lang_bins is the language used on gainbins page (EN markets force 'en').
    """
    m = (market or "").strip().upper()

    lang_bins = "en" if m in EN_BINS_MARKETS else lang

    # hard disable
    if m in NO_GAINBINS_MARKETS:
        return ([], lang_bins)

    gain_rows = build_gain_bins_rows(payload)
    if not isinstance(gain_rows, list):
        gain_rows = []

    return (gain_rows, lang_bins)


def should_force_paging(gain_rows: List[Dict[str, Any]]) -> bool:
    """
    If gainbins page exists, we force sector overview pages to be named _p1/_p2...,
    so gainbins can become the last page deterministically.
    """
    return bool(gain_rows)