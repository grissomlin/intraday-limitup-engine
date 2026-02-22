# scripts/render_images_tw/sector_blocks/_badge.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

# =============================================================================
# Move bands (10–20/20–30/... labels + colors)
# =============================================================================
def _move_band(ret_decimal: float) -> Optional[int]:
    """
    band 0..5 => 10–20, 20–30, 30–40, 40–50, 50–100, >=100
    """
    r = float(ret_decimal)
    if r < 0.10:
        return None
    if r < 0.20:
        return 0
    if r < 0.30:
        return 1
    if r < 0.40:
        return 2
    if r < 0.50:
        return 3
    if r < 1.00:
        return 4
    return 5


_BAND_BG: Dict[int, Tuple[float, float, float]] = {
    0: (0.25, 0.60, 0.95),  # 10–20
    1: (0.10, 0.70, 0.80),  # 20–30
    2: (0.20, 0.78, 0.35),  # 30–40
    3: (0.98, 0.72, 0.10),  # 40–50
    4: (0.97, 0.45, 0.05),  # 50–100
    5: (0.88, 0.20, 0.20),  # >=100
}


def pick_move_band_tag(ret_decimal: float, *, t_func=None, lang: str = "zh_hant") -> Tuple[str, Tuple[float, float, float]]:
    """
    Returns (label, bg_rgb).
    If t_func provided: t_func(lang, key, default) for i18n.
    """
    b = _move_band(ret_decimal)
    if b is None:
        label = "大漲" if t_func is None else t_func(lang, "move_band_0", "大漲")
        return label, _BAND_BG[0]
    key = f"move_band_{b}"
    label = "大漲" if t_func is None else t_func(lang, key, "大漲")
    return label, _BAND_BG.get(b, _BAND_BG[0])


# =============================================================================
# Return tag color
# =============================================================================
def get_ret_color(ret: float, theme: str = "light") -> str:
    if ret >= 1.00:
        return "#1565c0" if theme == "light" else "#1e88e5"
    elif ret >= 0.50:
        return "#1976d2" if theme == "light" else "#2196f3"
    elif ret >= 0.20:
        return "#2196f3" if theme == "light" else "#42a5f5"
    else:
        return "#42a5f5" if theme == "light" else "#64b5f6"


# =============================================================================
# Surge classification
# =============================================================================
def is_surge_row(r: Dict[str, Any], badge_text: str) -> bool:
    """
    Decide whether this row belongs to surge(10%+) semantic,
    so renderer can pick move-band style, *unless* badge is manual streak text.
    """
    if (str(r.get("limitup_status") or "")).strip().lower() == "surge":
        return True
    try:
        if bool(r.get("is_surge_ge10", False)):
            return True
    except Exception:
        pass
    b = (badge_text or "").strip()
    return ("10%" in b) or ("漲幅" in b)


def badge_is_manual_streak_text(b: str) -> bool:
    """
    If tw_rows already computed streak badge (e.g. 3連10%+ / 2連大漲 / 3連漲停),
    do NOT allow renderer to overwrite it as move-band "大漲".
    """
    s = (b or "").strip()
    if not s:
        return False
    return "連" in s


def badge_is_generic_surge(b: str) -> bool:
    """
    Only these are considered generic and can be overwritten by move-band label.
    """
    s = (b or "").strip()
    return s in ("漲幅10%+", "10%+", "大漲")
