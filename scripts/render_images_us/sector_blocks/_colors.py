# scripts/render_images_us/sector_blocks/_colors.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Tuple


def get_ret_color(ret: float, theme: str = "light") -> str:
    if theme == "dark":
        return "#40c057" if ret >= 0 else "#ff6b6b"
    return "#2f9e44" if ret >= 0 else "#c92a2a"


def pick_big_tag(ret_decimal: float) -> Tuple[str, str]:
    """
    6 tiers with clearly distinct color families:

    10–20%  : MOVER  (Blue)
    20–30%  : JUMP   (Green)
    30–40%  : SURGE  (Purple)
    40–50%  : RALLY  (Orange)
    50–100% : ROCKET (Red)
    100%+   : MOON   (Gold)
    """
    if ret_decimal >= 1.00:
        return ("MOON", "#f59f00")        # Gold
    if ret_decimal >= 0.50:
        return ("ROCKET", "#e03131")      # Red
    if ret_decimal >= 0.40:
        return ("RALLY", "#f76707")       # Orange
    if ret_decimal >= 0.30:
        return ("SURGE", "#7048e8")       # Purple
    if ret_decimal >= 0.20:
        return ("JUMP", "#2f9e44")        # Green
    return ("MOVER", "#4dabf7")           # Blue
