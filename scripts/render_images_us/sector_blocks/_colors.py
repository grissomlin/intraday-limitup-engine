# scripts/render_images_us/sector_blocks/_colors.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Tuple


def get_ret_color(ret: float, theme: str = "light") -> str:
    if theme == "dark":
        return "#40c057" if ret >= 0 else "#ff6b6b"
    return "#2f9e44" if ret >= 0 else "#c92a2a"


def pick_big_tag(ret_decimal: float) -> Tuple[str, str]:
    if ret_decimal >= 1.00:
        return ("MOON", "#f59f00")
    if ret_decimal >= 0.30:
        return ("SURGE", "#fa5252")
    return ("MOVER", "#4dabf7")