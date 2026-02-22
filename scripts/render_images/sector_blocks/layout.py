@'
# scripts/render_images_us/sector_blocks/layout.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple


@dataclass(frozen=True)
class LayoutSpec:
    # -------------------------
    # Header
    # -------------------------
    header_title_y: float = 0.972
    header_subtitle_y: float = 0.898

    # -------------------------
    # Footer
    # -------------------------
    footer_y1: float = 0.040
    footer_y2: float = 0.018

    # -------------------------
    # Boxes
    # -------------------------
    top_box_y0: float = 0.860
    top_box_y1: float = 0.505   # two-line 建議略縮下框空間，留給上框更多字
    bot_box_y0: float = 0.485
    bot_box_y1: float = 0.085

    # -------------------------
    # Columns
    # -------------------------
    x_name: float = 0.08
    x_prev: float = 0.76
    x_tag: float = 0.94

    # -------------------------
    # Fonts
    # -------------------------
    title_fs: int = 70
    subtitle_fs: int = 34
    page_fs: int = 26

    box_title_fs: int = 32
    empty_hint_fs: int = 34

    # row fonts
    row_name_fs: int = 28
    row_line2_fs: int = 22
    row_tag_fs: int = 26
    row_prev_fs: int = 24

    # behavior
    two_line: bool = True

    footer_fs_1: int = 22
    footer_fs_2: int = 20
    footer_note_fs: int = 20

    badge_pad_limitup: float = 0.23
    badge_pad_peer: float = 0.20


def calc_rows_layout(y_top: float, y_bottom: float, max_rows: int, *, two_line: bool = False) -> Tuple[float, float]:
    """
    回傳 y_start, row_h
    two_line=True 代表每列要容納兩行字，會自動更省 padding、提高 row_h
    """
    span = y_top - y_bottom

    if max_rows >= 8:
        top_pad = span * (0.012 if two_line else 0.018)
        bottom_pad = span * (0.012 if two_line else 0.018)
        title_h = span * (0.070 if two_line else 0.085)
    else:
        top_pad = span * (0.020 if two_line else 0.030)
        bottom_pad = span * (0.020 if two_line else 0.030)
        title_h = span * (0.090 if two_line else 0.110)

    usable = span - top_pad - bottom_pad - title_h
    row_h = usable / max_rows
    y_start = y_top - top_pad - title_h - row_h * 0.5
    return y_start, row_h


PRESETS: Dict[str, LayoutSpec] = {
    "tw": LayoutSpec(two_line=False),
    "us": LayoutSpec(two_line=True),
    "cn": LayoutSpec(two_line=True),  # 你說中國也要兩行，先預留
}


def get_layout(name: str) -> LayoutSpec:
    name = (name or "").strip().lower() or "us"
    if name == "default":
        name = "us"
    if name not in PRESETS:
        raise KeyError(name)
    return PRESETS[name]


# 兼容舊 import 名稱（如果你別處還在 import pick_layout）
def pick_layout(market: str) -> LayoutSpec:
    return get_layout(market)
'@ | Set-Content -Encoding utf8 scripts/render_images_us/sector_blocks/layout.py
