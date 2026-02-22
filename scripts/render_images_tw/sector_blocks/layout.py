# scripts/render_images_tw/sector_blocks/layout.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple


@dataclass(frozen=True)
class LayoutSpec:
    header_title_y: float = 0.972
    header_subtitle_y: float = 0.910

    footer_y1: float = 0.040
    footer_y2: float = 0.020

    top_box_y0: float = 0.840
    top_box_y1: float = 0.485
    bot_box_y0: float = 0.465
    bot_box_y1: float = 0.085

    x_name: float = 0.08
    x_tag: float = 0.94

    title_fs: int = 62
    subtitle_fs: int = 30
    page_fs: int = 26

    box_title_fs: int = 32
    empty_hint_fs: int = 34

    # TW 公司名短，你說兩行夠放，所以用 two_line=True
    row_name_fs: int = 30
    row_line2_fs: int = 24
    row_tag_fs: int = 26

    footer_fs_1: int = 22
    footer_fs_2: int = 20

    badge_pad_limitup: float = 0.23
    badge_pad_peer: float = 0.20

    two_line: bool = True


def calc_rows_layout(y_top: float, y_bottom: float, max_rows: int, *, two_line: bool) -> Tuple[float, float]:
    max_rows = max(1, int(max_rows))
    span = float(y_top - y_bottom)

    if two_line:
        top_pad = span * 0.010
        bottom_pad = span * 0.010
        title_h = span * 0.055
    else:
        top_pad = span * 0.015
        bottom_pad = span * 0.015
        title_h = span * 0.075

    usable = max(1e-6, span - top_pad - bottom_pad - title_h)
    row_h = usable / max_rows
    y_start = y_top - top_pad - title_h - row_h * 0.5
    return y_start, row_h


PRESETS: Dict[str, LayoutSpec] = {
    "tw": LayoutSpec(two_line=True),
    "tw1": LayoutSpec(two_line=False),
    # 兼容：有人會不小心填 default
    "default": LayoutSpec(two_line=True),
}


def get_layout(name: str) -> LayoutSpec:
    k = (name or "").strip().lower() or "tw"
    if k not in PRESETS:
        k = "tw"
    return PRESETS[k]
