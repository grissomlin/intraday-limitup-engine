# scripts/render_images_tw/sector_blocks/policy.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any


def board_badge(market_detail: Any) -> str:
    """
    Only show board badge for emerging (興櫃).
    You said later you may split 主/創/科/特…，那就把 mapping 放這裡。
    """
    md = (str(market_detail).strip().lower() if market_detail is not None else "")
    if md == "rotc":
        return "興櫃"
    return ""
