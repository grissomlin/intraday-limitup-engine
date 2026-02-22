# scripts/render_images_tw/sector_blocks/_font.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Optional

import matplotlib.font_manager as fm
import matplotlib.pyplot as plt


def setup_cjk_font() -> Optional[str]:
    """
    TW: CJK safe font selection.
    Returns chosen font name or None.
    """
    try:
        font_candidates = [
            "Noto Sans CJK TC",
            "Noto Sans TC",
            "Microsoft JhengHei",
            "PingFang TC",
            "Arial Unicode MS",
            "Noto Sans CJK SC",
            "Noto Sans",
            "DejaVu Sans",
        ]
        available = {f.name for f in fm.fontManager.ttflist}
        for f in font_candidates:
            if f in available:
                plt.rcParams["font.sans-serif"] = [f]
                plt.rcParams["axes.unicode_minus"] = False
                return f
    except Exception:
        pass
    return None
