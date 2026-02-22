# scripts/render_images_us/sector_blocks/_font.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm


def setup_chinese_font() -> str | None:
    """
    Keep CJK-safe because you still have Chinese line2 sometimes.
    Harmless for EN markets; helpful if mixed chars appear.
    """
    try:
        font_candidates = [
            "Microsoft JhengHei",
            "Microsoft YaHei",
            "PingFang TC",
            "PingFang SC",
            "Noto Sans CJK TC",
            "Noto Sans CJK SC",
            "Noto Sans CJK JP",
            "SimHei",
            "WenQuanYi Zen Hei",
            "Noto Sans",
            "Arial Unicode MS",
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