# scripts/render_images_tw/sector_blocks/_font.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import List, Optional

import matplotlib.font_manager as fm
import matplotlib.pyplot as plt


def _env_on(name: str) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def setup_cjk_font() -> Optional[str]:
    """
    TW sector pages: CJK-safe font selection (CI-friendly).

    Why sector pages got tofu:
      - CI often has 'Noto Sans CJK JP' but NOT 'Noto Sans CJK TC'
      - Old candidates list didn't include JP fallback, so it fell back to DejaVu Sans.

    This function:
      - builds a fallback font list (not single font)
      - includes Noto Sans CJK JP/SC/HK/TC variants
      - sets rcParams["font.sans-serif"] to that list
      - returns the first chosen font name (or None)
    """
    try:
        available = {f.name for f in fm.fontManager.ttflist}

        # CI (ubuntu) commonly exposes these names depending on fontconfig build
        candidates: List[str] = [
            # Preferred for TW
            "Noto Sans CJK TC",
            "Noto Sans TC",
            "Noto Sans HK",
            # CI-safe fallbacks (very common when fonts-noto-cjk is installed)
            "Noto Sans CJK JP",
            "Noto Sans JP",
            "Noto Sans CJK SC",
            "Noto Sans SC",
            # Windows/macOS fallbacks (harmless on CI)
            "Microsoft JhengHei",
            "PingFang TC",
            "Arial Unicode MS",
            # generic fallbacks
            "Noto Sans",
            "DejaVu Sans",
        ]

        font_list: List[str] = []
        for name in candidates:
            if name in available and name not in font_list:
                font_list.append(name)

        # If still empty, don't touch rcParams (matplotlib will use default)
        if not font_list:
            if _env_on("SECTOR_DEBUG_FONTS"):
                print("[SECTOR_FONT_DEBUG] no candidate fonts available; keep default rcParams")
                print("  available(sample) =", sorted(list(available))[:50])
            return None

        plt.rcParams["font.family"] = "sans-serif"
        plt.rcParams["font.sans-serif"] = font_list
        plt.rcParams["axes.unicode_minus"] = False

        if _env_on("SECTOR_DEBUG_FONTS"):
            try:
                print("[SECTOR_FONT_DEBUG]")
                print("  selected_font_list =", font_list)
                print("  rcParams.font.family =", plt.rcParams.get("font.family"))
                print("  rcParams.font.sans-serif =", plt.rcParams.get("font.sans-serif"))
            except Exception:
                pass

        return font_list[0]
    except Exception:
        return None
