# scripts/render_images_tw/sector_blocks/_font.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Optional, List

import matplotlib.font_manager as fm
import matplotlib.pyplot as plt


def _env_on(name: str) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def setup_cjk_font() -> Optional[str]:
    """
    TW: CJK safe font selection (CI-friendly).

    Key fixes vs old version:
    - Use a fallback chain (do NOT set only one font).
    - Include SC/JP/KR Noto CJK families as backup (prevents tofu on Linux CI).
    - Optional debug print via env: TW_SECTOR_DEBUG_FONTS=1
    """
    font_candidates: List[str] = [
        # ---- Best on Linux CI (with fonts-noto-cjk installed) ----
        "Noto Sans CJK TC",
        "Noto Sans CJK SC",
        "Noto Sans CJK JP",
        "Noto Sans CJK KR",

        # ---- Some distros expose these names instead ----
        "Noto Sans TC",
        "Noto Sans SC",
        "Noto Sans JP",
        "Noto Sans KR",

        # ---- Windows / macOS ----
        "Microsoft JhengHei",
        "PingFang TC",
        "Arial Unicode MS",

        # ---- Last resort ----
        "Noto Sans",
        "DejaVu Sans",
    ]

    try:
        available = {f.name for f in fm.fontManager.ttflist}

        chosen: Optional[str] = None
        for f in font_candidates:
            if f in available:
                chosen = f
                break

        if not chosen:
            if _env_on("TW_SECTOR_DEBUG_FONTS"):
                sample = sorted([n for n in available if ("noto" in n.lower()) or ("cjk" in n.lower())])[:120]
                print("[TW_SECTOR_FONT_DEBUG] no candidate matched.")
                print("[TW_SECTOR_FONT_DEBUG] sample(notocjk) =", sample)
            return None

        # ✅ keep a fallback chain (critical on CI)
        chain = [chosen] + [x for x in font_candidates if x != chosen and x in available]
        if not chain:
            chain = [chosen]

        plt.rcParams["font.family"] = "sans-serif"
        plt.rcParams["font.sans-serif"] = chain
        plt.rcParams["axes.unicode_minus"] = False

        if _env_on("TW_SECTOR_DEBUG_FONTS"):
            print("[TW_SECTOR_FONT_DEBUG] chosen =", chosen)
            print("[TW_SECTOR_FONT_DEBUG] chain  =", chain)
            print("[TW_SECTOR_FONT_DEBUG] rcParams.font.sans-serif =", plt.rcParams.get("font.sans-serif"))

        return chosen
    except Exception as e:
        if _env_on("TW_SECTOR_DEBUG_FONTS"):
            print(f"[TW_SECTOR_FONT_DEBUG] failed: {type(e).__name__}: {e}")
        return None
