# scripts/render_images_tw/sector_blocks/_font.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import shutil
from pathlib import Path
from typing import Optional, List

import matplotlib.font_manager as fm
import matplotlib.pyplot as plt


def _env_on(name: str) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


# one-time guard
_FONT_REFRESH_DONE = False


def _maybe_refresh_mpl_fonts() -> None:
    """
    CI-safe refresh:
    - Clear matplotlib font cache
    - Force rebuild font manager

    This DOES NOT install fonts. It only makes newly-installed system fonts
    (installed by workflow) visible to matplotlib in the same job.
    """
    global _FONT_REFRESH_DONE
    if _FONT_REFRESH_DONE:
        return
    _FONT_REFRESH_DONE = True

    # Only do this on Linux CI by default (avoid slow local runs)
    is_ci = _env_on("CI") or bool(os.getenv("GITHUB_ACTIONS"))
    is_linux = sys.platform.startswith("linux")

    if not (is_ci and is_linux):
        # still allow forcing on any platform
        if not _env_on("TW_SECTOR_FORCE_REFRESH_FONTS"):
            return

    # optional: allow disabling even on CI
    if _env_on("TW_SECTOR_DISABLE_REFRESH_FONTS"):
        return

    # Clear cache dir
    try:
        cache_dir = Path.home() / ".cache" / "matplotlib"
        if cache_dir.exists():
            shutil.rmtree(cache_dir, ignore_errors=True)
    except Exception:
        pass

    # Force rebuild font manager (ignore cached fontlist)
    try:
        fm._load_fontmanager(try_read_cache=False)  # type: ignore[attr-defined]
    except Exception:
        # fallback: touching fontManager triggers lazy rebuild in some versions
        try:
            _ = fm.fontManager.ttflist
        except Exception:
            pass


def setup_cjk_font() -> Optional[str]:
    """
    TW: CJK safe font selection (CI-friendly).

    Notes:
    - This function does NOT install fonts.
    - It can refresh matplotlib font cache so CI can see fonts installed earlier
      in the same workflow.
    """
    _maybe_refresh_mpl_fonts()

    font_candidates: List[str] = [
        # Best on Linux CI (fonts-noto-cjk)
        "Noto Sans CJK TC",
        "Noto Sans CJK SC",
        "Noto Sans CJK JP",
        "Noto Sans CJK KR",

        # Some distros expose these names
        "Noto Sans TC",
        "Noto Sans SC",
        "Noto Sans JP",
        "Noto Sans KR",

        # Windows/macOS
        "Microsoft JhengHei",
        "PingFang TC",
        "Arial Unicode MS",

        # Last resort (may tofu for CJK)
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
                sample = sorted([n for n in available if ("noto" in n.lower()) or ("cjk" in n.lower())])[:150]
                print("[TW_SECTOR_FONT_DEBUG] no candidate matched.")
                print("[TW_SECTOR_FONT_DEBUG] sample(notocjk) =", sample)
            return None

        chain = [chosen] + [x for x in font_candidates if x != chosen and x in available]
        if not chain:
            chain = [chosen]

        plt.rcParams["font.family"] = "sans-serif"
        plt.rcParams["font.sans-serif"] = chain
        plt.rcParams["axes.unicode_minus"] = False

        if _env_on("TW_SECTOR_DEBUG_FONTS"):
            print("[TW_SECTOR_FONT_DEBUG] chosen =", chosen)
            print("[TW_SECTOR_FONT_DEBUG] chain  =", chain)
            try:
                path = fm.findfont(fm.FontProperties(family=chosen), fallback_to_default=True)
                print("[TW_SECTOR_FONT_DEBUG] findfont(chosen) =", path)
            except Exception as e:
                print("[TW_SECTOR_FONT_DEBUG] findfont failed:", type(e).__name__, e)

        return chosen
    except Exception as e:
        if _env_on("TW_SECTOR_DEBUG_FONTS"):
            print(f"[TW_SECTOR_FONT_DEBUG] failed: {type(e).__name__}: {e}")
        return None
