# scripts/render_images_common/overview/text.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Optional

import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties


def _rc_sans_list() -> list[str]:
    ss = plt.rcParams.get("font.sans-serif") or []
    # rcParams may contain non-str in weird cases; sanitize
    out: list[str] = []
    for x in ss:
        try:
            s = str(x).strip()
        except Exception:
            continue
        if s:
            out.append(s)
    return out or ["sans-serif"]


def _looks_like_dejavu_only(fp: FontProperties) -> bool:
    """
    Detect cases where caller passed a too-narrow fontprops that causes tofu,
    e.g. family=['DejaVu Sans'] or just ['sans-serif'].
    """
    try:
        fam = fp.get_family() or []
    except Exception:
        fam = []
    fam = [str(x).strip() for x in fam if str(x).strip()]
    if not fam:
        return True

    # Single family that is DejaVu or generic sans-serif → risky for CJK measurement
    if len(fam) == 1:
        f0 = fam[0].lower()
        if "dejavu sans" in f0:
            return True
        if f0 in {"sans-serif", "sans serif", "sans"}:
            return True

    return False


def _rc_head_is_not_dejavu() -> bool:
    """
    If rcParams sans-serif head is not DejaVu, it usually means market-specific
    font setup already chose a better primary (e.g. JP: Noto Sans CJK JP).
    In that case, measurement should follow rcParams even if caller passes DejaVu-only fp.
    """
    ss = _rc_sans_list()
    head = (ss[0] if ss else "").lower()
    return ("dejavu" not in head) and (head not in {"sans-serif", "sans serif", "sans"})


def _ensure_fp(fp: Optional[FontProperties]) -> FontProperties:
    """
    Ensure we ALWAYS have a FontProperties object for measurement.

    Key behavior (safe, quasi "JP-only"):
    - If caller passes a DejaVu-only / generic fp (often causes tofu warnings),
      AND rcParams sans-serif head is NOT DejaVu (meaning a market-specific primary
      font was chosen, e.g. JP: Noto Sans CJK JP),
      then we override to use rcParams sans-serif fallback list.
    """
    if isinstance(fp, FontProperties):
        # ✅ guard: override "bad" caller fp only when rcParams indicates a better primary
        if _looks_like_dejavu_only(fp) and _rc_head_is_not_dejavu():
            fam = _rc_sans_list()
            return FontProperties(family=fam)
        return fp

    fam = _rc_sans_list()
    # FontProperties can take a list as family; matplotlib will fallback in order.
    return FontProperties(family=fam)


def text_px(
    fig,
    renderer,
    text: str,
    fontprops: Optional[FontProperties],
    fontsize: float,
) -> float:
    """
    Measure text width in pixels using the SAME FontProperties as actual drawing.
    """
    fp = _ensure_fp(fontprops)
    t = plt.Text(0, 0, text, fontproperties=fp, fontsize=fontsize)
    t.set_figure(fig)
    bbox = t.get_window_extent(renderer=renderer)
    return float(bbox.width)


def ellipsize_to_px(
    fig,
    renderer,
    text: str,
    max_px: float,
    fontprops: Optional[FontProperties],
    fontsize: float,
) -> str:
    """
    Ellipsize `text` so its measured width <= max_px, using the SAME FontProperties.
    """
    if not text:
        return ""
    if max_px <= 0:
        return "..."

    fp = _ensure_fp(fontprops)

    if text_px(fig, renderer, text, fp, fontsize) <= max_px:
        return text

    suffix = "..."
    base = text.strip()
    lo, hi = 0, len(base)
    best = suffix

    while lo <= hi:
        mid = (lo + hi) // 2
        cand = base[:mid].rstrip() + suffix
        if text_px(fig, renderer, cand, fp, fontsize) <= max_px:
            best = cand
            lo = mid + 1
        else:
            hi = mid - 1

    return best


def safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""
