# scripts/render_images_common/overview_mpl.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from scripts.render_images_common.overview.render import (
    render_overview_png as _render_overview_png,
)

# ✅ for debug (same logic as overview/render.py)
from scripts.render_images_common.overview.metrics import (  # noqa: E402
    auto_metric,
    badge_text,
    compute_pct,
    compute_value,
)
from scripts.render_images_common.overview.i18n_font import (  # noqa: E402
    normalize_market,
    resolve_lang,
)

__all__ = ["render_overview_png"]


def _env_float(name: str, default: float) -> float:
    v = (os.getenv(name) or "").strip()
    if not v:
        return float(default)
    try:
        return float(v)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    v = (os.getenv(name) or "").strip()
    if not v:
        return int(default)
    try:
        return int(float(v))
    except Exception:
        return int(default)


def _normalize_market(m: str) -> str:
    """
    Normalize market labels to short codes used across repo.
    """
    m = (m or "").strip().upper()
    alias = {
        "JPX": "JP",
        "JPN": "JP",
        "JAPAN": "JP",
        "TSE": "JP",
        "TOSE": "JP",
        "TOKYO": "JP",
    }
    return alias.get(m, m or "")


def _force_metric_for_market(payload: Dict[str, Any], metric: str) -> str:
    """
    Market-specific default overrides (only when caller uses auto):

    - JP: auto -> mix
    - TH: auto -> bigmove10   (✅ avoid touch-only pollution)
    - TW: auto -> mix         (✅ align with metrics.auto_metric)
    """
    m = _normalize_market(str(payload.get("market") or ""))
    met = (metric or "").strip().lower() or "auto"

    if met == "auto" and m == "JP":
        return "mix"

    # ✅ TH: always use bigmove10 for overview when metric=auto
    if met == "auto" and m == "TH":
        return "bigmove10"

    # ✅ TW: auto -> mix
    if met == "auto" and m == "TW":
        return "mix"

    return metric


def _default_page_size(payload: Dict[str, Any], metric: str, page_size: int) -> int:
    """
    Make bar height stable by limiting how many sectors per page.

    JP mix pages tend to have more sectors and long labels, so use smaller page size by default.
    - OVERRIDE only when caller didn't explicitly pass a custom page_size (i.e. using default).
    """
    # If caller explicitly sets page_size (not default 15), keep it.
    if page_size != 15:
        return int(page_size)

    m = _normalize_market(str(payload.get("market") or ""))
    met = (metric or "").strip().lower() or "auto"

    # Global default
    default_ps = _env_int("OVERVIEW_PAGE_SIZE_DEFAULT", 15)

    if m == "JP" and met in {"mix", "all", "bigmove10+locked+touched"}:
        return _env_int("OVERVIEW_PAGE_SIZE_JP_MIX", 12)

    return default_ps


# =============================================================================
# Debug: overview pct diagnostics (env-gated)
# =============================================================================
def _debug_overview_pct(payload: Dict[str, Any], metric_arg: str) -> None:
    """
    Prints sector-level pct diagnostics for overview badges.

    Enable by setting env:
      - OVERVIEW_DEBUG_PCT=1
    """
    if not (os.getenv("OVERVIEW_DEBUG_PCT") or "").strip():
        return

    try:
        market = normalize_market(str(payload.get("market", "") or ""))
        lang = resolve_lang(payload, market)

        met = (metric_arg or "auto").strip().lower() or "auto"
        metric_eff = auto_metric(payload, normalize_market) if met == "auto" else met
        if metric_eff in ("all", "bigmove10+locked+touched"):
            metric_eff = "mix"
        if metric_eff == "locked_plus_touched":
            metric_eff = "locked+touched"

        sector_summary = payload.get("sector_summary", []) or []
        if not isinstance(sector_summary, list):
            sector_summary = []

        sector_rows = [x for x in sector_summary if compute_value(x, metric_eff) > 0]

        print("\n" + "=" * 96)
        print(f"[OVERVIEW_DEBUG_PCT] market={market} metric={metric_eff} lang={lang} rows={len(sector_rows)}")
        print("=" * 96)

        for i, r in enumerate(sector_rows[:12]):
            if not isinstance(r, dict):
                continue

            sec = str(r.get("sector") or "")
            raw_locked = r.get("locked_pct")
            raw_touched = r.get("touched_pct")
            raw_big10 = r.get("bigmove10_pct")
            raw_mix = r.get("mix_pct")

            v = compute_value(r, metric_eff)
            p = compute_pct(r, metric_eff)
            c_text, p_text = badge_text(r, metric_eff, lang)

            print(f"[{i:02d}] {sec}")
            print(f"     raw: locked_pct={raw_locked} touched_pct={raw_touched} bigmove10_pct={raw_big10} mix_pct={raw_mix}")
            print(f"     calc: value={v} compute_pct={p}")
            print(f"     badge_text: count='{c_text}' pct='{p_text}'")

        print("=" * 96 + "\n")
    except Exception as e:
        # Never break rendering because of debug
        print(f"[OVERVIEW_DEBUG_PCT] ⚠️ debug failed: {type(e).__name__}: {e}")


def render_overview_png(
    payload: Dict[str, Any],
    out_dir: Path,
    *,
    width: int = 1080,
    height: int = 1920,
    page_size: int = 15,
    metric: str = "auto",
    bar_max_fill: Optional[float] = None,
    **kwargs,
) -> List[Path]:
    """
    Compatibility wrapper:

    - Some callers pass extra kwargs (e.g. lang=..., normalize_market=...).
      New overview renderer resolves lang internally, so we ignore those.

    - bar_max_fill:
        Controls the maximum bar height ratio (0~1).
        Smaller => more whitespace, less "always full" look.
        Default reads env OVERVIEW_BAR_MAX_FILL, fallback to 0.50.
    """
    # ignore legacy kwargs
    kwargs.pop("lang", None)
    kwargs.pop("normalize_market", None)
    kwargs.pop("market", None)

    # ✅ JP/TH/TW default overrides when metric=auto
    metric = _force_metric_for_market(payload, metric)

    # ✅ stabilize bar height by using smaller page size for JP mix (unless user overrides)
    page_size = _default_page_size(payload, metric, page_size)

    if bar_max_fill is None:
        bar_max_fill = _env_float("OVERVIEW_BAR_MAX_FILL", 0.50)

    # clamp
    try:
        bar_max_fill = float(bar_max_fill)
    except Exception:
        bar_max_fill = 0.50
    bar_max_fill = max(0.10, min(0.95, bar_max_fill))

    # ✅ env-gated debug (KR/TH/ALL markets)
    _debug_overview_pct(payload, metric)

    # Try pass-through; fallback if underlying renderer doesn't accept it yet.
    try:
        return _render_overview_png(
            payload,
            out_dir,
            width=width,
            height=height,
            page_size=page_size,
            metric=metric,
            bar_max_fill=bar_max_fill,
        )
    except TypeError:
        return _render_overview_png(
            payload,
            out_dir,
            width=width,
            height=height,
            page_size=page_size,
            metric=metric,
        )
