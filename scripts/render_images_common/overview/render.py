# scripts/render_images_common/overview/render.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
from matplotlib.font_manager import FontProperties

from .metrics import (
    auto_metric,
    badge_text,
    compute_value,
    compute_pct,  # for debug/sanity
)

from .strings import (
    empty_text_for_metric,
    footer_right_for_market,
    title_for_metric,
)

from .footer import (
    build_footer_center_lines,  # ✅ footer numbers decided by footer_calc.py
)

from .gain_bins import (
    gainbins_footer_center_lines,
)

# ✅ i18n font (add fontprops_for_text)
try:
    from .i18n_font import (
        normalize_market,
        resolve_lang,
        setup_cjk_font,
        fontprops_for_text,  # ✅ NEW
    )
except Exception:
    from .i18n_font import normalize_market, resolve_lang, setup_cjk_font

    # Fallback (won't crash if your i18n_font.py hasn't added it yet)
    def fontprops_for_text(
        _text: str,
        *,
        market: str = "",
        payload: Optional[Dict[str, Any]] = None,
        weight: Optional[str] = None,
    ) -> FontProperties:
        return FontProperties(family="sans-serif", weight=(weight or "regular"))


from .text import ellipsize_to_px, text_px
from .timefmt import date_for_display, subtitle_one_line

# ✅ Step B: move CN adapter out
from .adapters import cn_row_for_mix

# ✅ Step A: centralize gainbins policy/paging
from .paging import get_gainbins_rows_and_lang, should_force_paging


# =============================================================================
# Badge positioning helpers (pixel-aware)
# =============================================================================
def _px_to_data(ax, renderer, x_max: float, px: float) -> float:
    bbox = ax.get_window_extent(renderer=renderer)
    w = float(bbox.width) if bbox.width else 1.0
    return (x_max / w) * float(px)


def _text_w_data(fig, renderer, ax, x_max: float, text: str, fp: FontProperties, fs: float) -> float:
    if not text:
        return 0.0
    w_px = text_px(fig, renderer, text, fp, fs)
    return _px_to_data(ax, renderer, x_max, w_px)


# =============================================================================
# Small env helpers
# =============================================================================
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


def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return bool(default)
    return v in ("1", "true", "yes", "y", "on")


# =============================================================================
# Label / badge styles
# =============================================================================
def _sector_label_color(market: str) -> str:
    m = (market or "").upper()
    if m in {"JP", "JPX", "JPN"}:
        return "white"
    if m in {"US", "CA"}:
        return "#A8D8FF"
    if m in {"AU"}:
        return "#B6FFB0"
    if m in {"UK", "EU"}:
        return "#D8B6FF"
    return "white"


def _pct_badge_style(_market: str) -> tuple[str, float, float]:
    # keep the same tuned style
    return ("#FFD54A", 0.95, 0.35)


# =============================================================================
# Debug: pct dump
# =============================================================================
def _debug_pct_dump(
    *,
    market: str,
    metric: str,
    lang: str,
    sector_rows: List[Dict[str, Any]],
    max_n: int = 8,
) -> None:
    """
    Print pct source & computed values.
    Enable:
      $env:OVERVIEW_DEBUG_PCT="1"
    """
    print("\n" + "=" * 96)
    print(f"[OVERVIEW_DEBUG_PCT] market={market} metric={metric} lang={lang} rows={len(sector_rows)}")
    print("=" * 96)

    for i, r0 in enumerate(sector_rows[: max_n]):
        r = cn_row_for_mix(r0, market, metric)

        sec = str(r.get("sector", ""))
        locked_pct = r.get("locked_pct", None)
        touched_pct = r.get("touched_pct", None)
        bigmove10_pct = r.get("bigmove10_pct", None)
        mix_pct = r.get("mix_pct", None)

        p = compute_pct(r, metric)
        v = compute_value(r, metric)
        ct, pt = badge_text(r, metric, lang)

        print(f"[{i:02d}] {sec}")
        print(f"     raw: locked_pct={locked_pct} touched_pct={touched_pct} bigmove10_pct={bigmove10_pct} mix_pct={mix_pct}")
        print(f"     calc: value={v} compute_pct={p}")
        print(f"     badge_text: count='{ct}' pct='{pt}'")

        if p is not None and float(p) >= 0.999 and (r.get("sector_total") not in (None, 0, 1)):
            print("     !!! WARNING: pct~=1.0 but sector_total not 0/1 (check import/version or data mutation)")
    print("=" * 96 + "\n")


# =============================================================================
# Core render (single page)
# =============================================================================
def _render_one_page(
    *,
    payload: Optional[Dict[str, Any]],
    sector_rows: List[Dict[str, Any]],
    out_path: Path,
    market: str,
    width: int,
    height: int,
    metric: str,
    subtitle: str,
    lang: str,
    footer_right: str = "",
    footer_note_text: str = "",
    footer_center1: str = "",
    footer_center2: str = "",
    footer_center3: str = "",
    footer_center4: str = "",
    bar_max_fill: float | None = None,
) -> None:
    # force overwrite (avoid viewer caching / not replaced)
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if out_path.exists():
            out_path.unlink()
    except Exception as e:
        print(f"[WARN] could not delete existing output: {out_path} err={e}")

    # -------------------------------------------------------------------------
    # Layout
    # -------------------------------------------------------------------------
    bar_left = 0.55
    bar_right = 0.96

    top = _env_float("OVERVIEW_BAR_TOP", 0.830)
    bottom = _env_float("OVERVIEW_BAR_BOTTOM", 0.174)

    top = max(0.30, min(0.95, float(top)))
    bottom = max(0.05, min(0.35, float(bottom)))
    if bottom >= top:
        top, bottom = 0.830, 0.174

    label_left = 0.06
    label_right = bar_left - 0.02

    # ✅ CN mix_ex_st mapping (per-row)
    rows_eff = [cn_row_for_mix(r, market, metric) for r in sector_rows]

    sectors_raw = [str(x.get("sector", "") or "") for x in rows_eff]
    values = [compute_value(x, metric) for x in rows_eff]
    max_v = max(values) if values else 0

    if max_v <= 0:
        x_max = 1.0
    elif max_v == 1:
        x_max = 4.0
    else:
        x_max = float(max_v) * 1.2

    fig = plt.figure(figsize=(width / 100, height / 100), dpi=100, facecolor="#0f0f1e")
    ax_lbl = fig.add_axes(
        [label_left, bottom, max(0.01, label_right - label_left), top - bottom],
        facecolor="#0f0f1e",
    )
    ax_bar = fig.add_axes([bar_left, bottom, bar_right - bar_left, top - bottom], facecolor="#0f0f1e")

    for ax in (ax_lbl, ax_bar):
        ax.set_xticks([])
        ax.set_yticks([])
        for s in ("top", "right", "bottom", "left"):
            ax.spines[s].set_visible(False)

    y_pos = list(range(len(sectors_raw)))

    colors: List[str] = []
    for i, v in enumerate(values):
        intensity = 0.5 + 0.5 * (v / max_v) if max_v > 0 else 0.8
        if i == 0:
            colors.append(f"#{int(255 * intensity):02x}3030")
        elif i < 3:
            colors.append(f"#{int(255 * intensity):02x}5030")
        else:
            colors.append(f"#{int(200 * intensity):02x}6050")

    # -------------------------------------------------------------------------
    # Bar thickness strategy
    # -------------------------------------------------------------------------
    n = len(rows_eff)
    ref_rows = _env_int("OVERVIEW_REF_ROWS", 15)

    if n <= 0:
        bar_h = 0.78
    elif n <= ref_rows:
        bar_h = 0.78
    else:
        bar_h = 0.70

    ax_bar.barh(y_pos, values, color=colors, height=bar_h, edgecolor="none")
    ax_bar.set_xlim(0, x_max)
    ax_bar.invert_yaxis()

    # -------------------------------------------------------------------------
    # Y-axis range strategy
    # -------------------------------------------------------------------------
    if n > 0:
        if n <= ref_rows:
            pad_rows = max(0.0, (ref_rows - n) / 2.0)
            min_pad = _env_float("OVERVIEW_MIN_PAD_ROWS", 0.30)
            pad_rows = max(pad_rows, max(0.0, min_pad))
        else:
            if bar_max_fill is not None:
                base_fill = float(bar_max_fill)
            else:
                base_fill = _env_float("OVERVIEW_BAR_MAX_FILL", _env_float("OVERVIEW_Y_FILL", 0.60))
            base_fill = max(0.35, min(0.92, base_fill))

            y_fill = base_fill
            pad_rows = max(0.0, (n / y_fill - n) / 2.0)

            min_pad = _env_float("OVERVIEW_MIN_PAD_ROWS", 0.30)
            pad_rows = max(pad_rows, max(0.0, min_pad))

        y_top = -0.5 - pad_rows
        y_bot = (n - 0.5) + pad_rows
        ax_bar.set_ylim(y_bot, y_top)
        ax_lbl.set_ylim(ax_bar.get_ylim())

    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()

    # ---- sector labels ----
    label_fontsize = 44
    # ✅ Use same font selection for label text (Thai/CJK safe)
    fp_lbl = fontprops_for_text(
        " ".join(sectors_raw[:3]) if sectors_raw else "",
        market=market,
        payload=payload,
        weight="medium",
    )

    label_area_px = width * (label_right - label_left)
    max_label_px = max(80.0, label_area_px - 40)
    sectors = [ellipsize_to_px(fig, renderer, s, max_label_px, fp_lbl, label_fontsize) for s in sectors_raw]

    sector_color = _sector_label_color(market)
    for i, s in enumerate(sectors):
        ax_lbl.text(
            0.98,
            i,
            s,
            transform=ax_lbl.get_yaxis_transform(),
            ha="right",
            va="center",
            fontsize=label_fontsize,
            color=sector_color,
            fontproperties=fp_lbl,  # ✅
        )

    # ---- badges ----
    fs_count = 46
    fs_pct = 28
    if (market or "").upper() in {"US", "CA", "AU", "UK", "EU"}:
        fs_pct = 32

    pad_in_px = 18.0
    gap_out_px = 14.0
    min_gap_px = 2.0
    right_margin_px = 12.0

    pad_in = _px_to_data(ax_bar, renderer, x_max, pad_in_px)
    gap_out = _px_to_data(ax_bar, renderer, x_max, gap_out_px)
    min_gap = _px_to_data(ax_bar, renderer, x_max, min_gap_px)
    right_margin = _px_to_data(ax_bar, renderer, x_max, right_margin_px)

    OUTSIDE_COUNTS = {1, 2}
    pct_color, pct_text_alpha, pct_box_alpha = _pct_badge_style(market)

    for i, row_eff in enumerate(rows_eff):
        v = float(compute_value(row_eff, metric))
        count_text, pct_text = badge_text(row_eff, metric, lang)

        # ✅ badges are mostly latin digits, but still use unified resolver
        fp_count = fontprops_for_text(count_text or "0", market=market, payload=payload, weight="bold")
        fp_pct = fontprops_for_text(pct_text or "0%", market=market, payload=payload, weight="bold")

        if v <= 0:
            count_x = x_max * 0.02
            count_ha = "left"
        else:
            count_x = max(v - pad_in, x_max * 0.08)
            count_ha = "right"

        ax_bar.text(
            count_x,
            i - 0.06,
            count_text,
            va="center",
            ha=count_ha,
            fontsize=fs_count,
            color="white",
            weight="bold",
            fontproperties=fp_count,  # ✅
            bbox=dict(boxstyle="round,pad=0.3", facecolor="black", alpha=0.3, edgecolor="none"),
        )

        if not pct_text:
            continue

        pct_w = _text_w_data(fig, renderer, ax_bar, x_max, pct_text, fp_pct, fs_pct)
        max_start = x_max - right_margin - pct_w

        try:
            cnt_int = int(
                str(
                    row_eff.get(
                        "cnt",
                        row_eff.get("value", row_eff.get("locked_cnt", row_eff.get("touched_cnt", 0))),
                    )
                )
                or 0
            )
        except Exception:
            cnt_int = 0

        want_outside = (cnt_int in OUTSIDE_COUNTS) or (v <= max(2.0, x_max * 0.12))

        if v <= 0:
            pct_x = x_max * 0.02
            pct_ha = "left"
        elif want_outside:
            pct_x = v + gap_out
            pct_ha = "left"
            if pct_x > max_start:
                pct_x = max(v - pad_in, x_max * 0.02)
                pct_ha = "right"
            if pct_ha == "left":
                pct_x = max(pct_x, v + min_gap)
        else:
            pct_x = max(v - pad_in, x_max * 0.02)
            pct_ha = "right"

        m_up = (market or "").upper()
        stroke_w = 4 if m_up in {"US", "CA", "AU", "UK", "EU"} else 3

        ax_bar.text(
            pct_x,
            i + 0.20,
            pct_text,
            va="center",
            ha=pct_ha,
            fontsize=fs_pct,
            color=pct_color,
            weight="bold",
            fontproperties=fp_pct,  # ✅
            alpha=pct_text_alpha,
            bbox=dict(boxstyle="round,pad=0.20", facecolor="black", alpha=pct_box_alpha, edgecolor="none"),
            path_effects=[pe.withStroke(linewidth=stroke_w, foreground="black", alpha=0.90)],
        )

    # ---- title + subtitle ----
    title = title_for_metric(metric, market, lang)

    # ✅ Title font selection must match actual render & measurement
    title_fp = fontprops_for_text(title, market=market, payload=payload, weight="bold")

    fig.canvas.draw()
    renderer = fig.canvas.get_renderer()

    if "\n" in title:
        fig.text(
            0.5,
            0.965,
            title,
            ha="center",
            va="top",
            fontsize=44,
            color="white",
            weight="bold",
            linespacing=1.1,
            fontproperties=title_fp,  # ✅
        )
        subtitle_y = 0.885
    else:
        title_fs = 56
        max_title_px = width * 0.92
        if text_px(fig, renderer, title, title_fp, title_fs) <= max_title_px:
            fig.text(
                0.5,
                0.965,
                title,
                ha="center",
                va="top",
                fontsize=title_fs,
                color="white",
                weight="bold",
                fontproperties=title_fp,  # ✅
            )
            subtitle_y = 0.91
        else:
            cut = max(10, min(14, len(title) // 2))
            title2 = title[:cut] + "\n" + title[cut:]
            title2_fp = fontprops_for_text(title2, market=market, payload=payload, weight="bold")
            fig.text(
                0.5,
                0.965,
                title2,
                ha="center",
                va="top",
                fontsize=48,
                color="white",
                weight="bold",
                linespacing=1.1,
                fontproperties=title2_fp,  # ✅
            )
            subtitle_y = 0.885

    if subtitle:
        subtitle_fp = fontprops_for_text(subtitle, market=market, payload=payload, weight="regular")
        fig.text(
            0.5,
            subtitle_y,
            subtitle,
            ha="center",
            va="top",
            fontsize=32,
            color="#aaa",
            style="italic",
            linespacing=1.25,
            fontproperties=subtitle_fp,  # ✅
        )

    # ---- bottom footer ----
    y1 = 0.112
    y2 = 0.074
    y3 = 0.044
    y4 = 0.020

    fs_center_env = _env_int("OVERVIEW_FOOTER_CENTER_FS", 0)

    line_len = max(len(footer_center1 or ""), len(footer_center2 or ""), len(footer_center3 or ""))
    fs_auto = 30
    if line_len >= 32:
        fs_auto = 28
    if line_len >= 38:
        fs_auto = 26
    if line_len >= 44:
        fs_auto = 24
    if line_len >= 52:
        fs_auto = 22
    if line_len >= 60:
        fs_auto = 20

    fs_center = fs_center_env if (fs_center_env and fs_center_env > 0) else fs_auto
    fs_disc_env = _env_int("OVERVIEW_FOOTER_DISCLAIMER_FS", 22)

    footer_fp = fontprops_for_text(
        " ".join([footer_center1 or "", footer_center2 or "", footer_center3 or "", footer_center4 or ""]).strip(),
        market=market,
        payload=payload,
        weight="bold",
    )
    footer_disc_fp = fontprops_for_text(footer_center4 or "", market=market, payload=payload, weight="regular")
    footer_right_fp = fontprops_for_text(footer_right or "", market=market, payload=payload, weight="regular")

    if footer_center1:
        fig.text(
            0.5,
            y1,
            footer_center1,
            ha="center",
            va="center",
            fontsize=fs_center,
            color="#d9d9d9",
            weight="bold",
            fontproperties=footer_fp,  # ✅
        )
    if footer_center2:
        fig.text(
            0.5,
            y2,
            footer_center2,
            ha="center",
            va="center",
            fontsize=fs_center,
            color="#d9d9d9",
            weight="bold",
            alpha=0.92,
            fontproperties=footer_fp,  # ✅
        )
    if footer_center3:
        fig.text(
            0.5,
            y3,
            footer_center3,
            ha="center",
            va="center",
            fontsize=fs_center,
            color="#d9d9d9",
            weight="bold",
            alpha=0.92,
            fontproperties=footer_fp,  # ✅
        )
    if footer_center4:
        fig.text(
            0.5,
            y4,
            footer_center4,
            ha="center",
            va="center",
            fontsize=fs_disc_env,
            color="#FFD54A",
            alpha=0.65,
            fontproperties=footer_disc_fp,  # ✅
        )

    if footer_right:
        fig.text(
            0.98,
            0.010,
            footer_right,
            ha="right",
            va="bottom",
            fontsize=22,
            color="#555",
            alpha=0.6,
            fontproperties=footer_right_fp,  # ✅
        )

    _ = footer_note_text  # intentionally unused

    fig.savefig(out_path, dpi=100, facecolor="#0f0f1e", edgecolor="none", bbox_inches=None, pad_inches=0.0)
    plt.close(fig)
    print(f"✅ 已產生：{out_path}")


def _render_empty(
    *,
    payload: Optional[Dict[str, Any]],
    out_path: Path,
    ymd: str,
    width: int,
    height: int,
    metric: str,
    lang: str,
    subtitle: str = "",
    market: str = "",
) -> None:
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if out_path.exists():
            out_path.unlink()
    except Exception:
        pass

    fig = plt.figure(figsize=(width / 100, height / 100), dpi=100, facecolor="#1a1a2e")

    main_text = empty_text_for_metric(metric, lang)
    fp_main = fontprops_for_text(main_text, market=market, payload=payload, weight="bold")
    fig.text(
        0.5,
        0.52,
        main_text,
        ha="center",
        va="center",
        fontsize=56,
        color="white",
        weight="bold",
        fontproperties=fp_main,  # ✅
    )

    if ymd:
        fp_ymd = fontprops_for_text(ymd, market=market, payload=payload, weight="regular")
        fig.text(0.5, 0.42, ymd, ha="center", va="center", fontsize=36, color="#888", fontproperties=fp_ymd)  # ✅

    if subtitle:
        fp_sub = fontprops_for_text(subtitle, market=market, payload=payload, weight="regular")
        fig.text(
            0.5,
            0.34,
            subtitle,
            ha="center",
            va="center",
            fontsize=30,
            color="#888",
            style="italic",
            linespacing=1.25,
            fontproperties=fp_sub,  # ✅
        )

    fig.savefig(out_path, dpi=100, facecolor="#1a1a2e", bbox_inches=None, pad_inches=0.0)
    plt.close(fig)
    print(f"✅ 已產生：{out_path}")


# =============================================================================
# Public API
# =============================================================================
def render_overview_png(
    payload: Dict[str, Any],
    out_dir: Path,
    *,
    width: int = 1080,
    height: int = 1920,
    page_size: int = 15,
    metric: str = "auto",
    bar_max_fill: float | None = None,
) -> List[Path]:
    # ✅ must set rcParams font list first
    setup_cjk_font(payload)

    out_dir.mkdir(parents=True, exist_ok=True)

    sector_summary = payload.get("sector_summary", []) or []
    asof = str(payload.get("asof", "") or "")

    market = normalize_market(str(payload.get("market", "") or ""))
    lang = resolve_lang(payload, market)

    metric_in = (metric or "auto").strip().lower()
    metric_eff = auto_metric(payload, normalize_market) if metric_in == "auto" else (metric_in or "locked")
    if metric_eff in ("all", "bigmove10+locked+touched"):
        metric_eff = "mix"
    if metric_eff == "locked_plus_touched":
        metric_eff = "locked+touched"

    # -------------------------
    # DEBUG: raw sector_summary
    # -------------------------
    if (market or "").upper() == "CN" and _env_bool("OVERVIEW_DEBUG_CN_SECTOR_SUMMARY", False):
        secs = sorted({str(x.get("sector", "") or "") for x in sector_summary})
        print("\n" + "=" * 96)
        print(f"[CN_SECTOR_SUMMARY_DEBUG] metric={metric_eff} raw_rows={len(sector_summary)} uniq_sectors={len(secs)}")
        print("has 光伏加工设备 ?", "光伏加工设备" in secs)
        print("has 半导体设备 ?", "半导体设备" in secs)
        print("sectors(sample 30):", secs[:30])
        print("=" * 96 + "\n")

    # ✅ aggregator-first: bigmove10_cnt/mix_pct already in payload['sector_summary']
    # ✅ CN-only: map mix -> mix_ex_st_* for value/pct/badge
    sector_rows0: List[Dict[str, Any]] = []
    for x in sector_summary:
        xr = cn_row_for_mix(x, market, metric_eff)
        if compute_value(xr, metric_eff) > 0:
            sector_rows0.append(xr)

    if _env_bool("OVERVIEW_DEBUG_PCT", False):
        _debug_pct_dump(
            market=market,
            metric=metric_eff,
            lang=lang,
            sector_rows=sector_rows0,
            max_n=12,
        )

    subtitle = subtitle_one_line(payload, market=market, asof=asof, lang=lang, normalize_market=normalize_market)
    footer_r = footer_right_for_market(market, lang, normalize_market)
    ymd_disp = date_for_display(payload)

    footer_c1, footer_c2, footer_c3, footer_c4 = build_footer_center_lines(
        payload,
        metric=metric_eff,
        market=market,
        lang=lang,
        normalize_market=normalize_market,
        sector_rows=sector_rows0,  # harmless extra kwarg (footer ignores / uses for debug)
    )

    out_paths: List[Path] = []

    # ✅ Step A: centralized gainbins policy (CN/TW disabled here)
    gain_rows, lang_bins = get_gainbins_rows_and_lang(payload, market=market, lang=lang)

    if not sector_rows0:
        out_path = out_dir / f"overview_sectors_{metric_eff}_p1.png"
        _render_empty(
            payload=payload,
            out_path=out_path,
            ymd=ymd_disp,
            width=width,
            height=height,
            metric=metric_eff,
            lang=lang,
            subtitle=subtitle,
            market=market,
        )
        out_paths.append(out_path)

        if gain_rows:
            out_path2 = out_dir / f"overview_sectors_{metric_eff}_p2.png"
            c1, c2, c3, c4 = gainbins_footer_center_lines(payload, lang_bins)
            _render_one_page(
                payload=payload,
                sector_rows=gain_rows,
                out_path=out_path2,
                market=market,
                width=width,
                height=height,
                metric="gainbins",
                subtitle=subtitle,
                lang=lang_bins,
                footer_right=footer_r,
                footer_note_text="",
                footer_center1=c1,
                footer_center2=c2,
                footer_center3=c3,
                footer_center4=c4,
                bar_max_fill=bar_max_fill,
            )
            out_paths.append(out_path2)

        return out_paths

    # sort by effective value (CN mix uses mix_ex_st)
    sector_rows = sorted(
        sector_rows0,
        key=lambda r: compute_value(cn_row_for_mix(r, market, metric_eff), metric_eff),
        reverse=True,
    )

    # -------------------------
    # DEBUG: final sector_rows
    # -------------------------
    if (market or "").upper() == "CN" and _env_bool("OVERVIEW_DEBUG_CN_SECTOR_ROWS", False):
        print("\n" + "=" * 96)
        print(f"[CN_SECTOR_ROWS_DEBUG] metric={metric_eff} rows(value>0)={len(sector_rows)} page_size={page_size}")
        for i, r in enumerate(sector_rows[:80], 1):
            rr = cn_row_for_mix(r, market, metric_eff)
            v = compute_value(rr, metric_eff)
            print(f"{i:02d}. {rr.get('sector')} value={v}")
        print("=" * 96 + "\n")

    pages: List[List[Dict[str, Any]]] = [sector_rows[i : i + page_size] for i in range(0, len(sector_rows), page_size)]
    force_paging = should_force_paging(gain_rows)

    for idx, rows in enumerate(pages, start=1):
        if force_paging:
            fname = f"overview_sectors_{metric_eff}_p{idx}.png"
        else:
            fname = f"overview_sectors_{metric_eff}.png" if len(pages) == 1 else f"overview_sectors_{metric_eff}_p{idx}.png"

        out_path = out_dir / fname
        _render_one_page(
            payload=payload,  # ✅ NEW
            sector_rows=rows,
            out_path=out_path,
            market=market,
            width=width,
            height=height,
            metric=metric_eff,
            subtitle=subtitle,
            lang=lang,
            footer_right=footer_r,
            footer_note_text="",
            footer_center1=footer_c1,
            footer_center2=footer_c2,
            footer_center3=footer_c3,
            footer_center4=footer_c4,
            bar_max_fill=bar_max_fill,
        )
        out_paths.append(out_path)

    if gain_rows:
        out_path2 = out_dir / f"overview_sectors_{metric_eff}_p{len(pages) + 1}.png"
        c1, c2, c3, c4 = gainbins_footer_center_lines(payload, lang_bins)
        _render_one_page(
            payload=payload,  # ✅ NEW
            sector_rows=gain_rows,
            out_path=out_path2,
            market=market,
            width=width,
            height=height,
            metric="gainbins",
            subtitle=subtitle,
            lang=lang_bins,
            footer_right=footer_r,
            footer_note_text="",
            footer_center1=c1,
            footer_center2=c2,
            footer_center3=c3,
            footer_center4=c4,
            bar_max_fill=bar_max_fill,
        )
        out_paths.append(out_path2)

    return out_paths
