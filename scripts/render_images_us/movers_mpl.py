# scripts/render_images_us/movers_mpl.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any, Dict, List

import matplotlib.pyplot as plt

_US_NAME_CLEAN_RE = re.compile(
    r"\s*(?:Common Stock|COmmon Stock|Ordinary Shares?|"
    r"American Depository Shares|ADS|Depositary Shares|"
    r"Class\s+[A-Z]\s+Ordinary Shares|Class\s+[A-Z]\s+Common Stock|"
    r"\([^)]+\))\s*$",
    re.IGNORECASE
)


def clean_us_name(name: str) -> str:
    s = (name or "").strip()
    for _ in range(3):
        s2 = _US_NAME_CLEAN_RE.sub("", s).strip()
        if s2 == s:
            break
        s = s2
    s = re.sub(r"\s+", " ", s).strip()
    return s


def ellipsis_1line(s: str, max_chars: int) -> str:
    s = " ".join((s or "").strip().split())
    if len(s) <= max_chars:
        return s
    if max_chars <= 1:
        return "…"
    return s[: max_chars - 1].rstrip() + "…"


def _get_movers(payload: Dict[str, Any], min_ret: float) -> List[Dict[str, Any]]:
    rows = payload.get("snapshot_open") or payload.get("snapshot_main") or []
    out = []
    for r in rows:
        try:
            ret = float(r.get("ret", 0) or 0)
        except Exception:
            continue
        if ret >= min_ret:
            out.append(r)
    out.sort(key=lambda x: float(x.get("ret", 0) or 0), reverse=True)
    return out


def render_us_movers_pages(
    *,
    payload: Dict[str, Any],
    out_dir: Path,
    min_ret: float = 0.10,
    top: int = 120,
    rows_per_page: int = 18,
    name_max_chars: int = 28,
    width: int = 1080,
    height: int = 1920,
) -> List[Path]:
    movers = _get_movers(payload, min_ret=min_ret)[: max(0, int(top))]
    if not movers:
        return []

    ymd_eff = str(payload.get("ymd_effective") or payload.get("ymd") or "")
    asof = str(payload.get("asof") or payload.get("slot") or "")
    title = f"US Movers ≥ {min_ret*100:.0f}%"
    subtitle = f"{ymd_eff}  asof {asof}".strip()

    pages = int(math.ceil(len(movers) / float(rows_per_page)))
    out_paths: List[Path] = []

    dpi = 100
    fig_w = width / dpi
    fig_h = height / dpi

    for pi in range(pages):
        chunk = movers[pi * rows_per_page : (pi + 1) * rows_per_page]

        fig = plt.figure(figsize=(fig_w, fig_h), dpi=dpi)
        ax = fig.add_axes([0, 0, 1, 1])
        ax.set_axis_off()

        # header
        ax.text(0.06, 0.95, title, fontsize=36, fontweight="bold", va="top")
        ax.text(0.06, 0.91, subtitle, fontsize=18, va="top")
        ax.text(0.94, 0.95, f"{pi+1}/{pages}", fontsize=18, va="top", ha="right")

        # columns
        y0 = 0.86
        dy = 0.80 / max(1, rows_per_page)  # fit rows
        x_sym = 0.06
        x_ret = 0.28
        x_stk = 0.45
        x_sec = 0.56
        x_name = 0.06

        # row draw
        for i, r in enumerate(chunk):
            y = y0 - i * dy
            sym = str(r.get("symbol") or "")
            sector = str(r.get("sector") or "Unknown")
            name = clean_us_name(str(r.get("name") or r.get("company_name") or ""))
            name = ellipsis_1line(name, name_max_chars)

            ret = float(r.get("ret", 0) or 0)
            streak = int(r.get("streak", 0) or 0)

            ax.text(x_sym, y, f"{sym}", fontsize=22, fontweight="bold", va="center")
            ax.text(x_ret, y, f"{ret*100:,.2f}%", fontsize=22, va="center")
            ax.text(x_stk, y, f"🔥{streak}" if streak > 0 else "", fontsize=20, va="center")
            ax.text(x_sec, y, f"{sector}", fontsize=18, va="center")

            # company name: single line + ellipsis (placed under the row)
            ax.text(x_name, y - dy * 0.45, name, fontsize=18, va="center")

        out = out_dir / f"us_movers_{ymd_eff}_p{pi+1}.png"
        fig.savefig(out, dpi=dpi)
        plt.close(fig)
        out_paths.append(out)

    return out_paths
