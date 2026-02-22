# scripts/render_images/overview_mpl.py
# -*- coding: utf-8 -*-
"""
產業漲停家數 Overview（Matplotlib 版）
- 專為 YouTube Shorts / TikTok / Reels 設計（9:16 直式）
- 穩定、不依賴 Kaleido

✅ 行為：
- 只統計 locked_cnt > 0 的產業
- 若產業數 > page_size（預設 15）才自動分頁
- 回傳所有產生的 overview 圖片路徑（List[Path]）
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, Optional, List

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm


# =============================================================================
# Font
# =============================================================================
def _setup_chinese_font() -> Optional[str]:
    try:
        font_candidates = [
            "Microsoft JhengHei",
            "Microsoft YaHei",
            "PingFang TC",
            "PingFang SC",
            "Noto Sans CJK TC",
            "SimHei",
        ]
        available_fonts = {f.name for f in fm.fontManager.ttflist}
        for font in font_candidates:
            if font in available_fonts:
                plt.rcParams["font.sans-serif"] = [font]
                plt.rcParams["axes.unicode_minus"] = False
                return font
        return None
    except Exception:
        return None


# =============================================================================
# Value pickers (compat)
# =============================================================================
def _pick_locked_cnt(row: Dict[str, Any]) -> int:
    for k in ("locked_cnt", "limitup_locked", "locked", "lock_cnt"):
        if k in row and row[k] is not None:
            try:
                return int(row[k])
            except Exception:
                pass
    return 0


# =============================================================================
# Core render (single page)
# =============================================================================
def _render_one_page(
    *,
    sector_rows: List[Dict[str, Any]],
    out_path: Path,
    ymd: str,
    asof: str,
    width: int,
    height: int,
) -> None:
    sectors = [x.get("sector", "") for x in sector_rows]
    values = [_pick_locked_cnt(x) for x in sector_rows]

    max_v = max(values) if values else 0
    x_max = (max_v * 1.2) if max_v > 0 else 1

    fig = plt.figure(figsize=(width / 100, height / 100), dpi=100, facecolor="#0f0f1e")
    ax = fig.add_subplot(111, facecolor="#0f0f1e")

    y_pos = range(len(sectors))

    colors = []
    for i, v in enumerate(values):
        intensity = 0.5 + 0.5 * (v / max_v) if max_v > 0 else 0.8
        if i == 0:
            colors.append(f"#{int(255 * intensity):02x}3030")
        elif i < 3:
            colors.append(f"#{int(255 * intensity):02x}5030")
        else:
            colors.append(f"#{int(200 * intensity):02x}6050")

    ax.barh(y_pos, values, color=colors, height=0.7, edgecolor="none")

    ax.set_yticks(list(y_pos))
    ax.set_yticklabels(sectors, fontsize=42, color="white", weight="medium")
    ax.invert_yaxis()

    ax.set_xlim(0, x_max)
    ax.set_xticks([])
    for s in ("top", "right", "bottom", "left"):
        ax.spines[s].set_visible(False)

    for i, v in enumerate(values):
        text_x = max(v - x_max * 0.03, x_max * 0.02)
        ax.text(
            text_x,
            i,
            str(v),
            va="center",
            ha="right" if v > 0 else "left",
            fontsize=46,
            color="white",
            weight="bold",
            bbox=dict(
                boxstyle="round,pad=0.3",
                facecolor="black",
                alpha=0.3,
                edgecolor="none",
            ),
        )

    # 標題
    fig.text(
        0.5,
        0.965,
        "產業別漲停上榜家數（Top）",
        ha="center",
        va="top",
        fontsize=64,
        color="white",
        weight="bold",
    )

    subtitle = ""
    if ymd and asof:
        time_str = asof.split("T")[-1][:5] if "T" in asof else asof[:5]
        subtitle = f"{ymd}  |  截至 {time_str}"
    elif ymd:
        subtitle = ymd

    if subtitle:
        fig.text(
            0.5,
            0.91,
            subtitle,
            ha="center",
            va="top",
            fontsize=32,
            color="#aaa",
            style="italic",
        )

    fig.text(
        0.98,
        0.02,
        "台股盤中快照",
        ha="right",
        va="bottom",
        fontsize=24,
        color="#555",
        alpha=0.6,
    )

    plt.subplots_adjust(left=0.32, right=0.96, top=0.86, bottom=0.06)

    fig.savefig(
        out_path,
        dpi=100,
        facecolor="#0f0f1e",
        edgecolor="none",
        bbox_inches="tight",
        pad_inches=0.1,
    )
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
) -> List[Path]:
    """
    產業別漲停家數 overview（自動分頁）
    - 只顯示 locked_cnt > 0 的產業
    - 若產業數 <= page_size：只出 1 張
    - 回傳所有 overview 圖片路徑
    """
    _setup_chinese_font()

    sector_summary = payload.get("sector_summary", []) or []
    ymd = payload.get("ymd", "")
    asof = payload.get("asof", "")

    # 只保留真的有漲停的產業
    sector_rows = [x for x in sector_summary if _pick_locked_cnt(x) > 0]

    if not sector_rows:
        # 沒任何漲停產業 → 出 1 張「無資料」圖
        out_path = out_dir / f"overview_sectors_top{page_size}.png"
        fig = plt.figure(figsize=(width / 100, height / 100), dpi=100, facecolor="#1a1a2e")
        fig.text(
            0.5,
            0.5,
            "📊 今日無漲停產業",
            ha="center",
            va="center",
            fontsize=56,
            color="white",
            weight="bold",
        )
        if ymd:
            fig.text(0.5, 0.4, ymd, ha="center", va="center", fontsize=36, color="#888")
        fig.savefig(out_path, dpi=100, bbox_inches="tight", facecolor="#1a1a2e")
        plt.close(fig)
        print(f"✅ 已產生：{out_path}")
        return [out_path]

    # 依漲停家數排序
    sector_rows = sorted(sector_rows, key=_pick_locked_cnt, reverse=True)

    pages: List[List[Dict[str, Any]]] = [
        sector_rows[i : i + page_size] for i in range(0, len(sector_rows), page_size)
    ]

    out_paths: List[Path] = []

    for idx, rows in enumerate(pages, start=1):
        if len(pages) == 1:
            fname = f"overview_sectors_top{page_size}.png"
        else:
            fname = f"overview_sectors_top{page_size}_p{idx}.png"

        out_path = out_dir / fname
        _render_one_page(
            sector_rows=rows,
            out_path=out_path,
            ymd=ymd,
            asof=asof,
            width=width,
            height=height,
        )
        out_paths.append(out_path)

    return out_paths
