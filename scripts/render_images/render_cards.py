# scripts/render_images/render_cards.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Dict, Any, List
import math

import pandas as pd
import plotly.graph_objects as go

from .adapters import (
    limitup_df_from_payload,
    peers_df_from_payload,
    market_label,
    status_label,
)
from .layout_rules import (
    rank_sectors,
    sector_counts,
    build_sector_pages,
)


# =============================================================================
# Style config
# =============================================================================
STATUS_COLOR = {
    "locked": "#D32F2F",         # 深紅
    "touch_only": "#F57C00",     # 橘
    "no_limit_theme": "#1976D2", # 藍
    "other": "#9E9E9E",
}


# =============================================================================
# Card helpers
# =============================================================================
def _status_color(limitup_status: str) -> str:
    return STATUS_COLOR.get(limitup_status, "#9E9E9E")


def _calc_grid(n: int, max_cols: int = 4) -> (int, int):
    """
    根據股票數量決定 rows x cols
    """
    if n <= max_cols:
        return 1, n
    cols = max_cols
    rows = math.ceil(n / cols)
    return rows, cols


# =============================================================================
# Single page renderer (CARDS)
# =============================================================================
def render_sector_cards_page(
    *,
    sector: str,
    cutoff_text: str,
    locked_cnt: int,
    touch_cnt: int,
    theme_cnt: int,
    limitup_rows: List[Dict[str, Any]],
    peer_rows: List[Dict[str, Any]],
    width: int,
    height: int,
    font_title: int,
    font_subtitle: int,
    font_card: int,
) -> go.Figure:
    """
    單一產業卡片牆（一頁）
    """
    fig = go.Figure()

    # -------------------------
    # 標題
    # -------------------------
    fig.add_annotation(
        text=sector,
        x=0.02,
        y=0.98,
        xref="paper",
        yref="paper",
        showarrow=False,
        font=dict(size=font_title),
    )

    fig.add_annotation(
        text=f"鎖死 {locked_cnt} ｜ 打開 {touch_cnt} ｜ 題材 {theme_cnt}",
        x=0.02,
        y=0.93,
        xref="paper",
        yref="paper",
        showarrow=False,
        font=dict(size=font_subtitle),
    )

    if cutoff_text:
        fig.add_annotation(
            text=cutoff_text,
            x=0.02,
            y=0.02,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=max(24, font_card - 10)),
        )

    # -------------------------
    # Cards layout
    # -------------------------
    n = len(limitup_rows)
    if n == 0:
        fig.add_annotation(
            text="（本產業無漲停股）",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=font_card),
        )
        return fig

    rows, cols = _calc_grid(n, max_cols=4)

    grid_top = 0.88
    grid_bottom = 0.30 if peer_rows else 0.08
    grid_height = grid_top - grid_bottom

    cell_w = 1.0 / cols
    cell_h = grid_height / rows

    # -------------------------
    # Draw cards
    # -------------------------
    for i, r in enumerate(limitup_rows):
        row = i // cols
        col = i % cols

        x0 = col * cell_w + 0.02
        x1 = (col + 1) * cell_w - 0.02
        y1 = grid_top - row * cell_h
        y0 = y1 - cell_h + 0.02

        status = r.get("limitup_status", "")
        color = _status_color(status)

        # 卡片背景
        fig.add_shape(
            type="rect",
            xref="paper",
            yref="paper",
            x0=x0,
            x1=x1,
            y0=y0,
            y1=y1,
            line=dict(color=color, width=4),
            fillcolor="white",
        )

        # 公司名稱（中央）
        fig.add_annotation(
            text=r.get("name", ""),
            x=(x0 + x1) / 2,
            y=(y0 + y1) / 2,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=font_card),
        )

        # 市場別（左上）
        fig.add_annotation(
            text=market_label(r.get("market_detail", "")),
            x=x0 + 0.01,
            y=y1 - 0.02,
            xref="paper",
            yref="paper",
            showarrow=False,
            align="left",
            font=dict(size=max(18, font_card - 16), color="#555"),
        )

        # 狀態（右下）
        fig.add_annotation(
            text=status_label(status),
            x=x1 - 0.01,
            y=y0 + 0.02,
            xref="paper",
            yref="paper",
            showarrow=False,
            align="right",
            font=dict(size=max(20, font_card - 14), color=color),
        )

    # -------------------------
    # Peers (text list)
    # -------------------------
    if peer_rows:
        names = [p.get("name", "") for p in peer_rows]
        txt = "、".join(names[:12])
        fig.add_annotation(
            text=f"同產業未漲停：{txt}",
            x=0.02,
            y=0.24,
            xref="paper",
            yref="paper",
            showarrow=False,
            align="left",
            font=dict(size=max(26, font_card - 10)),
        )

    fig.update_layout(
        width=width,
        height=height,
        margin=dict(l=24, r=24, t=24, b=24),
        paper_bgcolor="white",
    )

    return fig


# =============================================================================
# High-level API
# =============================================================================
def render_sector_cards(
    payload: Dict[str, Any],
    *,
    cutoff_text: str,
    width: int,
    height: int,
    rows_per_page: int,
    peers_max_per_page: int,
    font_title: int,
    font_subtitle: int,
    font_card: int,
) -> List[go.Figure]:
    """
    高階入口：回傳所有產業的卡片頁
    """
    limitup_df = limitup_df_from_payload(payload)
    peers_df = peers_df_from_payload(payload)

    if limitup_df.empty:
        return []

    figures: List[go.Figure] = []

    sectors = rank_sectors(limitup_df)

    for sec in sectors:
        sdf = limitup_df[limitup_df["sector"] == sec].copy()

        if not peers_df.empty:
            pdf = peers_df[peers_df["sector"] == sec].copy()
            pdf = pdf[~pdf["symbol"].isin(set(sdf["symbol"]))]
        else:
            pdf = pd.DataFrame()

        locked_cnt, touch_cnt, theme_cnt = sector_counts(sdf)

        pages = build_sector_pages(
            sdf,
            pdf,
            rows_per_page=rows_per_page,
            peers_max_per_page=peers_max_per_page,
        )

        for pack in pages:
            fig = render_sector_cards_page(
                sector=sec,
                cutoff_text=cutoff_text,
                locked_cnt=locked_cnt,
                touch_cnt=touch_cnt,
                theme_cnt=theme_cnt,
                limitup_rows=pack["limitup_rows"],
                peer_rows=pack["peer_rows"],
                width=width,
                height=height,
                font_title=font_title,
                font_subtitle=font_subtitle,
                font_card=font_card,
            )
            figures.append(fig)

    return figures
