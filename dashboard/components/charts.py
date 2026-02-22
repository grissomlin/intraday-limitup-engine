# dashboard/components/charts.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Tuple, Optional, Dict, Any, List
import pandas as pd


# ============================================================
# Normalize helpers
# ============================================================
def _to_sector_bar_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    將各種來源（sector_summary / 已統計df）正規化成：
      columns: ["產業", "家數"] 且 家數為 numeric
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["產業", "家數"])

    d = df.copy()

    # 兼容：sector_summary: sector/count
    if "產業" not in d.columns and "sector" in d.columns:
        d = d.rename(columns={"sector": "產業"})
    if "家數" not in d.columns and "count" in d.columns:
        d = d.rename(columns={"count": "家數"})

    # 最低限度
    if "產業" not in d.columns:
        d["產業"] = "未分類"
    if "家數" not in d.columns:
        d["家數"] = 0

    d["產業"] = d["產業"].fillna("").astype(str).replace("", "未分類")
    d["家數"] = pd.to_numeric(d["家數"], errors="coerce").fillna(0)

    # ✅ 排序（大到小）
    d = d.sort_values("家數", ascending=False).reset_index(drop=True)

    return d[["產業", "家數"]]


def _apply_top_n(df: pd.DataFrame, top_n: Optional[int]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if top_n is None or top_n <= 0:
        return df
    return df.head(top_n).reset_index(drop=True)


# ============================================================
# Data builders
# ============================================================
def build_sector_bar_df(
    limitup_df: pd.DataFrame,
    *,
    emerging_keyword: str = "emerging",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    從「漲停清單 limitup_df」統計：
      - 主榜：非 emerging
      - 興櫃：emerging
    回傳兩個 df，欄位：["產業","家數"]（已排序大到小）
    """
    if limitup_df is None or limitup_df.empty:
        empty = pd.DataFrame(columns=["產業", "家數"])
        return empty, empty

    df = limitup_df.copy()

    # 欄位兼容
    if "產業" not in df.columns and "sector" in df.columns:
        df = df.rename(columns={"sector": "產業"})
    if "symbol" not in df.columns and "ticker" in df.columns:
        df = df.rename(columns={"ticker": "symbol"})
    if "market_detail" not in df.columns:
        df["market_detail"] = ""

    df["產業"] = df["產業"].fillna("").astype(str).replace("", "未分類")
    df["market_detail"] = df["market_detail"].fillna("").astype(str)

    # 興櫃判斷
    df["is_emerging"] = df["market_detail"].str.lower().eq(emerging_keyword.lower())

    main = (
        df.loc[~df["is_emerging"]]
        .groupby("產業", as_index=False)
        .agg(家數=("symbol", "count"))
    )
    emg = (
        df.loc[df["is_emerging"]]
        .groupby("產業", as_index=False)
        .agg(家數=("symbol", "count"))
    )

    main = _to_sector_bar_df(main)
    emg = _to_sector_bar_df(emg)

    return main, emg


def build_sector_bar_df_from_sector_summary(
    sector_summary_df: pd.DataFrame,
    emerging_sector_summary_df: Optional[pd.DataFrame] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    payload 對應版本：
    - sector_summary: 欄位 sector/count/limitup_locked/limitup_touch...
    - emerging_sector_summary: 同上（若有）
    """
    main = _to_sector_bar_df(sector_summary_df)
    emg = _to_sector_bar_df(emerging_sector_summary_df) if emerging_sector_summary_df is not None else pd.DataFrame(
        columns=["產業", "家數"]
    )
    return main, emg


# ============================================================
# Plotly figures (bars)
# ============================================================
def build_sector_bar_fig_plotly(
    sector_df: pd.DataFrame,
    title: str,
    *,
    top_n: Optional[int] = None,
    show_values: bool = True,
    tick_angle: int = -45,
    height: int = 420,
) -> "object":
    import plotly.express as px  # lazy import

    if sector_df is None or sector_df.empty:
        fig = px.bar(pd.DataFrame({"產業": [], "家數": []}), x="產業", y="家數", title=title)
        fig.update_layout(height=height)
        return fig

    df = _to_sector_bar_df(sector_df)
    df = _apply_top_n(df, top_n)

    cat_order = df["產業"].astype(str).tolist()

    fig = px.bar(
        df,
        x="產業",
        y="家數",
        title=title,
        text="家數" if show_values else None,
        category_orders={"產業": cat_order},
    )

    if show_values:
        fig.update_traces(texttemplate="%{text}", textposition="outside", cliponaxis=False)

    fig.update_layout(
        xaxis_title="產業",
        yaxis_title="漲停/觸及家數",
        xaxis_tickangle=tick_angle,
        height=height,
        margin=dict(t=70, l=45, r=20, b=90),
    )
    return fig


def build_sector_barh_fig_plotly(
    sector_df: pd.DataFrame,
    title: str,
    *,
    top_n: Optional[int] = 12,
    show_values: bool = True,
    height: int = 980,
) -> "object":
    import plotly.express as px  # lazy import

    if sector_df is None or sector_df.empty:
        fig = px.bar(
            pd.DataFrame({"產業": [], "家數": []}),
            y="產業",
            x="家數",
            title=title,
            orientation="h",
        )
        fig.update_layout(height=height)
        return fig

    df = _to_sector_bar_df(sector_df)
    df = _apply_top_n(df, top_n)

    y_order = df["產業"].astype(str).tolist()[::-1]

    fig = px.bar(
        df,
        y="產業",
        x="家數",
        title=title,
        orientation="h",
        text="家數" if show_values else None,
        category_orders={"產業": y_order},
    )

    if show_values:
        fig.update_traces(texttemplate="%{text}", textposition="outside", cliponaxis=False)

    fig.update_layout(
        xaxis_title="漲停/觸及家數",
        yaxis_title="",
        height=height,
        margin=dict(t=90, l=170, r=30, b=30),
    )
    return fig


# ============================================================
# NEW: Plotly Table for Shorts (no bars)
# ============================================================
def build_stock_table_fig_plotly(
    df: pd.DataFrame,
    title: str,
    *,
    height: int = 980,
    rows_per_page: int = 16,
    show_pct: bool = True,
    show_volume: bool = True,
    show_consecutive: bool = True,
) -> "object":
    """
    9:16 影片用：表格版（不畫柱狀圖）
    - 支援小產業堆疊（含「區塊標題列」row_type='header'）
    - 狀態：🔒(鎖) / ⚔️(盤中觸及但未鎖)
    - 欄位：股票(名稱+代碼)、狀態、連板、成交量、漲幅%
    """
    import plotly.graph_objects as go  # lazy import

    if df is None or df.empty:
        fig = go.Figure()
        fig.update_layout(title=title, height=height)
        return fig

    d = df.copy()

    # ---- Normalize columns
    if "名稱" not in d.columns and "name" in d.columns:
        d = d.rename(columns={"name": "名稱"})
    if "代碼" not in d.columns and "symbol" in d.columns:
        d = d.rename(columns={"symbol": "代碼"})
    if "產業" not in d.columns and "sector" in d.columns:
        d = d.rename(columns={"sector": "產業"})

    if "名稱" not in d.columns:
        d["名稱"] = ""
    if "代碼" not in d.columns:
        d["代碼"] = ""

    # ---- % column: allow NaN (blank rows)
    if "漲幅%" not in d.columns:
        if "ret" in d.columns:
            d["漲幅%"] = pd.to_numeric(d["ret"], errors="coerce") * 100
        elif "return" in d.columns:
            d["漲幅%"] = pd.to_numeric(d["return"], errors="coerce") * 100
        else:
            d["漲幅%"] = pd.NA
    d["漲幅%"] = pd.to_numeric(d["漲幅%"], errors="coerce")

    # ---- consecutive
    if "連板" not in d.columns:
        for cand in ["consecutive_days", "streak", "limitup_streak"]:
            if cand in d.columns:
                d["連板"] = pd.to_numeric(d[cand], errors="coerce")
                break
        else:
            d["連板"] = pd.NA

    # ---- volume
    if "成交量" not in d.columns:
        for cand in ["volume", "vol"]:
            if cand in d.columns:
                d["成交量"] = pd.to_numeric(d[cand], errors="coerce")
                break
        else:
            d["成交量"] = pd.NA

    # ---- status (locked / touched)
    def _infer_status(row: pd.Series) -> str:
        # header row
        if str(row.get("row_type", "")) == "header":
            return ""
        # locked signals
        for k in ["is_limitup_locked", "limitup_locked", "locked", "is_locked"]:
            if k in row.index and pd.notna(row[k]):
                try:
                    if bool(row[k]):
                        return "🔒"
                except Exception:
                    pass
        # touched signals
        for k in ["is_limitup_touch", "limitup_touch", "touched", "is_touched", "hit_limitup"]:
            if k in row.index and pd.notna(row[k]):
                try:
                    if bool(row[k]):
                        return "⚔️"
                except Exception:
                    pass
        # optional: string status
        s = str(row.get("status", "")).lower()
        if "lock" in s:
            return "🔒"
        if "touch" in s or "hit" in s:
            return "⚔️"
        return ""

    d["狀態"] = d.apply(_infer_status, axis=1)

    # ---- stock label
    name = d["名稱"].fillna("").astype(str)
    code = d["代碼"].fillna("").astype(str)
    d["股票"] = (name + " (" + code + ")").where(~((name == "") & (code == "")), "")

    # ---- header rows: show as sector title
    # row_type == header means: sector title row, no other info
    if "row_type" not in d.columns:
        d["row_type"] = ""

    # ---- Render columns selection
    columns: List[str] = ["股票", "狀態"]
    if show_consecutive:
        columns.append("連板")
    if show_volume:
        columns.append("成交量")
    if show_pct:
        columns.append("漲幅%")

    # ---- format cells
    out = d[columns + ["row_type"]].copy()

    def _fmt_int(x) -> str:
        if pd.isna(x):
            return ""
        try:
            return str(int(float(x)))
        except Exception:
            return ""

    def _fmt_vol(x) -> str:
        if pd.isna(x):
            return ""
        try:
            v = float(x)
            if v >= 1e8:
                return f"{v/1e8:.2f}億"
            if v >= 1e4:
                return f"{v/1e4:.1f}萬"
            return str(int(v))
        except Exception:
            return ""

    def _fmt_pct(x) -> str:
        if pd.isna(x):
            return ""
        try:
            return f"{float(x):.2f}"
        except Exception:
            return ""

    if "連板" in out.columns:
        out["連板"] = out["連板"].map(_fmt_int)
    if "成交量" in out.columns:
        out["成交量"] = out["成交量"].map(_fmt_vol)
    if "漲幅%" in out.columns:
        out["漲幅%"] = out["漲幅%"].map(_fmt_pct)

    # ---- Build per-row styles (header rows)
    is_header = out["row_type"].astype(str).eq("header").tolist()

    # Header row: put sector name in 股票 col (already)
    # And blank out other cols on header row
    for col in columns:
        if col == "股票":
            continue
        out.loc[out["row_type"].astype(str).eq("header"), col] = ""

    # ---- Prepare table values
    values = [out[c].tolist() for c in columns]
    header_vals = columns

    # ---- colors
    # default cell background: white; header rows: light gray
    fill_colors = []
    for _ in columns:
        col_colors = []
        for h in is_header:
            col_colors.append("#f2f2f2" if h else "white")
        fill_colors.append(col_colors)

    # ---- font bold on header rows (plotly table doesn't support per-cell font weight well)
    # workaround: prefix header text with '【】'
    header_mask = out["row_type"].astype(str).eq("header")
    out.loc[header_mask, "股票"] = out.loc[header_mask, "股票"].map(lambda s: f"" if s else "")
    values = [out[c].tolist() for c in columns]

    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=header_vals,
                    fill_color="#111827",
                    font=dict(color="white", size=24),
                    align=["left"] + ["center"] * (len(columns) - 1),
                    height=42,
                ),
                cells=dict(
                    values=values,
                    fill_color=fill_colors,
                    font=dict(color="#111827", size=22),
                    align=["left"] + ["center"] * (len(columns) - 1),
                    height=40,
                ),
            )
        ]
    )

    fig.update_layout(
        title=dict(text=title, x=0.02, xanchor="left"),
        height=height,
        margin=dict(t=110, l=30, r=30, b=30),
        paper_bgcolor="white",
    )
    return fig


# ============================================================
# Matplotlib fallback (no plotly)
# ============================================================
def build_sector_bar_fig_mpl(
    sector_df: pd.DataFrame,
    title: str,
    *,
    top_n: Optional[int] = None,
    show_values: bool = True,
    tick_angle: int = 45,
    figsize: Tuple[int, int] = (10, 4),
) -> "object":
    import matplotlib.pyplot as plt  # lazy import

    fig, ax = plt.subplots(figsize=figsize)

    if sector_df is None or sector_df.empty:
        ax.set_title(title)
        ax.set_xlabel("產業")
        ax.set_ylabel("漲停/觸及家數")
        ax.text(0.5, 0.5, "無資料", ha="center", va="center", transform=ax.transAxes)
        fig.tight_layout()
        return fig

    df = _to_sector_bar_df(sector_df)
    df = _apply_top_n(df, top_n)

    ax.bar(df["產業"], df["家數"])
    ax.set_title(title)
    ax.set_xlabel("產業")
    ax.set_ylabel("漲停/觸及家數")
    ax.tick_params(axis="x", rotation=tick_angle)

    if show_values:
        for i, v in enumerate(df["家數"].tolist()):
            ax.text(i, v, str(int(v)), ha="center", va="bottom", fontsize=10)

    fig.tight_layout()
    return fig


def make_sector_bar_figs(
    limitup_df: pd.DataFrame,
    *,
    title_main: str = "主榜｜產業漲停/觸及家數",
    title_emg: str = "興櫃｜產業漲停/觸及家數",
    top_n: Optional[int] = None,
    show_values: bool = True,
    prefer_plotly: bool = True,
    plotly_kwargs: Optional[Dict[str, Any]] = None,
    mpl_kwargs: Optional[Dict[str, Any]] = None,
) -> Tuple["object", "object"]:
    """
    一次產出兩張圖（主榜/興櫃），給 dashboard 用（直條版）。
    """
    main_df, emg_df = build_sector_bar_df(limitup_df)

    plotly_kwargs = plotly_kwargs or {}
    mpl_kwargs = mpl_kwargs or {}

    if prefer_plotly:
        try:
            fig_main = build_sector_bar_fig_plotly(
                main_df, title_main, top_n=top_n, show_values=show_values, **plotly_kwargs
            )
            fig_emg = build_sector_bar_fig_plotly(
                emg_df, title_emg, top_n=top_n, show_values=show_values, **plotly_kwargs
            )
            return fig_main, fig_emg
        except Exception:
            pass

    fig_main = build_sector_bar_fig_mpl(main_df, title_main, top_n=top_n, show_values=show_values, **mpl_kwargs)
    fig_emg = build_sector_bar_fig_mpl(emg_df, title_emg, top_n=top_n, show_values=show_values, **mpl_kwargs)
    return fig_main, fig_emg
