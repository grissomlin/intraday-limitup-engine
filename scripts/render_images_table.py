# scripts/render_images.py
# -*- coding: utf-8 -*-
"""
TW 盤中/收盤快照產圖（Plotly + Kaleido）｜「老人一眼懂」表格版（完整版）

✅ 你指定的最終規格（已凍結，照做）：
- 產業頁不再用柱狀圖，改「大字表格」
- 頁首超大：
    半導體業
    鎖死 2 ｜ 打開 1 ｜ 題材 1
- 先列「漲停股」（含：鎖死 / 打開 / 題材）
- 若畫面有空白，再補「同產業未漲停（不含觸及漲停）」最多 10 筆
  - 不夠就下一頁繼續補
  - 空間剩 2 行就只放 2 行，其餘留給下一頁
- 狀態一定要「文字」：鎖死 / 打開 / 題材（可搭配圖示，但文字不可省）
- 成交量拿掉
- .TW / .TWO 不顯示，改成「上市/上櫃/興櫃/創新板/DR」
- 不顯示 main/emg/asof/slot 這種沒意義字眼
  - 只顯示：「日期 + 截至時間」

⚠️ 關於「同產業未漲停」資料：
- 本腳本會優先從 payload 讀取下列任一欄位（擇一即可）：
  1) payload["peers_not_limitup"] : list[dict]（每筆至少含 symbol,name,sector,ret,market_detail）
  2) payload["peers_by_sector"]   : dict[sector] -> list[dict]
- 如果 payload 沒有提供 peers，腳本仍可產出「漲停股表格」，但「未漲停補空」會自動略過（不報錯）。
  （你要完整體驗，下一步在 downloader.py 把 peers 塞進 payload 即可。）

用法：
  python scripts/render_images.py --payload data/cache/tw/2026-01-17/midday.payload.json
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# =============================================================================
# Repo import path
# =============================================================================
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

IS_WINDOWS = platform.system() == "Windows"
IS_CI = os.getenv("CI") == "true" or os.getenv("GITHUB_ACTIONS") == "true"


# =============================================================================
# Utilities
# =============================================================================
def _safe_filename(s: str, max_len: int = 90) -> str:
    s = (s or "").strip()
    s = re.sub(r"[\\/:*?\"<>|\n\r\t]+", "_", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace(" ", "_")
    if len(s) > max_len:
        s = s[:max_len].rstrip("_")
    return s


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _auto_find_latest_payload(repo_root: Path, slot: str = "midday") -> Optional[Path]:
    base = repo_root / "data" / "cache" / "tw"
    if not base.exists():
        return None
    cand = sorted(base.glob(f"*/{slot}.payload.json"), key=lambda p: p.parent.name)
    return cand[-1] if cand else None


def _find_chrome_exe() -> Optional[str]:
    env_path = os.getenv("BROWSER_PATH") or os.getenv("KALEIDO_BROWSER_PATH")
    if env_path and Path(env_path).exists():
        return env_path

    for name in ["chrome", "chrome.exe", "google-chrome", "chromium", "chromium-browser"]:
        p = shutil.which(name)
        if p:
            return p

    candidates = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        rf"{os.getenv('LOCALAPPDATA','')}\Google\Chrome\Application\chrome.exe",
    ]
    for c in candidates:
        if c and Path(c).exists():
            return c
    return None


def _windows_kaleido_preflight() -> None:
    if not IS_WINDOWS or IS_CI:
        return
    if os.getenv("SKIP_IMAGE_RENDER"):
        return

    chrome = _find_chrome_exe()
    if chrome:
        print(f"🧩 找到 Chrome：{chrome}")
        if not os.getenv("BROWSER_PATH"):
            print('   建議（可選）：set BROWSER_PATH="上面那個 chrome.exe 完整路徑"')
        return

    print("❌ 找不到可用的 Chrome/Chromium（Kaleido v1 需要它才能輸出圖片）")
    print("   你可以擇一處理：")
    print("   A) 安裝 Google Chrome（一般版即可）")
    print("   B) 或執行 plotly_get_chrome（需可連網）")
    print("   C) 或先 set SKIP_IMAGE_RENDER=1，改用 WSL2/CI 產圖")
    print('   也可手動指定：set BROWSER_PATH="C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"')
    print("")


def _save_fig(fig: "object", out_path: Path, *, fmt: str, width: int, height: int, scale: float) -> bool:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if IS_WINDOWS and not IS_CI and os.getenv("SKIP_IMAGE_RENDER"):
        print(f"⚠️  [SKIP] {out_path.name}")
        return False

    try:
        fig.write_image(str(out_path), format=fmt, width=width, height=height, scale=scale)
        print(f"✅ {out_path.name}")
        return True
    except Exception as e:
        if IS_WINDOWS and not IS_CI:
            print(f"⚠️  [Windows] write_image 失敗: {out_path.name}")
            print("    這通常是 Chrome/權限/防毒攔截造成無頭瀏覽器立刻關閉。")
            print("    你可以：")
            print("    - set BROWSER_PATH=chrome.exe 完整路徑（最常解）")
            print("    - 或執行 plotly_get_chrome 安裝相容 Chrome")
            print("    - 或用 WSL2/CI 產圖")
            print(f"    錯誤：{str(e)[:260]}")
            return False
        raise


def _write_list_txt(out_dir: Path, paths: List[Path]) -> None:
    try:
        lines = [str(p.relative_to(out_dir)).replace("\\", "/") for p in paths]
        (out_dir / "list.txt").write_text("\n".join(lines), encoding="utf-8")
    except Exception:
        pass


def _parse_cutoff_text(payload: Dict[str, Any]) -> str:
    """
    你要的：不要寫 slot/main/asof；改寫「截至時間」
    payload 常見：
      - asof: "2026-01-17T11:00:00"
      - generated_at: "2026-01-17T11:00:03"
    """
    ymd = str(payload.get("ymd") or "").strip()
    asof = str(payload.get("asof") or "").strip()
    gen = str(payload.get("generated_at") or "").strip()

    t = asof or gen
    # 盡量抓 HH:MM
    hhmm = ""
    if "T" in t:
        try:
            hhmm = t.split("T", 1)[1][:5]
        except Exception:
            hhmm = ""
    elif len(t) >= 5 and ":" in t:
        hhmm = t[:5]

    if ymd and hhmm:
        return f"{ymd} ｜ 截至 {hhmm}"
    if ymd:
        return f"{ymd}"
    if hhmm:
        return f"截至 {hhmm}"
    return ""


# =============================================================================
# Data normalization
# =============================================================================
def _market_label(market_detail: str) -> str:
    md = (market_detail or "").strip().lower()
    mapping = {
        "listed": "上市",
        "otc": "上櫃",
        "emerging": "興櫃",
        "innovation_a": "創新板",
        "innovation_c": "創新板",
        "dr": "DR",
    }
    return mapping.get(md, "上市/上櫃")


def _status_label(limitup_status: str) -> str:
    s = (limitup_status or "").strip().lower()
    if s == "locked":
        return "鎖死"
    if s == "touch_only":
        return "打開"
    if s == "no_limit_theme":
        return "題材"
    # fallback：若只有 tick 欄位
    return "—"


def _limitup_df_from_payload(payload: Dict[str, Any]) -> pd.DataFrame:
    rows = payload.get("limitup", []) or []
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # 標準化欄位（盡量兼容）
    if "sector" not in df.columns:
        df["sector"] = df.get("產業", "未分類")
    if "name" not in df.columns:
        df["name"] = df.get("名稱", "")
    if "symbol" not in df.columns:
        df["symbol"] = df.get("代碼", "")

    if "market_detail" not in df.columns:
        df["market_detail"] = ""

    # 連板
    if "streak" not in df.columns:
        df["streak"] = 0

    # 狀態（你 downloader.py 已提供 limitup_status）
    if "limitup_status" not in df.columns:
        # fallback from is_limitup_locked / is_limitup_touch / limit_type
        lt = df.get("limit_type", "standard").astype(str)
        locked = df.get("is_limitup_locked", False).astype(bool)
        touch = df.get("is_limitup_touch", False).astype(bool)

        def _infer(i: int) -> str:
            if lt.iloc[i] == "no_limit":
                return "no_limit_theme"
            if locked.iloc[i]:
                return "locked"
            if touch.iloc[i]:
                return "touch_only"
            return "other"

        df["limitup_status"] = [_infer(i) for i in range(len(df))]

    df["sector"] = df["sector"].fillna("").astype(str).replace("", "未分類")
    df["name"] = df["name"].fillna("").astype(str)
    df["symbol"] = df["symbol"].fillna("").astype(str)
    df["market_detail"] = df["market_detail"].fillna("").astype(str)
    df["streak"] = pd.to_numeric(df["streak"], errors="coerce").fillna(0).astype(int)

    return df


def _peers_df_from_payload(payload: Dict[str, Any]) -> pd.DataFrame:
    """
    可接受兩種格式：
    1) peers_not_limitup: list[dict]
    2) peers_by_sector: dict[sector] -> list[dict]
    """
    if isinstance(payload.get("peers_not_limitup"), list):
        df = pd.DataFrame(payload["peers_not_limitup"])
        return _normalize_peers_df(df)

    if isinstance(payload.get("peers_by_sector"), dict):
        rows: List[Dict[str, Any]] = []
        for sec, lst in payload["peers_by_sector"].items():
            if not isinstance(lst, list):
                continue
            for r in lst:
                if isinstance(r, dict):
                    rr = dict(r)
                    rr["sector"] = rr.get("sector") or sec
                    rows.append(rr)
        df = pd.DataFrame(rows) if rows else pd.DataFrame()
        return _normalize_peers_df(df)

    return pd.DataFrame()


def _normalize_peers_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    d = df.copy()

    if "sector" not in d.columns:
        d["sector"] = d.get("產業", "未分類")
    if "name" not in d.columns:
        d["name"] = d.get("名稱", "")
    if "symbol" not in d.columns:
        d["symbol"] = d.get("代碼", "")
    if "market_detail" not in d.columns:
        d["market_detail"] = ""

    # ret 用來排序（可以沒有，沒有就當 0）
    if "ret" in d.columns:
        d["ret"] = pd.to_numeric(d["ret"], errors="coerce")
    else:
        d["ret"] = 0.0

    d["sector"] = d["sector"].fillna("").astype(str).replace("", "未分類")
    d["name"] = d["name"].fillna("").astype(str)
    d["symbol"] = d["symbol"].fillna("").astype(str)
    d["market_detail"] = d["market_detail"].fillna("").astype(str)

    return d


# =============================================================================
# Rendering (BIG TABLE)
# =============================================================================
def _sector_counts(limitup_sector_df: pd.DataFrame) -> Tuple[int, int, int]:
    if limitup_sector_df is None or limitup_sector_df.empty:
        return 0, 0, 0
    s = limitup_sector_df["limitup_status"].astype(str).str.lower()
    locked = int((s == "locked").sum())
    touch_only = int((s == "touch_only").sum())
    theme = int((s == "no_limit_theme").sum())
    return locked, touch_only, theme


def _build_sector_pages(
    sector: str,
    limitup_sector_df: pd.DataFrame,
    peers_sector_df: pd.DataFrame,
    *,
    rows_per_page: int,
    peers_max_per_page: int,
) -> List[Dict[str, Any]]:
    """
    產業內分頁規則：
    - 上方永遠先塞「漲停股」(含 鎖死/打開/題材)，按：鎖死→打開→題材→連板數
    - 每頁若還有空間才塞 peers（未漲停），每頁最多 peers_max_per_page
    - peers 不夠就留白；peers 需要更多行就下一頁繼續塞
    """
    L = limitup_sector_df.copy() if limitup_sector_df is not None else pd.DataFrame()
    P = peers_sector_df.copy() if peers_sector_df is not None else pd.DataFrame()

    # limitup 排序（先鎖死、再打開、再題材；同類再看連板高）
    if not L.empty:
        order_map = {"locked": 0, "touch_only": 1, "no_limit_theme": 2}
        L["_ord"] = L["limitup_status"].astype(str).str.lower().map(order_map).fillna(9).astype(int)
        L = L.sort_values(["_ord", "streak"], ascending=[True, False]).drop(columns=["_ord"])
        L = L.reset_index(drop=True)

    # peers 排序（ret 高到低）
    if not P.empty:
        P["ret"] = pd.to_numeric(P.get("ret"), errors="coerce").fillna(0.0)
        P = P.sort_values(["ret"], ascending=False).reset_index(drop=True)

    pages: List[Dict[str, Any]] = []

    # 若沒 peers，就只做 limitup 分頁
    # 每頁可放 rows_per_page 行「股票列」（不含標題文字）
    li = 0
    pi = 0
    L_total = len(L)
    P_total = len(P)

    # 至少一頁（即使沒有資料也讓流程穩）
    while True:
        if li >= L_total and pi >= P_total:
            if pages:
                break
            # no data sector (rare)
            pages.append({"limitup_rows": [], "peer_rows": []})
            break

        remaining = rows_per_page

        # 先放 limitup
        limit_rows = []
        if li < L_total:
            takeL = min(remaining, L_total - li)
            limit_rows = L.iloc[li : li + takeL].to_dict(orient="records")
            li += takeL
            remaining -= takeL

        # 再放 peers（只有剩空間才放）
        peer_rows = []
        if remaining > 0 and pi < P_total:
            takeP = min(remaining, peers_max_per_page, P_total - pi)
            peer_rows = P.iloc[pi : pi + takeP].to_dict(orient="records")
            pi += takeP
            remaining -= takeP

        pages.append({"limitup_rows": limit_rows, "peer_rows": peer_rows})

        # 若 limitup 已塞完、peers 也塞完，就結束
        if li >= L_total and pi >= P_total:
            break

    return pages


def _render_sector_table_figure(
    sector: str,
    cutoff_text: str,
    locked_cnt: int,
    touch_cnt: int,
    theme_cnt: int,
    *,
    limitup_rows: List[Dict[str, Any]],
    peer_rows: List[Dict[str, Any]],
    page_idx: int,
    page_total: int,
    width: int,
    height: int,
    font_title: int,
    font_subtitle: int,
    font_table: int,
) -> "object":
    """
    用 go.Table 做「兩段式」大字表格：
    - 上段：漲停股（含狀態+連板）
    - 下段：同產業未漲停（不含觸及漲停）
    """
    import plotly.graph_objects as go

    # -------------------------
    # Title lines (超大)
    # -------------------------
    title_line_1 = f"{sector}"
    title_line_2 = f"鎖死 {locked_cnt} ｜ 打開 {touch_cnt} ｜ 題材 {theme_cnt}"
    # 右上角小小頁碼（不干擾老人閱讀）
    page_badge = f"{page_idx}/{page_total}" if page_total > 1 else ""

    # -------------------------
    # Build table data: Limitup
    # -------------------------
    lim_stock: List[str] = []
    lim_status: List[str] = []
    lim_streak: List[str] = []

    for r in limitup_rows:
        sym = str(r.get("symbol", "")).strip()
        name = str(r.get("name", "")).strip()
        md = _market_label(str(r.get("market_detail", "")).strip())
        status = _status_label(str(r.get("limitup_status", "")).strip())
        streak = int(r.get("streak", 0) or 0)
        streak_txt = f"{streak}連板" if streak > 0 else ""

        lim_stock.append(f"{name}({sym}｜{md})")
        lim_status.append(status)
        lim_streak.append(streak_txt)

    # 若沒有任何漲停股（理論上不會）
    if not lim_stock:
        lim_stock = ["（無）"]
        lim_status = [""]
        lim_streak = [""]

    # -------------------------
    # Build table data: Peers
    # -------------------------
    peer_stock: List[str] = []
    peer_note: List[str] = []

    for r in peer_rows:
        sym = str(r.get("symbol", "")).strip()
        name = str(r.get("name", "")).strip()
        md = _market_label(str(r.get("market_detail", "")).strip())
        peer_stock.append(f"{name}({sym}｜{md})")
        peer_note.append("")  # 保留欄位一致性

    # -------------------------
    # Decide layout domains
    # -------------------------
    # 以行數比例給 table domain（看起來更自然）
    lim_n = max(1, len(lim_stock))
    peer_n = len(peer_stock)

    # 如果沒有 peers，就讓 limitup table 吃滿
    if peer_n == 0:
        dom_lim = [0.08, 0.86]
        dom_peer = None
    else:
        dom_lim = [0.36, 0.86]
        dom_peer = [0.08, 0.28]


    fig = go.Figure()

    # -------------------------
    # Limitup table
    # -------------------------
    fig.add_trace(
        go.Table(
            header=dict(
                values=["漲停股", "狀態", "連板"],
                font=dict(size=font_table + 6),
                align=["left", "center", "center"],
                height=52,
            ),
            cells=dict(
                values=[lim_stock, lim_status, lim_streak],
                font=dict(size=font_table),
                align=["left", "center", "center"],
                height=52,
            ),
            domain=dict(x=[0.02, 0.98], y=dom_lim),
        )
    )

    # -------------------------
    # Peers table (optional)
    # -------------------------
    if dom_peer is not None:
        # peers 的 header 要寫清楚「不含觸及漲停」
        fig.add_trace(
            go.Table(
                header=dict(
                    values=["同產業未漲停（不含觸及漲停）", "", ""],
                    font=dict(size=font_table + 4),
                    align=["left", "center", "center"],
                    height=48,
                ),
                cells=dict(
                    values=[peer_stock, peer_note, peer_note],
                    font=dict(size=font_table),
                    align=["left", "center", "center"],
                    height=48,
                ),
                domain=dict(x=[0.02, 0.98], y=dom_peer),
            )
        )

    # -------------------------
    # Titles as annotations
    # -------------------------
    annotations = [
        dict(
            text=title_line_1,
            x=0.02,
            y=0.99,
            xref="paper",
            yref="paper",
            xanchor="left",
            yanchor="top",
            showarrow=False,
            font=dict(size=font_title),
        ),
        dict(
            text=title_line_2,
            x=0.02,
            y=0.935,
            xref="paper",
            yref="paper",
            xanchor="left",
            yanchor="top",
            showarrow=False,
            font=dict(size=font_subtitle),
        ),
    ]

    if cutoff_text:
        annotations.append(
            dict(
                text=cutoff_text,
                x=0.02,
                y=0.02,
                xref="paper",
                yref="paper",
                xanchor="left",
                yanchor="bottom",
                showarrow=False,
                font=dict(size=max(28, font_table - 8)),
            )
        )

    if page_badge:
        annotations.append(
            dict(
                text=page_badge,
                x=0.98,
                y=0.99,
                xref="paper",
                yref="paper",
                xanchor="right",
                yanchor="top",
                showarrow=False,
                font=dict(size=max(28, font_table - 8)),
            )
        )

    fig.update_layout(
        width=width,
        height=height,
        margin=dict(l=24, r=24, t=24, b=24),
        paper_bgcolor="white",
        annotations=annotations,
    )

    return fig


def render_tw_sector_tables(
    payload: Dict[str, Any],
    out_dir: Path,
    *,
    fmt: str,
    width: int,
    height: int,
    scale: float,
    rows_per_page: int,
    peers_max_per_page: int,
    sectors_top_n: int,
    font_title: int,
    font_subtitle: int,
    font_table: int,
) -> List[Path]:
    """
    產出：
      media/images/<ymd>/<slot>/sectors_main/ 下面每個產業一到多張（分頁）
    """
    limitup_df = _limitup_df_from_payload(payload)
    peers_df = _peers_df_from_payload(payload)

    if limitup_df.empty:
        return []

    cutoff_text = _parse_cutoff_text(payload)

    # 產業排序：以 locked 多的在前，再 touch_only，再 theme
    sectors = sorted(limitup_df["sector"].unique().tolist())

    def _sector_rank_key(sec: str) -> Tuple[int, int, int, str]:
        sdf = limitup_df.loc[limitup_df["sector"] == sec]
        locked, touch, theme = _sector_counts(sdf)
        # 多的優先（降序）所以取負數
        return (-locked, -touch, -theme, sec)

    sectors = sorted(sectors, key=_sector_rank_key)

    if sectors_top_n and sectors_top_n > 0:
        sectors = sectors[:sectors_top_n]

    out_paths: List[Path] = []

    # 每個產業各自分頁
    for idx, sec in enumerate(sectors, start=1):
        sdf = limitup_df.loc[limitup_df["sector"] == sec].copy()

        # peers：同產業、排除任何在 limitup 的 symbol（避免重複）
        if peers_df is not None and not peers_df.empty:
            pdf = peers_df.loc[peers_df["sector"] == sec].copy()
            lim_syms = set(sdf["symbol"].astype(str).tolist())
            pdf = pdf.loc[~pdf["symbol"].astype(str).isin(lim_syms)].copy()
        else:
            pdf = pd.DataFrame()

        locked_cnt, touch_cnt, theme_cnt = _sector_counts(sdf)

        pages = _build_sector_pages(
            sec,
            sdf,
            pdf,
            rows_per_page=rows_per_page,
            peers_max_per_page=peers_max_per_page,
        )
        page_total = len(pages)

        for p_i, pack in enumerate(pages, start=1):
            fig = _render_sector_table_figure(
                sector=sec,
                cutoff_text=cutoff_text,
                locked_cnt=locked_cnt,
                touch_cnt=touch_cnt,
                theme_cnt=theme_cnt,
                limitup_rows=pack["limitup_rows"],
                peer_rows=pack["peer_rows"],
                page_idx=p_i,
                page_total=page_total,
                width=width,
                height=height,
                font_title=font_title,
                font_subtitle=font_subtitle,
                font_table=font_table,
            )

            if page_total > 1:
                fname = f"tables_{idx:02d}_{_safe_filename(sec)}_p{p_i}of{page_total}.{fmt}"
            else:
                # 產業總漲停檔數（含鎖死/打開/題材）
                total_stocks = int(len(sdf))
                fname = f"tables_{idx:02d}_{_safe_filename(sec)}_{total_stocks}stocks.{fmt}"

            out_path = out_dir / "sectors_main" / fname
            if _save_fig(fig, out_path, fmt=fmt, width=width, height=height, scale=scale):
                out_paths.append(out_path)

    return out_paths


# =============================================================================
# CLI
# =============================================================================
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--payload", type=str, default="")
    ap.add_argument("--slot", type=str, default="midday")
    ap.add_argument("--out", type=str, default="")

    ap.add_argument("--format", type=str, default="png", choices=["png", "jpg", "jpeg"])
    ap.add_argument("--width", type=int, default=1080)
    ap.add_argument("--height", type=int, default=1920)
    ap.add_argument("--scale", type=float, default=2.0)

    # ✅ 你要的：一頁可以 16（預設），要 20 你就改成 20
    ap.add_argument("--rows-per-page", type=int, default=16, help="每頁最多顯示幾列（含漲停+未漲停）")
    ap.add_argument("--peers-max-per-page", type=int, default=10, help="同產業未漲停每頁最多補幾筆（僅在有空間時）")

    ap.add_argument("--sectors-top-n", type=int, default=0, help="0=全部產業；10=只做前10產業")

    # ✅ 字體：預設已放大到「老人模式」
    ap.add_argument("--font-title", type=int, default=82, help="產業名稱字體")
    ap.add_argument("--font-subtitle", type=int, default=62, help="鎖死/打開/題材那行字體")
    ap.add_argument("--font-table", type=int, default=46, help="表格字體")

    args = ap.parse_args()
    fmt = "jpg" if args.format == "jpeg" else args.format

    if IS_WINDOWS and not IS_CI:
        print("=" * 60)
        print("⚠️  Windows 本地開發模式")
        print("   Kaleido v1 需要可用的 Chrome/Chromium。")
        print("   建議先設：BROWSER_PATH 指到 chrome.exe，或用 plotly_get_chrome。")
        print("=" * 60)
        print()

    _windows_kaleido_preflight()

    # locate payload
    if args.payload:
        payload_path = Path(args.payload)
        if not payload_path.is_absolute():
            payload_path = (REPO_ROOT / payload_path).resolve()
    else:
        payload_path = _auto_find_latest_payload(REPO_ROOT, slot=args.slot)

    if not payload_path or not payload_path.exists():
        raise FileNotFoundError("找不到 payload.json（請用 --payload 指定，或確認 data/cache/tw 下有 */*.payload.json）")

    payload = _read_json(payload_path)
    ymd = payload.get("ymd") or "unknown_ymd"
    slot = payload.get("slot") or args.slot

    if args.out:
        out_dir = Path(args.out)
        if not out_dir.is_absolute():
            out_dir = (REPO_ROOT / out_dir).resolve()
    else:
        out_dir = (REPO_ROOT / "media" / "images" / ymd / slot).resolve()

    _ensure_dir(out_dir)

    print(f"[render_images] payload = {payload_path}")
    print(f"[render_images] out_dir  = {out_dir}")
    print(f"[render_images] rows-per-page={args.rows_per_page} peers-max-per-page={args.peers_max_per_page}")
    print(f"[render_images] sectors-top-n={args.sectors_top_n} (0=全部)")
    print(f"[render_images] fonts title/sub/table={args.font_title}/{args.font_subtitle}/{args.font_table}")

    # render
    paths = render_tw_sector_tables(
        payload,
        out_dir,
        fmt=fmt,
        width=args.width,
        height=args.height,
        scale=args.scale,
        rows_per_page=args.rows_per_page,
        peers_max_per_page=args.peers_max_per_page,
        sectors_top_n=args.sectors_top_n,
        font_title=args.font_title,
        font_subtitle=args.font_subtitle,
        font_table=args.font_table,
    )

    # list.txt (給影片串接)
    _write_list_txt(out_dir, paths)

    print(f"✅ 成功產生 {len(paths)} 張圖")
    if ("peers_not_limitup" not in payload) and ("peers_by_sector" not in payload):
        print("ℹ️ payload 未提供 peers_not_limitup / peers_by_sector：本次只輸出『漲停股』，未漲停補空自動略過。")
        print("   你要完整『同產業未漲停』，下一步把 peers 塞進 downloader 的 payload 即可。")
    print("Done.")


if __name__ == "__main__":
    main()
