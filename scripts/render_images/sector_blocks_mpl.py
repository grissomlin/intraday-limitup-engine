# scripts/render_images/sector_blocks_mpl.py
# -*- coding: utf-8 -*-
"""
產業個股清單圖（Matplotlib 版）- 7 rows 版
- 9:16 圖 (1080x1920)
- 漲停/同產業未漲停 各自分頁（避免後面頁面漲停空白）
- ✅ 7 筆顯示（rows_per_page / peers_max_per_page 預設 7）
- ✅ 興櫃強漲（ret>=10%）自動注入 peers
- ✅ badge 分級：強漲 / 暴漲 / 噴出 50%+
- ✅ 昨天也強漲 -> peers 顯示「昨1」（streak_prev=1）
- ✅ 版面微調：吃掉上下留白（更滿版）
- ✅ 檔名可選擇加 market 前綴（預設 ON）：tw_sector_...
  - ENV: RENDER_FILENAME_PREFIX_MARKET=0  -> 關掉前綴
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List

from .sector_blocks.draw_mpl import draw_block_table, parse_cutoff, sector_counts
from .sector_blocks.layout import pick_layout
from .sector_blocks.policy import build_yesterday_strong_set, collect_rows, paginate_sector


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "")
    if raw == "":
        return default
    raw = raw.strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


FILENAME_PREFIX_MARKET = _env_bool("RENDER_FILENAME_PREFIX_MARKET", True)

# Repo root (for looking up yesterday payload)
REPO_ROOT = Path(__file__).resolve().parents[2]


def render_sector_blocks(
    payload: Dict[str, Any],
    out_dir: Path,
    width: int = 1080,
    height: int = 1920,
    rows_per_page: int = 7,
    peers_max_per_page: int = 7,
    sectors_per_list: int = 8,
    top_n: int = 0,
    theme: str = "dark",
) -> List[Path]:
    limitup = payload.get("limitup", []) or []
    if not limitup:
        return []

    cutoff = parse_cutoff(payload)

    # ✅ market (先支援路徑/preset，不強迫你現在就做 US/CN)
    market = str(payload.get("market") or "tw").strip().lower() or "tw"
    layout = pick_layout(market)

    # yesterday strong set (for 昨1 on injected emerging strong)
    yesterday_strong = build_yesterday_strong_set(payload, repo_root=REPO_ROOT, market=market, strong_ret=0.10)

    sector_summary = payload.get("sector_summary", []) or []
    if sector_summary:

        def total_cnt(x):
            if "total_cnt" in x:
                return int(x.get("total_cnt", 0) or 0)
            if "count" in x:
                return int(x.get("count", 0) or 0)
            return 0

        sector_summary = sorted(sector_summary, key=total_cnt, reverse=True)
        sectors = [x.get("sector") or "未分類" for x in sector_summary]
    else:
        sectors = sorted({(r.get("sector") or "未分類") for r in limitup})

    if sectors_per_list and sectors_per_list > 0:
        sectors = sectors[:sectors_per_list]
    if top_n and top_n > 0:
        sectors = sectors[:top_n]

    prefix = f"{market}_" if (FILENAME_PREFIX_MARKET and market) else ""
    out_paths: List[Path] = []

    for si, sec in enumerate(sectors, start=1):
        # estimate pages count from limitup rows only (peers will be capped accordingly)
        L0 = [r for r in (payload.get("limitup") or []) if (r.get("sector") or "未分類") == sec]
        nL_hint = max(1, (len(L0) + max(1, rows_per_page) - 1) // max(1, rows_per_page))

        L, P = collect_rows(
            payload,
            sec,
            yesterday_strong=yesterday_strong,
            max_peers_per_page=max(1, int(peers_max_per_page)),
            num_pages_hint=nL_hint + 1,  # allow at most +1 peers page (same as paginate rule)
            strong_ret=0.10,
        )

        locked, touch, theme_cnt = sector_counts(L)

        pages = paginate_sector(L, P, max(1, int(rows_per_page)), max(1, int(peers_max_per_page)))

        for pi, pack in enumerate(pages, start=1):
            safe = "".join(ch if ch not in '\\/*?:"<>|' else "_" for ch in sec).strip()
            fname = (
                f"{prefix}sector_{si:02d}_{safe}_p{pi}of{len(pages)}.png"
                if len(pages) > 1
                else f"{prefix}sector_{si:02d}_{safe}.png"
            )
            out_path = out_dir / "sectors" / fname

            draw_block_table(
                out_path=out_path,
                layout=layout,
                sector=sec,
                cutoff=cutoff,
                locked_cnt=locked,
                touch_cnt=touch,
                theme_cnt=theme_cnt,
                limitup_rows=pack["limitup_rows"],
                peer_rows=pack["peer_rows"],
                page_idx=pi,
                page_total=len(pages),
                width=width,
                height=height,
                rows_per_page=rows_per_page,
                theme=theme,
            )

            out_paths.append(out_path)
            print(f"✅ {fname}")

    return out_paths
