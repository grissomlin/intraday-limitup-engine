# scripts/render_images_tw/pipeline.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, List

from scripts.render_images_common.overview_mpl import render_overview_png
from scripts.render_images_tw.sector_blocks.draw_mpl import (
    draw_block_table,
    get_market_time_info,
    parse_cutoff,
)
from scripts.render_images_tw.sector_blocks.layout import get_layout

from scripts.render_images_tw.tw_rows import (
    build_top_rows_by_sector_tw,
    build_peers_by_sector_tw,
    count_locked_touch_surge,
)

from scripts.render_images_tw.utils_tw import (
    safe_str,
    sanitize_filename,
    norm_sector,
    chunk,
)

from scripts.utils.drive_uploader import ensure_folder, get_drive_service, upload_dir


DEFAULT_ROOT_FOLDER = (
    os.getenv("GDRIVE_ROOT_FOLDER_ID", "").strip()
    or "1wxOxKDRLZ15dwm-V2G25l_vjaHQ-f2aE"
)


def _payload_ymd(payload: Dict[str, Any]) -> str:
    return safe_str(payload.get("ymd_effective") or payload.get("ymd") or "")


def _payload_slot(payload: Dict[str, Any]) -> str:
    s = safe_str(payload.get("slot") or "")
    return s or "unknown"


def _normalize_market(m: str) -> str:
    m = (m or "").strip().upper()
    alias = {"TAIWAN": "TW", "TWSE": "TW", "TPEX": "TW", "ROC": "TW"}
    return alias.get(m, m or "TW")


def _market_from_payload(payload: Dict[str, Any]) -> str:
    return _normalize_market(safe_str(payload.get("market") or "TW"))


def _first_ymd(payload: Dict[str, Any]) -> Optional[str]:
    for k in ("bar_date", "ymd", "ymd_effective", "date"):
        v = str(payload.get(k) or "").strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            return v
    for k in ("asof", "slot"):
        v = str(payload.get(k) or "").strip()
        m = re.search(r"(\d{4}-\d{2}-\d{2})", v)
        if m:
            return m.group(1)
    return None


def _infer_run_tag(payload: Dict[str, Any]) -> str:
    s = str(payload.get("slot") or payload.get("asof") or "").lower()
    if "open" in s:
        return "open"
    if "midday" in s or "noon" in s:
        return "midday"
    if "close" in s:
        return "close"
    m = re.search(r"(\d{1,2}):(\d{2})", s)
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2))
        return f"{hh:02d}{mm:02d}"
    return "run"


def make_drive_subfolder_name(payload: Dict[str, Any], market: str) -> str:
    ymd = _first_ymd(payload)
    tag = _infer_run_tag(payload)
    if ymd:
        return f"{market}_{ymd}_{tag}"
    now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"{market}_{now}"


def _sector_order_from_sector_summary(payload: Dict[str, Any]) -> List[str]:
    """拿 sector_summary 的順序（去重），用來排序 sector pages。"""
    out: List[str] = []
    seen = set()
    ss = payload.get("sector_summary") or []
    if not isinstance(ss, list):
        return out
    for r in ss:
        if not isinstance(r, dict):
            continue
        sec = norm_sector(r.get("sector"))
        if not sec or sec in seen:
            continue
        seen.add(sec)
        out.append(sec)
    return out


def render_tw(
    *,
    payload: Dict[str, Any],
    outdir: Path,
    theme: str = "dark",
    layout_name: str = "tw",
    rows_per_box: int = 7,
    max_sectors: int = 20,
    cap_pages: int = 5,
    no_overview: bool = False,
    overview_metric: str = "auto",
    overview_page_size: int = 15,
    overview_gainbins: bool = False,
    no_upload_drive: bool = False,
    drive_root_folder_id: str = DEFAULT_ROOT_FOLDER,
    drive_market: str = "TW",
    drive_client_secret: Optional[str] = None,
    drive_token: Optional[str] = None,
    drive_subfolder: Optional[str] = None,
    drive_workers: int = 16,
    drive_no_concurrent: bool = False,
    drive_no_overwrite: bool = False,
    drive_quiet: bool = False,
) -> None:
    outdir.mkdir(parents=True, exist_ok=True)

    market = _market_from_payload(payload)
    layout = get_layout(layout_name)
    cutoff = parse_cutoff(payload)
    _, time_note = get_market_time_info(payload)

    width, height = 1080, 1920
    rows_top = max(1, int(rows_per_box))
    rows_peer = rows_top + 1
    CAP_PAGES = max(1, int(cap_pages))

    # ---------------------------------------------------------------------
    # Overview
    # ---------------------------------------------------------------------
    if not no_overview:
        payload_for_overview = dict(payload)
        payload_for_overview["market"] = market
        payload_for_overview.setdefault(
            "asof",
            payload_for_overview.get("asof") or payload_for_overview.get("slot") or "",
        )

        overview_paths = render_overview_png(
            payload_for_overview,
            outdir,
            width=width,
            height=height,
            page_size=int(overview_page_size),
            metric=str(overview_metric or "auto"),
        )
        for p in overview_paths:
            print(f"[TW] wrote {p}")

        if overview_gainbins:
            gain_paths = render_overview_png(
                payload_for_overview,
                outdir,
                width=width,
                height=height,
                page_size=int(overview_page_size),
                metric="gainbins",
            )
            for p in gain_paths:
                print(f"[TW] wrote {p}")

    # ---------------------------------------------------------------------
    # Sector pages
    # ---------------------------------------------------------------------
    top_rows = build_top_rows_by_sector_tw(payload)

    # ✅ 只對「有 top」的 sector 出頁，但排序用 sector_summary 的順序
    order = _sector_order_from_sector_summary(payload)
    top_sectors = set(top_rows.keys())

    if order:
        sector_keys = [s for s in order if s in top_sectors]
    else:
        sector_keys = list(top_rows.keys())

    # cap
    sector_keys = sector_keys[: max(1, int(max_sectors))]

    # ✅ peers 只需要針對「會出頁的 sector」
    peers = build_peers_by_sector_tw(payload, sector_keys)

    print(f"[TW] sectors(top)={len(top_rows)} sectors(peers)={len(peers)} sectors(pages)={len(sector_keys)}")

    # sector_share map from sector_summary
    sec_share_map: Dict[str, float] = {}
    try:
        for r in (payload.get("sector_summary") or []):
            if not isinstance(r, dict):
                continue
            sec = norm_sector(r.get("sector"))
            sec_share_map[sec] = float(r.get("share_of_universe") or 0.0)
    except Exception:
        sec_share_map = {}

    for sector in sector_keys:
        L_total = top_rows.get(sector, [])
        if not L_total:
            # ✅ 核心規則：沒 top 就不出頁（即使 peers 有）
            continue

        P_all = peers.get(sector, [])

        max_top_show = CAP_PAGES * rows_top
        L_show = L_total[:max_top_show]

        L_pages = chunk(L_show, rows_top) if L_show else [[]]
        P_pages_all = chunk(P_all, rows_peer) if P_all else [[]]

        top_pages = len(L_pages)
        peer_pages = len(P_pages_all)

        total_pages = top_pages
        if peer_pages > top_pages:
            total_pages = top_pages + 1
        total_pages = min(total_pages, CAP_PAGES)

        locked_total, touch_total, surge_total = count_locked_touch_surge(L_total)

        sector_share = float(sec_share_map.get(norm_sector(sector), 0.0))
        sector_fn = sanitize_filename(sector)

        for i in range(total_pages):
            limitup_rows = L_pages[i] if i < len(L_pages) else []
            peer_rows = P_pages_all[i] if i < len(P_pages_all) else []

            # ✅ per-page prefix "shown" counts
            # - 顯示到本頁為止 top 區已呈現的累計筆數
            prefix_n = min(len(L_show), (i + 1) * rows_top)
            prefix_rows = L_show[:prefix_n]
            locked_shown_i, touch_shown_i, surge_shown_i = count_locked_touch_surge(prefix_rows)

            has_more_peers = (peer_pages > total_pages) and (i == total_pages - 1)
            out_path = outdir / f"tw_{sector_fn}_p{i+1}.png"

            draw_block_table(
                out_path=out_path,
                layout=layout,
                sector=sector,
                cutoff=cutoff,
                locked_cnt=locked_total,
                touch_cnt=touch_total,
                theme_cnt=surge_total,
                hit_shown=locked_shown_i,
                hit_total=locked_total,
                touch_shown=touch_shown_i,
                touch_total=touch_total,
                surge_shown=surge_shown_i,
                surge_total=surge_total,
                sector_share=sector_share,
                limitup_rows=limitup_rows,
                peer_rows=peer_rows,
                page_idx=i + 1,
                page_total=total_pages,
                width=width,
                height=height,
                rows_per_page=rows_top,
                theme=theme,
                time_note=time_note,
                has_more_peers=has_more_peers,
            )
            print(f"[TW] wrote {out_path}")

    # ---------------------------------------------------------------------
    # Drive upload
    # ---------------------------------------------------------------------
    if not no_upload_drive:
        print("\n🚀 Uploading PNGs to Google Drive...")

        svc = get_drive_service(
            client_secret_file=drive_client_secret,
            token_file=drive_token,
        )

        root_id = str(drive_root_folder_id).strip()
        market_name = str(drive_market or market or "TW").strip().upper()

        market_folder_id = ensure_folder(svc, root_id, market_name)

        subfolder = (
            str(drive_subfolder).strip()
            if drive_subfolder
            else make_drive_subfolder_name(payload, market=market_name)
        )

        print(f"📁 Target Drive folder: root/{market_name}/{subfolder}/")

        uploaded = upload_dir(
            svc,
            market_folder_id,
            outdir,
            pattern="*.png",
            recursive=False,
            overwrite=(not drive_no_overwrite),
            verbose=(not drive_quiet),
            concurrent=(not drive_no_concurrent),
            workers=int(drive_workers),
            subfolder_name=subfolder,
        )

        print(f"✅ Uploaded {uploaded} png(s)")