# scripts/render_images_tw/pipeline.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
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


def _sector_order_from_sector_summary(payload: Dict[str, Any]) -> List[str]:
    """拿 sector_summary 的順序（去重），用來排序 sector pages（fallback 用）。"""
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


# =============================================================================
# ✅ NEW: overview sector order helpers (TW)
# =============================================================================
def _normalize_sector_key_for_overview(s: Any) -> str:
    """
    Normalize sector for matching overview order.
    Keep consistent with other markets:
    - strip
    - collapse whitespace
    - lowercase
    """
    ss = safe_str(s)
    ss = re.sub(r"\s+", " ", ss).strip().lower()
    return ss


def _extract_overview_sector_order(payload: Dict[str, Any]) -> List[str]:
    """
    Read payload["_overview_sector_order"] exported by overview renderer,
    normalize + de-dup keep order.
    """
    raw = payload.get("_overview_sector_order", []) or []
    if not isinstance(raw, list):
        return []
    out: List[str] = []
    for x in raw:
        k = _normalize_sector_key_for_overview(x)
        if k:
            out.append(k)
    seen = set()
    out2: List[str] = []
    for k in out:
        if k not in seen:
            out2.append(k)
            seen.add(k)
    return out2


def _reorder_sectors_by_overview(
    *,
    sector_names: List[str],
    overview_order_keys: List[str],
) -> List[str]:
    """
    Reorder actual sector names by overview order keys; append remaining in original order.
    sector_names: original sector strings (as stored in top_rows keys)
    overview_order_keys: normalized keys (lowercase, collapsed spaces)
    """
    if not sector_names:
        return []
    if not overview_order_keys:
        return list(sector_names)

    # map normalized -> original (first wins)
    key_to_sector: Dict[str, str] = {}
    for sec in sector_names:
        k = _normalize_sector_key_for_overview(sec)
        if k and k not in key_to_sector:
            key_to_sector[k] = sec

    ordered: List[str] = []
    for k in overview_order_keys:
        sec = key_to_sector.get(k)
        if sec:
            ordered.append(sec)

    seen = set(_normalize_sector_key_for_overview(s) for s in ordered)
    for sec in sector_names:
        k = _normalize_sector_key_for_overview(sec)
        if k not in seen:
            ordered.append(sec)
            seen.add(k)

    return ordered


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

        # ✅ sync exported overview order back to original payload
        # (overview renderer writes into payload_for_overview, not payload)
        if isinstance(payload_for_overview.get("_overview_sector_order"), list):
            payload["_overview_sector_order"] = payload_for_overview.get("_overview_sector_order")
        if payload_for_overview.get("_overview_metric_eff") is not None:
            payload["_overview_metric_eff"] = payload_for_overview.get("_overview_metric_eff")

    # ---------------------------------------------------------------------
    # Sector pages
    # ---------------------------------------------------------------------
    top_rows = build_top_rows_by_sector_tw(payload)

    # ✅ only sectors with "top" get pages
    top_sectors = set(top_rows.keys())
    sectors_events = list(top_rows.keys())

    # 1) Prefer overview exported order (if present)
    overview_order_keys = _extract_overview_sector_order(payload)

    # Debug prints (safe)
    print("[TW][DEBUG] raw _overview_sector_order exists?:", isinstance(payload.get("_overview_sector_order"), list))
    print("[TW][DEBUG] raw overview order head:", (payload.get("_overview_sector_order", []) or [])[:20])
    if overview_order_keys:
        met_eff = safe_str(payload.get("_overview_metric_eff") or "")
        print(f"[TW] overview sector order loaded: n={len(overview_order_keys)}" + (f" metric={met_eff}" if met_eff else ""))
        print("[TW] normalized overview order head:", overview_order_keys[:20])

    # 2) Fallback: sector_summary order (your original logic)
    fallback_order = _sector_order_from_sector_summary(payload)

    # Build sector_keys with priority:
    # - overview order (only those in top_sectors)
    # - then remaining by original insertion order (or fallback sector_summary if no overview)
    if overview_order_keys:
        # reorder actual sector names based on overview order keys
        sector_keys = _reorder_sectors_by_overview(sector_names=sectors_events, overview_order_keys=overview_order_keys)
        # ensure only sectors with top
        sector_keys = [s for s in sector_keys if s in top_sectors]
    else:
        if fallback_order:
            sector_keys = [s for s in fallback_order if s in top_sectors]
        else:
            sector_keys = list(top_rows.keys())

    # cap
    sector_keys = sector_keys[: max(1, int(max_sectors))]

    # ✅ peers only for sectors that will be rendered
    peers = build_peers_by_sector_tw(payload, sector_keys)

    print(
        f"[TW] sectors(top)={len(top_rows)} "
        f"sectors(peers)={len(peers)} "
        f"sectors(pages)={len(sector_keys)}"
    )

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
