# markets/tw/aggregator/core.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Any, Dict, List

import pandas as pd

from ..snapshot import extract_effective_ymd, is_snapshot_effectively_empty
from ..limit_type import infer_limit_type
from ..limitup_flags import infer_limitup_flags_from_price

from ..builders import (
    build_limitup,
    build_sector_summary_main,
    build_peers_by_sector,
    flatten_peers,
    build_open_limit_watchlist,
    build_sector_summary_open_limit,
    merge_open_limit_into_limitup_df,
)

from ..config import EMERGING_STRONG_RET

from .normalize import normalize_snapshot_main, normalize_snapshot_open
from .open_limit import enrich_open_limit_df, normalize_open_limit_watchlist_rows, open_watchlist_enabled
from .touch_semantics import fix_touch_double_count_for_overview_rows
from .universe import build_universe
from .overview import build_overview, merge_sector_with_universe

from .flags import enrich_overview_flags
from .helpers import norm_ymd, sanitize_nan
from .meta import apply_filters, apply_stats, apply_meta


# =============================================================================
# TW-only debug default ON (can override manually)
# =============================================================================
os.environ.setdefault("TW_OVERVIEW_BUCKET_DEBUG", "1")


def _env_bool(name: str, default: str = "0") -> bool:
    v = str(os.getenv(name, default)).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _to_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, bool):
            return int(x)
        return int(float(x))
    except Exception:
        return default


def _sector_key(x: Any) -> str:
    s = _safe_str(x)
    return s if s else "未分類"


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, bool):
            return float(int(x))
        return float(x)
    except Exception:
        return default


# =============================================================================
# TW aggregator-first: fill bigmove10_cnt + pct fields on sector_summary
# =============================================================================
def _tw_bigmove10_counts_by_sector(
    dfS: pd.DataFrame,
    *,
    open_limit_watchlist: List[Dict[str, Any]] | None = None,
    surge_ret: float = 0.10,
) -> Dict[str, int]:
    """
    TW bigmove10_ex (用於 overview mix / bigmove10):

    ✅ 主板(dfS):
      - ret >= 10% 且排除 limitup locked / touched
      - 優先使用 flags.py 產出的 is_bigmove10_ex_locked

    ✅ 興櫃(open_limit_watchlist):
      - 只把「收盤 >= 10%」算進 bigmove10（避免把 '觸及但未收盤10%' 混進 overview）
      - 判斷優先 ret >= surge_ret；若缺 ret 才 fallback ret_pct/100
      - 不看 is_surge10_touch（那是 touch 語意），因為你不要把興櫃觸及失敗塞進 overview
    """
    out: Dict[str, int] = {}

    # --- main board from dfS ---
    if dfS is not None and (not dfS.empty) and ("sector" in dfS.columns):
        d = dfS.copy()
        d["sector"] = d["sector"].fillna("").astype(str).str.strip()
        d.loc[d["sector"].eq(""), "sector"] = "未分類"

        if "is_bigmove10_ex_locked" in d.columns:
            m = d["is_bigmove10_ex_locked"].fillna(False).astype(bool)
        else:
            r = pd.to_numeric(d.get("ret", 0.0), errors="coerce").fillna(0.0)

            is_locked = d.get("is_limitup_locked", False)
            is_touch = d.get("is_limitup_touch", False)
            try:
                is_locked = is_locked.fillna(False).astype(bool)  # type: ignore
            except Exception:
                is_locked = False
            try:
                is_touch = is_touch.fillna(False).astype(bool)  # type: ignore
            except Exception:
                is_touch = False

            m = (r >= float(surge_ret)) & (~is_locked) & (~is_touch)

        g = d.loc[m, ["sector"]].groupby("sector").size()
        for sec, cnt in g.items():
            out[str(sec)] = out.get(str(sec), 0) + int(cnt)

    # --- emerging/open_limit from watchlist (CLOSE >= 10% only) ---
    wl = open_limit_watchlist or []
    for row in wl:
        if not isinstance(row, dict):
            continue

        # ✅ only count CLOSE>=10% movers for overview bigmove10 bucket
        r = row.get("ret", None)
        if r is None:
            # fallback: some pipelines may only have ret_pct
            rp = row.get("ret_pct", None)
            if rp is not None:
                r = _to_float(rp, 0.0) / 100.0
            else:
                r = 0.0

        if _to_float(r, 0.0) < float(surge_ret):
            continue

        sec = str(row.get("sector", "") or "").strip() or "未分類"
        out[sec] = out.get(sec, 0) + 1

    return out


def _tw_attach_bigmove10_and_pcts(
    sector_rows: List[Dict[str, Any]],
    *,
    dfS: pd.DataFrame,
    open_limit_watchlist: List[Dict[str, Any]] | None,
    surge_ret: float = 0.10,
) -> List[Dict[str, Any]]:
    """
    在 merge_sector_with_universe 後做（因為此時 sector_total 已補齊）
    並且要求：呼叫前 sector_rows 的 touch_cnt 已是 touch-only（fix_touch... 已跑過）

    回填：
      - bigmove10_cnt / bigmove10_pct
      - mix_cnt / mix_pct
      - all_pct (alias) = mix_pct（讓 render/metrics 更穩）
    """
    if not sector_rows:
        return sector_rows

    per = _tw_bigmove10_counts_by_sector(
        dfS,
        open_limit_watchlist=open_limit_watchlist,
        surge_ret=surge_ret,
    ) or {}

    out: List[Dict[str, Any]] = []
    for r in sector_rows:
        rr = dict(r or {})
        sec = _safe_str(rr.get("sector")) or "未分類"

        big10 = int(per.get(sec, 0))
        rr["bigmove10_cnt"] = big10

        # denominator
        try:
            sector_total = int(rr.get("sector_total", rr.get("universe_cnt", 0)) or 0)
        except Exception:
            sector_total = 0

        # locked/touch（此時 touch_cnt 應該已是 touch-only）
        try:
            locked_cnt = int(rr.get("locked_cnt", 0) or 0)
        except Exception:
            locked_cnt = 0
        try:
            touch_cnt = int(rr.get("touch_cnt", 0) or 0)
        except Exception:
            touch_cnt = 0

        rr["bigmove10_pct"] = (big10 / sector_total) if sector_total > 0 else 0.0

        mix_cnt = int(big10 + locked_cnt + touch_cnt)
        rr["mix_cnt"] = mix_cnt
        rr["mix_pct"] = (mix_cnt / sector_total) if sector_total > 0 else 0.0

        # some older code paths refer to all_pct
        rr["all_pct"] = rr["mix_pct"]

        out.append(rr)

    return out


def aggregate(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(payload or {})

    requested_ymd = norm_ymd(payload.get("requested_ymd"))
    current_ymd = norm_ymd(payload.get("ymd"))
    slot = str(payload.get("slot") or "").lower()
    raw_ymd_effective = norm_ymd(payload.get("ymd_effective"))

    snapshot_main = payload.get("snapshot_main") or []

    snapshot_open = payload.get("snapshot_open")
    if snapshot_open is None:
        snapshot_open = payload.get("snapshot_emerging") or []
    else:
        snapshot_open = snapshot_open or []

    # 1) normalize
    dfS = normalize_snapshot_main(snapshot_main)
    dfO = normalize_snapshot_open(snapshot_open)

    # 2) open-limit snapshot normalize/enrich（興櫃池）
    dfO = enrich_open_limit_df(dfO)

    # 3) main board: infer limit_type + tick-based limitup flags (standard only)
    infer_limit_type(dfS)
    infer_limitup_flags_from_price(dfS)

    # 4) overview flags
    SURGE_RET = 0.10
    enrich_overview_flags(dfS, surge_ret=SURGE_RET)

    # 5) meta derived
    inferred_ymd_effective = norm_ymd(extract_effective_ymd(dfS, dfO))
    is_market_open = not is_snapshot_effectively_empty(dfS)

    ymd_effective_final = raw_ymd_effective or inferred_ymd_effective or current_ymd or requested_ymd
    payload["ymd_effective"] = ymd_effective_final
    payload["is_market_open"] = bool(is_market_open)

    # 6) open_limit watchlist（threshold-touch semantics；但 overview 只吃 close>=10%）
    if open_watchlist_enabled():
        open_limit_watchlist = build_open_limit_watchlist(dfO)
        open_limit_watchlist = normalize_open_limit_watchlist_rows(open_limit_watchlist)
    else:
        open_limit_watchlist = []

    open_limit_sector_summary = build_sector_summary_open_limit(open_limit_watchlist) if open_limit_watchlist else []

    payload["open_limit_watchlist"] = open_limit_watchlist
    payload["open_limit_sector_summary"] = open_limit_sector_summary

    # backward-compat aliases
    payload["emerging_watchlist"] = payload["open_limit_watchlist"]
    payload["emerging_sector_summary"] = payload["open_limit_sector_summary"]

    # 7) builders: main limitup/theme list + sector + peers
    limitup_df = build_limitup(dfS)

    # merge open-limit strong movers into sector pages top box list
    limitup_all_df = merge_open_limit_into_limitup_df(limitup_df, open_limit_rows=open_limit_watchlist)

    payload["limitup"] = (
        limitup_all_df.to_dict(orient="records")
        if (limitup_all_df is not None and not limitup_all_df.empty)
        else []
    )

    # ✅ IMPORTANT FIX:
    # sector_summary_main SHOULD represent main-board semantics only for locked/touch/10%+,
    # otherwise open_limit rows may get counted into touch/locked AND also into bigmove10_cnt later (double count).
    sector_summary_main = build_sector_summary_main(limitup_df, open_limit_rows=None)

    # ✅ normalize touch_cnt semantics (touch-only) BEFORE we compute mix_cnt later
    sector_summary_main = fix_touch_double_count_for_overview_rows(sector_summary_main)
    open_limit_sector_summary = fix_touch_double_count_for_overview_rows(open_limit_sector_summary)

    payload["sector_summary"] = sector_summary_main
    payload["open_limit_sector_summary"] = open_limit_sector_summary

    # ------------------------------------------------------------
    # DEBUG: confirm we reached peers builder + print sector pages
    # ------------------------------------------------------------
    sector_pages_sectors = None
    if _env_bool("TW_PEERS_DEBUG", "0"):
        try:
            ss = payload.get("sector_summary") or []
            sec_list = []
            for r in ss:
                if isinstance(r, dict):
                    s = _safe_str(r.get("sector"))
                    if s:
                        sec_list.append(s)
            # preserve order (first occurrence)
            seen = set()
            sec_list_uniq = []
            for s in sec_list:
                if s in seen:
                    continue
                seen.add(s)
                sec_list_uniq.append(s)

            sector_pages_sectors = set(sec_list_uniq)

            print("\n" + "=" * 72)
            print("[TW_PEERS_DEBUG] aggregator.core -> calling build_peers_by_sector")
            print(f"  dfO_len={int(len(dfO)) if dfO is not None else 0} | watchlist_len={len(open_limit_watchlist)}")
            print(f"  sector_pages_cnt(from sector_summary)={len(sec_list_uniq)}\n")

            print("  [sector pages list TOP 20] (from payload['sector_summary'])")
            for i, s in enumerate(sec_list_uniq[:20], start=1):
                print(f"    {i:02d}. {s}")
            print("=" * 72)
        except Exception:
            sector_pages_sectors = None

    # ✅ IMPORTANT:
    # - pass dfO so peers can include <10% emerging
    # - pass sector_pages_sectors so dfO peers that have NO page are skipped (no fallback to 未分類)
    payload["peers_by_sector"] = build_peers_by_sector(
        dfS,
        limitup_df,
        open_limit_rows=open_limit_watchlist,
        dfO=dfO,
        surge_ret=SURGE_RET,
        sector_pages_sectors=sector_pages_sectors,  # ✅ new (optional)
    )
    payload["peers_not_limitup"] = flatten_peers(payload.get("peers_by_sector") or {})

    # 8) universe (main + open)
    payload["universe"] = build_universe(dfS=dfS, dfO=dfO)

    # 9) attach denominators for sector % badges (keep in payload sector_summary)
    payload["sector_summary"] = merge_sector_with_universe(
        sector_rows=payload.get("sector_summary") or [],
        universe_by_sector=(payload.get("universe") or {}).get("by_sector") or [],
        key_count="count",
    )

    payload["open_limit_sector_summary"] = merge_sector_with_universe(
        sector_rows=payload.get("open_limit_sector_summary") or [],
        universe_by_sector=(payload.get("universe") or {}).get("by_sector") or [],
        key_count="count",
    )

    # ✅ TW aggregator-first: fill bigmove10_cnt + pct fields here (renderer no TW special needed)
    if str(payload.get("market", "")).strip().lower() == "tw":
        # sector_summary 的 touch_cnt 已在上面 fix 成 touch-only，這裡直接算 mix_cnt
        payload["sector_summary"] = _tw_attach_bigmove10_and_pcts(
            payload.get("sector_summary") or [],
            dfS=dfS,
            open_limit_watchlist=open_limit_watchlist,
            surge_ret=SURGE_RET,
        )

        # ---------------------------------------------------------------------
        # DEBUG: bucket accounting sanity (sector_summary vs open_limit_watchlist)
        # ---------------------------------------------------------------------
        if _env_bool("TW_OVERVIEW_BUCKET_DEBUG", "0"):
            try:
                ss = payload.get("sector_summary") or []
                wl = payload.get("open_limit_watchlist") or []

                # open_limit_watchlist count by sector (ALL) + (CLOSE>=10% only)
                wl_cnt_all: Dict[str, int] = {}
                wl_cnt_10: Dict[str, int] = {}

                for row in wl:
                    if not isinstance(row, dict):
                        continue
                    sec = _sector_key(row.get("sector"))
                    wl_cnt_all[sec] = wl_cnt_all.get(sec, 0) + 1

                    r = row.get("ret", None)
                    if r is None:
                        rp = row.get("ret_pct", None)
                        r = (_to_float(rp, 0.0) / 100.0) if rp is not None else 0.0
                    if _to_float(r, 0.0) >= float(SURGE_RET):
                        wl_cnt_10[sec] = wl_cnt_10.get(sec, 0) + 1

                print("\n" + "=" * 100)
                print("[TW_OVERVIEW_BUCKET_DEBUG] sector bucket accounting")
                print("  NOTE: wl_all is total watchlist; wl10 is CLOSE>=10% subset that IS counted into big10.")
                print("-" * 100)
                print(
                    f"{'sector':<14} "
                    f"{'locked':>6} {'touch':>6} {'big10':>6} {'mix':>6} "
                    f"{'wl_all':>7} {'wl10':>6} "
                    f"{'mix-(l+t+big10)':>16}  hint"
                )
                print("-" * 100)

                for r in ss:
                    if not isinstance(r, dict):
                        continue
                    sec = _sector_key(r.get("sector"))

                    locked = _to_int(r.get("locked_cnt", 0))
                    touch = _to_int(r.get("touch_cnt", 0))
                    big10 = _to_int(r.get("bigmove10_cnt", 0))
                    mix = _to_int(r.get("mix_cnt", 0))

                    w_all = int(wl_cnt_all.get(sec, 0))
                    w10 = int(wl_cnt_10.get(sec, 0))

                    diff = mix - (locked + touch + big10)

                    hint = ""
                    if w10 > 0:
                        hint = "wl10->big10 OK"
                    elif w_all > 0 and touch > 0:
                        hint = "⚠ wl_all & touch>0 (ok: wl touch not counted)"
                    elif touch > 0:
                        hint = "touch only"

                    print(
                        f"{sec:<14} "
                        f"{locked:>6} {touch:>6} {big10:>6} {mix:>6} "
                        f"{w_all:>7} {w10:>6} "
                        f"{diff:>16}  {hint}"
                    )

                tot_locked = sum(_to_int(r.get("locked_cnt", 0)) for r in ss if isinstance(r, dict))
                tot_touch = sum(_to_int(r.get("touch_cnt", 0)) for r in ss if isinstance(r, dict))
                tot_big10 = sum(_to_int(r.get("bigmove10_cnt", 0)) for r in ss if isinstance(r, dict))
                tot_mix = sum(_to_int(r.get("mix_cnt", 0)) for r in ss if isinstance(r, dict))
                tot_w_all = sum(wl_cnt_all.values())
                tot_w10 = sum(wl_cnt_10.values())

                print("-" * 100)
                print(
                    f"{'TOTAL':<14} "
                    f"{tot_locked:>6} {tot_touch:>6} {tot_big10:>6} {tot_mix:>6} "
                    f"{tot_w_all:>7} {tot_w10:>6} "
                    f"{'':>16}"
                )
                print("=" * 100)

                if wl_cnt_all:
                    print("[TW_OVERVIEW_BUCKET_DEBUG] open_limit_watchlist sectors (debug only):")
                    for sec, c in sorted(wl_cnt_all.items(), key=lambda kv: (-kv[1], kv[0])):
                        extra = f" (wl10={wl_cnt_10.get(sec, 0)})"
                        print(f"  - {sec}: {c}{extra}")
                    print("=" * 100 + "\n")

            except Exception as e:
                print("[TW_OVERVIEW_BUCKET_DEBUG] ERROR:", repr(e))

    # 10) overview bundle
    payload["overview"] = build_overview(
        dfS=dfS,
        dfO=dfO,
        open_limit_watchlist=open_limit_watchlist,
        sector_summary_main=payload.get("sector_summary") or [],
        open_limit_sector_summary=payload.get("open_limit_sector_summary") or [],
        universe=payload.get("universe") or {},
        surge_ret=SURGE_RET,
    )
    payload["overview_counts"] = payload["overview"].get("counts", {})
    payload["overview_pct"] = payload["overview"].get("pct", {})

    # 11) filters
    apply_filters(payload, slot=slot, surge_ret=SURGE_RET)

    # 12) stats
    apply_stats(
        payload,
        dfS_len=int(len(dfS)) if dfS is not None else 0,
        dfO_len=int(len(dfO)) if dfO is not None else 0,
        is_market_open=bool(is_market_open),
    )

    # 13) meta totals/metrics（overview_mpl 會吃）
    apply_meta(
        payload,
        requested_ymd=requested_ymd,
        current_ymd=current_ymd,
        raw_ymd_effective=raw_ymd_effective,
        inferred_ymd_effective=inferred_ymd_effective,
    )

    # 14) snapshot_open output key unify
    payload["snapshot_open"] = snapshot_open

    # 15) snapshot_main enrichment: keep ALL rows with new flags (json-safe)
    if dfS is not None and not dfS.empty:
        df_safe = dfS.where(pd.notna(dfS), None)
        payload["snapshot_main"] = sanitize_nan(df_safe.to_dict(orient="records"))

    # also output normalized snapshot_open rows (helps debugging)
    if dfO is not None and not dfO.empty:
        dfO_safe = dfO.where(pd.notna(dfO), None)
        payload["snapshot_open_norm"] = sanitize_nan(dfO_safe.to_dict(orient="records"))

    return sanitize_nan(payload)