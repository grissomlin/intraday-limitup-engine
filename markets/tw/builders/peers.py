# markets/tw/builders/peers.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Set

import pandas as pd


def _env_bool(name: str, default: str = "0") -> bool:
    v = str(os.getenv(name, default)).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _maybe_to_decimal(v: Any) -> float:
    """
    Normalize return-like value to decimal.
    Accepts:
      - ret (0.123)
      - ret_pct (12.3)
    Heuristic:
      abs(v) > 1.5 -> treat as percent, /100
    """
    x = _safe_float(v, 0.0)
    if abs(x) > 1.5:
        return x / 100.0
    return x


def _compute_ret_decimal(row: Dict[str, Any]) -> float:
    """
    Prefer ret / ret_pct; fallback compute from close & prev_close.
    Return decimal (0.10 == 10%).
    """
    if row.get("ret") is not None:
        return _maybe_to_decimal(row.get("ret"))
    if row.get("ret_pct") is not None:
        return _maybe_to_decimal(row.get("ret_pct"))

    prev = _safe_float(row.get("prev_close"), 0.0)
    close = _safe_float(row.get("close"), 0.0)
    if prev > 0:
        return (close - prev) / prev
    return 0.0


def _compute_ret_high_decimal(row: Dict[str, Any]) -> float:
    """
    Prefer ret_high / ret_high_pct; fallback compute from high & prev_close.
    Return decimal.
    """
    if row.get("ret_high") is not None:
        return _maybe_to_decimal(row.get("ret_high"))
    if row.get("ret_high_pct") is not None:
        return _maybe_to_decimal(row.get("ret_high_pct"))

    prev = _safe_float(row.get("prev_close"), 0.0)
    high = _safe_float(row.get("high"), 0.0)
    if prev > 0:
        return (high - prev) / prev
    return 0.0


def _is_emerging_row(row: Dict[str, Any]) -> bool:
    md = _safe_str(row.get("market_detail") or row.get("board_kind") or "").lower()
    if "rotc" in md:
        return True
    if md in ("emerging", "open_limit", "otc_emerging"):
        return True
    if bool(row.get("is_emerging", False)):
        return True
    return False


def _row_to_peer_dict(row: Dict[str, Any], *, force_peer: bool = True) -> Dict[str, Any]:
    out = dict(row)
    if not _safe_str(out.get("line1")):
        out["line1"] = _safe_str(out.get("name") or out.get("symbol") or "")
    if "line2" not in out:
        out["line2"] = _safe_str(out.get("status_text") or "")
    if force_peer:
        out["kind"] = "peer"
    return out


def build_peers_by_sector(
    dfS: pd.DataFrame,
    limitup_df: pd.DataFrame,
    *,
    open_limit_rows: Optional[List[Dict[str, Any]]] = None,
    dfO: Optional[pd.DataFrame] = None,
    surge_ret: float = 0.10,
    sector_pages_sectors: Optional[Set[str]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Sector pages (bottom box) peers:
    - dfS peers: same sector, excluding symbols already in top list
    - dfO (興櫃) peers:
        exclude watchlist,
        exclude if (close_ret >=10%) OR (high_ret >=10%),
        keep otherwise.

    ✅ NEW (fix):
    - If sector_pages_sectors is provided:
        - dfO peers whose sector NOT in pages -> SKIP (no fallback to 未分類)
      This prevents polluting 未分類 with sectors that don't even have pages today.
    """
    open_limit_rows = open_limit_rows or []
    tw_debug = _env_bool("TW_PEERS_DEBUG", "0")

    peers_by_sector: Dict[str, List[Dict[str, Any]]] = {}

    # ------------------------------------------------------------
    # 1) Main board peers (dfS)
    # ------------------------------------------------------------
    limit_symbols: set[str] = set()
    try:
        if limitup_df is not None and not limitup_df.empty and "symbol" in limitup_df.columns:
            limit_symbols = set(limitup_df["symbol"].astype(str).tolist())
    except Exception:
        limit_symbols = set()

    if dfS is not None and not dfS.empty:
        cols = set(dfS.columns)
        if "sector" in cols and "symbol" in cols:
            dfP = dfS[~dfS["symbol"].astype(str).isin(limit_symbols)].copy()
            for sec, g in dfP.groupby("sector"):
                sec_name = _safe_str(sec) or "未分類"
                rows: List[Dict[str, Any]] = []
                for _, rr in g.iterrows():
                    rows.append(_row_to_peer_dict(rr.to_dict()))
                if rows:
                    peers_by_sector.setdefault(sec_name, []).extend(rows)

    # ------------------------------------------------------------
    # 2) Emerging peers (dfO)
    # ------------------------------------------------------------
    if dfO is not None and not dfO.empty:
        watch_symbols: set[str] = set()
        for r in open_limit_rows:
            s = _safe_str(r.get("symbol"))
            if s:
                watch_symbols.add(s)

        emerging_total = 0
        excluded_watch = 0
        excluded_ge10 = 0
        kept_after_filter = 0
        used_into_pages = 0

        # extra debug stats
        close_ge10_cnt = 0
        high_ge10_cnt = 0
        any_ge10_cnt = 0

        # NEW: page-skip stats (no fallback)
        skipped_no_page_cnt = 0

        # sector counts before page-skip (after filtering watch & >=10)
        raw_sector_counts: Dict[str, int] = {}
        # sector counts actually inserted into peers_by_sector
        used_sector_counts: Dict[str, int] = {}

        for _, rr in dfO.iterrows():
            row = rr.to_dict()
            if not _is_emerging_row(row):
                continue

            emerging_total += 1

            sym = _safe_str(row.get("symbol"))
            if sym and sym in watch_symbols:
                excluded_watch += 1
                continue

            close_ret = _compute_ret_decimal(row)
            high_ret = _compute_ret_high_decimal(row)
            is_ge10 = (close_ret >= float(surge_ret)) or (high_ret >= float(surge_ret))

            if close_ret >= float(surge_ret):
                close_ge10_cnt += 1
            if high_ret >= float(surge_ret):
                high_ge10_cnt += 1
            if is_ge10:
                any_ge10_cnt += 1
                excluded_ge10 += 1
                continue

            # passed filters -> candidate peer
            kept_after_filter += 1

            sec_raw = _safe_str(row.get("sector")) or "未分類"
            raw_sector_counts[sec_raw] = raw_sector_counts.get(sec_raw, 0) + 1

            # ✅ NEW: If sector has no page today -> SKIP (do NOT fallback to 未分類)
            if sector_pages_sectors is not None and sec_raw not in sector_pages_sectors:
                skipped_no_page_cnt += 1
                continue

            peers_by_sector.setdefault(sec_raw, []).append(_row_to_peer_dict(row))
            used_into_pages += 1
            used_sector_counts[sec_raw] = used_sector_counts.get(sec_raw, 0) + 1

        if tw_debug:
            # sector pages set debug
            sec_set = set(sector_pages_sectors or set())
            raw_sec_set = set(raw_sector_counts.keys())
            diff = sorted(list(raw_sec_set - sec_set)) if sec_set else sorted(list(raw_sec_set))

            def _top20_from_counts(m: Dict[str, int]) -> List[tuple[str, int]]:
                items = list(m.items())
                items.sort(key=lambda x: (-int(x[1]), str(x[0])))
                return items[:20]

            print("\n" + "=" * 72)
            print("[TW_PEERS_DEBUG] Emerging peer filter summary")
            print(f"  興櫃總數: {emerging_total}")
            print(f"  被 watchlist 排除數: {excluded_watch}")
            print(
                f"  被 >=10% 排除數: {excluded_ge10}   (close>=10%={close_ge10_cnt}, high>=10%={high_ge10_cnt}, any={any_ge10_cnt})"
            )
            print(f"  通過過濾(可當 peers 候選)的興櫃數: {kept_after_filter}")

            if sector_pages_sectors is not None:
                print(f"  sector_set(頁面集合) size: {len(sec_set)}")
                print(f"  dfO_kept_raw_sector size: {len(raw_sec_set)}")
                print(f"  skipped_no_page_cnt(無頁面 sector 直接略過): {skipped_no_page_cnt}")
                print(f"  最後真的塞進 peers_by_sector 的興櫃數: {used_into_pages}")
            else:
                print(f"  最後進 peers 的興櫃數: {used_into_pages}")

            print("\n  [dfO peers raw sector TOP 20] (after filters, before page-skip)")
            for i, (sec, cnt) in enumerate(_top20_from_counts(raw_sector_counts), start=1):
                print(f"    {i:02d}. {sec}  ({cnt})")

            if sector_pages_sectors is not None:
                print("\n  [dfO peers used sector TOP 20] (actually inserted into pages)")
                for i, (sec, cnt) in enumerate(_top20_from_counts(used_sector_counts), start=1):
                    print(f"    {i:02d}. {sec}  ({cnt})")

                print("\n  [DIFF] dfO_peers_raw_sectors - sector_pages_sectors")
                if diff:
                    for s in diff:
                        print(f"    - {s}")
                else:
                    print("    (empty)")
            print("=" * 72)

    return peers_by_sector


def flatten_peers(peers_by_sector: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Backward-compat: builders/__init__.py expects this symbol from peers.py
    """
    out: List[Dict[str, Any]] = []
    if not peers_by_sector:
        return out
    for sec, rows in peers_by_sector.items():
        if not rows:
            continue
        for r in rows:
            d = dict(r) if isinstance(r, dict) else {}
            if "sector" not in d or not _safe_str(d.get("sector")):
                d["sector"] = sec
            out.append(d)
    return out
