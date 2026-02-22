# scripts/render_images_common/overview/footer_calc.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Tuple


# =============================================================================
# Safe helpers
# =============================================================================
def _as_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        if isinstance(x, bool):
            return int(x)
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return default


def _as_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _get_dict(d: Any) -> Dict[str, Any]:
    return d if isinstance(d, dict) else {}


def _get_list(d: Any) -> List[Any]:
    return d if isinstance(d, list) else []


def _market_of(payload: Dict[str, Any]) -> str:
    return str(payload.get("market") or (_get_dict(payload.get("meta")).get("market")) or "").strip().upper()


def _filters_of(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _get_dict(payload.get("filters"))


def _stats_of(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _get_dict(payload.get("stats"))


# =============================================================================
# Market groups (no daily limit-up制度：open_limit / 英文市場)
# =============================================================================
NO_LIMIT_MARKETS = {"US", "CA", "AU", "UK", "EU"}


# =============================================================================
# Read meta
# =============================================================================
def _get_totals(payload: Dict[str, Any]) -> Dict[str, Any]:
    meta = _get_dict(payload.get("meta"))
    return _get_dict(meta.get("totals"))


def _get_metrics(payload: Dict[str, Any]) -> Dict[str, Any]:
    meta = _get_dict(payload.get("meta"))
    return _get_dict(meta.get("metrics"))


def _pick_key(payload: Dict[str, Any], keys: Tuple[str, ...]) -> Tuple[int, str]:
    """
    Try totals first, then metrics.
    Returns: (value, source_string) ; source_string is "missing" if none found.
    """
    totals = _get_totals(payload)
    metrics = _get_metrics(payload)

    for k in keys:
        if k in totals:
            return _as_int(totals.get(k), 0), f"totals.{k}"
        if k in metrics:
            return _as_int(metrics.get(k), 0), f"metrics.{k}"
    return 0, "missing"


# =============================================================================
# Open-limit (no-limit) fallback helpers
# =============================================================================
def _sum_sector_summary(payload: Dict[str, Any], key: str) -> int:
    ss = _get_list(payload.get("sector_summary"))
    s = 0
    for row in ss:
        d = _get_dict(row)
        s += _as_int(d.get(key), 0)
    return int(s)


def _fallback_open_limit_bigmove10(payload: Dict[str, Any]) -> Tuple[int, str]:
    """
    For no-limit markets (US/CA/AU/UK/EU), pipeline commonly provides counts in:
      1) stats.open_limit_watchlist_count
      2) sum(sector_summary.bigmove10_cnt)
    """
    stats = _stats_of(payload)
    v = _as_int(stats.get("open_limit_watchlist_count"), 0)
    if v > 0:
        return v, "fallback:stats.open_limit_watchlist_count"

    s = _sum_sector_summary(payload, "bigmove10_cnt")
    if s > 0:
        return s, "fallback:sum(sector_summary.bigmove10_cnt)"

    return 0, "missing"


# =============================================================================
# Market total (universe)
# =============================================================================
def get_market_total(payload: Dict[str, Any]) -> int:
    """
    Universe denominator for footer line1: "Market N".
    Priority:
      1) meta.market_total / meta.universe_total (or stats)
      2) stats.market_total / stats.universe_total
      3) filters.<mkt>_sync.total / success   (for open_limit markets, snapshot_main may be empty)
      4) filters.sync.total / success
      5) stats.snapshot_open_count / len(snapshot_open) (last resort)
      6) len(snapshot_main) fallback
    """
    meta = _get_dict(payload.get("meta"))
    stats = _stats_of(payload)

    for k in ("market_total", "universe_total", "total_symbols", "symbols_total", "n_symbols"):
        if k in meta:
            v = _as_int(meta.get(k), 0)
            if v > 0:
                return v
        if k in stats:
            v = _as_int(stats.get(k), 0)
            if v > 0:
                return v

    # ✅ open_limit / english markets often store total in filters.<mkt>_sync
    f = _filters_of(payload)
    mkt = _market_of(payload).lower()
    if mkt:
        sd = _get_dict(f.get(f"{mkt}_sync"))
        v = _as_int(sd.get("total"), 0)
        if v > 0:
            return v
        v = _as_int(sd.get("success"), 0)
        if v > 0:
            return v

    sd2 = _get_dict(f.get("sync"))
    v = _as_int(sd2.get("total"), 0)
    if v > 0:
        return v
    v = _as_int(sd2.get("success"), 0)
    if v > 0:
        return v

    # last resort: open snapshot counts (not full universe sometimes)
    v = _as_int(stats.get("snapshot_open_count"), 0)
    if v > 0:
        return v

    snap_open = _get_list(payload.get("snapshot_open"))
    if snap_open:
        return int(len(snap_open))

    snap_main = _get_list(payload.get("snapshot_main"))
    return int(len(snap_main))


# Backward-compat: some callers (gain_bins / legacy) still expect this name
def get_market_universe_total(payload: Dict[str, Any]) -> int:
    return int(get_market_total(payload))


# =============================================================================
# Pickers: locked / touched / mix
# =============================================================================
def pick_locked_total(payload: Dict[str, Any]) -> Tuple[int, str]:
    return _pick_key(payload, ("locked_total", "limitup_locked_total"))


def pick_touched_total(payload: Dict[str, Any]) -> Tuple[int, str]:
    """
    Convention:
      touched_total = touch-only (exclude locked)
    (Your TW aggregator is already doing that.)
    """
    v, src = _pick_key(payload, ("touched_total", "touch_only_total", "touched_only_total"))
    if src != "missing":
        return v, src

    # fallback legacy (may include locked) — try not to use
    v2, src2 = _pick_key(payload, ("touch_total", "touched_cnt_total"))
    if src2 != "missing":
        return v2, f"{src2}(legacy)"
    return 0, "missing"


def pick_mix_total(payload: Dict[str, Any]) -> Tuple[int, str]:
    """
    ✅ Mix total used in footer/overview:
    - Default: use mix_total if provided
    - CN special: prefer mix_ex_st_total (exclude ST locked from mix strength)
    - Fallback: locked + touched + big10_ex
    """
    mkt = _market_of(payload)

    # ✅ CN special: prefer mix_ex_st_total (or compatible aliases)
    if mkt == "CN":
        v_cn, src_cn = _pick_key(
            payload,
            (
                "mix_ex_st_total",
                "mix_ex_st_cnt_total",
                "mix_ex_st_exclusive_total",
            ),
        )
        if src_cn != "missing":
            return int(v_cn), src_cn

    # default: normal mix_total
    v, src = _pick_key(payload, ("mix_total",))
    if src != "missing":
        return v, src

    # fallback: locked + touched + big10_ex
    locked, _ = pick_locked_total(payload)
    touched, _ = pick_touched_total(payload)
    big10_ex, _ = pick_bigmove10_ex(payload)
    return int(locked + touched + big10_ex), "fallback.locked+touched+big10_ex"


# =============================================================================
# Pickers: 10%+ (exclusive / inclusive)
# =============================================================================
def _pick_base_big10_ex(payload: Dict[str, Any]) -> Tuple[int, str]:
    """
    Base "exclusive" for main board:
      close>=10% excluding limit-up family (touch/locked etc).
    """
    # preferred
    v, src = _pick_key(payload, ("bigmove10_ex_total", "bigmove10_exclusive_total"))
    if src != "missing":
        return v, src

    # legacy variants
    v2, src2 = _pick_key(
        payload,
        (
            "bigmove10_ex_locked_total",
            "bigmove10_ex_limitup_total",
            "bigmove10_ex_touch_total",
        ),
    )
    if src2 != "missing":
        return v2, src2

    return 0, "missing"


def _pick_open_limit_close_ge10(payload: Dict[str, Any]) -> Tuple[int, str]:
    """
    Open-limit pool close>=10% (興櫃/rotc/etc) — if aggregator provides it.
    """
    return _pick_key(payload, ("open_limit_close_ge10_total",))


def _pick_open_limit_theme_total(payload: Dict[str, Any]) -> Tuple[int, str]:
    """
    open_limit_theme_total = count of open-limit 'theme/watchlist' rows
    (your EMERGING_STRONG_RET pool).
    """
    return _pick_key(payload, ("open_limit_theme_total",))


def pick_bigmove10_ex(payload: Dict[str, Any]) -> Tuple[int, str]:
    """
    ✅ EXCLUSIVE 10%+ for footer / overview buckets.

    Default (non-TW):
      big10_ex = base_ex + open_limit_close_ge10_total (if present)

    TW Rule C:
      big10_ex = base_ex + open_limit_theme_total
        - 把興櫃也算在 10%+ 桶（用你定義的 theme/watchlist pool）
        - 仍排除主板 touch/locked（由 base_ex 保證）

    NO_LIMIT_MARKETS:
      If missing, fallback to stats.open_limit_watchlist_count / sector_summary.bigmove10_cnt
    """
    mkt = _market_of(payload)
    base_ex, base_src = _pick_base_big10_ex(payload)

    # ✅ No-limit markets: they don't have locked/touched semantics; 10%+ often comes from watchlist/sector_summary.
    if mkt in NO_LIMIT_MARKETS:
        if base_src != "missing" and base_ex > 0:
            return int(base_ex), base_src
        fb, fb_src = _fallback_open_limit_bigmove10(payload)
        if fb_src != "missing":
            return int(fb), fb_src
        return int(base_ex), base_src  # likely 0/missing

    if mkt == "TW":
        ol_theme, ol_theme_src = _pick_open_limit_theme_total(payload)
        if ol_theme_src != "missing":
            return int(base_ex + ol_theme), f"{base_src}+{ol_theme_src}"
        return int(base_ex), base_src

    ol_close10, ol_close10_src = _pick_open_limit_close_ge10(payload)
    if ol_close10_src != "missing":
        return int(base_ex + ol_close10), f"{base_src}+{ol_close10_src}"
    return int(base_ex), base_src


def pick_bigmove10_inclusive(payload: Dict[str, Any]) -> Tuple[int, str]:
    """
    ✅ INCLUSIVE 10%+ (gain-bins style union).
    Prefer explicit inclusive keys if provided by aggregator.

    NO_LIMIT_MARKETS:
      If missing, fallback to stats.open_limit_watchlist_count / sector_summary.bigmove10_cnt
      (inclusive == exclusive for these markets in your current definition)
    """
    mkt = _market_of(payload)

    v, src = _pick_key(payload, ("bigmove10_inclusive_total", "bigmove10_union_total"))
    if src != "missing":
        return v, src

    # fallback: if aggregator stored inclusive in bigmove10_total
    v2, src2 = _pick_key(payload, ("bigmove10_total", "bigmove10_ge10_total", "ge10_total"))
    if src2 != "missing":
        return v2, src2

    if mkt in NO_LIMIT_MARKETS:
        fb, fb_src = _fallback_open_limit_bigmove10(payload)
        if fb_src != "missing":
            return int(fb), fb_src

    # final fallback: count snapshot_main ret>=0.10
    rows = payload.get("snapshot_main") or []
    if isinstance(rows, list) and rows:
        c = 0
        for r in rows:
            if not isinstance(r, dict):
                continue
            if _as_float(r.get("ret"), 0.0) >= 0.10:
                c += 1
        return int(c), "fallback.snapshot_main.ret>=0.10"

    return 0, "missing"