# scripts/debug/check_tw_sector_coverage.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

EPS = 1e-9


def _s(x: Any) -> str:
    return str(x or "").strip()


def _f(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# =============================================================================
# Payload pickers
# =============================================================================
def _pick_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Best-effort for sector coverage report (keep original behavior)."""
    for key in ("snapshot_all", "snapshot_main", "snapshot_open", "snapshot"):
        rows = payload.get(key)
        if isinstance(rows, list) and rows:
            return [r for r in rows if isinstance(r, dict)]
    return []


def _pick_rows_standard(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Standard pool rows (上市/上櫃). Prefer snapshot_main."""
    rows = payload.get("snapshot_main")
    if isinstance(rows, list) and rows:
        return [r for r in rows if isinstance(r, dict)]
    # fallback: if not present, try snapshot_all but filter by limit_type/market_detail
    rows2 = payload.get("snapshot_all")
    if isinstance(rows2, list) and rows2:
        out = []
        for r in rows2:
            if not isinstance(r, dict):
                continue
            lt = _s(r.get("limit_type") or "standard").lower()
            md = _s(r.get("market_detail")).lower()
            if lt == "standard" and md not in {"rotc", "emerging", "open_limit"}:
                out.append(r)
        if out:
            return out
    return []


def _pick_rows_open_limit(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Open-limit pool rows (興櫃/開放漲跌幅). Prefer snapshot_open."""
    rows = payload.get("snapshot_open")
    if isinstance(rows, list) and rows:
        return [r for r in rows if isinstance(r, dict)]
    # fallback: some pipelines may store watchlist-style arrays
    wl = payload.get("open_limit_watchlist") or payload.get("emerging_watchlist")
    if isinstance(wl, list) and wl:
        return [r for r in wl if isinstance(r, dict)]
    # last resort: filter snapshot_all by market_detail/limit_type
    rows2 = payload.get("snapshot_all")
    if isinstance(rows2, list) and rows2:
        out = []
        for r in rows2:
            if not isinstance(r, dict):
                continue
            lt = _s(r.get("limit_type") or "").lower()
            md = _s(r.get("market_detail")).lower()
            if md in {"rotc", "emerging", "open_limit"} or (lt and lt != "standard"):
                out.append(r)
        if out:
            return out
    return []


def _ymd_effective(payload: Dict[str, Any]) -> str:
    ymd_eff = _s(payload.get("ymd_effective") or payload.get("ymd"))
    if not ymd_eff:
        raise SystemExit("payload has no ymd_effective/ymd")
    return ymd_eff


def _get_sector_from_row(r: Dict[str, Any]) -> str:
    return _s(r.get("sector") or r.get("industry") or r.get("sector_name"))


def sector_source_stats(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    cnt = {"sector": 0, "industry": 0, "sector_name": 0, "missing": 0}
    for r in rows:
        if _s(r.get("sector")):
            cnt["sector"] += 1
        elif _s(r.get("industry")):
            cnt["industry"] += 1
        elif _s(r.get("sector_name")):
            cnt["sector_name"] += 1
        else:
            cnt["missing"] += 1
    return cnt


def payload_sector_distribution(rows: List[Dict[str, Any]]) -> Tuple[Counter, List[str]]:
    c = Counter()
    missing = []
    for r in rows:
        sym = _s(r.get("symbol"))
        if not sym:
            continue
        sec = _get_sector_from_row(r)
        if not sec:
            missing.append(sym)
            sec = "(missing_sector)"
        c[sec] += 1
    return c, missing


def load_stock_list_map(path: str) -> Dict[str, Dict[str, Any]]:
    obj = _load_json(path)

    if isinstance(obj, list):
        out: Dict[str, Dict[str, Any]] = {}
        for r in obj:
            if not isinstance(r, dict):
                continue
            sym = _s(r.get("symbol") or r.get("ticker") or r.get("code"))
            if sym:
                out[sym] = r
        return out

    if isinstance(obj, dict):
        if isinstance(obj.get("symbols"), list):
            out2: Dict[str, Dict[str, Any]] = {}
            for r in obj["symbols"]:
                if not isinstance(r, dict):
                    continue
                sym = _s(r.get("symbol") or r.get("ticker") or r.get("code"))
                if sym:
                    out2[sym] = r
            return out2

        out3: Dict[str, Dict[str, Any]] = {}
        for k, v in obj.items():
            if isinstance(v, dict):
                out3[_s(k)] = v
        return out3

    return {}


def _get_sector_from_stockmeta(meta: Dict[str, Any]) -> str:
    return _s(
        meta.get("sector")
        or meta.get("industry")
        or meta.get("sector_name")
        or meta.get("industry_name")
        or meta.get("category")
    )


def stocklist_sector_distribution(stock_map: Dict[str, Dict[str, Any]]) -> Tuple[Counter, List[str]]:
    c = Counter()
    missing = []
    for sym, meta in stock_map.items():
        sec = _get_sector_from_stockmeta(meta)
        if not sec:
            missing.append(sym)
            sec = "(missing_sector)"
        c[sec] += 1
    return c, missing


# =============================================================================
# TW limit price approximation (with tick rounding)
# =============================================================================
def tw_tick_size(price: float) -> float:
    """
    Common TWSE/TPEx tick size table (approx).
    If you have an in-repo exact rules module, we can swap this out.
    """
    p = float(price)
    if p < 0:
        return 0.01
    if p < 10:
        return 0.01
    if p < 50:
        return 0.05
    if p < 100:
        return 0.10
    if p < 500:
        return 0.50
    if p < 1000:
        return 1.00
    return 5.00


def floor_to_tick(price: float, tick: float) -> float:
    if tick <= 0:
        return price
    q = int((price + EPS) / tick)
    return q * tick


def tw_calc_limit_up(prev_close: float, up_pct: float = 0.10) -> float:
    """
    Approx limit-up price:
      raw = prev_close * (1 + up_pct)
      limit = floor_to_tick(raw, tick(limit))
    Note: exact TW rules have some edge cases; this is good for diagnostics.
    """
    if prev_close <= 0:
        return 0.0
    raw = prev_close * (1.0 + up_pct)
    tick = tw_tick_size(raw)
    return floor_to_tick(raw, tick)


# =============================================================================
# Use cached prices CSV as "DB"
# =============================================================================
@dataclass(frozen=True)
class PriceRow:
    high: float
    close: float
    last_close: float


def load_prices_csv_as_db(csv_path: str) -> pd.DataFrame:
    p = Path(csv_path)
    if not p.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    cols = {c.lower(): c for c in df.columns}

    def pick(*names: str) -> Optional[str]:
        for n in names:
            if n in cols:
                return cols[n]
        return None

    col_date = pick("date", "ymd", "trade_date", "trading_date", "dt")
    col_sym = pick("ticker", "symbol", "code")
    col_high = pick("high", "h")
    col_close = pick("close", "c")

    if not (col_date and col_sym and col_close):
        raise SystemExit(
            f"CSV missing required cols. Have={list(df.columns)}; need date + symbol/ticker + close"
        )

    ren = {col_date: "date", col_sym: "symbol", col_close: "close"}
    if col_high:
        ren[col_high] = "high"
    df = df.rename(columns=ren)

    df["date"] = df["date"].astype(str)
    df["symbol"] = df["symbol"].astype(str)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    if "high" in df.columns:
        df["high"] = pd.to_numeric(df["high"], errors="coerce")
    else:
        df["high"] = 0.0

    df = df.dropna(subset=["close"])
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    df["last_close"] = df.groupby("symbol")["close"].shift(1)
    return df


def _csv_price_for_day(df: pd.DataFrame, *, ymd: str, sym: str) -> Optional[PriceRow]:
    sub = df[(df["symbol"] == sym) & (df["date"] == ymd)]
    if sub.empty:
        return None
    r0 = sub.iloc[0]
    return PriceRow(
        high=float(r0.get("high", 0.0) or 0.0),
        close=float(r0.get("close", 0.0) or 0.0),
        last_close=float(r0.get("last_close", 0.0) or 0.0),
    )


def fetch_prices_for_day(df: pd.DataFrame, *, ymd: str, symbols: List[str]) -> Dict[str, PriceRow]:
    if not symbols:
        return {}
    sub = df[df["symbol"].isin(symbols) & (df["date"] == ymd)].copy()
    out: Dict[str, PriceRow] = {}
    for _, r in sub.iterrows():
        sym = str(r["symbol"])
        out[sym] = PriceRow(
            high=float(r.get("high", 0.0) or 0.0),
            close=float(r.get("close", 0.0) or 0.0),
            last_close=float(r.get("last_close", 0.0) or 0.0),
        )
    return out


# =============================================================================
# Streak computation (kept)
# =============================================================================
@dataclass(frozen=True)
class StreakResult:
    streak: int
    mode: str  # locked/touch
    is_locked: bool
    is_touch: bool
    limit_price: float


def compute_streak_for_symbol(
    df_sym: pd.DataFrame,
    *,
    ymd: str,
    mode: str = "locked",
    up_pct: float = 0.10,
    max_lookback_days: int = 60,
) -> StreakResult:
    mode = "touch" if mode == "touch" else "locked"

    idx = df_sym.index[df_sym["date"] == ymd]
    if len(idx) == 0:
        return StreakResult(streak=0, mode=mode, is_locked=False, is_touch=False, limit_price=0.0)

    i = int(idx[0])
    streak = 0

    def flags_at(row: pd.Series) -> Tuple[bool, bool, float]:
        lc = float(row.get("last_close") or 0.0)
        h = float(row.get("high") or 0.0)
        c = float(row.get("close") or 0.0)
        lp = tw_calc_limit_up(lc, up_pct=up_pct) if lc > 0 else 0.0
        is_touch = (lp > 0) and (h >= lp - EPS)
        is_locked = (lp > 0) and (c >= lp - EPS)
        return is_locked, is_touch, lp

    is_locked_last, is_touch_last, lp_last = flags_at(df_sym.iloc[i])

    steps = 0
    j = i
    while j >= 0 and steps < max_lookback_days:
        row = df_sym.iloc[j]
        is_locked, is_touch, _lp = flags_at(row)
        ok = is_touch if mode == "touch" else is_locked
        if ok:
            streak += 1
            j -= 1
            steps += 1
            continue
        break

    return StreakResult(
        streak=streak,
        mode=mode,
        is_locked=is_locked_last,
        is_touch=is_touch_last,
        limit_price=lp_last,
    )


# =============================================================================
# Reports
# =============================================================================
def print_top_counter(title: str, c: Counter, top: int) -> None:
    print("\n" + title)
    for sec, n in c.most_common(top):
        print(f"  {sec:>28}  {n}")


def print_sector_coverage_report(*, rows: List[Dict[str, Any]], stock_list_path: str = "", top: int = 30) -> None:
    print("\n" + "=" * 88)
    print("[SECTOR] coverage & distribution (payload / stock list)")
    print("=" * 88)

    src = sector_source_stats(rows)
    print("[payload] sector field source:", src)

    p_dist, p_missing = payload_sector_distribution(rows)
    n_syms_payload = sum(p_dist.values())
    n_sectors_payload = len([k for k in p_dist.keys() if k != "(missing_sector)"])
    print(f"[payload] symbols={n_syms_payload} unique_sectors={n_sectors_payload} missing_sector={len(p_missing)}")
    if p_missing:
        print("  sample missing symbols:", p_missing[:30])

    print_top_counter("[payload] top sectors by #companies:", p_dist, top)

    if stock_list_path:
        smap = load_stock_list_map(stock_list_path)
        if not smap:
            print(f"\n[stocklist] could not parse: {stock_list_path}")
            return

        s_dist, s_missing = stocklist_sector_distribution(smap)
        n_syms_stock = sum(s_dist.values())
        n_sectors_stock = len([k for k in s_dist.keys() if k != "(missing_sector)"])
        print("\n" + "-" * 88)
        print(f"[stocklist] symbols={n_syms_stock} unique_sectors={n_sectors_stock} missing_sector={len(s_missing)}")
        if s_missing:
            print("  sample missing symbols:", s_missing[:30])

        print_top_counter("[stocklist] top sectors by #companies:", s_dist, top)

        p_keys = set(p_dist.keys())
        s_keys = set(s_dist.keys())
        only_in_payload = sorted(p_keys - s_keys)
        only_in_stock = sorted(s_keys - p_keys)

        print("\n[compare] sectors only in payload (sample):", only_in_payload[:50], f"(n={len(only_in_payload)})")
        print("[compare] sectors only in stocklist (sample):", only_in_stock[:50], f"(n={len(only_in_stock)})")


# =============================================================================
# Existing: CSV-based "footer-style 10%+ excluding limit/touch" report (kept)
# =============================================================================
def _is_open_limit_board(r: Dict[str, Any]) -> bool:
    md = _s(r.get("market_detail")).lower()
    return md in {"rotc", "emerging", "open_limit"}


def _board_name(r: Dict[str, Any]) -> str:
    md = _s(r.get("market_detail")).lower()
    if md in {"rotc", "emerging", "open_limit"}:
        return "OPEN_LIMIT"
    if md:
        return md.upper()
    return "STANDARD"


def _ret_from_prices(pr: PriceRow) -> float:
    if pr.last_close <= 0:
        return 0.0
    return (pr.close / pr.last_close) - 1.0


def print_ge10_nonlimit_report(
    *,
    payload_rows: List[Dict[str, Any]],
    ymd: str,
    prices_df: pd.DataFrame,
    top: int,
    up_pct: float,
    include_open_limit: bool,
    show_symbols: int,
) -> None:
    """
    List >=10% movers excluding (locked/touched) for STANDARD board.
    Also optionally list >=10% movers for OPEN_LIMIT board separately.
    """
    row_by_sym: Dict[str, Dict[str, Any]] = {}
    symbols: List[str] = []
    for r in payload_rows:
        sym = _s(r.get("symbol"))
        if not sym:
            continue
        symbols.append(sym)
        if sym not in row_by_sym:
            row_by_sym[sym] = r

    uniq_syms = sorted(set(symbols))
    g = prices_df[prices_df["symbol"].isin(uniq_syms)].groupby("symbol", sort=False)

    std_ge10 = []
    std_ge10_ex = []
    std_locked = []
    std_touched = []
    open_ge10 = []

    missing_price = 0

    for sym in uniq_syms:
        r = row_by_sym.get(sym) or {}
        if sym not in g.groups:
            missing_price += 1
            continue

        df_sym = g.get_group(sym).sort_values("date").reset_index(drop=True)
        idx = df_sym.index[df_sym["date"] == ymd]
        if len(idx) == 0:
            continue

        pr = PriceRow(
            high=float(df_sym.loc[idx[0], "high"] or 0.0),
            close=float(df_sym.loc[idx[0], "close"] or 0.0),
            last_close=float(df_sym.loc[idx[0], "last_close"] or 0.0),
        )
        if pr.last_close <= 0:
            continue

        ret = _ret_from_prices(pr)
        if ret < 0.10 - EPS:
            continue

        sector = _get_sector_from_row(r) or "(missing_sector)"
        board = _board_name(r)

        if include_open_limit and _is_open_limit_board(r):
            open_ge10.append((ret, sym, sector, board, pr))
            continue

        limit_price = tw_calc_limit_up(pr.last_close, up_pct=up_pct)
        is_touch = (limit_price > 0) and (pr.high >= limit_price - EPS)
        is_locked = (limit_price > 0) and (pr.close >= limit_price - EPS)

        std_ge10.append((ret, sym, sector, board, pr, limit_price, is_locked, is_touch))

        if is_locked:
            std_locked.append((ret, sym, sector, board, pr, limit_price))
        elif is_touch:
            std_touched.append((ret, sym, sector, board, pr, limit_price))
        else:
            std_ge10_ex.append((ret, sym, sector, board, pr, limit_price))

    std_ge10.sort(reverse=True, key=lambda x: x[0])
    std_ge10_ex.sort(reverse=True, key=lambda x: x[0])
    std_locked.sort(reverse=True, key=lambda x: x[0])
    std_touched.sort(reverse=True, key=lambda x: x[0])
    open_ge10.sort(reverse=True, key=lambda x: x[0])

    sec_ex = Counter([x[2] for x in std_ge10_ex])
    sec_open = Counter([x[2] for x in open_ge10])

    print("\n" + "=" * 88)
    print("[GE10 NON-LIMIT] verify footer-style 10%+ (exclude limit-up/touch) for TW")
    print("=" * 88)
    print(f"ymd_effective = {ymd}")
    print(f"payload_symbols = {len(uniq_syms)}")
    print(f"missing_price_in_csv = {missing_price}")
    print(f"up_pct = {up_pct:.2%}")

    print("\n[STANDARD] >=10% totals:")
    print(f"  ge10_total                   : {len(std_ge10)}")
    print(f"  ge10_excluding_locked&touched: {len(std_ge10_ex)}   <-- bigmove10_ex_* expectation")
    print(f"  locked(limit-up close)       : {len(std_locked)}")
    print(f"  touched(limit-up high)       : {len(std_touched)}")

    print_top_counter("\n[STANDARD ge10_ex] top sectors:", sec_ex, top)

    if show_symbols > 0:
        print("\n[STANDARD ge10_ex] symbols (ret, symbol, sector, last_close, close, high, limit_price):")
        for ret, sym, sector, board, pr, lp in std_ge10_ex[:show_symbols]:
            print(
                f"  {ret*100:>6.2f}%  {sym:<12}  {sector}  "
                f"lc={pr.last_close:.4g} c={pr.close:.4g} h={pr.high:.4g}  lp~{lp:.4g}"
            )

    if include_open_limit:
        print("\n" + "-" * 88)
        print("[OPEN_LIMIT] >=10% movers (no limit-up exclusion, for emerging/open-limit pool)")
        print("-" * 88)
        print(f"  ge10_total : {len(open_ge10)}")
        print_top_counter("\n[OPEN_LIMIT ge10] top sectors:", sec_open, top)

        if show_symbols > 0:
            print("\n[OPEN_LIMIT ge10] symbols (ret, symbol, sector, last_close, close, high):")
            for ret, sym, sector, board, pr in open_ge10[:show_symbols]:
                print(
                    f"  {ret*100:>6.2f}%  {sym:<12}  {sector}  "
                    f"lc={pr.last_close:.4g} c={pr.close:.4g} h={pr.high:.4g}"
                )


# =============================================================================
# ✅ NEW: JSON vs CSV 四宮格驗證 + OPEN_LIMIT ≥10% 列表 + 差異清單
# =============================================================================
def _bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if x is None:
        return False
    s = str(x).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _payload_ret(r: Dict[str, Any]) -> float:
    """
    Prefer r['ret'] (ratio like 0.123). Fallback to r['ret_pct']/100.
    """
    if r.get("ret") is not None:
        try:
            return float(r["ret"])
        except Exception:
            pass
    if r.get("ret_pct") is not None:
        try:
            return float(r["ret_pct"]) / 100.0
        except Exception:
            pass
    return 0.0


def _payload_is_limit_touch(r: Dict[str, Any]) -> bool:
    # best-effort flags used across markets
    if _bool(r.get("is_limitup_touch")):
        return True
    if _bool(r.get("is_touch_only")):
        return True
    if _bool(r.get("is_surge10_touch")):
        return True
    return False


def _payload_is_limit_locked(r: Dict[str, Any]) -> bool:
    if _bool(r.get("is_limitup_locked")):
        return True
    if _bool(r.get("is_true_limitup")):
        return True
    if _bool(r.get("is_surge10_locked")):
        return True
    return False


@dataclass(frozen=True)
class Ge10Item:
    sym: str
    sector: str
    ret: float  # ratio


def _collect_ge10_from_json(rows: List[Dict[str, Any]], *, ge10: float) -> Dict[str, Ge10Item]:
    out: Dict[str, Ge10Item] = {}
    for r in rows:
        sym = _s(r.get("symbol"))
        if not sym:
            continue
        ret = _payload_ret(r)
        if ret >= ge10 - EPS:
            out[sym] = Ge10Item(sym=sym, sector=_get_sector_from_row(r) or "(missing_sector)", ret=ret)
    return out


def _collect_ge10_from_csv(
    df: pd.DataFrame,
    symbols: List[str],
    *,
    ymd: str,
    ge10: float,
    sector_map: Dict[str, str],
) -> Tuple[Dict[str, Ge10Item], List[str]]:
    out: Dict[str, Ge10Item] = {}
    missing: List[str] = []
    for sym in sorted(set(symbols)):
        pr = _csv_price_for_day(df, ymd=ymd, sym=sym)
        if pr is None or pr.last_close <= 0:
            missing.append(sym)
            continue
        ret = _ret_from_prices(pr)
        if ret >= ge10 - EPS:
            out[sym] = Ge10Item(sym=sym, sector=sector_map.get(sym, "(missing_sector)"), ret=ret)
    return out, missing


def _collect_std_touch_locked_from_csv(
    df: pd.DataFrame,
    symbols: List[str],
    *,
    ymd: str,
    up_pct: float,
) -> Tuple[set, set, List[str]]:
    touched: set = set()
    locked: set = set()
    missing: List[str] = []

    for sym in sorted(set(symbols)):
        pr = _csv_price_for_day(df, ymd=ymd, sym=sym)
        if pr is None or pr.last_close <= 0:
            missing.append(sym)
            continue
        lp = tw_calc_limit_up(pr.last_close, up_pct=up_pct)
        if lp <= 0:
            continue
        if pr.high >= lp - EPS:
            touched.add(sym)
        if pr.close >= lp - EPS:
            locked.add(sym)
    return touched, locked, missing


def _print_items(title: str, items: Dict[str, Ge10Item], *, max_n: int) -> None:
    print("\n" + title)
    if not items:
        print("  (empty)")
        return
    for i, it in enumerate(sorted(items.values(), key=lambda x: x.ret, reverse=True)):
        if max_n > 0 and i >= max_n:
            break
        print(f"  {it.ret*100:>6.2f}%  {it.sym:<12}  {it.sector}")
    if max_n > 0 and len(items) > max_n:
        print(f"  ... ({len(items) - max_n} more)")


def _print_sym_list(title: str, syms: List[str], *, max_n: int) -> None:
    print("\n" + title)
    if not syms:
        print("  (none)")
        return
    show = syms if max_n <= 0 else syms[:max_n]
    for s in show:
        print(f"  {s}")
    if max_n > 0 and len(syms) > max_n:
        print(f"  ... ({len(syms) - max_n} more)")


def print_ge10_json_csv_verify(
    *,
    payload_std: List[Dict[str, Any]],
    payload_open: List[Dict[str, Any]],
    ymd: str,
    prices_df: pd.DataFrame,
    ge10: float,
    up_pct: float,
    max_symbols: int,
) -> None:
    std_syms = [ _s(r.get("symbol")) for r in payload_std if _s(r.get("symbol")) ]
    open_syms = [ _s(r.get("symbol")) for r in payload_open if _s(r.get("symbol")) ]

    std_sec_map = { _s(r.get("symbol")): (_get_sector_from_row(r) or "(missing_sector)")
                    for r in payload_std if _s(r.get("symbol")) }
    open_sec_map = { _s(r.get("symbol")): (_get_sector_from_row(r) or "(missing_sector)")
                     for r in payload_open if _s(r.get("symbol")) }

    # JSON sets
    json_std_ge10 = _collect_ge10_from_json(payload_std, ge10=ge10)
    json_open_ge10 = _collect_ge10_from_json(payload_open, ge10=ge10)

    # JSON std exclude touched/locked by payload flags (best-effort)
    json_std_excluded = set()
    for r in payload_std:
        sym = _s(r.get("symbol"))
        if not sym:
            continue
        if _payload_is_limit_touch(r) or _payload_is_limit_locked(r):
            json_std_excluded.add(sym)
    json_std_ge10_ex = {k: v for k, v in json_std_ge10.items() if k not in json_std_excluded}

    # CSV sets
    csv_std_ge10, miss_std = _collect_ge10_from_csv(
        prices_df, std_syms, ymd=ymd, ge10=ge10, sector_map=std_sec_map
    )
    csv_open_ge10, miss_open = _collect_ge10_from_csv(
        prices_df, open_syms, ymd=ymd, ge10=ge10, sector_map=open_sec_map
    )

    # CSV std exclude by computed touch/locked
    csv_std_touched, csv_std_locked, miss_std2 = _collect_std_touch_locked_from_csv(
        prices_df, std_syms, ymd=ymd, up_pct=up_pct
    )
    csv_std_ge10_ex = {k: v for k, v in csv_std_ge10.items() if k not in csv_std_touched}

    miss_std_all = sorted(set(miss_std) | set(miss_std2))
    miss_open_all = sorted(set(miss_open))

    # diffs
    only_json_open = sorted(set(json_open_ge10.keys()) - set(csv_open_ge10.keys()))
    only_csv_open = sorted(set(csv_open_ge10.keys()) - set(json_open_ge10.keys()))

    only_json_std = sorted(set(json_std_ge10.keys()) - set(csv_std_ge10.keys()))
    only_csv_std = sorted(set(csv_std_ge10.keys()) - set(json_std_ge10.keys()))

    only_json_std_ex = sorted(set(json_std_ge10_ex.keys()) - set(csv_std_ge10_ex.keys()))
    only_csv_std_ex = sorted(set(csv_std_ge10_ex.keys()) - set(json_std_ge10_ex.keys()))

    print("\n" + "=" * 88)
    print("[VERIFY] JSON vs CSV 四宮格驗證（STD / OPEN_LIMIT 的 ≥10%）")
    print("=" * 88)
    print(f"ymd_effective = {ymd}")
    print(f"ge10_threshold = {ge10:.2%}")
    print(f"std_up_pct(for limit calc) = {up_pct:.2%}")
    print(f"payload_std_symbols  = {len(set(std_syms))}")
    print(f"payload_open_symbols = {len(set(open_syms))}")

    print("\n[FOUR-GRID] totals")
    print(f"  STANDARD  JSON ge10_total = {len(json_std_ge10)}")
    print(f"  STANDARD  CSV  ge10_total = {len(csv_std_ge10)}")
    print(f"  OPEN_LIMIT JSON ge10_total = {len(json_open_ge10)}")
    print(f"  OPEN_LIMIT CSV  ge10_total = {len(csv_open_ge10)}")

    print("\n[STANDARD] footer-style exclude (排除漲停/觸及)")
    print(f"  STANDARD JSON ge10_ex = {len(json_std_ge10_ex)}  (best-effort by payload flags)")
    print(f"  STANDARD CSV  ge10_ex = {len(csv_std_ge10_ex)}  (by ticked limit-price touch)")

    if miss_std_all:
        print(f"\n[CSV missing] STANDARD missing ymd/last_close rows (n={len(miss_std_all)})")
        _print_sym_list("  sample:", miss_std_all, max_n=min(max_symbols, 60))
    if miss_open_all:
        print(f"\n[CSV missing] OPEN_LIMIT missing ymd/last_close rows (n={len(miss_open_all)})")
        _print_sym_list("  sample:", miss_open_all, max_n=min(max_symbols, 60))

    # requested lists: OPEN_LIMIT JSON vs CSV
    _print_items(f"[OPEN_LIMIT] ≥10% LIST (JSON) n={len(json_open_ge10)}", json_open_ge10, max_n=max_symbols)
    _print_items(f"[OPEN_LIMIT] ≥10% LIST (CSV ) n={len(csv_open_ge10)}", csv_open_ge10, max_n=max_symbols)

    # diffs
    print("\n" + "-" * 88)
    print("[DIFF] 只在 JSON / 只在 CSV（讓你一眼定位）")
    print("-" * 88)

    if only_json_open:
        _print_sym_list(f"[OPEN_LIMIT ge10] only in JSON (n={len(only_json_open)})", only_json_open, max_n=max_symbols)
    if only_csv_open:
        _print_sym_list(f"[OPEN_LIMIT ge10] only in CSV  (n={len(only_csv_open)})", only_csv_open, max_n=max_symbols)
    if not only_json_open and not only_csv_open:
        print("\n[OPEN_LIMIT ge10] JSON vs CSV sets match ✅")

    if only_json_std:
        _print_sym_list(f"[STANDARD ge10] only in JSON (n={len(only_json_std)})", only_json_std, max_n=max_symbols)
    if only_csv_std:
        _print_sym_list(f"[STANDARD ge10] only in CSV  (n={len(only_csv_std)})", only_csv_std, max_n=max_symbols)
    if not only_json_std and not only_csv_std:
        print("\n[STANDARD ge10] JSON vs CSV sets match ✅")

    if only_json_std_ex:
        _print_sym_list(f"[STANDARD ge10_ex] only in JSON (n={len(only_json_std_ex)})", only_json_std_ex, max_n=max_symbols)
    if only_csv_std_ex:
        _print_sym_list(f"[STANDARD ge10_ex] only in CSV  (n={len(only_csv_std_ex)})", only_csv_std_ex, max_n=max_symbols)
    if not only_json_std_ex and not only_csv_std_ex:
        print("\n[STANDARD ge10_ex] JSON vs CSV sets match ✅")


# =============================================================================
# Streak report (kept)
# =============================================================================
def print_streak_report(
    *,
    payload_rows: List[Dict[str, Any]],
    ymd: str,
    prices_df: pd.DataFrame,
    top: int,
    streak_mode: str,
    up_pct: float,
    max_lookback_days: int,
    min_streak_show: int,
) -> None:
    sec_by_sym: Dict[str, str] = {}
    syms = []
    for r in payload_rows:
        sym = _s(r.get("symbol"))
        if not sym:
            continue
        syms.append(sym)
        sec = _get_sector_from_row(r) or "(missing_sector)"
        sec_by_sym[sym] = sec

    uniq_syms = sorted(set(syms))
    g = prices_df[prices_df["symbol"].isin(uniq_syms)].groupby("symbol", sort=False)

    streak_by_sym: Dict[str, int] = {}
    sector_counter_ge2 = Counter()
    sector_counter_ge3 = Counter()
    sector_counter_ge4 = Counter()
    sector_counter_ge5 = Counter()

    hist = Counter()
    details: List[Tuple[int, str, str]] = []

    missing_price = 0

    for sym in uniq_syms:
        if sym not in g.groups:
            missing_price += 1
            continue
        df_sym = g.get_group(sym).sort_values("date").reset_index(drop=True)
        r = compute_streak_for_symbol(
            df_sym,
            ymd=ymd,
            mode=streak_mode,
            up_pct=up_pct,
            max_lookback_days=max_lookback_days,
        )
        st = int(r.streak)
        streak_by_sym[sym] = st
        if st > 0:
            hist[st] += 1
        sec = sec_by_sym.get(sym, "(missing_sector)")
        if st >= 2:
            sector_counter_ge2[sec] += 1
        if st >= 3:
            sector_counter_ge3[sec] += 1
        if st >= 4:
            sector_counter_ge4[sec] += 1
        if st >= 5:
            sector_counter_ge5[sec] += 1
        if st >= min_streak_show:
            details.append((st, sym, sec))

    details.sort(reverse=True, key=lambda x: x[0])

    print("\n" + "=" * 88)
    print(f"[STREAK] consecutive limit-up streaks (mode={streak_mode}, up_pct={up_pct:.2%})")
    print("=" * 88)
    print(f"ymd_effective = {ymd}")
    print(f"payload_symbols = {len(uniq_syms)}")
    print(f"missing_price_in_csv = {missing_price}")

    if hist:
        max_st = max(hist.keys())
        show_to = min(max_st, 10)
        print("\n[STREAK] histogram (streak_len -> #symbols):")
        for k in range(1, show_to + 1):
            if hist.get(k, 0):
                print(f"  {k:>2} -> {hist[k]}")
        if max_st > show_to:
            print(f"  ... max_streak={max_st}")

    ge2 = sum(1 for v in streak_by_sym.values() if v >= 2)
    ge3 = sum(1 for v in streak_by_sym.values() if v >= 3)
    ge4 = sum(1 for v in streak_by_sym.values() if v >= 4)
    ge5 = sum(1 for v in streak_by_sym.values() if v >= 5)
    print("\n[STREAK] totals:")
    print(f"  streak>=2 : {ge2}")
    print(f"  streak>=3 : {ge3}")
    print(f"  streak>=4 : {ge4}")
    print(f"  streak>=5 : {ge5}")

    print_top_counter("\n[STREAK>=2] top sectors:", sector_counter_ge2, top)
    if ge3:
        print_top_counter("\n[STREAK>=3] top sectors:", sector_counter_ge3, top)
    if ge4:
        print_top_counter("\n[STREAK>=4] top sectors:", sector_counter_ge4, top)
    if ge5:
        print_top_counter("\n[STREAK>=5] top sectors:", sector_counter_ge5, top)

    print("\n[STREAK] top symbols (streak, symbol, sector):")
    for st, sym, sec in details[:top]:
        print(f"  {st:>2}  {sym:<12}  {sec}")


# =============================================================================
# Main
# =============================================================================
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", required=True, help="path to close.payload.json")
    ap.add_argument("--stock-list", default="", help="optional path to tw_stock_list.json for sector compare")
    ap.add_argument("--csv", default="", help="optional path to cached TW prices CSV (use as DB)")
    ap.add_argument("--top", type=int, default=30, help="show top K sectors/symbols (default=30)")

    # streak options
    ap.add_argument("--streak", action="store_true", help="enable streak report (requires --csv)")
    ap.add_argument(
        "--streak-mode",
        choices=["locked", "touch"],
        default="locked",
        help="locked: close at limit-up; touch: high touched limit-up",
    )
    ap.add_argument("--streak-up-pct", type=float, default=0.10, help="limit-up percent (default 0.10)")
    ap.add_argument("--streak-lookback", type=int, default=60, help="max lookback trading days per symbol")
    ap.add_argument("--streak-min-show", type=int, default=2, help="only list symbols with streak >= this")

    # ge10 non-limit report (CSV-only logic)
    ap.add_argument(
        "--ge10-nonlimit",
        action="store_true",
        help="list >=10% movers excluding limit-up/touch for STANDARD; and OPEN_LIMIT separately (requires --csv)",
    )
    ap.add_argument("--ge10-up-pct", type=float, default=0.10, help="limit-up percent for exclusion calc (default 0.10)")
    ap.add_argument(
        "--ge10-include-openlimit",
        action="store_true",
        default=True,
        help="also list OPEN_LIMIT/興櫃 pool >=10% separately (default ON)",
    )
    ap.add_argument(
        "--ge10-max-symbols",
        type=int,
        default=80,
        help="max symbols to print per section (default 80; 0 to suppress listing)",
    )

    # ✅ NEW: JSON vs CSV verify
    ap.add_argument(
        "--verify-ge10",
        action="store_true",
        help="JSON vs CSV 四宮格驗證：STD/OPEN 的 10%+ totals + STD ge10_ex + 差異清單（requires --csv）",
    )
    ap.add_argument("--verify-th", type=float, default=0.10, help="threshold for verify-ge10 (default 0.10)")
    ap.add_argument(
        "--verify-max-symbols",
        type=int,
        default=120,
        help="max symbols to print in verify lists/diffs (default 120; 0 to suppress)",
    )

    args = ap.parse_args()

    payload = _load_json(args.json)
    if not isinstance(payload, dict):
        raise SystemExit("payload json root must be an object/dict")

    ymd = _ymd_effective(payload)

    rows_any = _pick_rows(payload)
    if not rows_any:
        raise SystemExit("payload snapshot rows empty (snapshot_all/main/open/snapshot not found or empty)")

    payload_std = _pick_rows_standard(payload)
    payload_open = _pick_rows_open_limit(payload)

    print("============================================================")
    print("[INPUT]")
    print("============================================================")
    print("json =", args.json)
    print("ymd_effective =", ymd)
    print("snapshot_rows(any) =", len(rows_any))
    print("snapshot_rows(standard) =", len(payload_std))
    print("snapshot_rows(open_limit) =", len(payload_open))

    # sector coverage report stays based on "any"
    print_sector_coverage_report(rows=rows_any, stock_list_path=args.stock_list, top=args.top)

    prices_df: Optional[pd.DataFrame] = None
    if args.ge10_nonlimit or args.streak or args.verify_ge10:
        if not args.csv:
            raise SystemExit("--ge10-nonlimit / --streak / --verify-ge10 requires --csv")
        prices_df = load_prices_csv_as_db(args.csv)

        print("\n" + "=" * 88)
        print("[CSV-as-DB] loaded")
        print("=" * 88)
        print("csv =", args.csv)
        print("rows =", len(prices_df))
        print("symbols =", int(prices_df["symbol"].nunique()))
        print("date range =", prices_df["date"].min(), "~", prices_df["date"].max())

    if args.verify_ge10:
        assert prices_df is not None
        print_ge10_json_csv_verify(
            payload_std=payload_std,
            payload_open=payload_open,
            ymd=ymd,
            prices_df=prices_df,
            ge10=float(args.verify_th),
            up_pct=float(args.ge10_up_pct),
            max_symbols=max(0, int(args.verify_max_symbols)),
        )

    if args.ge10_nonlimit:
        assert prices_df is not None
        print_ge10_nonlimit_report(
            payload_rows=rows_any,
            ymd=ymd,
            prices_df=prices_df,
            top=args.top,
            up_pct=float(args.ge10_up_pct),
            include_open_limit=bool(args.ge10_include_openlimit),
            show_symbols=max(0, int(args.ge10_max_symbols)),
        )

    if args.streak:
        assert prices_df is not None
        print_streak_report(
            payload_rows=rows_any,
            ymd=ymd,
            prices_df=prices_df,
            top=args.top,
            streak_mode=args.streak_mode,
            up_pct=float(args.streak_up_pct),
            max_lookback_days=int(args.streak_lookback),
            min_streak_show=int(args.streak_min_show),
        )

    print("\n✅ Done.")


if __name__ == "__main__":
    main()
