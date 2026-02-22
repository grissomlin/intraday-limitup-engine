# scripts/debug/check_jp_mix_counts.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
from pathlib import Path

THIS = Path(__file__).resolve()
REPO_ROOT = THIS.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import argparse
import json
import os
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional, Set

EPS = 1e-6


def _f(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _b(x: Any) -> bool:
    return bool(x) is True


def _s(x: Any) -> str:
    return str(x or "").strip()


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _pick_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("snapshot_all", "snapshot_main", "snapshot_open", "snapshot"):
        rows = payload.get(key)
        if isinstance(rows, list) and rows:
            return [r for r in rows if isinstance(r, dict)]
    return []


def _ymd_effective(payload: Dict[str, Any]) -> str:
    ymd_eff = _s(payload.get("ymd_effective") or payload.get("ymd"))
    if not ymd_eff:
        raise SystemExit("payload has no ymd_effective/ymd")
    return ymd_eff


# -----------------------------------------------------------------------------
# Canonical classification (JSON side)
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class Flags:
    sym: str
    name: str
    sector: str
    ret: float
    is_locked: bool
    is_touch: bool
    is_touch_only: bool
    is_ge10: bool
    is_ge10_ex_touch: bool


def build_flags_from_json(rows: List[Dict[str, Any]]) -> Dict[str, Flags]:
    out: Dict[str, Flags] = {}
    for r in rows:
        sym = _s(r.get("symbol"))
        if not sym:
            continue
        name = _s(r.get("name"))
        sector = _s(r.get("sector") or r.get("industry") or r.get("sector_name"))
        ret = _f(r.get("ret"))
        is_locked = _b(r.get("is_limitup_locked"))
        is_touch = _b(r.get("is_limitup_touch"))
        is_touch_only = is_touch and (not is_locked)

        is_ge10 = ret >= 0.10
        is_ge10_ex_touch = (ret >= 0.10) and (not is_touch)

        out[sym] = Flags(
            sym=sym,
            name=name,
            sector=sector,
            ret=ret,
            is_locked=is_locked,
            is_touch=is_touch,
            is_touch_only=is_touch_only,
            is_ge10=is_ge10,
            is_ge10_ex_touch=is_ge10_ex_touch,
        )
    return out


# -----------------------------------------------------------------------------
# DB helpers: introspection + flexible query (ENHANCED DIAGNOSTICS)
# -----------------------------------------------------------------------------
def _sqlite_master_dump(conn: sqlite3.Connection) -> List[Tuple[str, str]]:
    rows = conn.execute(
        "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
    ).fetchall()
    return [(str(t), str(n)) for t, n in rows]


def _list_tables_or_views(conn: sqlite3.Connection) -> List[str]:
    """
    Return all user tables AND views (some pipelines store data in views).
    """
    rows = conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE (type='table' OR type='view') AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name"
    ).fetchall()
    return [str(r[0]) for r in rows]


def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(r[1]) for r in rows]


def _find_price_table(conn: sqlite3.Connection, db_path: str) -> Tuple[str, Dict[str, str]]:
    """
    Find a table/view that looks like OHLCV daily prices.
    Must have: symbol/date/close. Prefer: high.
    """
    # diagnostics
    try:
        size = os.path.getsize(db_path)
    except Exception:
        size = -1
    print(f"[DB] path={db_path} size={size} bytes")
    master = _sqlite_master_dump(conn)
    print(f"[DB] sqlite_master entries={len(master)}")
    if master:
        # show up to first 50
        for t, n in master[:50]:
            print(f"[DB]   {t}: {n}")
        if len(master) > 50:
            print(f"[DB]   ... +{len(master)-50} more")

    candidates = _list_tables_or_views(conn)
    if not candidates:
        raise SystemExit(
            "DB has no user tables/views. "
            "This file is likely empty, not the expected DB, or not a valid SQLite DB."
        )

    sym_cols = ["symbol", "ticker", "code"]
    date_cols = ["date", "ymd", "trade_date", "trading_date", "dt"]
    close_cols = ["close", "c"]
    high_cols = ["high", "h"]

    def pick(cols: List[str], options: List[str]) -> Optional[str]:
        lower = {c.lower(): c for c in cols}
        for opt in options:
            if opt in lower:
                return lower[opt]
        return None

    scored: List[Tuple[int, str, Dict[str, str]]] = []
    for t in candidates:
        cols = _table_columns(conn, t)
        col_symbol = pick(cols, sym_cols)
        col_date = pick(cols, date_cols)
        col_close = pick(cols, close_cols)
        col_high = pick(cols, high_cols)

        if not (col_symbol and col_date and col_close):
            continue

        score = 0
        tl = t.lower()
        if "price" in tl:
            score += 2
        if "stock" in tl or "ohlcv" in tl or "day" in tl:
            score += 1
        if col_high:
            score += 3

        scored.append(
            (
                score,
                t,
                {"symbol": col_symbol, "date": col_date, "close": col_close, "high": col_high or ""},
            )
        )

    if not scored:
        print("[DB] candidates:", candidates)
        for t in candidates[:30]:
            print(f"[DB] {t} cols:", _table_columns(conn, t))
        raise SystemExit(
            "Could not find a price table/view with required columns (symbol/date/close). "
            "See printed candidates/columns above."
        )

    scored.sort(key=lambda x: x[0], reverse=True)
    _score, table, colmap = scored[0]
    print(f"[DB] using table/view={table} colmap={colmap}")
    return table, colmap


def _get_prev_trade_date(conn: sqlite3.Connection, table: str, col_date: str, col_close: str, ymd: str) -> Optional[str]:
    sql = f"SELECT MAX({col_date}) FROM {table} WHERE {col_date} < ? AND {col_close} IS NOT NULL"
    row = conn.execute(sql, (ymd,)).fetchone()
    if row and row[0]:
        return str(row[0])
    return None


def _fetch_day_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    col_symbol: str,
    col_date: str,
    col_high: str,
    col_close: str,
    ymd: str,
    symbols: List[str],
) -> Dict[str, Dict[str, float]]:
    if not symbols:
        return {}

    out: Dict[str, Dict[str, float]] = {}
    chunk = 800
    high_expr = col_high if col_high else "0.0"

    for i in range(0, len(symbols), chunk):
        sub = symbols[i : i + chunk]
        qs = ",".join(["?"] * len(sub))

        sql = f"""
        WITH p AS (
          SELECT
            {col_symbol} AS symbol,
            {col_date}   AS date,
            {high_expr}  AS high,
            {col_close}  AS close,
            LAG({col_close}) OVER (PARTITION BY {col_symbol} ORDER BY {col_date}) AS last_close
          FROM {table}
          WHERE {col_symbol} IN ({qs}) AND {col_date} <= ?
        )
        SELECT symbol, high, close, last_close
        FROM p
        WHERE date = ?
          AND close IS NOT NULL
        """
        params = list(sub) + [ymd, ymd]
        for sym, high, close, last_close in conn.execute(sql, params).fetchall():
            out[str(sym)] = {"high": _f(high), "close": _f(close), "last_close": _f(last_close)}
    return out


def build_flags_from_db(
    payload: Dict[str, Any], db_path: str, json_flags: Dict[str, Flags]
) -> Tuple[Dict[str, Flags], Dict[str, Any]]:
    from markets.jp.jp_limit_rules import jp_calc_limit  # type: ignore

    ymd = _ymd_effective(payload)
    syms = sorted(json_flags.keys())

    conn = sqlite3.connect(db_path)
    try:
        table, colmap = _find_price_table(conn, db_path)
        prev = _get_prev_trade_date(conn, table, colmap["date"], colmap["close"], ymd)
        day_map = _fetch_day_rows(
            conn,
            table=table,
            col_symbol=colmap["symbol"],
            col_date=colmap["date"],
            col_high=colmap["high"],
            col_close=colmap["close"],
            ymd=ymd,
            symbols=syms,
        )
    finally:
        conn.close()

    out: Dict[str, Flags] = {}
    for sym in syms:
        jf = json_flags[sym]
        info = day_map.get(sym)
        if not info:
            continue

        h = float(info["high"])
        c = float(info["close"])
        lc = float(info["last_close"])

        ret = (c / lc - 1.0) if (lc > 0 and c > 0) else 0.0

        is_touch = False
        is_locked = False
        if lc > 0:
            lp = float(jp_calc_limit(lc).limit_price)
            is_touch = (h > 0) and (h >= lp - EPS)
            is_locked = (c > 0) and (c >= lp - EPS)

        is_touch_only = is_touch and (not is_locked)
        is_ge10 = ret >= 0.10
        is_ge10_ex_touch = (ret >= 0.10) and (not is_touch)

        out[sym] = Flags(
            sym=sym,
            name=jf.name,
            sector=jf.sector,
            ret=ret,
            is_locked=is_locked,
            is_touch=is_touch,
            is_touch_only=is_touch_only,
            is_ge10=is_ge10,
            is_ge10_ex_touch=is_ge10_ex_touch,
        )

    meta = {
        "ymd_effective": ymd,
        "prev_trade_date": prev,
        "n_symbols": len(syms),
        "n_db_rows_found": len(out),
        "n_missing_in_db": len(syms) - len(out),
    }
    return out, meta


# -----------------------------------------------------------------------------
# Sector aggregation + compare (unchanged)
# -----------------------------------------------------------------------------
@dataclass
class SectorCounts:
    locked: Set[str]
    touch_only: Set[str]
    ge10_ex_touch: Set[str]

    @property
    def sum(self) -> int:
        return len(self.locked) + len(self.touch_only) + len(self.ge10_ex_touch)

    @property
    def union(self) -> Set[str]:
        return set().union(self.locked, self.touch_only, self.ge10_ex_touch)

    @property
    def union_n(self) -> int:
        return len(self.union)


def sector_counts(flags_map: Dict[str, Flags]) -> Dict[str, SectorCounts]:
    by = defaultdict(lambda: SectorCounts(set(), set(), set()))
    for sym, f in flags_map.items():
        sec = f.sector or "(no_sector)"
        if f.is_locked:
            by[sec].locked.add(sym)
        if f.is_touch_only:
            by[sec].touch_only.add(sym)
        if f.is_ge10_ex_touch:
            by[sec].ge10_ex_touch.add(sym)
    return dict(by)


def _payload_sector_summary(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    for k in ("sector_summary", "sectors", "sector_stats", "sector_rank", "sector_ranks"):
        v = payload.get(k)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return v
    return []


def _extract_payload_counts(row: Dict[str, Any]) -> Dict[str, int]:
    def _i(x: Any) -> int:
        try:
            return int(x)
        except Exception:
            return 0

    return {
        "locked": _i(row.get("locked_cnt") or row.get("limitup_locked") or row.get("locked")),
        "touch": _i(row.get("touched_cnt") or row.get("limitup_touched") or row.get("touched") or row.get("touch")),
        "bigmove10": _i(
            row.get("bigmove10_cnt") or row.get("bigmove_10_cnt") or row.get("move10_cnt") or row.get("bigmove10")
        ),
        "mix": _i(row.get("mix_cnt") or row.get("all_cnt") or row.get("mix") or row.get("all")),
    }


def _print_sector_report(
    title: str,
    sc: Dict[str, SectorCounts],
    payload: Dict[str, Any],
    *,
    top_k: int,
    show_overlaps: bool,
) -> None:
    print("\n" + "=" * 76)
    print(title)
    print("=" * 76)

    ps = _payload_sector_summary(payload)
    ps_map: Dict[str, Dict[str, int]] = {}
    for r in ps:
        sec = _s(r.get("sector") or r.get("name") or r.get("sector_name") or r.get("industry"))
        if not sec:
            continue
        ps_map[sec] = _extract_payload_counts(r)

    items = sorted(sc.items(), key=lambda kv: kv[1].union_n, reverse=True)
    if top_k > 0:
        items = items[:top_k]

    header = "Sector | locked | touch_only | ge10_ex_touch | SUM | UNION | payload_mix | payload_locked | payload_touch | payload_10%+"
    print(header)
    print("-" * len(header))

    mismatches = []
    for sec, c in items:
        p = ps_map.get(sec, {})
        payload_mix = p.get("mix", 0)
        payload_locked = p.get("locked", 0)
        payload_touch = p.get("touch", 0)
        payload_10 = p.get("bigmove10", 0)

        line = (
            f"{sec} | {len(c.locked):>6} | {len(c.touch_only):>9} | {len(c.ge10_ex_touch):>12} |"
            f" {c.sum:>3} | {c.union_n:>5} | {payload_mix:>10} | {payload_locked:>12} | {payload_touch:>11} | {payload_10:>10}"
        )
        print(line)

        if payload_mix:
            if payload_mix != c.union_n:
                mismatches.append((sec, c, payload_mix))
        else:
            if c.sum != c.union_n:
                mismatches.append((sec, c, None))

    if mismatches:
        print("\n[!] Mismatch / suspicious sectors")
        for sec, c, pm in mismatches[:30]:
            if pm is None:
                print(f"- {sec}: SUM={c.sum} vs UNION={c.union_n}  (likely double-count in overview if using SUM)")
            else:
                print(f"- {sec}: payload_mix={pm} vs UNION={c.union_n}  (payload/overview likely using wrong formula)")

            if show_overlaps:
                both_locked_touch = c.locked & c.touch_only
                locked_ge10 = c.locked & c.ge10_ex_touch
                touch_ge10 = c.touch_only & c.ge10_ex_touch
                if both_locked_touch:
                    print(f"  overlap locked∩touch_only = {len(both_locked_touch)} (should be 0 by definition)")
                if locked_ge10:
                    print(f"  overlap locked∩ge10_ex_touch = {len(locked_ge10)}")
                if touch_ge10:
                    print(f"  overlap touch_only∩ge10_ex_touch = {len(touch_ge10)}")
    else:
        print("\n✅ No obvious sector mismatch found (within checked sectors).")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", required=True, help="path to close.payload.json")
    ap.add_argument("--db", default="", help="optional sqlite db path (jp_stock_warehouse.db)")
    ap.add_argument("--top", type=int, default=30, help="show top K sectors by UNION count (0=all)")
    ap.add_argument("--show-overlaps", action="store_true", help="print overlap diagnostics for mismatched sectors")
    args = ap.parse_args()

    payload = _load_json(args.json)
    rows = _pick_rows(payload)
    if not rows:
        raise SystemExit("payload snapshot rows empty (snapshot_all/main/open/snapshot not found or empty)")

    jf = build_flags_from_json(rows)
    sc_json = sector_counts(jf)

    print("============================================================")
    print("[JSON] overall counts (dedupe-safe)")
    print("============================================================")
    all_locked = {s for s, f in jf.items() if f.is_locked}
    all_touch_only = {s for s, f in jf.items() if f.is_touch_only}
    all_ge10_ex_touch = {s for s, f in jf.items() if f.is_ge10_ex_touch}
    mix_union = set().union(all_locked, all_touch_only, all_ge10_ex_touch)
    mix_sum = len(all_locked) + len(all_touch_only) + len(all_ge10_ex_touch)

    print("rows =", len(rows), "symbols =", len(jf))
    print("locked =", len(all_locked))
    print("touch_only =", len(all_touch_only))
    print("10%+ EXCLUDE stop-high touch =", len(all_ge10_ex_touch))
    print("MIX (SUM)   =", mix_sum, "  <-- if overview uses this, it WILL inflate")
    print("MIX (UNION) =", len(mix_union), "  <-- this should match sector pages logic")

    _print_sector_report(
        "[JSON] sector-level counts (locked / touch_only / 10%+ex_touch) vs payload sector_summary (if any)",
        sc_json,
        payload,
        top_k=args.top,
        show_overlaps=args.show_overlaps,
    )

    if args.db:
        print("\n============================================================")
        print("[DB] recompute touch/locked & ret from DB then sector aggregate")
        print("============================================================")
        df, meta = build_flags_from_db(payload, args.db, jf)
        for k, v in meta.items():
            print(f"{k} = {v}")

    print("\n✅ Done.")


if __name__ == "__main__":
    main()
