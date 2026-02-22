# scripts/debug/check_kr_db_vs_json.py
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
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

EPS = 1e-6


def _f(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _i(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _b(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if x is None:
        return False
    s = str(x).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


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
# JSON side row
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class JRow:
    sym: str
    name: str
    sector: str

    ret_json: float

    prev_close_json: float
    open_json: float
    close_json: float
    high_json: float
    low_json: float

    is_touch_json: bool
    is_locked_json: bool
    is_bigup10_json: bool

    limit_rate_json: float
    limit_price_json: float


def build_from_json(rows: List[Dict[str, Any]]) -> Dict[str, JRow]:
    out: Dict[str, JRow] = {}
    for r in rows:
        sym = _s(r.get("symbol"))
        if not sym:
            continue

        # ✅ KR keys (support both new/old)
        is_touch = _b(r.get("is_limitup30_touch")) or _b(r.get("is_limitup_touch"))
        is_locked = _b(r.get("is_limitup30_locked")) or _b(r.get("is_limitup_locked"))
        is_bigup10 = _b(r.get("is_bigup10")) or _b(r.get("is_bigup"))  # 10%+

        out[sym] = JRow(
            sym=sym,
            name=_s(r.get("name")),
            sector=_s(r.get("sector") or r.get("industry") or r.get("sector_name") or "UNKNOWN"),
            ret_json=_f(r.get("ret")),
            prev_close_json=_f(r.get("prev_close") or r.get("last_close")),
            open_json=_f(r.get("open")),
            close_json=_f(r.get("close")),
            high_json=_f(r.get("high")),
            low_json=_f(r.get("low")),
            is_touch_json=is_touch,
            is_locked_json=is_locked,
            is_bigup10_json=is_bigup10,
            limit_rate_json=_f(r.get("limit_rate")),
            limit_price_json=_f(r.get("limit_price")),
        )
    return out


# -----------------------------------------------------------------------------
# DB helpers (auto-detect price table)
# -----------------------------------------------------------------------------
def _sqlite_master_dump(conn: sqlite3.Connection) -> List[Tuple[str, str]]:
    rows = conn.execute(
        "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
    ).fetchall()
    return [(str(t), str(n)) for t, n in rows]


def _list_tables_or_views(conn: sqlite3.Connection) -> List[str]:
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
    try:
        size = os.path.getsize(db_path)
    except Exception:
        size = -1
    print(f"[DB] path={db_path} size={size} bytes")

    master = _sqlite_master_dump(conn)
    print(f"[DB] sqlite_master entries={len(master)}")
    for t, n in master[:50]:
        print(f"[DB]   {t}: {n}")
    if len(master) > 50:
        print(f"[DB]   ... +{len(master)-50} more")

    candidates = _list_tables_or_views(conn)
    if not candidates:
        raise SystemExit("DB has no user tables/views (empty or wrong DB).")

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
        raise SystemExit("Could not find a price table/view with (symbol/date/close).")

    scored.sort(key=lambda x: x[0], reverse=True)
    _score, table, colmap = scored[0]
    print(f"[DB] using table/view={table} colmap={colmap}")
    return table, colmap


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


# -----------------------------------------------------------------------------
# NEW: compute sector_summary needed fields from snapshot_main
# -----------------------------------------------------------------------------
@dataclass
class SectorAgg:
    sector: str
    sector_total: int = 0
    locked_cnt: int = 0
    touched_cnt: int = 0          # touch-only
    bigmove10_cnt: int = 0        # pure 10%+ (exclude touch/locked)
    mix_cnt: int = 0

    locked_pct: Optional[float] = None
    touched_pct: Optional[float] = None
    bigmove10_pct: Optional[float] = None
    mix_pct: Optional[float] = None

    # debug
    has_pct_field: bool = False
    pct_field_value: Optional[float] = None


def compute_sector_summary_from_snapshot(rows: List[Dict[str, Any]]) -> List[SectorAgg]:
    m: Dict[str, SectorAgg] = {}

    for r in rows:
        sec = _s(r.get("sector") or r.get("industry") or r.get("sector_name") or "UNKNOWN")
        if not sec:
            sec = "UNKNOWN"
        a = m.get(sec)
        if a is None:
            a = SectorAgg(sector=sec)
            m[sec] = a

        a.sector_total += 1

        locked = _b(r.get("is_limitup30_locked")) or _b(r.get("is_limitup_locked"))
        touch = _b(r.get("is_limitup30_touch")) or _b(r.get("is_limitup_touch"))
        big10 = _b(r.get("is_bigup10")) or _b(r.get("is_bigup")) or (_f(r.get("ret")) >= 0.10)

        touch_only = touch and (not locked)
        pure_big10 = big10 and (not touch) and (not locked)

        if locked:
            a.locked_cnt += 1
        if touch_only:
            a.touched_cnt += 1
        if pure_big10:
            a.bigmove10_cnt += 1

    out: List[SectorAgg] = []
    for a in m.values():
        a.mix_cnt = a.locked_cnt + a.touched_cnt + a.bigmove10_cnt
        denom = float(a.sector_total) if a.sector_total > 0 else 0.0
        if denom > 0:
            a.locked_pct = a.locked_cnt / denom
            a.touched_pct = a.touched_cnt / denom
            a.bigmove10_pct = a.bigmove10_cnt / denom
            a.mix_pct = a.mix_cnt / denom
        out.append(a)

    out.sort(key=lambda x: (x.mix_cnt, x.locked_cnt, x.touched_cnt, x.bigmove10_cnt), reverse=True)
    return out


def summarize_sector_summary_payload(sector_summary: Any) -> List[Dict[str, Any]]:
    if not isinstance(sector_summary, list):
        return []
    out: List[Dict[str, Any]] = []
    for r in sector_summary:
        if not isinstance(r, dict):
            continue
        sec = _s(r.get("sector") or "UNKNOWN")

        # main fields we care
        rec = {
            "sector": sec,
            "sector_total": _i(r.get("sector_total") or r.get("sector_cnt") or r.get("total_cnt")),
            "locked_cnt": _i(r.get("locked_cnt") or r.get("limitup_locked") or r.get("locked")),
            "touched_cnt": _i(r.get("touched_cnt") or r.get("limitup_touched") or r.get("touched")),
            "bigmove10_cnt": _i(r.get("bigmove10_cnt") or r.get("move10_cnt") or r.get("gt10_cnt")),
            "mix_cnt": _i(r.get("mix_cnt") or r.get("display_limitup_count") or 0),
            "locked_pct": _f(r.get("locked_pct"), default=-1.0),
            "touched_pct": _f(r.get("touched_pct"), default=-1.0),
            "bigmove10_pct": _f(r.get("bigmove10_pct"), default=-1.0),
            "mix_pct": _f(r.get("mix_pct"), default=-1.0),
            # suspicious generic pct
            "pct": r.get("pct", None),
        }
        out.append(rec)
    return out


# -----------------------------------------------------------------------------
# Compare report
# -----------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", required=True, help="path to close.payload.json (KR)")
    ap.add_argument("--db", required=True, help="path to kr_stock_warehouse.db")
    ap.add_argument("--top", type=int, default=30, help="print top N sectors/symbols")
    ap.add_argument("--delta", type=float, default=0.03, help="flag if |ret_json-ret_db| >= delta")
    ap.add_argument("--only-events", action="store_true", help="only check event stocks")
    ap.add_argument("--watch", default="", help="comma-separated symbols to always print")
    ap.add_argument("--show-sector", default="", help="comma-separated sectors to print details")
    args = ap.parse_args()

    payload = _load_json(args.json)
    rows = _pick_rows(payload)
    if not rows:
        raise SystemExit("payload snapshot rows empty (snapshot_all/main/open/snapshot not found or empty)")

    ymd = _ymd_effective(payload)
    jm = build_from_json(rows)
    syms = sorted(jm.keys())

    # DB compare part (keep your old logic)
    conn = sqlite3.connect(args.db)
    try:
        table, colmap = _find_price_table(conn, args.db)
        dm = _fetch_day_rows(
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

    print("\n" + "=" * 110)
    print(f"KR DB vs JSON compare | ymd_effective={ymd} | symbols(json)={len(jm)} | rows_in_db={len(dm)}")
    print("=" * 110)

    # -----------------------------------------------------------------------------
    # NEW: sector_summary diagnostics for the 100% issue
    # -----------------------------------------------------------------------------
    ss_payload = payload.get("sector_summary")
    ss_payload_norm = summarize_sector_summary_payload(ss_payload)

    ss_calc = compute_sector_summary_from_snapshot(rows)

    print("\n" + "=" * 110)
    print("[SECTOR SUMMARY] payload vs recompute(snapshot_main)")
    print("Goal: detect why badge shows 100% (usually sector_summary row has `pct: 1.0` overriding *_pct).")
    print("=" * 110)

    # 1) scan payload sector_summary for suspicious 'pct'
    if ss_payload_norm:
        pct_ones = [r for r in ss_payload_norm if (r.get("pct") is not None and abs(_f(r.get("pct")) - 1.0) < 1e-9)]
        pct_any = [r for r in ss_payload_norm if (r.get("pct") is not None)]
        print(f"[payload] sector_summary rows={len(ss_payload_norm)}")
        print(f"[payload] rows_with_pct_field={len(pct_any)}  rows_with_pct==1.0={len(pct_ones)}")
        if pct_ones:
            print("⚠️  Found sector_summary rows with pct==1.0 (this WILL force badge to 100% if renderer reads row['pct']).")
            for r in pct_ones[: min(args.top, 20)]:
                print(f"  - sector={r['sector']!r} pct={r['pct']} locked_cnt={r['locked_cnt']} touched_cnt={r['touched_cnt']} big10={r['bigmove10_cnt']} mix_pct={r['mix_pct']}")
    else:
        print("[payload] sector_summary not found or empty.")

    # 2) print top recomputed sectors
    print("\n[recompute] Top sectors by mix_cnt (computed from snapshot_main):")
    for a in ss_calc[: args.top]:
        def fmtp(x: Optional[float]) -> str:
            if x is None:
                return "None"
            return f"{x*100:5.1f}%"
        print(
            f"  - {a.sector[:30]:<30} total={a.sector_total:4d} "
            f"locked={a.locked_cnt:3d} touched={a.touched_cnt:3d} big10={a.bigmove10_cnt:3d} mix={a.mix_cnt:3d} "
            f"| locked_pct={fmtp(a.locked_pct)} touched_pct={fmtp(a.touched_pct)} big10_pct={fmtp(a.bigmove10_pct)} mix_pct={fmtp(a.mix_pct)}"
        )

    # 3) compare payload vs recompute for same sector name (if payload exists)
    if ss_payload_norm:
        m_payload = {r["sector"]: r for r in ss_payload_norm}
        print("\n[diff] payload vs recompute (first N computed sectors):")
        for a in ss_calc[: args.top]:
            pr = m_payload.get(a.sector)
            if not pr:
                continue
            # show only interesting diffs
            pct_field = pr.get("pct", None)
            show = False
            if pct_field is not None:
                show = True
            if pr.get("sector_total", 0) != a.sector_total:
                show = True
            if pr.get("mix_pct", -1.0) >= 0 and a.mix_pct is not None and abs(pr["mix_pct"] - a.mix_pct) > 1e-6:
                show = True
            if show:
                print(f"  - sector={a.sector!r}")
                print(f"      payload: total={pr['sector_total']} locked={pr['locked_cnt']} touched={pr['touched_cnt']} big10={pr['bigmove10_cnt']} mix_pct={pr['mix_pct']} pct_field={pct_field}")
                print(f"      recompute: total={a.sector_total} locked={a.locked_cnt} touched={a.touched_cnt} big10={a.bigmove10_cnt} mix_pct={a.mix_pct}")

    # optionally print specific sectors
    if args.show_sector.strip():
        wanted = [s.strip() for s in args.show_sector.split(",") if s.strip()]
        print("\n[sector details] show-sector requested:")
        mcalc = {a.sector: a for a in ss_calc}
        mpay = {r["sector"]: r for r in ss_payload_norm} if ss_payload_norm else {}
        for sec in wanted:
            a = mcalc.get(sec)
            pr = mpay.get(sec)
            print(f"\n--- {sec} ---")
            if pr:
                print(f"payload row keys={list(pr.keys())}")
                print(f"payload: {pr}")
            else:
                print("payload: (no sector_summary row)")
            if a:
                print(f"recompute: {a}")
            else:
                print("recompute: (sector not found in snapshot_main)")

    # -----------------------------------------------------------------------------
    # Keep your original suspicious-symbol logic (ret mismatch etc.)
    # -----------------------------------------------------------------------------
    watch = [s.strip() for s in args.watch.split(",") if s.strip()]
    suspicious: List[Tuple[float, str, str]] = []

    def add(score: float, sym: str, reason: str) -> None:
        suspicious.append((score, sym, reason))

    def is_event(r: JRow) -> bool:
        if r.is_locked_json or r.is_touch_json or r.is_bigup10_json:
            return True
        return r.ret_json >= 0.10

    for sym, r in jm.items():
        if args.only_events and (not is_event(r)):
            continue

        info = dm.get(sym)
        if not info:
            add(9.0, sym, "missing_in_db (no price row for ymd_effective)")
            continue

        lc = float(info["last_close"])
        c = float(info["close"])
        h = float(info["high"])

        ret_db = (c / lc - 1.0) if (lc > 0 and c > 0) else 0.0
        ret_high_db = (h / lc - 1.0) if (lc > 0 and h > 0) else 0.0

        if abs(r.ret_json) < 1e-9 and ret_db > 1e-6:
            add(9.6, sym, f"ret_json==0 but db_ret={ret_db:+.4f} (badge suppressed in draw_mpl)")

        d = abs(r.ret_json - ret_db)
        if d >= args.delta:
            add(8.0 + min(2.0, d * 10), sym, f"ret_mismatch | json={r.ret_json:+.4f} db={ret_db:+.4f} Δ={d:.4f}")

        if (r.is_touch_json or r.is_locked_json) and (r.prev_close_json <= 0 or r.close_json <= 0):
            add(9.2, sym, "event_flag_true but json prev_close/close missing (ret computed as 0 upstream?)")

        if r.is_touch_json and (r.high_json <= 0):
            add(8.8, sym, "touch_json=True but json high<=0 (touch inferred elsewhere?)")

        if r.is_touch_json and abs(r.ret_json) < 1e-9 and ret_high_db > 0.05:
            add(9.4, sym, f"touch_json=True, ret_json==0 but db_ret_high={ret_high_db:+.4f} (ret field likely not set)")

    for sym in watch:
        if sym in jm:
            add(10.0, sym, "WATCH")

    suspicious.sort(key=lambda x: x[0], reverse=True)

    print("\n" + "-" * 110)
    print(f"[SYMBOL] Top suspicious (show {args.top}): score | symbol | reason")
    print("-" * 110)

    shown = 0
    printed = set()

    def print_one(sym: str, reason: str, score: float) -> None:
        nonlocal shown
        r = jm.get(sym)
        info = dm.get(sym, {})
        if r is None:
            return
        lc = _f(info.get("last_close"))
        c = _f(info.get("close"))
        h = _f(info.get("high"))
        ret_db = (c / lc - 1.0) if (lc > 0 and c > 0) else 0.0
        ret_high_db = (h / lc - 1.0) if (lc > 0 and h > 0) else 0.0

        print(f"{score:>4.1f} | {sym:<12} | {reason}")
        print(f"      name={r.name} | sector={r.sector}")
        print(
            f"      json: ret={r.ret_json:+.4f} prev_close={r.prev_close_json:.4f} "
            f"open={r.open_json:.4f} close={r.close_json:.4f} high={r.high_json:.4f} low={r.low_json:.4f} "
            f"touch={r.is_touch_json} locked={r.is_locked_json} bigup10={r.is_bigup10_json} "
            f"limit_rate={r.limit_rate_json:.4f} limit_price={r.limit_price_json:.4f}"
        )
        print(
            f"      db  : last_close={lc:.4f} close={c:.4f} high={h:.4f} "
            f"ret={ret_db:+.4f} ret_high={ret_high_db:+.4f}"
        )
        shown += 1

    for sym in watch:
        if sym in jm and sym not in printed:
            print_one(sym, "WATCH", 10.0)
            printed.add(sym)

    for score, sym, reason in suspicious:
        if shown >= args.top:
            break
        if sym in printed:
            continue
        print_one(sym, reason, score)
        printed.add(sym)

    print("\n✅ Done.")


if __name__ == "__main__":
    main()
