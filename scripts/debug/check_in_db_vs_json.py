# scripts/debug/check_in_db_vs_json.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

EPS = 1e-6

THIS = Path(__file__).resolve()
REPO_ROOT = THIS.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# =============================================================================
# small coercions
# =============================================================================
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


# =============================================================================
# JSON side row
# =============================================================================
@dataclass(frozen=True)
class JRow:
    sym: str
    name: str
    sector: str

    ret_json: float

    prev_close_json: float
    close_json: float
    high_json: float

    is_touch_json: bool
    is_locked_json: bool

    limit_rate_json: float
    limit_price_json: float


def build_from_json(rows: List[Dict[str, Any]]) -> Dict[str, JRow]:
    out: Dict[str, JRow] = {}
    for r in rows:
        sym = _s(r.get("symbol"))
        if not sym:
            continue
        out[sym] = JRow(
            sym=sym,
            name=_s(r.get("name")),
            sector=_s(r.get("sector") or r.get("industry") or r.get("sector_name")),
            ret_json=_f(r.get("ret")),
            prev_close_json=_f(r.get("prev_close")),
            close_json=_f(r.get("close")),
            high_json=_f(r.get("high")),
            is_touch_json=_b(r.get("is_limitup_touch")),
            is_locked_json=_b(r.get("is_limitup_locked")),
            limit_rate_json=_f(r.get("limit_rate")),
            limit_price_json=_f(r.get("limit_price")),
        )
    return out


# =============================================================================
# DB helpers (schema-agnostic)
# =============================================================================
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


def _fetch_day_rows_for_symbols(
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


def _scan_db_day_all(
    conn: sqlite3.Connection,
    *,
    table: str,
    col_symbol: str,
    col_date: str,
    col_high: str,
    col_close: str,
    ymd: str,
) -> List[Tuple[str, float, float, float, float]]:
    """
    Return list of (symbol, high, close, last_close, ret_db) for ALL symbols on ymd.
    """
    high_expr = col_high if col_high else "0.0"
    sql = f"""
    WITH p AS (
      SELECT
        {col_symbol} AS symbol,
        {col_date}   AS date,
        {high_expr}  AS high,
        {col_close}  AS close,
        LAG({col_close}) OVER (PARTITION BY {col_symbol} ORDER BY {col_date}) AS last_close
      FROM {table}
      WHERE {col_date} <= ?
    )
    SELECT symbol, high, close, last_close
    FROM p
    WHERE date = ?
      AND close IS NOT NULL
      AND last_close IS NOT NULL
      AND last_close > 0
    """
    rows = conn.execute(sql, [ymd, ymd]).fetchall()
    out: List[Tuple[str, float, float, float, float]] = []
    for sym, high, close, last_close in rows:
        h = _f(high)
        c = _f(close)
        lc = _f(last_close)
        ret = (c / lc - 1.0) if (lc > 0 and c > 0) else 0.0
        out.append((str(sym), h, c, lc, ret))
    return out


# =============================================================================
# IN limit rule (multi-strategy)
# =============================================================================
def _in_limit_price(prev_close: float, *, limit_rate: float) -> float:
    """
    Prefer repo India limit rule if exists.
    Otherwise fallback to prev_close * (1 + limit_rate).

    Note: India circuit limits are not always fixed 20%.
    Some scrips may be 5% / 10% / 20%. If payload includes per-symbol limit_rate,
    we will prefer that; otherwise use --limit-rate-default.
    """
    candidates = [
        ("markets.india.in_limit_rules", "in_calc_limit"),
        ("markets.india.limit_rules", "in_calc_limit"),
        ("markets.in.in_limit_rules", "in_calc_limit"),
        ("markets.in.limit_rules", "in_calc_limit"),
        ("markets.india.in_limit_rules", "calc_limit"),
        ("markets.india.limit_rules", "calc_limit"),
    ]
    for mod, fn in candidates:
        try:
            m = __import__(mod, fromlist=[fn])
            f = getattr(m, fn, None)
            if callable(f):
                obj = f(prev_close)

                if hasattr(obj, "limit_price"):
                    return float(getattr(obj, "limit_price"))

                if isinstance(obj, dict) and "limit_price" in obj:
                    return float(obj["limit_price"])
        except Exception:
            continue

    return prev_close * (1.0 + float(limit_rate))


def _best_limit_rate_for_row(r: JRow, *, default_rate: float) -> float:
    if r.limit_rate_json > 0:
        return float(r.limit_rate_json)
    return float(default_rate)


# =============================================================================
# Compare / report
# =============================================================================
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", required=True, help="path to in close.payload.json (or agg json)")
    ap.add_argument("--db", required=True, help="path to in_stock_warehouse.db")
    ap.add_argument("--limit-rate-default", type=float, default=0.20, help="fallback limit rate if JSON/IN rule unavailable")
    ap.add_argument("--top", type=int, default=60, help="print top N suspicious symbols")
    ap.add_argument("--delta", type=float, default=0.03, help="flag if |ret_json-ret_db| >= delta")
    ap.add_argument(
        "--scan-db-10p",
        action="store_true",
        help="scan DB for ALL symbols on ymd and list >=threshold movers; compare to JSON snapshot membership",
    )
    ap.add_argument("--threshold", type=float, default=0.10, help="threshold for movers scan (default 0.10)")
    args = ap.parse_args()

    payload = _load_json(args.json)
    rows = _pick_rows(payload)
    if not rows:
        raise SystemExit("payload snapshot rows empty (snapshot_all/main/open/snapshot not found or empty)")

    ymd = _ymd_effective(payload)
    jm = build_from_json(rows)
    syms_json = sorted(jm.keys())

    conn = sqlite3.connect(args.db)
    try:
        table, colmap = _find_price_table(conn, args.db)
        dm = _fetch_day_rows_for_symbols(
            conn,
            table=table,
            col_symbol=colmap["symbol"],
            col_date=colmap["date"],
            col_high=colmap["high"],
            col_close=colmap["close"],
            ymd=ymd,
            symbols=syms_json,
        )

        db_all: List[Tuple[str, float, float, float, float]] = []
        if args.scan_db_10p:
            db_all = _scan_db_day_all(
                conn,
                table=table,
                col_symbol=colmap["symbol"],
                col_date=colmap["date"],
                col_high=colmap["high"],
                col_close=colmap["close"],
                ymd=ymd,
            )
    finally:
        conn.close()

    print("\n" + "=" * 96)
    print(f"IN DB vs JSON compare | ymd_effective={ymd}")
    print(f"  symbols(json snapshot)={len(jm)} | rows_in_db_for_json_symbols={len(dm)}")
    print(f"  db={args.db}")
    print("=" * 96)

    # -------------------------------------------------------------------------
    # Extra check: DB all movers vs JSON presence
    # -------------------------------------------------------------------------
    if args.scan_db_10p:
        thr = float(args.threshold)
        movers_db = [(sym, ret, c, lc, h) for (sym, h, c, lc, ret) in db_all if ret >= thr]
        movers_db.sort(key=lambda x: x[1], reverse=True)

        set_json = set(jm.keys())
        set_db = set(sym for sym, *_ in movers_db)

        only_in_db = sorted(set_db - set_json)
        only_in_json = sorted(set_json - set_db)

        print("\n" + "-" * 96)
        print(f"[DB scan] {len(db_all)} rows on {ymd} with last_close available")
        print(f"[DB scan] movers >= {thr*100:.1f}% : {len(movers_db)}")
        print(f"[DB scan] movers in DB but NOT in JSON snapshot: {len(only_in_db)}")
        print(f"[DB scan] symbols in JSON snapshot but NOT >= {thr*100:.1f}% in DB: {len(only_in_json)}")

        if only_in_db[:20]:
            print("\n[DB only] first 20 symbols:")
            print("  " + ", ".join(only_in_db[:20]))

        if only_in_json[:20]:
            print("\n[JSON only] first 20 symbols:")
            print("  " + ", ".join(only_in_json[:20]))

        print("\n[DB movers top 20]: symbol ret close last_close high")
        for sym, ret, c, lc, h in movers_db[:20]:
            print(f"  {sym:<14} ret={ret:+.4f} close={c:.4f} last_close={lc:.4f} high={h:.4f}")

    # -------------------------------------------------------------------------
    # Main compare for snapshot symbols
    # -------------------------------------------------------------------------
    suspicious: List[Tuple[float, str, str]] = []

    def add(score: float, sym: str, reason: str) -> None:
        suspicious.append((score, sym, reason))

    n_ret_ge_20 = 0
    n_ret_ge_20_not_limit = 0

    for sym, r in jm.items():
        info = dm.get(sym)
        if not info:
            add(9.0, sym, "missing_in_db_for_symbol (no day row or no last_close)")
            continue

        lc = float(info["last_close"])
        c = float(info["close"])
        h = float(info["high"])

        ret_db = (c / lc - 1.0) if (lc > 0 and c > 0) else 0.0

        limit_rate = _best_limit_rate_for_row(r, default_rate=args.limit_rate_default)
        lp = _in_limit_price(lc, limit_rate=limit_rate) if lc > 0 else 0.0

        touch_db = (h > 0) and (lp > 0) and (h >= lp - EPS)
        locked_db = (c > 0) and (lp > 0) and (c >= lp - EPS)

        if ret_db >= 0.20 - 1e-6 or r.ret_json >= 0.20 - 1e-6:
            n_ret_ge_20 += 1
            if not (r.is_touch_json or r.is_locked_json or touch_db or locked_db):
                n_ret_ge_20_not_limit += 1

        d = abs(r.ret_json - ret_db)
        if d >= args.delta:
            add(8.0 + min(2.0, d * 10), sym, f"ret_mismatch | json={r.ret_json:+.4f} db={ret_db:+.4f} Δ={d:.4f}")

        if r.is_touch_json != touch_db:
            add(7.5, sym, f"touch_flag_mismatch | json={r.is_touch_json} db={touch_db} (lp={lp:.4f}, rate={limit_rate:.3f})")
        if r.is_locked_json != locked_db:
            add(7.5, sym, f"locked_flag_mismatch | json={r.is_locked_json} db={locked_db} (lp={lp:.4f}, rate={limit_rate:.3f})")

        if ret_db >= (limit_rate - 0.002) and not (r.is_touch_json or r.is_locked_json):
            add(
                9.2,
                sym,
                f"ret_db≈limit_rate but json flags are false | db_ret={ret_db:+.4f} rate={limit_rate:.3f}",
            )

        if (abs(r.ret_json) < 1e-6) and (r.limit_rate_json > 0) and (r.is_touch_json or r.is_locked_json):
            add(9.5, sym, "ret_json≈0 but has limit flags -> JSON ret likely wrong/missing")

        if (r.limit_rate_json >= 0.199) and (ret_db >= 0.18) and not (touch_db or locked_db):
            add(
                8.7,
                sym,
                f"json limit_rate≈0.20 but DB prices not reaching recompute lp | db_ret={ret_db:+.4f} lp={lp:.4f} close={c:.4f} high={h:.4f}",
            )

    print("\n" + "-" * 96)
    print(f"[STAT] (snapshot symbols) count with ret>=20% (json or db) = {n_ret_ge_20}")
    print(f"[STAT] ret>=20% but no limit flags (json & db recompute all false) = {n_ret_ge_20_not_limit}")

    suspicious.sort(key=lambda x: x[0], reverse=True)

    print("\n" + "=" * 96)
    print(f"Top suspicious (show {args.top}): score | symbol | reason")
    print("=" * 96)

    shown = 0
    for score, sym, reason in suspicious:
        if shown >= args.top:
            break

        r = jm.get(sym)
        info = dm.get(sym, {})
        if r is None:
            continue

        lc = _f(info.get("last_close"))
        c = _f(info.get("close"))
        h = _f(info.get("high"))
        ret_db = (c / lc - 1.0) if (lc > 0 and c > 0) else 0.0

        limit_rate = _best_limit_rate_for_row(r, default_rate=args.limit_rate_default)
        lp = _in_limit_price(lc, limit_rate=limit_rate) if lc > 0 else 0.0

        touch_db = (h > 0) and (lp > 0) and (h >= lp - EPS)
        locked_db = (c > 0) and (lp > 0) and (c >= lp - EPS)

        print(f"{score:>4.1f} | {sym:<14} | {reason}")
        print(f"      name={r.name}")
        print(
            f"      json: ret={r.ret_json:+.4f} prev_close={r.prev_close_json:.4f} close={r.close_json:.4f} high={r.high_json:.4f} "
            f"touch={r.is_touch_json} locked={r.is_locked_json} limit_rate={r.limit_rate_json:.4f} limit_price={r.limit_price_json:.4f}"
        )
        print(
            f"      db  : last_close={lc:.4f} close={c:.4f} high={h:.4f} ret={ret_db:+.4f} "
            f"recompute_lp={lp:.4f} touch_db={touch_db} locked_db={locked_db} rate_used={limit_rate:.3f}"
        )
        shown += 1

    print("\n✅ Done.")


if __name__ == "__main__":
    main()
