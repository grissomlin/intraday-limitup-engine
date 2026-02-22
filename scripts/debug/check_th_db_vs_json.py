# scripts/debug/check_th_db_vs_json.py
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
# JSON side
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class JRow:
    sym: str
    name: str
    sector: str

    # json returns
    ret_json: float

    # json ohlc
    prev_close_json: float
    close_json: float
    high_json: float

    # json flags
    is_touch_json: bool
    is_locked_json: bool

    # json limit meta (often the bug source)
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


# -----------------------------------------------------------------------------
# DB helpers (copied/trimmed from your JP checker)
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

        # Using window function to get last_close
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
# TH limit rule (try import, else use constant)
# -----------------------------------------------------------------------------
def _th_limit_price(prev_close: float, *, limit_rate_default: float) -> float:
    """
    Prefer repo's TH rule if exists, else fallback to prev_close*(1+rate).
    """
    # Try common names (adapt if your repo uses a different file/function name)
    try:
        from markets.th.th_limit_rules import th_calc_limit  # type: ignore

        # expected: th_calc_limit(prev_close) -> obj.limit_price
        return float(th_calc_limit(prev_close).limit_price)
    except Exception:
        return prev_close * (1.0 + limit_rate_default)


# -----------------------------------------------------------------------------
# Compare report
# -----------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", required=True, help="path to close.payload.agg.json")
    ap.add_argument("--db", required=True, help="path to th_stock_warehouse.db")
    ap.add_argument("--limit-rate", type=float, default=0.30, help="fallback limit rate if TH rule not importable")
    ap.add_argument("--top", type=int, default=40, help="print top N suspicious symbols")
    ap.add_argument("--delta", type=float, default=0.05, help="flag if |ret_json-ret_db| >= delta")
    args = ap.parse_args()

    payload = _load_json(args.json)
    rows = _pick_rows(payload)
    if not rows:
        raise SystemExit("payload snapshot rows empty (snapshot_all/main/open/snapshot not found or empty)")

    ymd = _ymd_effective(payload)
    jm = build_from_json(rows)
    syms = sorted(jm.keys())

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

    print("\n" + "=" * 88)
    print(f"TH DB vs JSON compare | ymd_effective={ymd} | symbols(json)={len(jm)} | rows_in_db={len(dm)}")
    print("=" * 88)

    # Find "50%" style anomalies
    n_limit_50 = sum(1 for r in jm.values() if abs(r.limit_rate_json - 0.5) < 1e-9)
    n_ret_50 = sum(1 for r in jm.values() if abs(r.ret_json - 0.5) < 1e-9)
    print(f"[JSON] limit_rate == 0.50 count = {n_limit_50}")
    print(f"[JSON] ret      == 0.50 count = {n_ret_50}")

    suspicious: List[Tuple[float, str, str]] = []

    def add(score: float, sym: str, reason: str) -> None:
        suspicious.append((score, sym, reason))

    for sym, r in jm.items():
        info = dm.get(sym)
        if not info:
            add(9.0, sym, "missing_in_db")
            continue

        lc = float(info["last_close"])
        c = float(info["close"])
        h = float(info["high"])

        ret_db = (c / lc - 1.0) if (lc > 0 and c > 0) else 0.0

        # recompute limit price
        lp = _th_limit_price(lc, limit_rate_default=args.limit_rate) if lc > 0 else 0.0
        touch_db = (h > 0) and (lp > 0) and (h >= lp - EPS)
        locked_db = (c > 0) and (lp > 0) and (c >= lp - EPS)

        # 1) ret mismatch
        d = abs(r.ret_json - ret_db)
        if d >= args.delta:
            add(8.0 + min(2.0, d * 10), sym, f"ret_mismatch | json={r.ret_json:+.4f} db={ret_db:+.4f} Δ={d:.4f}")

        # 2) limit_rate suspicious (0.50) or doesn't match common TH 0.30
        if abs(r.limit_rate_json - 0.5) < 1e-9:
            add(8.5, sym, f"limit_rate_json_is_0.50 (badge may be using limit_rate)")

        # 3) touched/locked mismatch
        if r.is_touch_json != touch_db:
            add(7.5, sym, f"touch_flag_mismatch | json={r.is_touch_json} db={touch_db} (lp={lp:.4f})")
        if r.is_locked_json != locked_db:
            add(7.5, sym, f"locked_flag_mismatch | json={r.is_locked_json} db={locked_db} (lp={lp:.4f})")

        # 4) If badge is accidentally showing limit_rate instead of ret
        #    Heuristic: ret_json is ~0 but limit_rate_json is 0.5 and touched/locked present
        if (abs(r.ret_json) < 1e-6) and (abs(r.limit_rate_json - 0.5) < 1e-9) and (r.is_touch_json or r.is_locked_json):
            add(9.5, sym, "ret_json≈0 but limit_rate=0.50 with touch/locked -> badge likely wrong field")

        # 5) If DB ret is impossible large but json ret looks normal -> prev_close baseline mismatch
        if abs(ret_db) >= 0.35 and abs(r.ret_json) <= 0.20:
            add(8.8, sym, f"db_ret_too_large (baseline mismatch?) | db={ret_db:+.4f} json={r.ret_json:+.4f}")

    suspicious.sort(key=lambda x: x[0], reverse=True)

    print("\n" + "-" * 88)
    print(f"Top suspicious (show {args.top}): score | symbol | reason")
    print("-" * 88)

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
        lp = _th_limit_price(lc, limit_rate_default=args.limit_rate) if lc > 0 else 0.0

        print(f"{score:>4.1f} | {sym:<10} | {reason}")
        print(f"      name={r.name}")
        print(f"      json: ret={r.ret_json:+.4f} prev_close={r.prev_close_json:.4f} close={r.close_json:.4f} high={r.high_json:.4f} "
              f"touch={r.is_touch_json} locked={r.is_locked_json} limit_rate={r.limit_rate_json:.4f} limit_price={r.limit_price_json:.4f}")
        print(f"      db  : last_close={lc:.4f} close={c:.4f} high={h:.4f} ret={ret_db:+.4f} recompute_lp={lp:.4f}")
        shown += 1

    print("\n✅ Done.")


if __name__ == "__main__":
    main()
