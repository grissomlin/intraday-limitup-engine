# scripts/check_jp_limitup_stats.py
# -*- coding: utf-8 -*-

import argparse
import json
import os
import sqlite3
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

EPS = 1e-6  # 浮點誤差容忍


# -----------------------------------------------------------------------------
# JP limit rule (DB side truth)
# NOTE:
# 你原本版本的 tier 表偏短（到 10,000+ 只回 1500），這會讓高價股全部不準。
# 建議直接改成跟 markets/jp/jp_limit_rules.py 一樣的完整 tier。
# 這裡我直接內建「完整 tier」，避免 DB/JSON 用不同規則造成永遠對不上。
# -----------------------------------------------------------------------------
def jp_limit_amount(last_close: float) -> float:
    p = float(last_close or 0.0)
    if p <= 0:
        return 0.0

    if p < 100:
        return 30
    if p < 200:
        return 50
    if p < 500:
        return 80
    if p < 700:
        return 100
    if p < 1000:
        return 150
    if p < 1500:
        return 300
    if p < 2000:
        return 400
    if p < 3000:
        return 500
    if p < 5000:
        return 700
    if p < 7000:
        return 1000
    if p < 10000:
        return 1500
    if p < 15000:
        return 3000
    if p < 20000:
        return 4000
    if p < 30000:
        return 5000
    if p < 50000:
        return 7000
    if p < 70000:
        return 10000
    if p < 100000:
        return 15000
    if p < 150000:
        return 30000
    if p < 200000:
        return 40000
    if p < 300000:
        return 50000
    if p < 500000:
        return 70000
    if p < 700000:
        return 100000
    if p < 1000000:
        return 150000
    if p < 1500000:
        return 300000
    return 300000


def jp_calc_limit_price(last_close: float) -> float:
    lc = float(last_close or 0.0)
    return lc + float(jp_limit_amount(lc))


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _to_bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if x is None:
        return False
    s = str(x).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _pick_db_path(payload: Dict[str, Any], cli_db: Optional[str]) -> str:
    if cli_db:
        return cli_db
    meta = payload.get("meta") or {}
    p = meta.get("db_path")
    if isinstance(p, str) and p.strip():
        return p.strip()
    env_db = os.getenv("JP_DB_PATH", "").strip()
    if env_db:
        return env_db
    return "markets/jp/jp_stock_warehouse.db"


def _pick_ymd(payload: Dict[str, Any], cli_ymd: Optional[str], conn: sqlite3.Connection) -> str:
    if cli_ymd and cli_ymd.strip():
        return cli_ymd.strip()
    y = _safe_str(payload.get("ymd_effective") or payload.get("ymd") or "")
    if y:
        return y
    row = conn.execute("SELECT MAX(date) FROM stock_prices").fetchone()
    return str(row[0]) if row and row[0] else ""


def load_payload(payload_path: str) -> Dict[str, Any]:
    with open(payload_path, "r", encoding="utf-8") as f:
        return json.load(f)


def pick_universe(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("snapshot_all", "snapshot_main", "snapshot_open", "snapshot"):
        rows = payload.get(key) or []
        if isinstance(rows, list) and rows:
            return rows
    return []


@dataclass
class DbRow:
    symbol: str
    name: str
    sector: str
    close: float
    last_close: float
    high: float


def fetch_db_rows(conn: sqlite3.Connection, ymd: str) -> List[DbRow]:
    sql = """
    WITH p AS (
      SELECT
        symbol,
        date,
        high,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS last_close
      FROM stock_prices
    )
    SELECT
      p.symbol,
      COALESCE(i.name, 'Unknown') AS name,
      COALESCE(i.sector, '未分類') AS sector,
      p.close,
      p.last_close,
      p.high
    FROM p
    LEFT JOIN stock_info i ON i.symbol = p.symbol
    WHERE p.date = ?
      AND p.last_close IS NOT NULL
    """
    out: List[DbRow] = []
    for sym, name, sector, close, last_close, high in conn.execute(sql, (ymd,)).fetchall():
        out.append(
            DbRow(
                symbol=str(sym),
                name=str(name or "Unknown"),
                sector=str(sector or "未分類"),
                close=_to_float(close, 0.0),
                last_close=_to_float(last_close, 0.0),
                high=_to_float(high, 0.0),
            )
        )
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--payload", required=True, help="data/cache/jp/YYYY-MM-DD/close.payload.json")
    ap.add_argument("--db", default=None, help="override DB path (else payload.meta.db_path / JP_DB_PATH / default)")
    ap.add_argument("--ymd", default=None, help="override ymd (else payload.ymd_effective / MAX(date))")
    ap.add_argument("--show", type=int, default=30, help="how many mismatches to print")
    args = ap.parse_args()

    payload = load_payload(args.payload)
    universe = pick_universe(payload)
    if not universe:
        print("❌ payload 中找不到 snapshot_* 列表")
        return

    db_path = _pick_db_path(payload, args.db)
    if not os.path.exists(db_path):
        print(f"❌ DB not found: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    try:
        ymd = _pick_ymd(payload, args.ymd, conn)
        if not ymd:
            print("❌ cannot determine ymd")
            return

        print(f"📦 payload = {args.payload}")
        print(f"📦 db_path  = {db_path}")
        print(f"📅 ymd_effective = {ymd}")

        # -----------------------------
        # DB truth
        # -----------------------------
        db_rows = fetch_db_rows(conn, ymd)
        db_map: Dict[str, DbRow] = {r.symbol: r for r in db_rows}
        print(f"snapshot_main (DB) = {len(db_rows)}")

        db_locked = set()
        db_touch = set()

        for r in db_rows:
            if r.last_close <= 0 or r.close <= 0:
                continue
            lp = jp_calc_limit_price(r.last_close)
            is_locked = r.close >= lp - EPS
            is_touch = (r.high > 0) and (r.high >= lp - EPS)
            if is_locked:
                db_locked.add(r.symbol)
            if is_touch:
                db_touch.add(r.symbol)

        # -----------------------------
        # JSON side
        # -----------------------------
        json_map: Dict[str, Dict[str, Any]] = {}
        for r in universe:
            if not isinstance(r, dict):
                continue
            sym = _safe_str(r.get("symbol") or "")
            if sym:
                json_map[sym] = r

        print(f"snapshot_main (JSON) = {len(json_map)}")

        json_locked = {s for s, r in json_map.items() if _to_bool(r.get("is_limitup_locked"))}
        json_touch = {s for s, r in json_map.items() if _to_bool(r.get("is_limitup_touch"))}

        # -----------------------------
        # Coverage
        # -----------------------------
        db_syms = set(db_map.keys())
        json_syms = set(json_map.keys())
        only_db = sorted(db_syms - json_syms)
        only_json = sorted(json_syms - db_syms)
        both = sorted(db_syms & json_syms)

        print("\n=== Coverage ===")
        print(f"both      = {len(both)}")
        print(f"only_db   = {len(only_db)}")
        print(f"only_json = {len(only_json)}")

        # -----------------------------
        # Count compare
        # -----------------------------
        print("\n=== Limitup count compare ===")
        print(f"DB   locked={len(db_locked)} touch={len(db_touch)}")
        print(f"JSON locked={len(json_locked)} touch={len(json_touch)}")

        # -----------------------------
        # Field-by-field mismatch
        # -----------------------------
        mismatches: List[Tuple[str, List[str]]] = []

        def _near(a: float, b: float, tol: float = 1e-6) -> bool:
            return abs(float(a) - float(b)) <= tol

        for sym in both:
            dr = db_map[sym]
            jr = json_map[sym]

            # JSON values (may be missing)
            j_close = _to_float(jr.get("close"), 0.0)
            j_last = _to_float(jr.get("last_close"), 0.0)
            j_high = _to_float(jr.get("high"), 0.0)
            j_lp = _to_float(jr.get("limit_price") or jr.get("jp_limit_price"), 0.0)
            j_touch = _to_bool(jr.get("is_limitup_touch"))
            j_locked = _to_bool(jr.get("is_limitup_locked"))

            # DB recompute
            db_lp = jp_calc_limit_price(dr.last_close) if dr.last_close > 0 else 0.0
            db_touch2 = (dr.high > 0) and (dr.high >= db_lp - EPS)
            db_locked2 = (dr.close > 0) and (dr.close >= db_lp - EPS)

            diffs = []

            if dr.last_close > 0 and (not _near(dr.close, j_close, tol=1e-4)):
                diffs.append(f"close DB={dr.close} JSON={j_close}")

            if dr.last_close > 0 and (not _near(dr.last_close, j_last, tol=1e-4)):
                diffs.append(f"last_close DB={dr.last_close} JSON={j_last}")

            # limit_price: allow a bit wider tol due to float formatting
            if dr.last_close > 0 and j_lp > 0 and (not _near(db_lp, j_lp, tol=1e-3)):
                diffs.append(f"limit_price DBcalc={db_lp} JSON={j_lp}")

            # high: not always critical, but can explain touch mismatch
            if dr.last_close > 0 and j_high > 0 and (not _near(dr.high, j_high, tol=1e-4)):
                diffs.append(f"high DB={dr.high} JSON={j_high}")

            if bool(db_touch2) != bool(j_touch):
                diffs.append(f"is_touch DBcalc={db_touch2} JSON={j_touch}")

            if bool(db_locked2) != bool(j_locked):
                diffs.append(f"is_locked DBcalc={db_locked2} JSON={j_locked}")

            if diffs:
                mismatches.append((sym, diffs))

        print("\n=== Mismatches (top) ===")
        print(f"mismatch_symbols = {len(mismatches)}")
        for sym, diffs in mismatches[: max(0, int(args.show))]:
            name = _safe_str(json_map.get(sym, {}).get("name") or db_map.get(sym, DbRow(sym, "", "", 0, 0, 0)).name)
            print(f"\n- {sym} | {name}")
            for d in diffs:
                print(f"  - {d}")

        # -----------------------------
        # Set-level mismatch for locked/touch
        # -----------------------------
        print("\n=== Set diff (locked) ===")
        locked_only_db = sorted(db_locked - json_locked)
        locked_only_json = sorted(json_locked - db_locked)
        print(f"locked_only_db   = {len(locked_only_db)}")
        print(f"locked_only_json = {len(locked_only_json)}")
        if locked_only_db[:10]:
            print("sample locked_only_db:", ", ".join(locked_only_db[:10]))
        if locked_only_json[:10]:
            print("sample locked_only_json:", ", ".join(locked_only_json[:10]))

        print("\n=== Set diff (touch) ===")
        touch_only_db = sorted(db_touch - json_touch)
        touch_only_json = sorted(json_touch - db_touch)
        print(f"touch_only_db   = {len(touch_only_db)}")
        print(f"touch_only_json = {len(touch_only_json)}")
        if touch_only_db[:10]:
            print("sample touch_only_db:", ", ".join(touch_only_db[:10]))
        if touch_only_json[:10]:
            print("sample touch_only_json:", ", ".join(touch_only_json[:10]))

        # -----------------------------
        # Sector distribution (DB locked) as before
        # -----------------------------
        c = Counter((db_map[s].sector or "未分類") for s in db_locked if s in db_map)
        if c:
            print("\n📊 DB 封板(locked) 產業分佈（Top 10）:")
            for sec, cnt in c.most_common(10):
                print(f"  {sec:<30} {cnt}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
