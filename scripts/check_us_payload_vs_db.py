# scripts/check_us_payload_vs_db.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple, Set

import pandas as pd


DEFAULT_DB = r"markets\us\us_stock_warehouse.db"


def q1(conn: sqlite3.Connection, sql: str, params: Tuple = ()) -> int:
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def qall(conn: sqlite3.Connection, sql: str, params: Tuple = ()) -> List[Tuple]:
    return conn.execute(sql, params).fetchall()


def print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def _norm_sector(x: Any) -> str:
    s = str(x or "").strip()
    return s if s else "Unknown"


def _pct(x: Any) -> float:
    try:
        return float(x or 0.0)
    except Exception:
        return 0.0


def _bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    s = str(x or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def resolve_ymd_effective(conn: sqlite3.Connection, ymd: str) -> str:
    row = conn.execute("SELECT MAX(date) FROM stock_prices WHERE date <= ?", (ymd,)).fetchone()
    eff = row[0] if row and row[0] else None
    if not eff:
        raise RuntimeError("DB has no stock_prices rows.")
    return str(eff)


def pick_ymd_from_payload(payload: Dict[str, Any], ymd_arg: str | None) -> str:
    if ymd_arg:
        return str(ymd_arg)[:10]
    ymd_eff = str(payload.get("ymd_effective") or "")[:10]
    if ymd_eff:
        return ymd_eff
    ymd = str(payload.get("ymd") or "")[:10]
    if ymd:
        return ymd
    # fallback: meta.time.market_finished_at
    meta = payload.get("meta") or {}
    t = (meta.get("time") or {}).get("market_finished_at")
    if t:
        return str(t)[:10]
    return datetime.now().strftime("%Y-%m-%d")


def payload_sector_counts(payload: Dict[str, Any], ret_th: float) -> pd.DataFrame:
    """
    用 payload['open_limit_watchlist'] 重算 sector 計數，並與 payload['sector_summary'] 比對。
    - bigmove10_cnt：ret>=ret_th 且不是 touch
    - touched_cnt：touched_only 或 is_limitup_touch
    """
    watch = payload.get("open_limit_watchlist") or payload.get("emerging_watchlist") or []
    dfw = pd.DataFrame(watch)
    if dfw.empty:
        return pd.DataFrame(columns=["sector", "bigmove10_cnt", "touched_cnt", "locked_cnt"])

    if "sector" not in dfw.columns and "industry" in dfw.columns:
        dfw["sector"] = dfw["industry"]

    dfw["sector"] = dfw["sector"].apply(_norm_sector)
    dfw["ret"] = pd.to_numeric(dfw.get("ret", 0.0), errors="coerce").fillna(0.0)

    touched = dfw.get("touched_only", False).apply(_bool) | dfw.get("is_limitup_touch", False).apply(_bool)
    hit = (dfw["ret"] >= float(ret_th)) & (~touched)

    g = dfw.assign(_touched=touched, _hit=hit).groupby("sector", dropna=False)
    out = pd.DataFrame(
        {
            "bigmove10_cnt": g["_hit"].sum().astype(int),
            "touched_cnt": g["_touched"].sum().astype(int),
        }
    ).reset_index()

    out["locked_cnt"] = 0
    out = out[["sector", "bigmove10_cnt", "touched_cnt", "locked_cnt"]]
    out = out.sort_values(["bigmove10_cnt", "touched_cnt"], ascending=False).reset_index(drop=True)
    return out


def payload_sector_summary_df(payload: Dict[str, Any]) -> pd.DataFrame:
    ss = payload.get("sector_summary") or []
    df = pd.DataFrame(ss)
    if df.empty:
        return pd.DataFrame(columns=["sector", "bigmove10_cnt", "touched_cnt", "locked_cnt"])
    df["sector"] = df["sector"].apply(_norm_sector)
    for c in ["bigmove10_cnt", "touched_cnt", "locked_cnt"]:
        df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0).astype(int)
    df = df[["sector", "bigmove10_cnt", "touched_cnt", "locked_cnt"]]
    df = df.sort_values(["bigmove10_cnt", "touched_cnt"], ascending=False).reset_index(drop=True)
    return df


def db_sector_ge10(conn: sqlite3.Connection, ymd_eff: str) -> pd.DataFrame:
    """
    DB: ymd_eff 當日 ret>=10% 的 sector 分佈
    """
    sql = """
    WITH px AS (
      SELECT
        p.symbol,
        p.date,
        p.close,
        LAG(p.close) OVER (PARTITION BY p.symbol ORDER BY p.date) AS prev_close
      FROM stock_prices p
      WHERE p.date <= ?
    ),
    today AS (
      SELECT
        symbol,
        (close / prev_close) - 1.0 AS ret
      FROM px
      WHERE date = ?
        AND prev_close IS NOT NULL AND prev_close > 0 AND close IS NOT NULL
    )
    SELECT
      COALESCE(NULLIF(TRIM(i.sector), ''), 'Unknown') AS sector,
      COUNT(*) AS bigmove10_cnt
    FROM today t
    LEFT JOIN stock_info i ON i.symbol = t.symbol
    WHERE t.ret >= 0.10
    GROUP BY sector
    ORDER BY bigmove10_cnt DESC
    """
    rows = qall(conn, sql, (ymd_eff, ymd_eff))
    df = pd.DataFrame(rows, columns=["sector", "bigmove10_cnt"])
    if df.empty:
        return pd.DataFrame(columns=["sector", "bigmove10_cnt"])
    df["sector"] = df["sector"].apply(_norm_sector)
    df["bigmove10_cnt"] = pd.to_numeric(df["bigmove10_cnt"], errors="coerce").fillna(0).astype(int)
    return df


def db_movers_symbols(conn: sqlite3.Connection, ymd_eff: str) -> Set[str]:
    sql = """
    WITH px AS (
      SELECT
        p.symbol,
        p.date,
        p.close,
        LAG(p.close) OVER (PARTITION BY p.symbol ORDER BY p.date) AS prev_close
      FROM stock_prices p
      WHERE p.date <= ?
    ),
    today AS (
      SELECT
        symbol,
        (close / prev_close) - 1.0 AS ret
      FROM px
      WHERE date = ?
        AND prev_close IS NOT NULL AND prev_close > 0 AND close IS NOT NULL
    )
    SELECT symbol FROM today WHERE ret >= 0.10
    """
    rows = qall(conn, sql, (ymd_eff, ymd_eff))
    return {str(r[0]) for r in rows}


def payload_hit_symbols(payload: Dict[str, Any], ret_th: float) -> Set[str]:
    watch = payload.get("open_limit_watchlist") or payload.get("emerging_watchlist") or []
    out: Set[str] = set()
    for r in watch:
        sym = str(r.get("symbol") or "").strip()
        if not sym:
            continue
        touched = _bool(r.get("touched_only")) or _bool(r.get("is_limitup_touch"))
        ret = _pct(r.get("ret"))
        if (ret >= float(ret_th)) and (not touched):
            out.add(sym)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--payload", required=True, help="path to reagg json, e.g. data/cache/us/YYYY-MM-DD/midday.payload.reagg.json")
    ap.add_argument("--db", default=DEFAULT_DB, help="path to us_stock_warehouse.db")
    ap.add_argument("--ymd", default=None, help="YYYY-MM-DD (default: from payload or today)")
    ap.add_argument("--ret-th", type=float, default=None, help="ret threshold (default: payload.filters.ret_th or 0.10)")
    ap.add_argument("--top", type=int, default=25, help="top N sectors to display in diffs")
    ap.add_argument("--show-symbol-diff", action="store_true", help="also diff mover symbols between DB and payload (hit only, excluding touched)")

    args = ap.parse_args()

    payload_path = Path(args.payload)
    payload = json.loads(payload_path.read_text(encoding="utf-8"))

    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    # ret_th: prefer args, else payload.filters.ret_th, else 0.10
    ret_th = args.ret_th
    if ret_th is None:
        ret_th = float((payload.get("filters") or {}).get("ret_th", 0.10) or 0.10)

    conn = sqlite3.connect(str(db_path))
    try:
        ymd_req = pick_ymd_from_payload(payload, args.ymd)
        ymd_eff = resolve_ymd_effective(conn, ymd_req)

        print_header("US Payload vs DB")
        print(f"payload  = {payload_path}")
        print(f"db       = {db_path}")
        print(f"ymd_req  = {ymd_req}")
        print(f"ymd_eff  = {ymd_eff}")
        print(f"ret_th   = {ret_th:.4f}")
        print("")

        # --- payload sanity: watchlist vs sector_summary ---
        df_watch_counts = payload_sector_counts(payload, ret_th=ret_th)
        df_ss = payload_sector_summary_df(payload)

        print_header("Payload sanity: sector_summary should match watchlist-derived counts")
        print(f"open_limit_watchlist_count = {len(payload.get('open_limit_watchlist') or payload.get('emerging_watchlist') or [])}")
        print(f"sector_summary_count       = {len(payload.get('sector_summary') or [])}")

        if df_ss.empty and df_watch_counts.empty:
            print("(payload has no watchlist / sector_summary)")
        else:
            # merge + diff
            M = df_watch_counts.merge(df_ss, on="sector", how="outer", suffixes=("_watch", "_ss")).fillna(0)
            for c in ["bigmove10_cnt", "touched_cnt", "locked_cnt"]:
                M[f"diff_{c}"] = (M[f"{c}_ss"].astype(int) - M[f"{c}_watch"].astype(int))
            bad = (M["diff_bigmove10_cnt"] != 0) | (M["diff_touched_cnt"] != 0) | (M["diff_locked_cnt"] != 0)
            n_bad = int(bad.sum())
            print(f"mismatch sectors = {n_bad}")
            if n_bad:
                show = M.loc[bad].copy()
                show["absdiff"] = show["diff_bigmove10_cnt"].abs() + show["diff_touched_cnt"].abs() + show["diff_locked_cnt"].abs()
                show = show.sort_values("absdiff", ascending=False).head(args.top)
                print(show.to_string(index=False))
            else:
                print("✅ OK: payload.sector_summary matches watchlist-derived counts")

        # --- DB sector distribution (ret>=10%) vs payload bigmove10 ---
        df_db = db_sector_ge10(conn, ymd_eff)

        print_header("DB vs Payload: sector bigmove10_cnt (ret>=10%)")
        if df_db.empty:
            print("(DB has no ret>=10% movers on ymd_eff)")
        else:
            A = df_db.rename(columns={"bigmove10_cnt": "db_bigmove10"})
            B = (df_ss[["sector", "bigmove10_cnt"]].rename(columns={"bigmove10_cnt": "pl_bigmove10"})
                 if not df_ss.empty else pd.DataFrame(columns=["sector", "pl_bigmove10"]))
            X = A.merge(B, on="sector", how="outer").fillna(0)
            X["diff"] = X["pl_bigmove10"].astype(int) - X["db_bigmove10"].astype(int)
            X["absdiff"] = X["diff"].abs()
            X = X.sort_values(["absdiff", "db_bigmove10", "pl_bigmove10"], ascending=[False, False, False])

            print("Top diffs:")
            print(X.head(args.top).to_string(index=False))

            total_db = int(A["db_bigmove10"].sum())
            total_pl = int(B["pl_bigmove10"].sum()) if not B.empty else 0
            print("")
            print(f"TOTAL ret>=10% (DB)     = {total_db}")
            print(f"TOTAL ret>=10% (payload)= {total_pl}")
            if total_db != total_pl:
                print("⚠️ totals differ: 可能是 ymd_eff / 使用的快照來源不同（DB=收盤日報、payload=盤中快照）")

        # --- optional: symbol diff (hit only; exclude touched) ---
        if args.show_symbol_diff:
            print_header("Symbol diff (hit only, excluding touched)")
            db_syms = db_movers_symbols(conn, ymd_eff)
            pl_syms = payload_hit_symbols(payload, ret_th=ret_th)

            only_db = sorted(db_syms - pl_syms)
            only_pl = sorted(pl_syms - db_syms)

            print(f"DB movers symbols     = {len(db_syms)}")
            print(f"payload hit symbols   = {len(pl_syms)}")
            print(f"only in DB            = {len(only_db)}")
            print(f"only in payload       = {len(only_pl)}")

            if only_db:
                print("\n-- only in DB (first 80) --")
                print(" ".join(only_db[:80]))
            if only_pl:
                print("\n-- only in payload (first 80) --")
                print(" ".join(only_pl[:80]))

    finally:
        conn.close()


if __name__ == "__main__":
    main()
