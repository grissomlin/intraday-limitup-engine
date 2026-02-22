# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, sqlite3
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", required=True, help="data/cache/au/YYYY-MM-DD/close.payload.json")
    ap.add_argument("--db", required=True, help="markets/au/au_stock_warehouse.db")
    ap.add_argument("--th", type=float, default=0.10)
    args = ap.parse_args()

    payload = json.load(open(args.json, "r", encoding="utf-8"))
    ymd_eff = str(payload.get("ymd_effective") or payload.get("ymd"))[:10]
    th = float(args.th)

    # JSON 端目前一定是 0（因為你 AU snapshot 硬塞 hit_prev=0）
    rows = payload.get("snapshot_open") or []
    dfj = pd.DataFrame(rows) if rows else pd.DataFrame()
    json_hit_prev = int((pd.to_numeric(dfj.get("hit_prev", 0), errors="coerce").fillna(0) == 1).sum()) if not dfj.empty else 0

    conn = sqlite3.connect(args.db)
    try:
        sql = """
        WITH p AS (
          SELECT
            symbol,
            date,
            close,
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
          FROM stock_prices
          WHERE date <= ?
        ),
        rets AS (
          SELECT
            symbol, date, close, prev_close,
            CASE
              WHEN prev_close IS NOT NULL AND prev_close > 0 AND close IS NOT NULL
              THEN (close / prev_close) - 1.0
              ELSE NULL
            END AS ret,
            CASE
              WHEN prev_close IS NOT NULL AND prev_close > 0 AND close IS NOT NULL
                   AND (close / prev_close) - 1.0 >= ?
              THEN 1 ELSE 0
            END AS hit
          FROM p
        ),
        final AS (
          SELECT
            r.*,
            COALESCE(LAG(r.hit) OVER (PARTITION BY r.symbol ORDER BY r.date), 0) AS hit_prev
          FROM rets r
          WHERE r.ret IS NOT NULL
        )
        SELECT COUNT(*) AS n_prev10
        FROM final f
        JOIN stock_info i ON i.symbol=f.symbol
        WHERE i.market='AU' AND f.date=? AND f.hit_prev=1
        """
        n_prev10 = conn.execute(sql, (ymd_eff, th, ymd_eff)).fetchone()[0]
    finally:
        conn.close()

    print("=" * 80)
    print(f"AU ymd_effective={ymd_eff} th={th}")
    print(f"[JSON] hit_prev==1 count = {json_hit_prev}")
    print(f"[DB ] hit_prev==1 count = {n_prev10}")
    print("=" * 80)
    if n_prev10 != json_hit_prev:
        print("=> 代表 AU snapshot 沒有把 hit_prev 算進 JSON（目前就是這個狀態）")

if __name__ == "__main__":
    main()
