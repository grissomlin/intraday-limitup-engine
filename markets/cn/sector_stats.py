# markets/cn/sector_stats.py
# -*- coding: utf-8 -*-
import argparse, sqlite3

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="markets/cn/cn_stock_warehouse.db")
    ap.add_argument("--top", type=int, default=30)
    args = ap.parse_args()

    c = sqlite3.connect(args.db)
    try:
        n = c.execute("SELECT COUNT(DISTINCT sector) FROM stock_info").fetchone()[0]
        n2 = c.execute("SELECT COUNT(DISTINCT sector) FROM stock_info WHERE sector<>'未分類'").fetchone()[0]
        print("distinct sectors (incl 未分類) =", n)
        print("distinct sectors (excl 未分類) =", n2)
        print()
        rows = c.execute(
            "SELECT sector, COUNT(*) cnt FROM stock_info GROUP BY sector ORDER BY cnt DESC LIMIT ?",
            (args.top,),
        ).fetchall()
        print(f"top {args.top}:")
        for s, cnt in rows:
            print(f"{s}\t{cnt}")
    finally:
        c.close()

if __name__ == "__main__":
    main()
