# scripts/check_jp_db.py
# -*- coding: utf-8 -*-

import sqlite3
from collections import Counter

DB = "markets/jp/jp_stock_warehouse.db"

def main():
    conn = sqlite3.connect(DB)
    try:
        rows = conn.execute(
            "SELECT symbol, name, sector, market_detail FROM stock_info"
        ).fetchall()

        print(f"📦 JP stock_info rows = {len(rows)}")

        missing = [r for r in rows if not r[2] or not str(r[2]).strip()]
        print(f"❌ sector missing = {len(missing)}")

        sectors = Counter(r[2] for r in rows if r[2])
        print("📊 top 15 sectors:")
        for sec, cnt in sectors.most_common(15):
            print(f"  {sec:<30} {cnt}")

        print("\n🔍 sample rows:")
        for r in rows[:10]:
            print(f"  {r[0]} | {r[1]} | {r[2]} | {r[3]}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
