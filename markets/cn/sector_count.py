# markets/cn/sector_count.py
# -*- coding: utf-8 -*-
import sqlite3

db = "markets/cn/cn_stock_warehouse.db"
c = sqlite3.connect(db)
try:
    n_all = c.execute("SELECT COUNT(DISTINCT sector) FROM stock_info").fetchone()[0]
    n_ex  = c.execute("SELECT COUNT(DISTINCT sector) FROM stock_info WHERE sector!='未分類'").fetchone()[0]
    print("distinct sectors (incl 未分類) =", n_all)
    print("distinct sectors (excl 未分類) =", n_ex)
finally:
    c.close()
