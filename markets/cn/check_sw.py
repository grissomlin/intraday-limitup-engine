# -*- coding: utf-8 -*-
import sqlite3

DB = r"markets\cn\cn_stock_warehouse.db"

c = sqlite3.connect(DB)

n = c.execute("""
SELECT COUNT(*) 
FROM stock_info 
WHERE sw_l3 IS NOT NULL AND LENGTH(TRIM(sw_l3)) > 0
""").fetchone()[0]

print("sw_l3 filled =", n)

print("sample =", c.execute("""
SELECT symbol, name, sw_l3
FROM stock_info
WHERE sw_l3 IS NOT NULL AND LENGTH(TRIM(sw_l3)) > 0
LIMIT 10
""").fetchall())

rows = c.execute("""
SELECT sector, COUNT(*) cnt
FROM stock_info
GROUP BY sector
ORDER BY cnt DESC
LIMIT 20
""").fetchall()

print("sector top20 =", rows)

n2 = c.execute("""
SELECT COUNT(*)
FROM stock_info
WHERE sector='未分類' AND sw_l3 IS NOT NULL AND LENGTH(TRIM(sw_l3)) > 0
""").fetchone()[0]

print("unclassified but has sw_l3 =", n2)

c.close()
