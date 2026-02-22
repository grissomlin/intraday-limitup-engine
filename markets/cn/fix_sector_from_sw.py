# -*- coding: utf-8 -*-
"""
fix_sector_from_sw.py
---------------------
把 stock_info.sector 從申萬行業欄位回填（sw_l1 / sw_l2 / sw_l3）

需求：
- 把 sector == A-Share / NULL / '' / '—' / '未分類' 等「壞值」統一視為缺失
- 若指定 level 有值（例如 sw_l3），則用它回填 sector（只回填壞值）
- 仍缺失者最後標為「未分類」

功能：
- --level l1|l2|l3  選用 sw_l1/2/3
- --dry-run         不寫入，只顯示會改幾筆
- --stats           顯示統計與 sample
"""

from __future__ import annotations

import argparse
import sqlite3

# 這些值一律視為「壞 sector」
BAD_SECTOR = {
    "", "A-Share", "未分類",
    "—", "-", "--", "－", "–",
}

# 申萬欄位也可能出現的「壞值」（不要拿來回填）
BAD_SW = {
    "", "—", "-", "--", "－", "–",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="path to cn_stock_warehouse.db")
    ap.add_argument("--level", choices=["l1", "l2", "l3"], default="l3")
    ap.add_argument("--dry-run", action="store_true", help="do not write, stats only")
    ap.add_argument("--stats", action="store_true", help="print stats and samples")
    args = ap.parse_args()

    col = {"l1": "sw_l1", "l2": "sw_l2", "l3": "sw_l3"}[args.level]

    # 產生 SQL 需要的 IN (...) 參數 placeholders
    bad_sector_ph = ",".join(["?"] * len(BAD_SECTOR))
    bad_sw_ph = ",".join(["?"] * len(BAD_SW))

    conn = sqlite3.connect(args.db)
    cur = conn.cursor()

    # -------------------------
    # stats (before)
    # -------------------------
    total = cur.execute("SELECT COUNT(*) FROM stock_info").fetchone()[0]

    # sector 壞值數（包含 未分類）
    bad_sector_cnt = cur.execute(
        f"""
        SELECT COUNT(*)
        FROM stock_info
        WHERE sector IS NULL OR TRIM(sector)='' OR TRIM(sector) IN ({bad_sector_ph})
        """,
        tuple(BAD_SECTOR),
    ).fetchone()[0]

    # sw 欄位有值且不是破折號的數量（可用來回填）
    sw_usable_cnt = cur.execute(
        f"""
        SELECT COUNT(*)
        FROM stock_info
        WHERE {col} IS NOT NULL
          AND TRIM({col}) <> ''
          AND TRIM({col}) NOT IN ({bad_sw_ph})
        """,
        tuple(BAD_SW),
    ).fetchone()[0]

    # 真正「可回填」的筆數：sector 是壞值 + sw 可用
    fill_cnt = cur.execute(
        f"""
        SELECT COUNT(*)
        FROM stock_info
        WHERE (sector IS NULL OR TRIM(sector)='' OR TRIM(sector) IN ({bad_sector_ph}))
          AND {col} IS NOT NULL
          AND TRIM({col}) <> ''
          AND TRIM({col}) NOT IN ({bad_sw_ph})
        """,
        tuple(BAD_SECTOR) + tuple(BAD_SW),
    ).fetchone()[0]

    # 最後仍會變成未分類：sector 壞值 + sw 不可用
    missing_cnt = cur.execute(
        f"""
        SELECT COUNT(*)
        FROM stock_info
        WHERE (sector IS NULL OR TRIM(sector)='' OR TRIM(sector) IN ({bad_sector_ph}))
          AND ({col} IS NULL OR TRIM({col})='' OR TRIM({col}) IN ({bad_sw_ph}))
        """,
        tuple(BAD_SECTOR) + tuple(BAD_SW),
    ).fetchone()[0]

    if args.stats:
        print(f"🎯 DB stock_info total: {total}")
        print(f"🕳️  sector missing/bad: {bad_sector_cnt}")
        print(f"🧾 sw usable ({col}): {sw_usable_cnt}")
        print(f"🧠 level={args.level} ({col})")
        print(f"✅ 可用 SW 回填筆數: {fill_cnt}")
        print(f"📦 仍會被標為 未分類 筆數: {missing_cnt}")

        samp = cur.execute(
            f"""
            SELECT symbol,name,sector,sw_l1,sw_l2,sw_l3
            FROM stock_info
            WHERE {col} IS NOT NULL AND TRIM({col})<>''
            LIMIT 10
            """
        ).fetchall()
        print("🔍 sample (before):")
        for r in samp:
            print(" ", r)

    if args.dry_run:
        print("🧪 dry-run：不寫入 DB（只顯示統計）")
        conn.close()
        return

    # -------------------------
    # 1) 回填 sector = sw_col（只針對 sector 壞值的）
    # -------------------------
    cur.execute(
        f"""
        UPDATE stock_info
        SET sector = TRIM({col})
        WHERE (sector IS NULL OR TRIM(sector)='' OR TRIM(sector) IN ({bad_sector_ph}))
          AND {col} IS NOT NULL
          AND TRIM({col}) <> ''
          AND TRIM({col}) NOT IN ({bad_sw_ph})
        """,
        tuple(BAD_SECTOR) + tuple(BAD_SW),
    )

    # -------------------------
    # 2) 剩下 still bad 的 sector 全部改成 未分類
    # -------------------------
    cur.execute(
        f"""
        UPDATE stock_info
        SET sector='未分類'
        WHERE sector IS NULL OR TRIM(sector)='' OR TRIM(sector) IN ({bad_sector_ph})
        """,
        tuple(BAD_SECTOR),
    )

    conn.commit()

    if args.stats:
        print("\n✅ sector 已回填完成")

        top = cur.execute(
            """
            SELECT sector, COUNT(*) cnt
            FROM stock_info
            GROUP BY sector
            ORDER BY cnt DESC
            LIMIT 15
            """
        ).fetchall()
        print("\n📊 sector TOP 15 (after):")
        for s, cnt in top:
            print(f"{s} {cnt}")

        samp2 = cur.execute(
            """
            SELECT symbol,name,sector,sw_l1,sw_l2,sw_l3
            FROM stock_info
            LIMIT 20
            """
        ).fetchall()
        print("\n🔍 sample rows (after):")
        for r in samp2:
            print(r)

    conn.close()


if __name__ == "__main__":
    main()
