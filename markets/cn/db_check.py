# -- coding utf-8 --
import sqlite3
import argparse

def main()
    ap = argparse.ArgumentParser()
    ap.add_argument(--db, required=True)
    ap.add_argument(--date, default=)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    try
        if args.date
            n = conn.execute(
                SELECT COUNT(DISTINCT symbol) FROM stock_prices WHERE date=,
                (args.date,),
            ).fetchone()[0]
            print(symbols_on_date =, n)
        # 最近一個交易日有幾檔
        row = conn.execute(SELECT MAX(date) FROM stock_prices).fetchone()
        last_date = row[0]
        n2 = conn.execute(
            SELECT COUNT(DISTINCT symbol) FROM stock_prices WHERE date=,
            (last_date,),
        ).fetchone()[0]
        print(latest_date =, last_date)
        print(symbols_on_latest_date =, n2)

        total_sym = conn.execute(SELECT COUNT(DISTINCT symbol) FROM stock_info).fetchone()[0]
        print(stock_info_symbols =, total_sym)
    finally
        conn.close()

if __name__ == __main__
    main()
