# -*- coding: utf-8 -*-
"""
Scan suspect IPO / no-price-limit days from ONE long-format daily CSV.

Input  : data/cache/tw/tw_prices_1d_120d_XXXX.csv
Output : data/cache/tw/suspect_no_limit_days.csv
         data/cache/tw/suspect_no_limit_symbols.csv

Logic (simple & practical):
1) For each symbol, find first_trade_date = first date where close is not NaN.
2) Mark first N trading days (default 5) after first_trade_date as "suspect_ipo_window".
3) Also mark any day whose abs(daily_return) >= THRESH as "suspect_big_move".
"""

import os
import pandas as pd

# ---- Config ----
CSV_PATH = r"data\cache\tw\tw_prices_1d_120d_7c3090b755b0.csv"  # <-- 改成你的檔名
OUT_DIR  = r"data\cache\tw"

IPO_DAYS = 5
RET_THR  = 0.12  # 12%: 超過就很可能是無漲跌幅日/資料異常/新掛牌等

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    df = pd.read_csv(CSV_PATH)

    # normalize
    for c in ["open","high","low","close","volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["symbol","date"]).sort_values(["symbol","date"]).reset_index(drop=True)

    # keep only rows with real close for return calc
    df_real = df[df["close"].notna()].copy()
    if df_real.empty:
        raise RuntimeError("No valid close data found in CSV.")

    # daily return per symbol
    df_real["prev_close"] = df_real.groupby("symbol")["close"].shift(1)
    df_real["ret"] = (df_real["close"] - df_real["prev_close"]) / df_real["prev_close"]

    # first trade date per symbol
    first_trade = (
        df_real.groupby("symbol", as_index=False)["date"].min()
        .rename(columns={"date":"first_trade_date"})
    )

    df_real = df_real.merge(first_trade, on="symbol", how="left")

    # trading day index since first trade (0,1,2,... only counting real trading rows)
    df_real["trade_idx"] = df_real.groupby("symbol").cumcount()

    # flags
    df_real["suspect_ipo_window"] = df_real["trade_idx"] < IPO_DAYS
    df_real["suspect_big_move"] = df_real["ret"].abs() >= RET_THR

    # output rows where any suspect flag true
    out = df_real[df_real["suspect_ipo_window"] | df_real["suspect_big_move"]].copy()

    # tidy columns
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    out["first_trade_date"] = pd.to_datetime(out["first_trade_date"]).dt.strftime("%Y-%m-%d")

    out = out[[
        "symbol","date","close","prev_close","ret",
        "first_trade_date","trade_idx",
        "suspect_ipo_window","suspect_big_move"
    ]].sort_values(["symbol","date"])

    out_path = os.path.join(OUT_DIR, "suspect_no_limit_days.csv")
    out.to_csv(out_path, index=False, encoding="utf-8-sig")

    # also output symbol list (quick use)
    sym_path = os.path.join(OUT_DIR, "suspect_no_limit_symbols.csv")
    out[["symbol"]].drop_duplicates().to_csv(sym_path, index=False, encoding="utf-8-sig")

    print(f"✅ wrote: {out_path} (rows={len(out)})")
    print(f"✅ wrote: {sym_path} (symbols={out['symbol'].nunique()})")

    # quick check: 7795.TW
    cg = out[out["symbol"] == "7795.TW"]
    if not cg.empty:
        print("\n--- 7795.TW preview ---")
        print(cg.head(20).to_string(index=False))

if __name__ == "__main__":
    main()
