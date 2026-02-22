# markets/in/snapshot_builder.py
# -*- coding: utf-8 -*-
"""
IN snapshot builder (stateless, GitHub Actions friendly)

Flow:
- Load universe from Google Drive (EQUITY_L + sec_list merged in-memory)
- Fetch intraday prices via yfinance (chunked to avoid rate-limit)
- Build snapshot_main payload (TH-compatible shape)

No local persistence: no CSV written.
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple

import pandas as pd
import yfinance as yf

from markets.in.universe import load_universe_df


# =============================================================================
# Env tuning
# =============================================================================
def _chunk_size() -> int:
    return int(os.getenv("IN_YF_CHUNK_SIZE", "80"))


def _chunk_sleep() -> float:
    return float(os.getenv("IN_YF_CHUNK_SLEEP", "2.0"))


def _max_failed_ratio_abort() -> float:
    # if too many tickers fail, abort early
    return float(os.getenv("IN_ABORT_FAILED_RATIO", "0.60"))


# =============================================================================
# Helpers
# =============================================================================
def _safe_float(x) -> float | None:
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def _download_chunk(tickers: List[str]) -> Tuple[pd.DataFrame, str | None]:
    """
    Download one chunk using yfinance.
    Returns (df, err)
    """
    if not tickers:
        return pd.DataFrame(), None

    tickers_str = " ".join(tickers)

    try:
        df = yf.download(
            tickers=tickers_str,
            period="1d",
            interval="1m",
            group_by="ticker",
            threads=True,
            auto_adjust=True,
            progress=False,
        )
        if df is None or df.empty:
            return pd.DataFrame(), "empty"
        return df, None
    except Exception as e:
        return pd.DataFrame(), f"exception: {e}"


def _extract_last_price(df: pd.DataFrame, sym: str) -> Dict[str, Any] | None:
    """
    From a multi-ticker yfinance df, extract last close + day high.
    """
    if not isinstance(df.columns, pd.MultiIndex):
        return None

    if sym not in df.columns.levels[1]:
        return None

    sub = df.xs(sym, axis=1, level=1)
    if sub is None or sub.empty:
        return None

    # last row
    last = sub.iloc[-1]

    close = _safe_float(last.get("Close"))
    high_day = _safe_float(sub["High"].max())

    if close is None:
        return None

    return {"close": close, "high": high_day}


# =============================================================================
# Main snapshot builder
# =============================================================================
def run_intraday_full() -> Dict[str, Any]:
    """
    Stateless intraday snapshot:
    Drive universe -> yfinance chunk fetch -> snapshot_main
    """
    print("📡 Loading IN universe from Drive...")
    df_uni = load_universe_df()

    if df_uni.empty:
        raise RuntimeError("Universe empty (Drive list missing?)")

    tickers = df_uni["yf_symbol"].astype(str).tolist()
    total = len(tickers)

    print(f"✅ Universe loaded: {total} tickers")
    print(f"⚙️ yfinance chunk_size={_chunk_size()} sleep={_chunk_sleep()}s")

    snapshot_main: List[Dict[str, Any]] = []
    failed: List[str] = []

    # chunk loop
    chunks = [tickers[i : i + _chunk_size()] for i in range(0, total, _chunk_size())]

    for idx, chunk in enumerate(chunks, start=1):
        print(f"\n--- Chunk {idx}/{len(chunks)} ({len(chunk)} tickers) ---")

        df_chunk, err = _download_chunk(chunk)

        if err:
            print(f"⚠️ Chunk download error: {err}")
            failed.extend(chunk)
            time.sleep(_chunk_sleep())
            continue

        # extract per symbol
        for sym in chunk:
            px = _extract_last_price(df_chunk, sym)
            if px is None:
                failed.append(sym)
                continue

            # lookup universe row
            row = df_uni.loc[df_uni["yf_symbol"] == sym].iloc[0]

            snapshot_main.append(
                {
                    # Yahoo symbol
                    "symbol": sym,
                    # local NSE symbol
                    "local_symbol": row["Symbol"],
                    "name": row.get("name", "Unknown"),

                    # sector/industry not available yet
                    "sector": "Unknown",
                    "industry": "Unknown",

                    # prices
                    "close": px["close"],
                    "high": px["high"],

                    # band info
                    "band": row.get("band"),
                    "limit_pct": row.get("limit_pct"),

                    # meta
                    "market": "in",
                }
            )

        print(f"Chunk done: snapshot_rows={len(snapshot_main)} failed_so_far={len(failed)}")

        # abort if too many failures
        if len(failed) / total > _max_failed_ratio_abort():
            raise RuntimeError(
                f"Too many failures: {len(failed)}/{total} "
                f"({len(failed)/total:.1%}) - aborting"
            )

        time.sleep(_chunk_sleep())

    # build payload
    payload = {
        "market": "in",
        "slot": "intraday",
        "asof": datetime.now().strftime("%H:%M"),
        "snapshot_main": snapshot_main,
        "snapshot_open": [],
        "stats": {
            "snapshot_main_count": len(snapshot_main),
            "failed_count": len(failed),
            "universe_total": total,
        },
        "meta": {
            "yf_chunk_size": _chunk_size(),
            "yf_failed": failed[:50],  # only show first 50
        },
    }

    print("\n✅ IN intraday snapshot finished.")
    print(payload["stats"])
    return payload


# =============================================================================
# CLI
# =============================================================================
if __name__ == "__main__":
    run_intraday_full()
