# markets/cn/downloader.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import random
import sqlite3
import subprocess
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm import tqdm

from .cn_config import (
    default_db_path,
    rolling_trading_days,
    fallback_rolling_cal_days,
    log,
    fix_sector_flag,
    db_vacuum,
    sync_sw_industry_flag,
    sw_only_missing,
    sw_max_industries,
    sw_sector_level,
    sw_sync_timeout,
)
from .cn_db import init_db
from .cn_calendar import infer_window_by_trading_days
from .cn_stock_list import get_cn_stock_list
from .cn_market import is_main, is_chinext, is_star
from .cn_prices import (
    download_one,
    download_batch,
    insert_prices,
    write_final_errors,
    batch_size,
    batch_sleep_sec,
    fallback_single_enabled,
    sleep_between,  # single fallback 節奏用
)

# -----------------------------------------------------------------------------
# sector clean (保留你原本功能)
# -----------------------------------------------------------------------------
BAD_SECTOR_SQL = """
UPDATE stock_info
SET sector='未分類'
WHERE sector IS NULL
   OR TRIM(sector)=''
   OR sector IN ('A-Share','—','-','--','－','–')
"""


def fix_sector_missing(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(BAD_SECTOR_SQL)
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        conn.close()


def _chunk_list(xs: List[str], n: int) -> List[List[str]]:
    if n <= 0:
        n = 1
    return [xs[i : i + n] for i in range(0, len(xs), n)]


# -----------------------------------------------------------------------------
# SW cache sync (FAST PATH)
# -----------------------------------------------------------------------------
def _sw_cache_path() -> str:
    # 預設找 markets/cn/cn_sw_merged_cache.json
    return os.getenv(
        "CN_SW_CACHE_PATH",
        os.path.join(os.path.dirname(__file__), "cn_sw_merged_cache.json"),
    )


def _sync_sw_from_cache(db_path: str) -> bool:
    """
    優先走快路：把 cn_sw_merged_cache.json 寫回 stock_info（只補缺失）
    成功回 True；失敗/檔不存在回 False（讓外層 fallback 到慢版）。
    """
    cache_path = _sw_cache_path()
    if not os.path.exists(cache_path):
        log(f"🏷️ SW cache not found, skip fast sync: {cache_path}")
        return False

    script_path = os.path.join(os.path.dirname(__file__), "sync_sw_cache_to_db.py")
    if not os.path.exists(script_path):
        log(f"🏷️ SW cache sync script missing, skip fast sync: {script_path}")
        return False

    timeout_s = sw_sync_timeout()
    try:
        log(f"🏷️ SW cache sync (FAST) ... only-missing | cache={os.path.basename(cache_path)}")
        cmd = ["python", script_path, "--db", db_path, "--cache", cache_path, "--only-missing"]
        subprocess.run(cmd, check=True, timeout=timeout_s)
        log("✅ SW cache sync done.")
        return True
    except subprocess.TimeoutExpired:
        log("⚠️ SW cache sync timeout (fallback to slow sync if enabled).")
        return False
    except Exception as e:
        log(f"⚠️ SW cache sync failed (fallback to slow sync if enabled): {e}")
        return False


def run_sync(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    *,
    refresh_list: bool = True,
    sample_n: int = 0,
    sample_mode: str = "mixed",
    symbols: Optional[List[str]] = None,
) -> Dict[str, Any]:
    db_path = default_db_path()
    init_db(db_path)

    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    n_days = rolling_trading_days()
    start_td, end_incl, end_excl = infer_window_by_trading_days(end_date, n_days)

    if (not start_date) and start_td and end_incl and end_excl:
        start_date = start_td
        end_date = end_incl
        end_excl_date = end_excl
        mode = "trading_days"
        log(
            f"📅 Trading-day window OK | last {n_days} trading days | {start_date} ~ {end_date} (end_excl={end_excl_date})"
        )
    else:
        if not start_date:
            start_date = (datetime.now() - timedelta(days=fallback_rolling_cal_days())).strftime("%Y-%m-%d")
        end_excl_date = (pd.to_datetime(end_date) + timedelta(days=1)).strftime("%Y-%m-%d")
        mode = "cal_days"
        log(f"⚠️ Trading-day window unavailable; fallback to cal-days | {start_date} ~ {end_date} (end_excl={end_excl_date})")

    log(f"📦 CN DB = {db_path}")
    log(f"🚀 CN run_sync | window: {start_date} ~ {end_date} | refresh_list={refresh_list}")

    # 1) build list
    if symbols:
        items = [(s.strip(), "Unknown") for s in symbols if str(s).strip()]
        log(f"🧪 指定 symbols 模式：{len(items)} 檔")
    else:
        items = get_cn_stock_list(db_path, refresh_list=refresh_list)

    if not items:
        return {"success": 0, "total": 0, "failed": 0, "has_changed": False, "db_path": db_path}

    # 2) optional sampling (debug)
    if sample_n and sample_n > 0:
        mode_s = (sample_mode or "mixed").lower().strip()

        main_items = [(s, n) for s, n in items if is_main(s)]
        chinext_items = [(s, n) for s, n in items if is_chinext(s)]
        star_items = [(s, n) for s, n in items if is_star(s)]

        if mode_s == "main":
            items = random.sample(main_items, min(sample_n, len(main_items)))
        elif mode_s == "chinext":
            items = random.sample(chinext_items, min(sample_n, len(chinext_items)))
        elif mode_s == "star":
            items = random.sample(star_items, min(sample_n, len(star_items)))
        else:
            k = max(1, sample_n // 3)
            pick_main = random.sample(main_items, min(k, len(main_items)))
            pick_chi = random.sample(chinext_items, min(k, len(chinext_items)))
            pick_star = random.sample(star_items, min(k, len(star_items)))
            items = pick_main + pick_chi + pick_star

        log(f"🧪 SAMPLE MODE: {mode_s} | symbols={len(items)}")

    # 3) rolling delete (avoid DB growing)
    conn = sqlite3.connect(db_path, timeout=120)
    try:
        conn.execute("DELETE FROM stock_prices WHERE date >= ?", (start_date,))
        conn.commit()
    finally:
        conn.close()

    # 4) batch download & upsert
    success = 0
    failed = 0

    name_map: Dict[str, str] = {s: (n or "Unknown") for s, n in items if s}
    all_syms: List[str] = [s for s, _ in items if s]

    bs = max(1, int(batch_size()))
    bs_sleep = float(batch_sleep_sec())
    do_fallback = bool(fallback_single_enabled())

    log(f"🧩 CN batch download enabled | batch_size={bs} | batch_sleep={bs_sleep} | fallback_single={do_fallback}")

    final_failed: Dict[str, str] = {}

    conn = sqlite3.connect(db_path, timeout=120)
    try:
        batches = _chunk_list(all_syms, bs)
        pbar = tqdm(batches, desc="CN同步(batch)", unit="批")

        for tickers in pbar:
            # (A) batch download
            df_long, failed_tickers, berr = download_batch(tickers, start_date, end_excl_date)

            # (A1) insert success rows
            if df_long is not None and (not df_long.empty):
                try:
                    ok_syms = set(df_long["symbol"].astype(str).unique().tolist())
                except Exception:
                    ok_syms = set()
                insert_prices(conn, df_long)
                if ok_syms:
                    success += len(ok_syms)

            # (A2) batch-level failure info
            if failed_tickers:
                if berr:
                    for sym in failed_tickers:
                        final_failed[sym] = berr
                else:
                    for sym in failed_tickers:
                        final_failed.setdefault(sym, "batch_failed")

                # (B) fallback single download (optional)
                if do_fallback:
                    for sym in failed_tickers:
                        df_one, err_one = download_one(sym, start_date, end_excl_date)
                        if df_one is not None and (not df_one.empty):
                            insert_prices(conn, df_one)
                            success += 1
                            final_failed.pop(sym, None)
                        else:
                            failed += 1
                            if err_one:
                                final_failed[sym] = err_one
                            else:
                                final_failed.setdefault(sym, "empty")

                        sleep_between()
                else:
                    failed += len(failed_tickers)

            if bs_sleep > 0:
                time.sleep(bs_sleep)

        conn.commit()

        # ✅ 只寫「最終仍失敗」的 ticker（乾淨、不重複）
        write_final_errors(conn, final_failed, name_map, start_date, end_date)

        # 先把 sector 空/壞值整理成「未分類」
        if fix_sector_flag():
            affected = fix_sector_missing(db_path)
            log(f"🏷️ sector 缺失/壞值 → 未分類：{affected} 筆")

        # ✅ SW industry：FAST PATH 優先用 cache；失敗才 fallback 慢版（依 flag）
        did_fast = _sync_sw_from_cache(db_path)

        if (not did_fast) and sync_sw_industry_flag():
            # fallback slow sync
            try:
                script_path = os.path.join(os.path.dirname(__file__), "sw_industry_sync.py")
                timeout_s = sw_sync_timeout()
                only_missing = sw_only_missing()
                max_ind = sw_max_industries()
                level = sw_sector_level()

                cmd = ["python", script_path, "--db", db_path, "--sector-level", level]
                if only_missing:
                    cmd.append("--only-missing")
                if max_ind:
                    cmd += ["--max-industries", max_ind]

                log(f"🏷️ SW industry sync (SLOW) ... ({'only-missing' if only_missing else 'full'}) | sector_level={level}")
                subprocess.run(cmd, check=True, timeout=timeout_s)
                log("✅ SW industry sync done.")
            except subprocess.TimeoutExpired:
                log("⚠️ SW industry sync timeout (continue).")
            except Exception as e:
                log(f"⚠️ SW industry sync failed (continue): {e}")
        else:
            if did_fast:
                log("🚀 SW industry: used FAST cache sync; skip slow sync.")
            else:
                log("🏷️ SW industry: slow sync disabled by flag; skip.")

        if db_vacuum():
            log("🧹 VACUUM...")
            conn.execute("VACUUM")
            conn.commit()

        total = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info").fetchone()[0]
    finally:
        conn.close()

    if final_failed:
        failed = len(final_failed)

    log(f"📊 CN 同步完成 | 成功:{success} 失敗:{failed} / {len(all_syms)}")
    return {
        "success": success,
        "total": int(total or 0),
        "failed": failed,
        "has_changed": success > 0,
        "window": {"start": start_date, "end": end_date, "end_excl": end_excl_date, "mode": mode},
        "db_path": db_path,
        "calendar": {
            "ticker": os.getenv("CN_CALENDAR_TICKER", "000001.SS"),
            "n_trading_days": int(n_days),
            "lookback_cal_days": int(os.getenv("CN_CAL_LOOKBACK_CAL_DAYS", "180")),
        },
        "batch": {
            "batch_size": int(bs),
            "batch_sleep_sec": float(bs_sleep),
            "fallback_single": bool(do_fallback),
            "final_failed": int(len(final_failed)),
        },
    }


def run_intraday(*, slot: str, asof: str, ymd: str) -> Dict[str, Any]:
    # ✅ 修正 ImportError：snapshot_builder 對外是 run_intraday，不是 build_snapshot_payload
    from .snapshot_builder import run_intraday as _run_intraday
    return _run_intraday(slot=slot, asof=asof, ymd=ymd)
