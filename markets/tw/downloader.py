# markets/tw/downloader.py
# -*- coding: utf-8 -*-
"""
TW Daily (1d) Intraday Snapshot Downloader (RAW snapshots only)
---------------------------------------------------------------
目標：
- 下載全市場「日K」(yfinance) → 合併成 1 個 long-format DataFrame → 快取成單一 CSV/Parquet
- 生成 RAW 快照：
    - snapshot_main：主榜（listed/otc/innovation/dr...）
    - snapshot_open：開放/非標準制度（目前用來放興櫃 emerging；未來也可放無漲跌幅/特殊制度）
- 不做漲停判斷、不做產業榜、不做同產業未漲停（全部交給 markets/tw/aggregator.py）

你需要的檔案：
- data/tw_stock_list.json  (symbol/name/sector/market_detail, 可選：limit_type / listed_date)

✅ 本版重點：
- payload key：snapshot_emerging -> snapshot_open
- stats / filters / 早退段一併改名

✅ 本次修正（你問的「抓不到最新交易日」問題）：
- 以前：每檔都取 lookback 期間「最後一根」當 latest → 不管 ymd 是什麼
- 現在：用 ymd 推出 ymd_effective（<= ymd 的最近交易日）
        並用「每檔在 ymd_effective 的 bar」+「該檔前一交易日」計算 ret
- 並且：cache 若比 ymd 還舊，會自動忽略 cache 重新下載（避免被舊 cache 卡死）
"""

from __future__ import annotations

import os
import json
import hashlib
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

import pandas as pd
import yfinance as yf
from tqdm import tqdm

# ✅ indicators enrichment (streak / streak_prev / future indicators)
from markets.tw.indicators import enrich_snapshot_main


# =============================================================================
# Paths / Cache switches
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))              # .../markets/tw
ROOT_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))    # project root

DATA_DIR = os.path.join(ROOT_DIR, "data")
CACHE_DIR = os.path.join(DATA_DIR, "cache", "tw")
os.makedirs(CACHE_DIR, exist_ok=True)

STOCKLIST_FILE = os.path.join(DATA_DIR, "tw_stock_list.json")

# NOTE:
# - 若在 GitHub Actions，通常不希望寫 cache（除非你明確設 CACHE_ENABLED=1）
_GA = os.getenv("GITHUB_ACTIONS", "false").strip().lower() == "true"
_cache_raw = os.getenv("CACHE_ENABLED", "").strip()
if _cache_raw == "":
    CACHE_ENABLED = (not _GA)  # local default True, GA default False
else:
    CACHE_ENABLED = _cache_raw == "1"

DAILY_LOOKBACK_DAYS = int(os.getenv("TW_DAILY_LOOKBACK_DAYS", "120"))

# 主榜要納入哪些 market_detail（興櫃不在此清單內）
INCLUDE_MARKET_DETAILS = set(
    x.strip()
    for x in os.getenv("TW_INCLUDE_DETAILS", "listed,otc,innovation_a,innovation_c,dr").split(",")
    if x.strip()
)

# 舊名仍支援：TW_ENABLE_EMERGING_WATCHLIST
# 新語意：open（開放制度）
ENABLE_OPEN_WATCHLIST = os.getenv("TW_ENABLE_OPEN_WATCHLIST", "").strip()
if ENABLE_OPEN_WATCHLIST == "":
    ENABLE_OPEN_WATCHLIST = os.getenv("TW_ENABLE_EMERGING_WATCHLIST", "1")
ENABLE_OPEN_WATCHLIST = str(ENABLE_OPEN_WATCHLIST).strip() == "1"

MAX_ERRORS = int(os.getenv("TW_MAX_ERRORS", "300"))
BATCH_SIZE = int(os.getenv("TW_DAILY_BATCH_SIZE", "200"))
CACHE_FORMAT = os.getenv("TW_CACHE_FORMAT", "csv").lower()  # csv / parquet

# ✅ 無漲跌幅限制判斷（目前仍只針對主榜新掛牌等，興櫃直接走 open）
NO_LIMIT_LISTING_DAYS = int(os.getenv("TW_NO_LIMIT_LISTING_DAYS", "5"))
NO_LIMIT_SYMBOLS = set(
    s.strip()
    for s in os.getenv("TW_NO_LIMIT_SYMBOLS", "").split(",")
    if s.strip()
)


# =============================================================================
# Types
# =============================================================================
@dataclass(frozen=True)
class IntradayResult:
    ymd: str
    slot: str
    asof: str
    total_symbols: int
    payload: Dict[str, Any]


# =============================================================================
# Helpers
# =============================================================================
def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    try:
        return float(x)
    except Exception:
        return None


def _hash_symbols(symbols: List[str]) -> str:
    s = ",".join(sorted(symbols)).encode("utf-8", errors="ignore")
    return hashlib.sha1(s).hexdigest()[:12]


def _market_label(mdetail: str) -> str:
    mp = {
        "listed": "上市",
        "otc": "上櫃",
        "dr": "DR",
        "innovation_a": "創新A",
        "innovation_c": "創新C",
        "emerging": "興櫃",
    }
    return mp.get((mdetail or "").strip(), "")


def _parse_ymd(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        # 只吃 YYYY-MM-DD
        return datetime.strptime(s[:10], "%Y-%m-%d")
    except Exception:
        return None


def _infer_limit_type(sym: str, meta: Dict[str, Any], ymd: str) -> str:
    """
    回傳：standard / no_limit
    no_limit 命中條件：
      1) stock list meta 明確寫 limit_type=no_limit
      2) sym 在 TW_NO_LIMIT_SYMBOLS
      3) meta 有 listed_date 且 (ymd - listed_date).days < TW_NO_LIMIT_LISTING_DAYS（預設 5）

    注意：
    - emerging 不在這裡判（呼叫端會直接指定 open 類型）
    """
    # 1) 明確標記
    lt = str(meta.get("limit_type") or "").strip().lower()
    if lt in ("no_limit", "standard"):
        return lt

    # 2) 環境強制
    if sym in NO_LIMIT_SYMBOLS:
        return "no_limit"

    # 3) 用 listed_date 推導
    listed_date = _parse_ymd(str(meta.get("listed_date") or ""))
    ymd_dt = _parse_ymd(ymd)
    if listed_date and ymd_dt:
        try:
            days = (ymd_dt.date() - listed_date.date()).days
            if days >= 0 and days < NO_LIMIT_LISTING_DAYS:
                return "no_limit"
        except Exception:
            pass

    return "standard"


# =============================================================================
# Load stock list (universe meta)
# =============================================================================
def load_tw_stock_list() -> Dict[str, Dict[str, Any]]:
    """
    讀取 data/tw_stock_list.json
    建議格式：list[dict]
      [
        {"symbol":"2330.TW","name":"台積電","sector":"半導體業","market_detail":"listed"},
        {"symbol":"7795.TW","name":"長廣","sector":"電子零組件業","market_detail":"listed",
         "limit_type":"no_limit", "listed_date":"2026-01-13"},
        {"symbol":"xxxx.TWO","name":"某上櫃","sector":"xx","market_detail":"otc"},
        {"symbol":"yyyy.TWO","name":"某興櫃","sector":"xx","market_detail":"emerging"},
        ...
      ]
    回傳：symbol -> meta dict
    """
    if not os.path.exists(STOCKLIST_FILE):
        return {}

    try:
        raw = json.loads(open(STOCKLIST_FILE, "r", encoding="utf-8").read())
        meta: Dict[str, Dict[str, Any]] = {}
        for it in raw:
            sym = (it.get("symbol") or "").strip()
            if not sym:
                continue
            meta[sym] = {
                "symbol": sym,
                "name": it.get("name", "") or "",
                "sector": it.get("sector", "") or "",
                "market": it.get("market", "") or "",
                "market_detail": it.get("market_detail", "") or "",
                # ✅ optional
                "limit_type": it.get("limit_type", "") or "",
                "listed_date": it.get("listed_date", "") or "",
            }
        return meta
    except Exception:
        return {}


# =============================================================================
# Fetch daily bars (pure 1d) + cache (single BIG file)
# =============================================================================
def _cache_daily_path(symbols: List[str]) -> str:
    h = _hash_symbols(symbols)
    if CACHE_FORMAT == "parquet":
        return os.path.join(CACHE_DIR, f"tw_prices_1d_{DAILY_LOOKBACK_DAYS}d_{h}.parquet")
    return os.path.join(CACHE_DIR, f"tw_prices_1d_{DAILY_LOOKBACK_DAYS}d_{h}.csv")


def _read_daily_cache(path: str, *, ymd_hint: Optional[str] = None) -> Optional[pd.DataFrame]:
    """
    ✅ 修正：如果 cache 的最大日期 < ymd_hint，視為「cache 太舊」→ 忽略 cache 重新下載
    （避免你今天跑還卡在昨天/前天的 cache）
    """
    if not os.path.exists(path):
        return None
    try:
        if path.endswith(".parquet"):
            df = pd.read_parquet(path)
        else:
            df = pd.read_csv(path)
        need = {"symbol", "date", "open", "high", "low", "close", "volume"}
        if not need.issubset(set(df.columns)):
            return None

        # cache stale check
        if ymd_hint:
            try:
                dmax = pd.to_datetime(df["date"].astype(str).str.slice(0, 10), errors="coerce").max()
                yh = pd.to_datetime(str(ymd_hint)[:10], errors="coerce")
                if pd.notna(dmax) and pd.notna(yh) and dmax.normalize() < yh.normalize():
                    return None
            except Exception:
                pass

        return df
    except Exception:
        return None


def _write_daily_cache(df: pd.DataFrame, path: str) -> None:
    try:
        if path.endswith(".parquet"):
            df.to_parquet(path, index=False)
        else:
            df.to_csv(path, index=False, encoding="utf-8-sig")
    except Exception:
        pass


def _download_batch(tickers: List[str]) -> Tuple[pd.DataFrame, List[str]]:
    """
    下載一批 tickers 的日K，回傳：
    - long-format df
    - failed symbols list
    """
    if not tickers:
        return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"]), []

    tickers_str = " ".join(tickers)

    df = yf.download(
        tickers=tickers_str,
        period=f"{DAILY_LOOKBACK_DAYS}d",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
    )

    if df is None or df.empty:
        return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"]), tickers

    rows: List[Dict[str, Any]] = []
    failed: List[str] = []

    # 單一 ticker：非 MultiIndex
    if not isinstance(df.columns, pd.MultiIndex):
        tmp = df.copy().reset_index()
        tmp.columns = [str(c).lower() for c in tmp.columns]
        if "date" not in tmp.columns and "index" in tmp.columns:
            tmp["date"] = tmp["index"]
        tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce").dt.tz_localize(None).dt.strftime("%Y-%m-%d")

        sym = tickers[0]
        if tmp["date"].notna().sum() == 0:
            failed.append(sym)
        else:
            for _, r in tmp.iterrows():
                rows.append(
                    {
                        "symbol": sym,
                        "date": r.get("date"),
                        "open": r.get("open"),
                        "high": r.get("high"),
                        "low": r.get("low"),
                        "close": r.get("close"),
                        "volume": r.get("volume"),
                    }
                )
    else:
        # MultiIndex：判斷 ticker 層級
        level1 = set([c[1] for c in df.columns])
        use_level = 1 if any(s in level1 for s in tickers[: min(3, len(tickers))]) else 0

        for sym in tickers:
            try:
                sub = df.xs(sym, axis=1, level=use_level, drop_level=False)
                if use_level == 1:
                    sub.columns = [c[0] for c in sub.columns]
                else:
                    sub.columns = [c[1] for c in sub.columns]

                tmp = sub.copy().reset_index()
                tmp.columns = [str(c).lower() for c in tmp.columns]
                if "date" not in tmp.columns and "index" in tmp.columns:
                    tmp["date"] = tmp["index"]
                tmp["date"] = pd.to_datetime(tmp["date"], errors="coerce").dt.tz_localize(None).dt.strftime("%Y-%m-%d")

                if tmp.get("close") is None or pd.to_numeric(tmp["close"], errors="coerce").notna().sum() == 0:
                    failed.append(sym)
                    continue

                for _, r in tmp.iterrows():
                    rows.append(
                        {
                            "symbol": sym,
                            "date": r.get("date"),
                            "open": r.get("open"),
                            "high": r.get("high"),
                            "low": r.get("low"),
                            "close": r.get("close"),
                            "volume": r.get("volume"),
                        }
                    )
            except Exception:
                failed.append(sym)
                continue

    out = pd.DataFrame(rows)
    out = out.dropna(subset=["symbol", "date"]).sort_values(["symbol", "date"]).reset_index(drop=True)
    return out, failed


def fetch_daily_bars(symbols: List[str], *, ymd_hint: Optional[str] = None) -> Tuple[pd.DataFrame, List[str], str]:
    """
    下載多檔日K，回傳：
    - daily_df (long format)
    - failed symbols
    - cache_path
    """
    if not symbols:
        return pd.DataFrame(columns=["symbol", "date", "open", "high", "low", "close", "volume"]), [], ""

    cache_path = _cache_daily_path(symbols)

    if CACHE_ENABLED:
        dfc = _read_daily_cache(cache_path, ymd_hint=ymd_hint)
        if dfc is not None and not dfc.empty:
            return dfc, [], cache_path

    all_rows: List[pd.DataFrame] = []
    failed_all: List[str] = []

    batches = [symbols[i: i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    pbar = tqdm(batches, desc="TW daily batches", total=len(batches))

    for batch in pbar:
        dfb, failed = _download_batch(batch)
        if not dfb.empty:
            all_rows.append(dfb)
        failed_all.extend(failed)

    out = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame(
        columns=["symbol", "date", "open", "high", "low", "close", "volume"]
    )

    out = out.dropna(subset=["symbol", "date"]).sort_values(["symbol", "date"]).reset_index(drop=True)

    if CACHE_ENABLED and not out.empty and cache_path:
        _write_daily_cache(out, cache_path)
        print(f"✅ Cached daily {CACHE_FORMAT.upper()}: {cache_path} (rows={len(out)})")

    failed_unique = sorted(list(set(failed_all)))
    if failed_unique:
        print(f"⚠️ Failed downloads: {len(failed_unique)} symbols (sample up to 30): {failed_unique[:30]}")

    return out, failed_unique, cache_path


# =============================================================================
# Main entry
# =============================================================================
def run_intraday(*, slot: str, asof: str, ymd: str) -> Dict[str, Any]:
    """
    只輸出 RAW 快照：
      - snapshot_main
      - snapshot_open   (原 snapshot_emerging)

    ✅ 修正：依 ymd 推 ymd_effective（<= ymd 的最近交易日），再用該日做 snapshot
    """
    meta_map = load_tw_stock_list()
    if not meta_map:
        return {
            "market": "tw",
            "ymd": ymd,
            "ymd_effective": ymd,
            "slot": slot,
            "asof": asof,
            "generated_at": _now_iso(),
            "note": f"no stock list (missing {STOCKLIST_FILE})",
            "snapshot_main": [],
            "snapshot_open": [],
            "failed_downloads": [],
            "errors": [],
        }

    symbols_all = list(meta_map.keys())
    symbols_main: List[str] = []
    symbols_open: List[str] = []

    for sym in symbols_all:
        md = (meta_map.get(sym, {}).get("market_detail") or "").strip()
        if not md:
            continue
        if md == "emerging":
            symbols_open.append(sym)
            continue
        if INCLUDE_MARKET_DETAILS and md not in INCLUDE_MARKET_DETAILS:
            continue
        symbols_main.append(sym)

    symbols_fetch = symbols_main + (symbols_open if ENABLE_OPEN_WATCHLIST else [])
    daily, failed, cache_path = fetch_daily_bars(symbols_fetch, ymd_hint=ymd)

    if daily.empty:
        return {
            "market": "tw",
            "ymd": ymd,
            "ymd_effective": ymd,
            "slot": slot,
            "asof": asof,
            "generated_at": _now_iso(),
            "note": "daily empty (yfinance returned no data)",
            "snapshot_main": [],
            "snapshot_open": [],
            "daily_cache_path": cache_path,
            "failed_downloads": failed,
            "errors": [{"reason": "daily_empty"}],
        }

    # 數字化 + 日期清理
    daily = daily.copy()
    daily["symbol"] = daily["symbol"].astype(str).str.strip()
    daily["date"] = daily["date"].astype(str).str.slice(0, 10)
    for c in ["open", "high", "low", "close", "volume"]:
        if c in daily.columns:
            daily[c] = pd.to_numeric(daily[c], errors="coerce")

    daily_sorted = daily.sort_values(["symbol", "date"]).reset_index(drop=True)

    # ✅ ymd_effective：全市場 <= ymd 的最近交易日（若沒有 <= ymd，退回全市場最大日期）
    try:
        avail = daily_sorted.loc[daily_sorted["date"] <= ymd, "date"]
        ymd_effective = str(avail.max()) if not avail.empty else str(daily_sorted["date"].max())
        ymd_effective = (ymd_effective or "").strip()[:10]
        if not ymd_effective:
            ymd_effective = ymd
    except Exception:
        ymd_effective = ymd

    # ✅ cut 到 ymd_effective，避免未來資料干擾
    cut = daily_sorted[daily_sorted["date"] <= ymd_effective].copy()

    # ✅ last = ymd_effective 當日；prev = 該檔在 ymd_effective 前一筆交易日
    last_rows = cut[cut["date"] == ymd_effective].copy()
    prev_rows = (
        cut[cut["date"] < ymd_effective]
        .sort_values(["symbol", "date"])
        .groupby("symbol", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )

    latest_map = {r["symbol"]: r for r in last_rows.to_dict(orient="records")}
    prev_map = {r["symbol"]: r for r in prev_rows.to_dict(orient="records")}

    snapshot_main: List[Dict[str, Any]] = []
    snapshot_open: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    def _make_row(sym: str, m: Dict[str, Any], mdetail: str) -> Optional[Dict[str, Any]]:
        last_row = latest_map.get(sym)
        prev_row = prev_map.get(sym)

        # 必須同時有：ymd_effective 當日 bar + 該檔前一日 bar
        if not last_row or not prev_row:
            return None

        prev_close = _safe_float(prev_row.get("close"))
        close = _safe_float(last_row.get("close"))
        high = _safe_float(last_row.get("high"))
        low = _safe_float(last_row.get("low"))
        opn = _safe_float(last_row.get("open"))
        vol = _safe_float(last_row.get("volume"))

        if prev_close is None or close is None or prev_close <= 0:
            return None

        ret = (close - prev_close) / prev_close

        # ✅ limit_type：興櫃直接標記（保持你原本語意）
        if mdetail == "emerging":
            limit_type = "emerging_no_limit"
        else:
            limit_type = _infer_limit_type(sym, m, ymd_effective)

        return {
            "symbol": sym,
            "name": m.get("name", ""),
            "sector": m.get("sector", "") or "未分類",
            "market": m.get("market", ""),
            "market_detail": mdetail,
            "market_label": _market_label(mdetail),
            "bar_date": str(last_row.get("date") or ""),
            "prev_close": float(prev_close),
            "open": opn,
            "high": high,
            "low": low,
            "close": float(close),
            "volume": vol,
            "ret": float(ret),
            "limit_type": limit_type,
            # ✅ streak / streak_prev 會在下面 enrich 補進來
        }

    # 主榜快照
    for sym in symbols_main:
        m = meta_map.get(sym, {})
        mdetail = (m.get("market_detail") or "").strip()
        row = _make_row(sym, m, mdetail)
        if row is None:
            errors.append({"symbol": sym, "reason": "missing_prev_or_last_or_rows", "market_detail": mdetail})
            if len(errors) >= MAX_ERRORS:
                break
            continue
        snapshot_main.append(row)

    # 開放制度快照（目前 = 興櫃）
    if ENABLE_OPEN_WATCHLIST:
        for sym in symbols_open:
            m = meta_map.get(sym, {})
            row = _make_row(sym, m, "emerging")
            if row is None:
                continue
            snapshot_open.append(row)

    # ✅ enrich snapshots with indicators (streak / streak_prev etc.)
    # 指標基於 daily_sorted（含更長歷史），但 snapshot 的基準日已固定在 ymd_effective
    snapshot_main = enrich_snapshot_main(
        snapshot_main=snapshot_main,
        daily_df=daily_sorted,
        ymd_effective=ymd_effective,   # ✅ FIX: 반드시 전달
    )
    snapshot_open = enrich_snapshot_main(
        snapshot_main=snapshot_open,
        daily_df=daily_sorted,
        ymd_effective=ymd_effective,   # ✅ FIX: 반드시 전달
    )

    symbols_open_total = int(len(symbols_open))
    symbols_open_fetch = int(len(symbols_open)) if ENABLE_OPEN_WATCHLIST else 0

    # optional: daily 最大日期（debug）
    try:
        daily_max_date = str(pd.to_datetime(daily_sorted["date"], errors="coerce").max().date())
    except Exception:
        daily_max_date = ""

    payload = {
        "market": "tw",
        "ymd": ymd,
        "ymd_effective": ymd_effective,  # ✅ 新增：實際使用的交易日（<= ymd）
        "slot": slot,
        "asof": asof,
        "generated_at": _now_iso(),
        "cache_enabled": bool(CACHE_ENABLED),
        "cache_format": str(CACHE_FORMAT),
        "daily_lookback_days": int(DAILY_LOOKBACK_DAYS),
        "daily_cache_path": cache_path,
        "daily_max_date": daily_max_date,
        "batch_size": int(BATCH_SIZE),
        "filters": {
            "include_market_details": sorted(list(INCLUDE_MARKET_DETAILS)),
            "enable_open_watchlist": bool(ENABLE_OPEN_WATCHLIST),
            "no_limit_listing_days": int(NO_LIMIT_LISTING_DAYS),
            "no_limit_symbols_env_count": int(len(NO_LIMIT_SYMBOLS)),
        },
        "stats": {
            "symbols_main": int(len(symbols_main)),
            "symbols_open_total": symbols_open_total,
            "symbols_open_fetch": symbols_open_fetch,
            "symbols_fetch_total": int(len(symbols_fetch)),
            "daily_rows": int(len(daily)),
            "failed_downloads": int(len(set(failed))),
            "snapshot_main_count": int(len(snapshot_main)),
            "snapshot_open_count": int(len(snapshot_open)),
            "errors_count": int(len(errors)),
        },
        "snapshot_main": snapshot_main,
        "snapshot_open": snapshot_open,

        # 如果你還有舊 code 讀 snapshot_emerging，可以暫時打開這行：
        # "snapshot_emerging": snapshot_open,

        "failed_downloads": failed,
        "errors": errors[:MAX_ERRORS],
    }

    result = IntradayResult(
        ymd=ymd,
        slot=slot,
        asof=asof,
        total_symbols=len(symbols_fetch),
        payload=payload,
    )
    return result.payload
