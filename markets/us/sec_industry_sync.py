# markets/us/sec_industry_sync.py
# -*- coding: utf-8 -*-
"""
SEC industry (SIC) sync — Local cache first, fetch missing only (complete overwrite)

你要的行為：
✅ 優先用同資料夾的本地 JSON 快取查產業（不連線）
✅ 只有「快取找不到該公司」才去 SEC 連線補抓該公司的 SIC / 產業描述
✅ 寫回 DB（stock_info.sector），預設「只補缺」不覆蓋既有 sector
✅ 快取落地到 markets/us/sec_industry_cache.json（下次就不用再連線）
✅ company_tickers.json 放在 markets/us/company_tickers.json（你已放好）

注意：
- SEC 的 company_tickers.json 本身不含產業，只有 ticker/title/cik
- 產業（SIC）要用 SEC submissions API 以 CIK 查

環境變數：
- US_SEC_CACHE_PATH                (default: markets/us/sec_industry_cache.json)
- US_SEC_TICKERS_PATH              (default: markets/us/company_tickers.json)
- US_SEC_CACHE_TTL_DAYS            (default: 30)  # 快取多久視為過期(可選)
- US_SEC_ONLY_FILL_MISSING         (default: 1)   # 只補 sector 空值/Unknown，不覆蓋
- US_SEC_HTTP_TIMEOUT              (default: 20)
- US_SEC_SLEEP_SEC                 (default: 0.12)  # 每次打 SEC sleep
- US_SEC_MAX_FETCH                 (default: 999999)  # 本輪最多連線抓幾家（保護）
- US_SEC_USER_AGENT                (default: "GrissomQuantLab/1.0 (contact: you@example.com)")
  ※ 建議你改成真的 email；SEC 會看 User-Agent
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests

# -----------------------------------------------------------------------------
# Small logger (avoid hard dependency)
# -----------------------------------------------------------------------------
try:
    from .us_config import log  # type: ignore
except Exception:

    def log(msg: str) -> None:
        print(msg, flush=True)


# -----------------------------------------------------------------------------
# Paths / env
# -----------------------------------------------------------------------------
HERE = Path(__file__).resolve().parent


def _cache_path() -> Path:
    return Path(os.getenv("US_SEC_CACHE_PATH", str(HERE / "sec_industry_cache.json")))


def _tickers_path() -> Path:
    return Path(os.getenv("US_SEC_TICKERS_PATH", str(HERE / "company_tickers.json")))


def _ttl_days() -> int:
    return int(os.getenv("US_SEC_CACHE_TTL_DAYS", "30"))


def _only_fill_missing() -> bool:
    return os.getenv("US_SEC_ONLY_FILL_MISSING", "1").strip().lower() in ("1", "true", "yes", "y", "on")


def _timeout() -> int:
    return int(os.getenv("US_SEC_HTTP_TIMEOUT", "20"))


def _sleep_sec() -> float:
    return float(os.getenv("US_SEC_SLEEP_SEC", "0.12"))


def _max_fetch() -> int:
    return int(os.getenv("US_SEC_MAX_FETCH", "999999"))


def _user_agent() -> str:
    return os.getenv("US_SEC_USER_AGENT", "GrissomQuantLab/1.0 (contact: you@example.com)")


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _norm_symbol(sym: str) -> str:
    s = (sym or "").strip().upper()
    # 你這邊通常會是 AAPL / BRK-B / BRK.B
    s = s.replace("/", "-")
    return s


def _is_missing_sector(v: Optional[str]) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    if not s:
        return True
    return s.lower() in ("unknown", "n/a", "na", "-", "—", "–", "none", "null")


def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _parse_iso(s: str) -> Optional[datetime]:
    try:
        # allow "...Z"
        t = s.replace("Z", "")
        return datetime.fromisoformat(t)
    except Exception:
        return None


def _cik10(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    # SEC CIK needs 10 digits, zero-padded
    s = re.sub(r"\D", "", s)
    if not s:
        return None
    return s.zfill(10)


@dataclass
class IndustryInfo:
    cik: str
    sic: Optional[str]
    sic_description: Optional[str]
    fetched_at: str  # ISO Z


# -----------------------------------------------------------------------------
# Load company_tickers.json (ticker -> cik/title)
# -----------------------------------------------------------------------------
def _load_company_tickers(path: Path) -> Dict[str, Dict[str, Any]]:
    """
    returns: { "AAPL": {"cik_str": "0000320193", "title": "...", "ticker": "AAPL"} , ... }
    """
    if not path.exists():
        raise FileNotFoundError(f"SEC tickers file not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    # SEC format: {"0": {"cik_str":..., "ticker":..., "title":...}, "1": {...}}
    out: Dict[str, Dict[str, Any]] = {}
    if isinstance(data, dict):
        for _, row in data.items():
            if not isinstance(row, dict):
                continue
            t = _norm_symbol(str(row.get("ticker", "")).strip())
            if not t:
                continue
            out[t] = row
    return out


# -----------------------------------------------------------------------------
# Local industry cache (ticker -> IndustryInfo)
# -----------------------------------------------------------------------------
def _load_industry_cache(path: Path) -> Dict[str, IndustryInfo]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}

    out: Dict[str, IndustryInfo] = {}
    for k, v in raw.items():
        if not isinstance(v, dict):
            continue
        sym = _norm_symbol(k)
        cik = _cik10(v.get("cik"))
        if not cik:
            continue
        out[sym] = IndustryInfo(
            cik=cik,
            sic=str(v.get("sic")) if v.get("sic") not in (None, "") else None,
            sic_description=str(v.get("sicDescription")) if v.get("sicDescription") not in (None, "") else None,
            fetched_at=str(v.get("fetched_at") or v.get("fetchedAt") or ""),
        )
    return out


def _save_industry_cache(path: Path, cache: Dict[str, IndustryInfo]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    out: Dict[str, Dict[str, Any]] = {}
    for sym, info in cache.items():
        out[_norm_symbol(sym)] = {
            "cik": info.cik,
            "sic": info.sic,
            "sicDescription": info.sic_description,
            "fetched_at": info.fetched_at,
        }
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")


def _is_cache_fresh(info: IndustryInfo, ttl_days: int) -> bool:
    if ttl_days <= 0:
        return True
    t = _parse_iso(info.fetched_at or "")
    if not t:
        return False
    return (datetime.utcnow() - t) <= timedelta(days=ttl_days)


# -----------------------------------------------------------------------------
# SEC fetch (missing only)
# -----------------------------------------------------------------------------
def _fetch_submissions_sic(cik10: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    returns (sic, sicDescription, err)
    """
    url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
    headers = {
        "User-Agent": _user_agent(),
        "Accept-Encoding": "gzip, deflate",
        "Host": "data.sec.gov",
    }
    try:
        r = requests.get(url, headers=headers, timeout=_timeout())
        if r.status_code != 200:
            return None, None, f"http_{r.status_code}"
        j = r.json()
        # typical keys: "sic", "sicDescription"
        sic = j.get("sic")
        sic_desc = j.get("sicDescription")
        sic_s = str(sic) if sic not in (None, "") else None
        sic_desc_s = str(sic_desc) if sic_desc not in (None, "") else None
        return sic_s, sic_desc_s, None
    except Exception as e:
        return None, None, f"exception:{e}"


# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------
def _ensure_stock_info_sector_column(conn: sqlite3.Connection) -> None:
    # 你的 schema 已有 sector，但保險
    cols = [r[1] for r in conn.execute("PRAGMA table_info(stock_info)").fetchall()]
    if "sector" not in cols:
        conn.execute("ALTER TABLE stock_info ADD COLUMN sector TEXT")
        conn.commit()


def _read_symbols(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    """
    returns {symbol: {"name":..., "sector":...}}
    """
    _ensure_stock_info_sector_column(conn)
    out: Dict[str, Dict[str, Any]] = {}
    for sym, name, sector in conn.execute("SELECT symbol, name, sector FROM stock_info").fetchall():
        if not sym:
            continue
        out[_norm_symbol(sym)] = {"name": name or "Unknown", "sector": sector}
    return out


def _update_sector(conn: sqlite3.Connection, symbol: str, sector: str) -> None:
    conn.execute("UPDATE stock_info SET sector = ? WHERE symbol = ?", (sector, symbol))


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
def sync_sec_industry(db_path: str) -> Dict[str, Any]:
    """
    主要給 downloader_us.py 呼叫的 API
    - 讀 markets/us/company_tickers.json（本地）
    - 讀 markets/us/sec_industry_cache.json（本地）
    - 對 DB stock_info.symbol 做「本地快取優先」，缺的才連線補
    - 寫回 DB（預設只補缺）
    """
    res: Dict[str, Any] = {
        "enabled": True,
        "db_path": db_path,
        "tickers_path": str(_tickers_path()),
        "cache_path": str(_cache_path()),
        "only_fill_missing": _only_fill_missing(),
        "ttl_days": _ttl_days(),
        "fetched": 0,
        "cache_hit": 0,
        "cache_stale": 0,
        "updated_db": 0,
        "failed": 0,
        "skipped_db_already_has_sector": 0,
        "missing_cik": 0,
        "notes": [],
    }

    tp = _tickers_path()
    cp = _cache_path()

    company = _load_company_tickers(tp)
    cache = _load_industry_cache(cp)
    ttl = _ttl_days()

    if not os.path.exists(db_path):
        res["status"] = "db_not_found"
        res["enabled"] = True
        res["notes"].append("db_not_found")
        return res

    conn = sqlite3.connect(db_path, timeout=120)
    try:
        symbols = _read_symbols(conn)

        # 要處理的 symbol 數量
        targets = list(symbols.keys())
        res["targets"] = len(targets)

        # 只補缺：若 DB 已有 sector 就直接跳過
        for sym in targets:
            db_sector = symbols[sym].get("sector")
            if _only_fill_missing() and not _is_missing_sector(db_sector):
                res["skipped_db_already_has_sector"] += 1
                continue

            # 先用本地 industry cache
            info = cache.get(sym)
            if info and _is_cache_fresh(info, ttl) and (info.sic_description or info.sic):
                res["cache_hit"] += 1
                sector = info.sic_description or f"SIC {info.sic}"
                _update_sector(conn, sym, sector)
                res["updated_db"] += 1
                continue

            # 有 cache 但過期 or 沒資料：算 stale（但仍可能用它避免連線？這裡照你需求：沒有才連線）
            if info:
                res["cache_stale"] += 1
                # 若你想「過期也先用」可改這裡；目前：過期就視為需要連線補新
            # 沒有 cache：需要連線（但要先有 CIK）
            row = company.get(sym)
            cik = _cik10(row.get("cik_str") if row else None)
            if not cik:
                res["missing_cik"] += 1
                continue

            # 限制本輪連線抓取數
            if res["fetched"] >= _max_fetch():
                res["notes"].append(f"max_fetch_reached:{_max_fetch()}")
                break

            sic, sic_desc, err = _fetch_submissions_sic(cik)
            res["fetched"] += 1
            time.sleep(_sleep_sec())

            if err:
                res["failed"] += 1
                # 失敗不寫 DB、不污染 cache（也可以寫 error cache，但先保持乾淨）
                continue

            # 更新 cache
            cache[sym] = IndustryInfo(
                cik=cik,
                sic=sic,
                sic_description=sic_desc,
                fetched_at=_now_iso(),
            )

            # 寫 DB
            if sic_desc or sic:
                sector = sic_desc or f"SIC {sic}"
                _update_sector(conn, sym, sector)
                res["updated_db"] += 1

        conn.commit()
    finally:
        conn.close()

    # 落地 cache
    try:
        _save_industry_cache(cp, cache)
        res["cache_saved"] = True
    except Exception as e:
        res["cache_saved"] = False
        res["notes"].append(f"cache_save_failed:{e}")

    res["status"] = "ok"
    return res


# -----------------------------------------------------------------------------
# CLI quick test
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    db = os.getenv("US_DB_PATH", str(HERE / "us_stock_warehouse.db"))
    out = sync_sec_industry(db)
    log(json.dumps(out, ensure_ascii=False, indent=2))
