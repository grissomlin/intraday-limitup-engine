# markets/us/us_snapshot.py
# -*- coding: utf-8 -*-
"""DB -> raw snapshot builder for US (split from downloader_us.py)."""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .us_config import log, _db_path


# -----------------------------------------------------------------------------
# ZoneInfo (DST-aware for America/New_York)
# -----------------------------------------------------------------------------
try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


# =============================================================================
# Tunables (env)
# =============================================================================
US_RET_TH = float(os.getenv("US_RET_TH", "0.10"))      # mover threshold (close ret)
US_TOUCH_TH = float(os.getenv("US_TOUCH_TH", "0.10"))  # touched threshold (high ret)
US_ROWS_PER_BOX = int(os.getenv("US_ROWS_PER_BOX", "6"))
US_PEER_EXTRA_PAGES = int(os.getenv("US_PEER_EXTRA_PAGES", "1"))


def _pick_latest_leq(conn: sqlite3.Connection, ymd: str) -> Optional[str]:
    row = conn.execute("SELECT MAX(date) FROM stock_prices WHERE date <= ?", (ymd,)).fetchone()
    return row[0] if row and row[0] else None


def _move_badge(ret: float) -> Tuple[str, int]:
    """
    10% ≤ ret < 20%：大漲
    20% ≤ ret < 30%：急漲
    30% ≤ ret < 40%：強漲
    40% ≤ ret < 50%：猛漲
    50% ≤ ret < 100%：狂漲
    ret ≥ 100%：噴出
    """
    if ret >= 1.00:
        return "噴出", 5
    if ret >= 0.50:
        return "狂漲", 4
    if ret >= 0.40:
        return "猛漲", 3
    if ret >= 0.30:
        return "強漲", 2
    if ret >= 0.20:
        return "急漲", 1
    if ret >= 0.10:
        return "大漲", 0
    return "", 0


def _ceil_div(a: int, b: int) -> int:
    return (a + b - 1) // b if b > 0 else 0


def _build_time_meta_us(*, asof: str = "", ymd_effective: str = "") -> Dict[str, Any]:
    """
    Build meta.time for overview subtitle (DST-aware).

    ✅ Design:
    - Always use "now" in US market local time (America/New_York).
    - Use UTC-now as source of truth, then convert to NY (avoid machine-local TZ issues).
    - If ZoneInfo is unavailable, return {} so renderer falls back gracefully.

    Extra:
    - Provide multiple keys for compatibility with different renderers:
      market_finished_hm, market_finished_at, market_finished_at_iso,
      market_finished_at_utc, market_finished_ymd, market_tz_offset, etc.
    """
    if ZoneInfo is None:
        return {}

    try:
        tz = ZoneInfo("America/New_York")

        # ✅ Source of truth: UTC now -> convert to market tz
        now_utc = datetime.now(timezone.utc)       # aware UTC
        now_local = now_utc.astimezone(tz)         # aware NY

        off = now_local.utcoffset()
        if off is None:
            return {}

        total_min = int(off.total_seconds() // 60)
        sign = "+" if total_min >= 0 else "-"
        hh = abs(total_min) // 60
        mm = abs(total_min) % 60

        hm = now_local.strftime("%H:%M")
        ymd_local = now_local.strftime("%Y-%m-%d")
        tz_off = f"{sign}{hh:02d}:{mm:02d}"

        # ✅ Common formats
        # - market_finished_at_market: NY local clock (no tz info in the string)
        market_finished_at_market = f"{ymd_local} {hm}"

        # - market_finished_at: legacy-friendly, many places slice date/time from it
        #   Keep it NY local clock string
        market_finished_at = market_finished_at_market

        # - market_finished_at_iso: NY aware ISO string (contains offset)
        market_finished_at_iso = now_local.isoformat(timespec="seconds")

        # - market_finished_at_utc: force Z suffix (avoid "+00:00" printing differences)
        market_finished_at_utc = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        return {
            # tz identity
            "market_tz": "America/New_York",
            "market_tz_offset": tz_off,  # e.g. -05:00 / -04:00 (DST)

            # display-friendly components
            "market_finished_ymd": ymd_local,
            "market_finished_hm": hm,

            # NY local datetime string (legacy + display)
            "market_finished_at_market": market_finished_at_market,
            "market_finished_at": market_finished_at,

            # robust timestamps (best for parsing)
            "market_finished_at_iso": market_finished_at_iso,
            "market_finished_at_utc": market_finished_at_utc,

            # traceability (optional, harmless)
            "asof": str(asof or ""),
            "ymd_effective": str(ymd_effective or ""),
        }
    except Exception:
        return {}


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def run_intraday(slot: str, asof: str, ymd: str, db_path: Optional[Path] = None) -> Dict[str, Any]:
    db_path = db_path or _db_path()
    if isinstance(db_path, str):
        db_path = Path(db_path)

    if not db_path.exists():
        raise FileNotFoundError(f"US DB not found: {db_path} (set US_DB_PATH to override)")

    conn = sqlite3.connect(str(db_path))
    try:
        ymd_effective = _pick_latest_leq(conn, ymd) or ymd
        log(f"🕒 requested ymd={ymd} slot={slot} asof={asof}")
        log(f"📅 ymd_effective = {ymd_effective}")

        # ✅ 修復：簡化 streak 計算邏輯，使用更清晰的方法
        sql = """
        WITH p AS (
          SELECT
            symbol,
            date,
            open, high, low, close, volume,
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
          FROM stock_prices
          WHERE date <= ?
        ),
        rets AS (
          SELECT
            symbol,
            date,
            open, high, low, close, volume,
            prev_close,
            CASE
              WHEN prev_close IS NOT NULL AND prev_close > 0 AND close IS NOT NULL
              THEN (close / prev_close) - 1.0
              ELSE NULL
            END AS ret,
            CASE
              WHEN prev_close IS NOT NULL AND prev_close > 0 AND close IS NOT NULL
                   AND (close / prev_close) - 1.0 >= ?
              THEN 1 ELSE 0
            END AS hit
          FROM p
        ),
        -- ✅ 修復：使用 SUM 累加當前連續區間內的 hit 數量
        grp AS (
          SELECT
            symbol,
            date,
            open, high, low, close, volume,
            prev_close,
            ret,
            hit,
            -- g 標記連續區間：每次遇到 hit=0 時 g 遞增
            SUM(CASE WHEN hit = 0 THEN 1 ELSE 0 END)
              OVER (PARTITION BY symbol ORDER BY date ROWS UNBOUNDED PRECEDING) AS g
          FROM rets
          WHERE ret IS NOT NULL
        ),
        streaked AS (
          SELECT
            symbol,
            date,
            open, high, low, close, volume,
            prev_close,
            ret,
            hit,
            -- ✅ 關鍵修復：只計算 hit=1 的記錄數量
            -- 使用 SUM(hit) 而不是 ROW_NUMBER()，這樣只會累加 hit=1 的天數
            CASE
              WHEN hit = 1 THEN
                SUM(hit) OVER (
                  PARTITION BY symbol, g
                  ORDER BY date
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
              ELSE 0
            END AS streak
          FROM grp
        ),
        final AS (
          SELECT
            s.*,
            LAG(s.hit)    OVER (PARTITION BY s.symbol ORDER BY s.date) AS hit_prev,
            LAG(s.streak) OVER (PARTITION BY s.symbol ORDER BY s.date) AS streak_prev
          FROM streaked s
        )
        SELECT
          f.symbol,
          f.date AS ymd,
          f.open, f.high, f.low, f.close, f.volume,
          f.prev_close,
          f.ret,
          f.hit,
          f.streak,
          COALESCE(f.hit_prev, 0) AS hit_prev,
          COALESCE(f.streak_prev, 0) AS streak_prev,
          i.name,
          i.sector,
          i.market_detail
        FROM final f
        JOIN stock_info i ON i.symbol = f.symbol
        WHERE i.market='US' AND f.date = ?
        """
        df = pd.read_sql_query(sql, conn, params=(ymd_effective, US_RET_TH, ymd_effective))
    finally:
        conn.close()

    # ---- build meta.time (DST-aware) early so even empty payload has it ----
    time_meta = _build_time_meta_us(asof=asof, ymd_effective=ymd_effective)

    if df.empty:
        return {
            "market": "us",
            "slot": slot,
            "asof": asof,
            "ymd": ymd,
            "ymd_effective": ymd_effective,
            "generated_at": _utcnow_iso(),
            "filters": {
                "enable_open_watchlist": True,
                "note": "US treated as open_limit (no daily limit). Built from local DB.",
                "ret_th": US_RET_TH,
                "touch_th": US_TOUCH_TH,
                "rows_per_box": US_ROWS_PER_BOX,
                "peer_extra_pages": US_PEER_EXTRA_PAGES,
            },
            "stats": {"snapshot_main_count": 0, "snapshot_open_count": 0},
            "snapshot_main": [],
            "snapshot_open": [],
            "peers_by_sector": {},
            "peers_not_limitup": [],
            "errors": [{"reason": "no_rows_for_ymd_effective"}],
            "meta": {
                "db_path": str(db_path),
                "ymd_effective": ymd_effective,
                "time": time_meta,
            },
        }

    df["name"] = df["name"].fillna("Unknown")
    df["sector"] = df["sector"].fillna("Unknown").replace("", "Unknown")
    df["market_detail"] = df["market_detail"].fillna("Unknown")

    for col in ("prev_close", "open", "high", "low", "close", "ret"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["volume"] = pd.to_numeric(df.get("volume"), errors="coerce").fillna(0).astype(int)
    df["streak"] = pd.to_numeric(df.get("streak"), errors="coerce").fillna(0).astype(int)
    df["streak_prev"] = pd.to_numeric(df.get("streak_prev"), errors="coerce").fillna(0).astype(int)
    df["hit"] = pd.to_numeric(df.get("hit"), errors="coerce").fillna(0).astype(int)
    df["hit_prev"] = pd.to_numeric(df.get("hit_prev"), errors="coerce").fillna(0).astype(int)

    m = df["prev_close"].notna() & (df["prev_close"] > 0) & df["close"].notna()
    skipped_no_prev = int((~m).sum())
    df = df[m].copy()

    # touched (intraday high)
    df["touch_ret"] = (df["high"] / df["prev_close"]) - 1.0
    df["touched_10"] = df["touch_ret"].notna() & (df["touch_ret"] >= US_TOUCH_TH)
    df["hit_10_close"] = df["ret"].notna() & (df["ret"] >= US_RET_TH)
    df["touched_only"] = df["touched_10"] & (~df["hit_10_close"])

    # -------------------------------------------------------------------------
    # ✅ 簡化邏輯：移除 Python 層的保底修正，讓 SQL 結果直接使用
    # -------------------------------------------------------------------------
    df.loc[~df["hit_10_close"], "streak"] = 0
    mask_first = df["hit_10_close"] & (df["hit_prev"] == 0)
    df.loc[mask_first, "streak"] = 1

    # badges (純漲幅等級，不含「連」)
    badges = df["ret"].fillna(0.0).apply(lambda x: _move_badge(float(x)))
    df["badge_text"] = badges.apply(lambda t: t[0])
    df["badge_level"] = badges.apply(lambda t: int(t[1]))

    snapshot_open: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        prev_close = float(r["prev_close"] or 0.0)
        close = float(r["close"] or 0.0)
        ret = float(r.get("ret") or 0.0)

        streak = int(r.get("streak") or 0)
        streak_prev = int(r.get("streak_prev") or 0)
        hit_prev = int(r.get("hit_prev") or 0)

        touched_only = bool(r.get("touched_only") or False)
        hit_today = bool(r.get("hit_10_close") or False)

        parts: List[str] = []

        if touched_only:
            parts.append(f"今日觸及 ≥{int(US_TOUCH_TH * 100)}%（收盤未達）")
            parts.append(f"昨日未達 ≥{int(US_RET_TH * 100)}%")
        elif hit_today and hit_prev == 1:
            parts.append(f"連續 {streak} 天 ≥{int(US_RET_TH * 100)}%")
            parts.append(f"昨日已連續 {streak_prev} 天 ≥{int(US_RET_TH * 100)}%")
        elif hit_today and hit_prev == 0:
            parts.append(f"今日收盤 ≥{int(US_RET_TH * 100)}%")
            parts.append(f"昨日未達 ≥{int(US_RET_TH * 100)}%")

        status_text = "｜".join(parts)

        snapshot_open.append(
            {
                "symbol": str(r["symbol"]),
                "name": str(r["name"]),
                "sector": str(r["sector"]),
                "market": "US",
                "market_detail": str(r.get("market_detail") or "Unknown"),
                "market_label": str(r.get("market_detail") or "Unknown"),
                "bar_date": str(r["ymd"]),
                "prev_close": prev_close,
                "open": float(r.get("open") or 0.0),
                "high": float(r.get("high") or 0.0),
                "low": float(r.get("low") or 0.0),
                "close": close,
                "volume": int(r.get("volume") or 0),
                "ret": ret,
                "touch_ret": float(r.get("touch_ret") or 0.0),
                "touched_only": bool(touched_only),
                "streak": int(streak),
                "streak_prev": int(streak_prev),
                "hit_prev": int(hit_prev),
                "badge_text": str(r.get("badge_text") or ""),
                "badge_level": int(r.get("badge_level") or 0),
                "limit_type": "open_limit",
                "status_text": status_text,
            }
        )

    # peers（不變）
    df_sort = df.copy()
    df_sort["ret_sort"] = df_sort["ret"].fillna(-999.0)
    df_sort["touch_sort"] = df_sort["touch_ret"].fillna(-999.0)

    peers_by_sector: Dict[str, List[Dict[str, Any]]] = {}
    movers_cnt = df_sort[df_sort["hit_10_close"]].groupby("sector").size().to_dict()

    df_peers = df_sort[~df_sort["hit_10_close"]].copy()

    for sector, g in df_peers.groupby("sector"):
        mover_n = int(movers_cnt.get(sector, 0))
        mover_pages = max(1, _ceil_div(mover_n, US_ROWS_PER_BOX))
        peer_cap = US_ROWS_PER_BOX * (mover_pages + max(0, US_PEER_EXTRA_PAGES))

        g2 = g.sort_values(["ret_sort", "touch_sort"], ascending=[False, False]).head(peer_cap)

        rows: List[Dict[str, Any]] = []
        for _, r in g2.iterrows():
            rows.append(
                {
                    "symbol": str(r["symbol"]),
                    "name": str(r["name"]),
                    "sector": str(r["sector"]),
                    "market": "US",
                    "market_detail": str(r.get("market_detail") or "Unknown"),
                    "market_label": str(r.get("market_detail") or "Unknown"),
                    "bar_date": str(r["ymd"]),
                    "prev_close": float(r["prev_close"] or 0.0),
                    "open": float(r.get("open") or 0.0),
                    "high": float(r.get("high") or 0.0),
                    "low": float(r.get("low") or 0.0),
                    "close": float(r.get("close") or 0.0),
                    "volume": int(r.get("volume") or 0),
                    "ret": float(r.get("ret") or 0.0),
                    "touch_ret": float(r.get("touch_ret") or 0.0),
                    "touched_only": bool(r.get("touched_only") or False),
                    "streak": int(r.get("streak") or 0),
                    "streak_prev": int(r.get("streak_prev") or 0),
                    "hit_prev": int(r.get("hit_prev") or 0),
                    "limit_type": "open_limit",
                    "badge_text": str(r.get("badge_text") or ""),
                    "badge_level": int(r.get("badge_level") or 0),
                    "status_text": "",
                }
            )

        peers_by_sector[str(sector)] = rows

    peers_not_limitup: List[Dict[str, Any]] = []
    for _, rows in peers_by_sector.items():
        peers_not_limitup.extend(rows)

    return {
        "market": "us",
        "slot": slot,
        "asof": asof,
        "ymd": ymd,
        "ymd_effective": ymd_effective,
        "generated_at": _utcnow_iso(),
        "filters": {
            "enable_open_watchlist": True,
            "note": "US treated as open_limit (no daily limit). Built from local DB.",
            "ret_th": US_RET_TH,
            "touch_th": US_TOUCH_TH,
            "rows_per_box": US_ROWS_PER_BOX,
            "peer_extra_pages": US_PEER_EXTRA_PAGES,
        },
        "stats": {
            "snapshot_main_count": 0,
            "snapshot_open_count": len(snapshot_open),
            "snapshot_open_skipped_no_prev": skipped_no_prev,
            "peers_sectors": int(len(peers_by_sector)),
            "peers_flat_count": int(len(peers_not_limitup)),
        },
        "snapshot_main": [],
        "snapshot_open": snapshot_open,
        "peers_by_sector": peers_by_sector,
        "peers_not_limitup": peers_not_limitup,
        "errors": [],
        "meta": {
            "db_path": str(db_path),
            "ymd_effective": ymd_effective,
            "time": time_meta,
        },
    }