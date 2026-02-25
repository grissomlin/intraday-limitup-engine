# markets/ca/ca_snapshot.py
# -*- coding: utf-8 -*-
"""DB -> raw snapshot builder for CA (open movers style, like UK).

Output:
- snapshot_open: movers by close ret >= CA_RET_TH (with abs_move gate)
- peers_by_sector: non-movers peers (for sector pages)
- move_band / move_key (render layer translates via i18n)

✅ Time fix in this revision:
- generated_at uses UTC (Z) to avoid local machine timezone leakage
- meta.time is built by shared North America builder:
  supports America/New_York / America/Toronto / America/Vancouver via CA_MARKET_TZ

✅ Logging time (optional):
- If CA_LOG_TZ is set (or fallback to CA_MARKET_TZ), log timestamps will use that tz.
- Otherwise logs fall back to local machine time.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# ✅ shared NA time builder
from markets.common.time_builders import build_meta_time_america

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


# =============================================================================
# Logging
# =============================================================================
def _log_now_str() -> str:
    """
    Prefer CA_LOG_TZ (or CA_MARKET_TZ) if ZoneInfo is available.
    Otherwise fallback to local pd.Timestamp.now().
    """
    tz_name = (os.getenv("CA_LOG_TZ") or os.getenv("CA_MARKET_TZ") or "").strip()
    if tz_name and ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo(tz_name)).strftime("%H:%M:%S")
        except Exception:
            pass
    return f"{pd.Timestamp.now():%H:%M:%S}"


def log(msg: str) -> None:
    print(f"{_log_now_str()}: {msg}", flush=True)


def _db_path() -> Path:
    return Path(os.getenv("CA_DB_PATH", os.path.join(os.path.dirname(__file__), "ca_stock_warehouse.db")))


# =============================================================================
# shared helpers
# =============================================================================
try:
    from scripts.render_images_common.move_bands import move_badge  # (band, key)
except Exception:

    def move_badge(ret: float) -> Tuple[int, str]:
        try:
            r = float(ret)
        except Exception:
            return -1, ""
        if r >= 1.00:
            return 5, "move_band_5"
        if r >= 0.50:
            return 4, "move_band_4"
        if r >= 0.40:
            return 3, "move_band_3"
        if r >= 0.30:
            return 2, "move_band_2"
        if r >= 0.20:
            return 1, "move_band_1"
        if r >= 0.10:
            return 0, "move_band_0"
        return -1, ""


try:
    from scripts.render_images_common.i18n import t as _t  # type: ignore
except Exception:

    def _t(lang: str, key: str, default: str = "", **kwargs: Any) -> str:
        try:
            return (default or key).format(**kwargs)
        except Exception:
            return default or key


# =============================================================================
# Config
# =============================================================================
CA_RET_TH = float(os.getenv("CA_RET_TH", "0.10"))
CA_TOUCH_TH = float(os.getenv("CA_TOUCH_TH", "0.10"))
CA_ROWS_PER_BOX = int(os.getenv("CA_ROWS_PER_BOX", "6"))
CA_PEER_EXTRA_PAGES = int(os.getenv("CA_PEER_EXTRA_PAGES", "1"))
CA_BADGE_FALLBACK_LANG = (os.getenv("CA_BADGE_FALLBACK_LANG", "en") or "en").strip().lower()

# ✅ NEW: absolute move gate (avoid penny 1-tick noise)
# Example: prev_close=0.05 -> close=0.06 is +20% but only +0.01, too noisy.
CA_MIN_ABS_MOVE = float(os.getenv("CA_MIN_ABS_MOVE", "0.02"))

# ✅ choose CA market timezone (DEFAULT: Toronto = market standard)
CA_MARKET_TZ = (os.getenv("CA_MARKET_TZ") or "America/Toronto").strip()


# =============================================================================
# Helpers
# =============================================================================
def _pick_latest_leq(conn: sqlite3.Connection, ymd: str) -> Optional[str]:
    row = conn.execute("SELECT MAX(date) FROM stock_prices WHERE date <= ?", (ymd,)).fetchone()
    return row[0] if row and row[0] else None


def _ceil_div(a: int, b: int) -> int:
    return (a + b - 1) // b if b > 0 else 0


# =============================================================================
# Main
# =============================================================================
def run_intraday(slot: str, asof: str, ymd: str, db_path: Optional[Path] = None) -> Dict[str, Any]:
    # ✅ single UTC timestamp for this payload (avoid local TZ leakage)
    dt_utc = datetime.now(timezone.utc)

    db_path = db_path or _db_path()
    if isinstance(db_path, str):
        db_path = Path(db_path)

    if not db_path.exists():
        raise FileNotFoundError(f"CA DB not found: {db_path} (set CA_DB_PATH to override)")

    conn = sqlite3.connect(str(db_path))
    try:
        ymd_effective = _pick_latest_leq(conn, ymd) or ymd
        log(f"🕒 requested ymd={ymd} slot={slot} asof={asof}")
        log(f"📅 ymd_effective = {ymd_effective}")

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
        grp AS (
          SELECT
            symbol,
            date,
            open, high, low, close, volume,
            prev_close,
            ret,
            hit,
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
        WHERE i.market='CA' AND f.date = ?
        """
        df = pd.read_sql_query(sql, conn, params=(ymd_effective, CA_RET_TH, ymd_effective))
    finally:
        conn.close()

    # ✅ build meta.time even when no rows (overview won't fall back wrongly)
    meta_time = build_meta_time_america(dt_utc, tz_name=CA_MARKET_TZ)

    if df.empty:
        return {
            "market": "ca",
            "slot": slot,
            "asof": asof,
            "ymd": ymd,
            "ymd_effective": ymd_effective,
            "generated_at": dt_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "filters": {
                "enable_open_watchlist": True,
                "ret_th": CA_RET_TH,
                "touch_th": CA_TOUCH_TH,
                "min_abs_move": CA_MIN_ABS_MOVE,
                "rows_per_box": CA_ROWS_PER_BOX,
                "peer_extra_pages": CA_PEER_EXTRA_PAGES,
            },
            "stats": {"snapshot_main_count": 0, "snapshot_open_count": 0},
            "snapshot_main": [],
            "snapshot_open": [],
            "peers_by_sector": {},
            "peers_not_limitup": [],
            "errors": [{"reason": "no_rows_for_ymd_effective"}],
            "meta": {"db_path": str(db_path), "ymd_effective": ymd_effective, "time": meta_time},
        }

    # -------------------------------------------------------------------------
    # Normalize fields
    # -------------------------------------------------------------------------
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

    # -------------------------------------------------------------------------
    # ✅ Absolute moves (close & high)
    # -------------------------------------------------------------------------
    df["abs_move"] = (df["close"] - df["prev_close"]).astype(float)
    df["abs_move_high"] = (df["high"] - df["prev_close"]).astype(float)

    # -------------------------------------------------------------------------
    # touch / hit logic with abs_move gate
    # -------------------------------------------------------------------------
    df["touch_ret"] = (df["high"] / df["prev_close"]) - 1.0

    df["touched_th"] = (
        df["touch_ret"].notna()
        & (df["touch_ret"] >= CA_TOUCH_TH)
        & df["abs_move_high"].notna()
        & (df["abs_move_high"] >= CA_MIN_ABS_MOVE)
    )

    df["hit_close"] = (
        df["ret"].notna()
        & (df["ret"] >= CA_RET_TH)
        & df["abs_move"].notna()
        & (df["abs_move"] >= CA_MIN_ABS_MOVE)
    )

    df["touched_only"] = df["touched_th"] & (~df["hit_close"])

    # streak normalize
    df.loc[~df["hit_close"], "streak"] = 0
    mask_first = df["hit_close"] & (df["hit_prev"] == 0)
    df.loc[mask_first, "streak"] = 1

    # badges
    badges = df["ret"].fillna(0.0).apply(lambda x: move_badge(float(x)))
    df["move_band"] = badges.apply(lambda t: int(t[0]) if t and len(t) >= 1 else -1)
    df["move_key"] = badges.apply(lambda t: str(t[1]) if t and len(t) >= 2 else "")

    df["badge_level"] = df["move_band"].where(df["move_band"] >= 0, 0).astype(int)
    df["badge_text"] = df["move_key"].apply(lambda k: _t(CA_BADGE_FALLBACK_LANG, k, default="") if k else "")

    # -------------------------------------------------------------------------
    # snapshot_open (movers + touched-only)
    # -------------------------------------------------------------------------
    snapshot_open: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        prev_close = float(r["prev_close"] or 0.0)
        ret = float(r.get("ret") or 0.0)

        streak = int(r.get("streak") or 0)
        streak_prev = int(r.get("streak_prev") or 0)
        hit_prev = int(r.get("hit_prev") or 0)

        touched_only = bool(r.get("touched_only") or False)
        hit_today = bool(r.get("hit_close") or False)

        parts: List[str] = []
        if touched_only:
            parts.append(f"touched ≥{int(CA_TOUCH_TH * 100)}% & abs≥{CA_MIN_ABS_MOVE:.2f}")
        elif hit_today and hit_prev == 1:
            parts.append(f"{int(CA_RET_TH * 100)}%+ & abs≥{CA_MIN_ABS_MOVE:.2f} streak: {streak}")
            parts.append(f"prev streak: {streak_prev}")
        elif hit_today and hit_prev == 0:
            parts.append(f"close ≥{int(CA_RET_TH * 100)}% & abs≥{CA_MIN_ABS_MOVE:.2f}")
            parts.append("prev not hit")

        snapshot_open.append(
            {
                "symbol": str(r["symbol"]),
                "name": str(r["name"]),
                "sector": str(r["sector"]),
                "market": "CA",
                "market_detail": str(r.get("market_detail") or "Unknown"),
                "market_label": str(r.get("market_detail") or "Unknown"),
                "bar_date": str(r["ymd"]),
                "prev_close": prev_close,
                "open": float(r.get("open") or 0.0),
                "high": float(r.get("high") or 0.0),
                "low": float(r.get("low") or 0.0),
                "close": float(r.get("close") or 0.0),
                "volume": int(r.get("volume") or 0),
                "ret": ret,
                "touch_ret": float(r.get("touch_ret") or 0.0),
                "touched_only": bool(touched_only),
                "streak": int(streak),
                "streak_prev": int(streak_prev),
                "hit_prev": int(hit_prev),
                "move_band": int(r.get("move_band") if r.get("move_band") is not None else -1),
                "move_key": str(r.get("move_key") or ""),
                "badge_text": str(r.get("badge_text") or ""),
                "badge_level": int(r.get("badge_level") or 0),
                "abs_move": float(r.get("abs_move") or 0.0),
                "abs_move_high": float(r.get("abs_move_high") or 0.0),
                "limit_type": "open_limit",
                "status_text": " | ".join(parts),
            }
        )

    # -------------------------------------------------------------------------
    # peers_by_sector (sector pages)
    # NOTE: df already has abs_move gate applied via hit/touch definitions.
    # We keep all non-hit rows as peers (so sector pages have context),
    # but they still carry abs_move fields for debug.
    # -------------------------------------------------------------------------
    df_sort = df.copy()
    df_sort["ret_sort"] = df_sort["ret"].fillna(-999.0)
    df_sort["touch_sort"] = df_sort["touch_ret"].fillna(-999.0)

    peers_by_sector: Dict[str, List[Dict[str, Any]]] = {}
    movers_cnt = df_sort[df_sort["hit_close"]].groupby("sector").size().to_dict()
    df_peers = df_sort[~df_sort["hit_close"]].copy()

    for sector, g in df_peers.groupby("sector"):
        mover_n = int(movers_cnt.get(sector, 0))
        mover_pages = max(1, _ceil_div(mover_n, CA_ROWS_PER_BOX))
        peer_cap = CA_ROWS_PER_BOX * (mover_pages + max(0, CA_PEER_EXTRA_PAGES))

        g2 = g.sort_values(["ret_sort", "touch_sort"], ascending=[False, False]).head(peer_cap)

        rows: List[Dict[str, Any]] = []
        for _, rr in g2.iterrows():
            rows.append(
                {
                    "symbol": str(rr["symbol"]),
                    "name": str(rr["name"]),
                    "sector": str(rr["sector"]),
                    "market": "CA",
                    "market_detail": str(rr.get("market_detail") or "Unknown"),
                    "market_label": str(rr.get("market_detail") or "Unknown"),
                    "bar_date": str(rr["ymd"]),
                    "prev_close": float(rr["prev_close"] or 0.0),
                    "open": float(rr.get("open") or 0.0),
                    "high": float(rr.get("high") or 0.0),
                    "low": float(rr.get("low") or 0.0),
                    "close": float(rr.get("close") or 0.0),
                    "volume": int(rr.get("volume") or 0),
                    "ret": float(rr.get("ret") or 0.0),
                    "touch_ret": float(rr.get("touch_ret") or 0.0),
                    "touched_only": bool(rr.get("touched_only") or False),
                    "streak": int(rr.get("streak") or 0),
                    "streak_prev": int(rr.get("streak_prev") or 0),
                    "hit_prev": int(rr.get("hit_prev") or 0),
                    "move_band": int(rr.get("move_band") if rr.get("move_band") is not None else -1),
                    "move_key": str(rr.get("move_key") or ""),
                    "badge_text": str(rr.get("badge_text") or ""),
                    "badge_level": int(rr.get("badge_level") or 0),
                    "abs_move": float(rr.get("abs_move") or 0.0),
                    "abs_move_high": float(rr.get("abs_move_high") or 0.0),
                    "limit_type": "open_limit",
                    "status_text": "",
                }
            )
        peers_by_sector[str(sector)] = rows

    peers_not_limitup: List[Dict[str, Any]] = []
    for _, rows in peers_by_sector.items():
        peers_not_limitup.extend(rows)

    return {
        "market": "ca",
        "slot": slot,
        "asof": asof,
        "ymd": ymd,
        "ymd_effective": ymd_effective,
        "generated_at": dt_utc.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "filters": {
            "enable_open_watchlist": True,
            "ret_th": CA_RET_TH,
            "touch_th": CA_TOUCH_TH,
            "min_abs_move": CA_MIN_ABS_MOVE,
            "rows_per_box": CA_ROWS_PER_BOX,
            "peer_extra_pages": CA_PEER_EXTRA_PAGES,
            "badge_fallback_lang": CA_BADGE_FALLBACK_LANG,
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
            "time": meta_time,
        },
    }
