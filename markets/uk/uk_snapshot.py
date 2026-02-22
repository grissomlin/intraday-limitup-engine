# markets/uk/uk_snapshot.py
# -*- coding: utf-8 -*-
"""DB -> raw snapshot builder for UK (adapted from markets/us/us_snapshot.py).

✅ Changes vs previous draft:
- NO hard-coded Chinese move words.
- Use shared move band helper: scripts.render_images_common.move_bands
- Output move_band / move_key (render layer translates via i18n)
- Keep badge_text as English fallback (from i18n 'en' pack) for backward compatibility
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# -----------------------------------------------------------------------------
# Optional imports (if you haven't created uk_config.py yet, this file still runs)
# -----------------------------------------------------------------------------
try:
    from .uk_config import log, _db_path  # type: ignore
except Exception:

    def log(msg: str) -> None:
        print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)

    def _db_path() -> Path:
        return Path(os.getenv("UK_DB_PATH", os.path.join(os.path.dirname(__file__), "uk_stock_warehouse.db")))


# -----------------------------------------------------------------------------
# ZoneInfo (DST-aware for Europe/London)
# -----------------------------------------------------------------------------
try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


# Shared helpers
try:
    from scripts.render_images_common.move_bands import move_badge  # (band, key)
except Exception:

    def move_badge(ret: float) -> Tuple[int, str]:
        # minimal fallback: same bands
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
# Tunables (env)
# =============================================================================
UK_RET_TH = float(os.getenv("UK_RET_TH", "0.10"))       # mover threshold (close ret)
UK_TOUCH_TH = float(os.getenv("UK_TOUCH_TH", "0.10"))   # touched threshold (high ret)
UK_ROWS_PER_BOX = int(os.getenv("UK_ROWS_PER_BOX", "6"))
UK_PEER_EXTRA_PAGES = int(os.getenv("UK_PEER_EXTRA_PAGES", "1"))

# Optional: what language to use for badge_text fallback (render should translate anyway)
UK_BADGE_FALLBACK_LANG = (os.getenv("UK_BADGE_FALLBACK_LANG", "en") or "en").strip().lower()


def _pick_latest_leq(conn: sqlite3.Connection, ymd: str) -> Optional[str]:
    row = conn.execute("SELECT MAX(date) FROM stock_prices WHERE date <= ?", (ymd,)).fetchone()
    return row[0] if row and row[0] else None


def _ceil_div(a: int, b: int) -> int:
    return (a + b - 1) // b if b > 0 else 0


def _build_time_meta_uk(*, ymd_effective: str, asof: str) -> Dict[str, Any]:
    """
    Build meta.time (DST-aware) for overview subtitle.

    We intentionally:
    - Prefer Europe/London ZoneInfo to compute UTC offset (+00:00 / +01:00)
    - Use ymd_effective + asof ("HH:MM") as the display timestamp
    - If ZoneInfo is unavailable (rare on Linux/GitHub, possible on some Windows),
      return {} so render falls back to showing only Data date.
    """
    if ZoneInfo is None:
        return {}

    try:
        tz = ZoneInfo("Europe/London")

        ymd2 = str(ymd_effective)[:10]
        hm2 = str(asof)[:5] if asof else ""
        if not ymd2 or len(ymd2) < 10 or not hm2 or len(hm2) < 4:
            return {}

        # local dt with tzinfo for correct DST offset
        dt_local = datetime.fromisoformat(f"{ymd2} {hm2}").replace(tzinfo=tz)

        off = dt_local.utcoffset()
        if off is None:
            return {}

        total_min = int(off.total_seconds() // 60)
        sign = "+" if total_min >= 0 else "-"
        hh = abs(total_min) // 60
        mm = abs(total_min) % 60

        return {
            "market_tz": "Europe/London",
            "market_tz_offset": f"{sign}{hh:02d}:{mm:02d}",  # +00:00 / +01:00
            "market_finished_hm": hm2,
            # timefmt.py uses this only for extracting date; keep it simple & stable
            "market_finished_at": dt_local.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M"),
        }
    except Exception:
        return {}


def run_intraday(slot: str, asof: str, ymd: str, db_path: Optional[Path] = None) -> Dict[str, Any]:
    db_path = db_path or _db_path()
    if isinstance(db_path, str):
        db_path = Path(db_path)

    if not db_path.exists():
        raise FileNotFoundError(f"UK DB not found: {db_path} (set UK_DB_PATH to override)")

    conn = sqlite3.connect(str(db_path))
    try:
        ymd_effective = _pick_latest_leq(conn, ymd) or ymd
        log(f"🕒 requested ymd={ymd} slot={slot} asof={asof}")
        log(f"📅 ymd_effective = {ymd_effective}")

        # streak 計算：hit=close ret >= TH，區間累加（沿用 US SQL pattern）
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
        WHERE i.market='UK' AND f.date = ?
        """
        df = pd.read_sql_query(sql, conn, params=(ymd_effective, UK_RET_TH, ymd_effective))
    finally:
        conn.close()

    # ---- build meta.time (DST-aware) early so even empty payload has it ----
    time_meta = _build_time_meta_uk(ymd_effective=str(ymd_effective)[:10], asof=str(asof))

    if df.empty:
        return {
            "market": "uk",
            "slot": slot,
            "asof": asof,
            "ymd": ymd,
            "ymd_effective": ymd_effective,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "filters": {
                "enable_open_watchlist": True,
                "note": "UK treated as open_limit (no daily limit). Built from local DB.",
                "ret_th": UK_RET_TH,
                "touch_th": UK_TOUCH_TH,
                "rows_per_box": UK_ROWS_PER_BOX,
                "peer_extra_pages": UK_PEER_EXTRA_PAGES,
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

    # normalize fields
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
    df["touched_10"] = df["touch_ret"].notna() & (df["touch_ret"] >= UK_TOUCH_TH)
    df["hit_10_close"] = df["ret"].notna() & (df["ret"] >= UK_RET_TH)
    df["touched_only"] = df["touched_10"] & (~df["hit_10_close"])

    # semantic correction: if not hit today -> streak=0; if hit today but prev not -> streak=1
    df.loc[~df["hit_10_close"], "streak"] = 0
    mask_first = df["hit_10_close"] & (df["hit_prev"] == 0)
    df.loc[mask_first, "streak"] = 1

    # move band / key (no text here; render should translate)
    badges = df["ret"].fillna(0.0).apply(lambda x: move_badge(float(x)))
    df["move_band"] = badges.apply(lambda t: int(t[0]) if t and len(t) >= 1 else -1)
    df["move_key"] = badges.apply(lambda t: str(t[1]) if t and len(t) >= 2 else "")

    # backward compatible fields
    # badge_level: reuse band (>=0). badge_text: translated string by i18n (fallback lang only)
    df["badge_level"] = df["move_band"].where(df["move_band"] >= 0, 0).astype(int)
    df["badge_text"] = df["move_key"].apply(lambda k: _t(UK_BADGE_FALLBACK_LANG, k, default="") if k else "")

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
            parts.append(f"touched ≥{int(UK_TOUCH_TH * 100)}% (close < {int(UK_RET_TH * 100)}%)")
            parts.append(f"prev close < {int(UK_RET_TH * 100)}%")
        elif hit_today and hit_prev == 1:
            parts.append(f"{int(UK_RET_TH * 100)}%+ streak: {streak}")
            parts.append(f"prev streak: {streak_prev}")
        elif hit_today and hit_prev == 0:
            parts.append(f"close ≥{int(UK_RET_TH * 100)}%")
            parts.append("prev not hit")

        status_text = " | ".join(parts)

        snapshot_open.append(
            {
                "symbol": str(r["symbol"]),
                "name": str(r["name"]),
                "sector": str(r["sector"]),
                "market": "UK",
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
                # ✅ new (preferred)
                "move_band": int(r.get("move_band") if r.get("move_band") is not None else -1),
                "move_key": str(r.get("move_key") or ""),
                # ✅ backward compatible
                "badge_text": str(r.get("badge_text") or ""),
                "badge_level": int(r.get("badge_level") or 0),
                "limit_type": "open_limit",
                "status_text": status_text,
            }
        )

    # peers (same logic as US open-limit style)
    df_sort = df.copy()
    df_sort["ret_sort"] = df_sort["ret"].fillna(-999.0)
    df_sort["touch_sort"] = df_sort["touch_ret"].fillna(-999.0)

    peers_by_sector: Dict[str, List[Dict[str, Any]]] = {}
    movers_cnt = df_sort[df_sort["hit_10_close"]].groupby("sector").size().to_dict()

    df_peers = df_sort[~df_sort["hit_10_close"]].copy()

    for sector, g in df_peers.groupby("sector"):
        mover_n = int(movers_cnt.get(sector, 0))
        mover_pages = max(1, _ceil_div(mover_n, UK_ROWS_PER_BOX))
        peer_cap = UK_ROWS_PER_BOX * (mover_pages + max(0, UK_PEER_EXTRA_PAGES))

        g2 = g.sort_values(["ret_sort", "touch_sort"], ascending=[False, False]).head(peer_cap)

        rows: List[Dict[str, Any]] = []
        for _, rr in g2.iterrows():
            rows.append(
                {
                    "symbol": str(rr["symbol"]),
                    "name": str(rr["name"]),
                    "sector": str(rr["sector"]),
                    "market": "UK",
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
                    # ✅ new
                    "move_band": int(rr.get("move_band") if rr.get("move_band") is not None else -1),
                    "move_key": str(rr.get("move_key") or ""),
                    # ✅ backward compatible
                    "badge_text": str(rr.get("badge_text") or ""),
                    "badge_level": int(rr.get("badge_level") or 0),
                    "limit_type": "open_limit",
                    "status_text": "",
                }
            )

        peers_by_sector[str(sector)] = rows

    peers_not_limitup: List[Dict[str, Any]] = []
    for _, rows in peers_by_sector.items():
        peers_not_limitup.extend(rows)

    return {
        "market": "uk",
        "slot": slot,
        "asof": asof,
        "ymd": ymd,
        "ymd_effective": ymd_effective,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "filters": {
            "enable_open_watchlist": True,
            "note": "UK treated as open_limit (no daily limit). Built from local DB.",
            "ret_th": UK_RET_TH,
            "touch_th": UK_TOUCH_TH,
            "rows_per_box": UK_ROWS_PER_BOX,
            "peer_extra_pages": UK_PEER_EXTRA_PAGES,
            "badge_fallback_lang": UK_BADGE_FALLBACK_LANG,
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


if __name__ == "__main__":
    # Quick test:
    # set UK_DB_PATH=...
    # python -m markets.uk.uk_snapshot
    res = run_intraday(
        slot="test",
        asof=datetime.now().strftime("%H:%M"),
        ymd=datetime.now().strftime("%Y-%m-%d"),
    )
    print("snapshot_open =", len(res.get("snapshot_open") or []))
    print("meta.time =", (res.get("meta") or {}).get("time"))