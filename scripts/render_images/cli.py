# scripts/render_images/cli.py
# -*- coding: utf-8 -*-
"""
CLI entry point for rendering images
- Overview: matplotlib (dark only; pagination)
- Sector blocks: matplotlib (no kaleido)
- Outputs list.txt for stitching videos

Updates:
- Prefer *.payload.agg.json when auto-finding latest payload
- Use ymd_effective (if present) for output folder naming
- Accept both payload.json and payload.agg.json explicitly

✅ 2026-01 新增：
- Overview (dark) 自動分頁：僅當「有漲停的產業數 > overview_top_n」才多頁
  - page_size = overview_top_n
  - 檔名：單頁 -> overview_sectors_top15.png
          多頁 -> overview_sectors_top15_p1.png, p2...
  - list.txt 會自動包含所有 overview 頁面（排在 sector blocks 前面）

✅ 2026-01-24 新增：
- Disclaimer 最後頁：render_disclaimer_page() 產出 overview_disclaimer.png
  - 預設啟用；可用 --skip-disclaimer 關閉
  - 可用 --market TW/HK/CN/US/JP/KR 指定語言組合
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, List

# ✅ overview_mpl.render_overview_png 已改成：
#    render_overview_png(payload, out_dir, width, height, page_size) -> List[Path]
from .overview_mpl import render_overview_png
from .sector_blocks_mpl import render_sector_blocks

# ✅ NEW: text status report (for X / debugging)
from .status_report import write_tw_status_txt
# ✅ NEW: detailed status report with price info
from .detailed_status_report import write_detailed_status_reports, generate_for_x_post

# ✅ NEW: disclaimer page
from .disclaimer_mpl import render_disclaimer_page


# -------------------------
# Helpers
# -------------------------
REPO_ROOT = Path(__file__).resolve().parents[2]


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _auto_find_latest_payload(repo_root: Path, slot: str = "midday") -> Optional[Path]:
    """
    Prefer aggregated payload (*.payload.agg.json) if exists; fallback to *.payload.json
    """
    base = repo_root / "data" / "cache" / "tw"
    if not base.exists():
        return None

    cand_agg = sorted(base.glob(f"*/{slot}.payload.agg.json"), key=lambda p: p.parent.name)
    if cand_agg:
        return cand_agg[-1]

    cand_raw = sorted(base.glob(f"*/{slot}.payload.json"), key=lambda p: p.parent.name)
    return cand_raw[-1] if cand_raw else None


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_list_txt(out_dir: Path, paths: List[Path]) -> None:
    """
    Write relative paths into out_dir/list.txt (forward slashes)
    """
    try:
        rels = [str(p.relative_to(out_dir)).replace("\\", "/") for p in paths]
        (out_dir / "list.txt").write_text("\n".join(rels), encoding="utf-8")
    except Exception as e:
        print(f"⚠️ write list.txt failed: {e}")


def _pick_ymd_for_output(payload: Dict[str, Any]) -> str:
    """
    Prefer ymd_effective if present; fallback to ymd.
    """
    y = str(payload.get("ymd_effective") or "").strip()
    if y:
        return y
    y = str(payload.get("ymd") or "").strip()
    return y or "unknown_ymd"


def _pick_market(payload: Dict[str, Any], args_market: str) -> str:
    """
    Determine market code for disclaimer language mapping.
    Priority:
      1) CLI --market
      2) payload common keys (market/region/exchange/country)
      3) env RENDER_MARKET
      4) fallback "TW"
    """
    m = (args_market or "").strip().upper()
    if m:
        return m

    # Try payload keys (best-effort; your TW payload may not have these yet)
    for k in ("market", "exchange", "region", "country", "market_code"):
        v = payload.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip().upper()

    # Some payloads nest meta info
    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    if isinstance(meta, dict):
        for k in ("market", "exchange", "region", "country", "market_code"):
            v = meta.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip().upper()

    env_m = os.getenv("RENDER_MARKET", "").strip().upper()
    if env_m:
        return env_m

    return "TW"


# -------------------------
# Main
# -------------------------
def main():
    ap = argparse.ArgumentParser(
        description="Render limitup images (overview + sector blocks + optional disclaimer) [matplotlib-only]"
    )

    ap.add_argument(
        "--payload",
        type=str,
        default="",
        help="Payload JSON path (e.g., data/cache/tw/2026-01-18/midday.payload.agg.json)",
    )
    ap.add_argument(
        "--slot",
        type=str,
        default="midday",
        help="Used when --payload empty; auto find latest slot payload",
    )
    ap.add_argument(
        "--out",
        type=str,
        default="",
        help="Output dir (default: media/images/<ymd_effective>/<slot>)",
    )

    # Image settings
    ap.add_argument("--format", type=str, default="png", choices=["png", "jpg", "jpeg"])
    ap.add_argument("--width", type=int, default=1080, help="Image width (px)")
    ap.add_argument("--height", type=int, default=1920, help="Image height (px)")

    # Overview settings
    ap.add_argument(
        "--overview-top-n",
        type=int,
        default=15,
        help="Overview: sectors per page (default 15). Auto paginate when sectors_with_limitup > this number.",
    )
    ap.add_argument(
        "--overview-theme",
        type=str,
        default="dark",
        choices=["dark", "light"],
        help="Theme for images (overview/sector/disclaimer).",
    )

    # -------------------------
    # Sector block settings
    # -------------------------
    ap.add_argument(
        "--sectors-per-list",
        type=int,
        default=0,
        help="How many sectors to generate (0=ALL sectors that have limitup)",
    )

    # ✅ 改成 7 / 7 的永久預設值
    ap.add_argument(
        "--rows-per-page",
        type=int,
        default=7,
        help="Limitup rows per page (recommended 6~8; larger => tighter spacing)",
    )
    ap.add_argument(
        "--peers-max-per-page",
        type=int,
        default=7,
        help="Peers (未漲停) rows per page (recommended 6~10)",
    )

    ap.add_argument("--skip-overview", action="store_true", help="Skip overview image generation")
    ap.add_argument("--skip-sectors", action="store_true", help="Skip sector block images")

    # ✅ NEW: status txt
    ap.add_argument(
        "--skip-status-txt",
        action="store_true",
        help="Skip writing tw_status.txt (default: enabled)",
    )

    # ✅ NEW: detailed report
    ap.add_argument(
        "--skip-detailed-report",
        action="store_true",
        help="Skip writing detailed status report (default: enabled)",
    )

    # ✅ NEW: disclaimer page
    ap.add_argument(
        "--skip-disclaimer",
        action="store_true",
        help="Skip rendering disclaimer page (default: enabled).",
    )
    ap.add_argument(
        "--market",
        type=str,
        default="",
        help="Market code for disclaimer language: TW/HK/CN/US/JP/KR. (default: infer from payload/env; fallback TW)",
    )
    ap.add_argument(
        "--disclaimer-filename",
        type=str,
        default="overview_disclaimer.png",
        help="Disclaimer output filename (default: overview_disclaimer.png)",
    )

    args = ap.parse_args()

    fmt = "jpg" if args.format == "jpeg" else args.format
    if fmt != "png":
        print("⚠️ sector_blocks_mpl 固定輸出 png（避免字體與透明度差異）；overview 仍使用指定格式。")
        print("⚠️ 但目前 overview(分頁版) 固定輸出 png（檔名內不含 fmt），建議不要用 jpg。")

    # ---- Locate payload
    if args.payload:
        payload_path = Path(args.payload)
        if not payload_path.is_absolute():
            payload_path = (REPO_ROOT / payload_path).resolve()
    else:
        payload_path = _auto_find_latest_payload(REPO_ROOT, slot=args.slot)

    if not payload_path or not payload_path.exists():
        raise FileNotFoundError(
            f"找不到 payload。請給 --payload，或確認 data/cache/tw/*/{args.slot}.payload(.agg).json 存在。"
        )

    payload = _read_json(payload_path)
    ymd_out = _pick_ymd_for_output(payload)
    slot = payload.get("slot") or args.slot

    # ---- Output dir
    if args.out:
        out_dir = Path(args.out)
        if not out_dir.is_absolute():
            out_dir = (REPO_ROOT / out_dir).resolve()
    else:
        out_dir = (REPO_ROOT / "media" / "images" / ymd_out / str(slot)).resolve()

    _ensure_dir(out_dir)

    print(f"[render_images] payload = {payload_path}")
    print(f"[render_images] out_dir  = {out_dir}")
    print(f"[render_images] rows-per-page={args.rows_per_page}, peers-max-per-page={args.peers_max_per_page}")

    # ✅ 原有的 status txt（保持相容性）
    env_write = os.getenv("RENDER_WRITE_TW_STATUS", "1").strip().lower() in ("1", "true", "yes", "y", "on")
    if (not args.skip_status_txt) and env_write:
        try:
            promo = os.getenv("TW_STATUS_PROMO", "").strip()
            p = write_tw_status_txt(
                payload,
                out_dir,
                filename=os.getenv("TW_STATUS_FILENAME", "tw_status_short.txt"),
                top_sectors=int(os.getenv("TW_STATUS_TOP_SECTORS", str(args.overview_top_n))),
                top_each_bucket=int(os.getenv("TW_STATUS_TOP_EACH", "20")),
                include_peers=os.getenv("TW_STATUS_INCLUDE_PEERS", "0").strip().lower() in ("1", "true", "yes", "y", "on"),
                top_peers=int(os.getenv("TW_STATUS_TOP_PEERS", "15")),
                promo_line=promo,
            )
            print(f"✅ wrote status txt: {p}")
        except Exception as e:
            print(f"⚠️ write tw_status.txt failed: {e}")

    # ✅ NEW: 生成詳細報告（包含鎖1產業明細和價格詳情）
    if not args.skip_detailed_report:
        try:
            max_sectors = int(os.getenv("DETAILED_MAX_SECTORS", "20"))
            include_debug = os.getenv("DETAILED_INCLUDE_DEBUG", "1").strip().lower() in ("1", "true", "yes", "y", "on")

            short_path, full_path = write_detailed_status_reports(
                payload,
                out_dir,
                short_filename="x_post.txt",
                full_filename="detailed_status.txt",
                max_sectors=max_sectors,
                include_price_details=True,
                include_debug_info=include_debug,
            )
            print(f"✅ wrote detailed reports: {short_path.name}, {full_path.name}")

            x_post_text = generate_for_x_post(
                payload,
                out_dir,
                max_sectors=max_sectors,
                include_price_details=False,
                include_debug_info=False,
                max_chars=2800
            )
            x_post_path = out_dir / "x_post_optimized.txt"
            x_post_path.write_text(x_post_text, encoding="utf-8")
            print(f"✅ wrote X-optimized post: {x_post_path.name}")

        except Exception as e:
            print(f"⚠️ write detailed reports failed: {e}")
            import traceback
            traceback.print_exc()

    all_paths: List[Path] = []

    # ---- Overview (auto paginate)
    if not args.skip_overview:
        print("🧠 Render overview (sector limitup counts) [matplotlib]")

        if args.overview_theme != "dark":
            print("⚠️ overview light theme is not implemented in pagination version. Please use --overview-theme dark.")
        else:
            overview_paths = render_overview_png(
                payload=payload,
                out_dir=out_dir,
                width=args.width,
                height=args.height,
                page_size=args.overview_top_n,
            )
            all_paths.extend(overview_paths)
            print(f"✅ Overview -> {len(overview_paths)} images")

    # ---- Sector blocks
    if not args.skip_sectors:
        print("📊 Render sector block tables [matplotlib]")
        sector_paths = render_sector_blocks(
            payload=payload,
            out_dir=out_dir,
            width=args.width,
            height=args.height,
            rows_per_page=args.rows_per_page,
            peers_max_per_page=args.peers_max_per_page,
            sectors_per_list=args.sectors_per_list,
            theme=args.overview_theme,
        )
        all_paths.extend(sector_paths)
        print(f"✅ Sector blocks -> {len(sector_paths)} images")

        if ("peers_not_limitup" not in payload) and ("peers_by_sector" not in payload):
            print("ℹ️ payload 未提供 peers_*，下半部『同產業未漲停』會是空白（但仍會出圖）。")

    # ---- Disclaimer page (LAST)
    if not args.skip_disclaimer:
        try:
            market = _pick_market(payload, args.market)
            out_file = out_dir / str(args.disclaimer_filename or "overview_disclaimer.png")

            print(f"🧾 Render disclaimer page (market={market}, theme={args.overview_theme})")
            p = render_disclaimer_page(
                out_path=out_file,
                market=market,
                theme=args.overview_theme,
                width_px=int(args.width),
                height_px=int(args.height),
                dpi=150,
                title=None,
                footer=None,
            )
            all_paths.append(Path(p))
            print(f"✅ Disclaimer -> {Path(p).name}")
        except Exception as e:
            print(f"⚠️ render disclaimer failed: {e}")
            import traceback
            traceback.print_exc()

    # ---- list.txt
    _write_list_txt(out_dir, all_paths)
    print(f"✨ Done. Total images: {len(all_paths)}")


if __name__ == "__main__":
    main()
