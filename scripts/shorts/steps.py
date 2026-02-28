# scripts/run_shorts.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
from pathlib import Path

# =============================================================================
# ✅ CRITICAL: ensure repo root is on sys.path BEFORE importing "scripts.*"
# This fixes: ModuleNotFoundError: No module named 'scripts'
# when running: python scripts/run_shorts.py ...
# =============================================================================
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# =============================================================================
# ✅ GLOBAL SWITCH (edit here)
# - True  : when --drive is enabled, also upload payload.json to Google Drive
# - False : do NOT upload payload.json
# =============================================================================
UPLOAD_PAYLOAD_TO_DRIVE = True

import argparse
import os
import re

from scripts.shorts.paths import (
    images_dir,
    post_align_images_dir,
    resolve_images_ymd,
    safe_rm,
    video_out,
)
from scripts.shorts.steps import (
    drive_upload,
    env_bool,
    ensure_json_file_from_env,
    import_build_video,
    import_timekit,
    normalize_market,
    resolve_payload_and_maybe_realign,
    run_cmd,
    summary_print,
    tree,
)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="One-shot Shorts pipeline: main.py -> render_images -> render_video -> youtube upload -> (optional) drive upload"
    )

    ap.add_argument("--market", required=True, help="jp/kr/tw/th/cn/us/uk/au/ca/hk/in ...")
    ap.add_argument("--ymd", default="auto", help="YYYY-MM-DD or 'auto' (default auto)")
    ap.add_argument("--slot", default="midday", help="open/midday/close")
    ap.add_argument("--images-ymd", default="requested", help="requested|payload|latest|YYYY-MM-DD (default requested)")
    ap.add_argument("--asof", default="auto", help='HH:MM or "auto" (default auto)')
    ap.add_argument("--theme", default="dark", help="render_images theme")
    ap.add_argument("--layout", default="", help="render_images layout (optional)")

    # video options
    ap.add_argument("--seconds", type=float, default=2.0)
    ap.add_argument("--fps", type=int, default=30)
    ap.add_argument("--crf", type=int, default=18)
    ap.add_argument("--scale", default="1080:1920")
    ap.add_argument("--fade", action="store_true")

    # YouTube
    ap.add_argument("--token", default="secrets/youtube_token.upload.json")
    ap.add_argument("--playlist-map", default="config/youtube_playlists.json")
    ap.add_argument("--privacy", default="private", help="private/unlisted/public (default private)")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--skip-playlist", action="store_true")

    # step switches
    ap.add_argument("--skip-main", action="store_true")
    ap.add_argument("--skip-images", action="store_true")
    ap.add_argument("--skip-video", action="store_true")

    ap.add_argument(
        "--full",
        action="store_true",
        help="Run full pipeline (video + YouTube upload). Default: main + images only.",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Force refresh: delete existing payload/done/images/video for the resolved ymd before running.",
    )

    # Drive
    ap.add_argument("--drive", action="store_true", help="Upload artifacts to Google Drive (default: off)")
    ap.add_argument("--drive-parent-id", default=os.getenv("GDRIVE_ROOT_FOLDER_ID", "").strip())
    ap.add_argument("--drive-order", default="after_youtube", choices=["after_video", "after_youtube", "end"])
    ap.add_argument("--drive-upload", default="both", choices=["video", "images", "both"])
    ap.add_argument("--drive-images-mode", default="zip", choices=["zip", "dir"])
    ap.add_argument("--drive-workers", type=int, default=8)

    # Debug tree
    ap.add_argument("--no-debug-tree", action="store_true")
    ap.add_argument("--debug-tree-depth", type=int, default=5)
    ap.add_argument("--debug-tree-max", type=int, default=250)

    args = ap.parse_args()

    debug_tree = (not args.no_debug_tree) and env_bool("RUN_SHORTS_DEBUG_TREE", "1")

    market_lower, market_upper = normalize_market(args.market)
    slot = str(args.slot).strip().lower() or "midday"

    market_today_ymd_fn, market_now_hhmm_fn = import_timekit()

    # auto ymd
    if str(args.ymd).strip().lower() in ("", "auto"):
        ymd = market_today_ymd_fn(market_upper)
        print(f"[auto] ymd({market_upper}) = {ymd}", flush=True)
    else:
        ymd = str(args.ymd).strip()

    # auto asof
    if str(args.asof).strip().lower() in ("", "auto"):
        asof = market_now_hhmm_fn(market_upper)
        print(f"[auto] asof({market_upper}) = {asof}", flush=True)
    else:
        asof = str(args.asof).strip()

    # default: main+images only
    if not args.full:
        args.skip_video = True
        args.skip_upload = True

    py = sys.executable
    is_gha = env_bool("GITHUB_ACTIONS", "0")

    # ✅ GHA: write env json to files if provided
    if is_gha:
        token_path = ensure_json_file_from_env("YOUTUBE_TOKEN_JSON", REPO_ROOT / args.token)
        # env 沒有也沒關係：repo 內本來就有 config/youtube_playlists.json
        playlist_map_path = ensure_json_file_from_env("YOUTUBE_PLAYLIST_MAP_JSON", REPO_ROOT / args.playlist_map)
    else:
        token_path = (REPO_ROOT / args.token).expanduser().resolve()
        playlist_map_path = (REPO_ROOT / args.playlist_map).expanduser().resolve()

    # 1) main.py -> payload (with fallback realign)
    payload, ymd = resolve_payload_and_maybe_realign(
        market_lower=market_lower,
        ymd=ymd,
        slot=slot,
        force=bool(args.force),
        skip_main=bool(args.skip_main),
        asof=asof,
        debug_tree=debug_tree,
        debug_depth=int(args.debug_tree_depth),
        debug_max=int(args.debug_tree_max),
    )

    # 2) resolve images ymd and dir
    images_ymd = resolve_images_ymd(
        requested_ymd=ymd,
        images_ymd_arg=str(args.images_ymd),
        repo_root=REPO_ROOT,
        market_lower=market_lower,
        slot=slot,
        payload_path=payload,
    )
    images_dir_path = images_dir(REPO_ROOT, market_lower, images_ymd, slot)
    print(f"[images] ymd source={args.images_ymd} -> {images_ymd}", flush=True)

    if args.force and images_ymd != ymd:
        safe_rm(images_dir(REPO_ROOT, market_lower, images_ymd, slot))
        safe_rm(video_out(REPO_ROOT, market_lower, images_ymd, slot))

    # 3) render_images
    if not args.skip_images:
        cli_path = REPO_ROOT / "scripts" / f"render_images_{market_lower}" / "cli.py"
        if not cli_path.exists():
            raise FileNotFoundError(f"market cli not found: {cli_path}")

        cmd = [py, str(cli_path), "--payload", str(payload)]
        if args.theme:
            cmd += ["--theme", str(args.theme)]
        if args.layout:
            cmd += ["--layout", str(args.layout)]
        run_cmd(cmd, cwd=REPO_ROOT)

        images_dir_path, images_ymd = post_align_images_dir(
            repo_root=REPO_ROOT,
            images_dir_path=images_dir_path,
            images_ymd=images_ymd,
            requested_ymd=ymd,
            images_ymd_arg=str(args.images_ymd),
            market_lower=market_lower,
            slot=slot,
            payload_path=payload,
        )

    if not images_dir_path.exists():
        if debug_tree:
            tree(
                REPO_ROOT / "media" / "images" / market_lower,
                enabled=True,
                max_depth=int(args.debug_tree_depth),
                max_items=int(args.debug_tree_max),
            )
        raise FileNotFoundError(f"images_dir not found: {images_dir_path}")

    # 4) render_video
    out_mp4 = video_out(REPO_ROOT, market_lower, ymd, slot)

    if not args.skip_video:
        build_video_from_images = import_build_video()
        build_video_from_images(
            images_dir=images_dir_path,
            out_mp4=out_mp4,
            seconds_per_image=float(args.seconds),
            fps=int(args.fps),
            crf=int(args.crf),
            force_scale=(args.scale.strip() if str(args.scale).strip() else None),
            fade=bool(args.fade),
            ext="png",
            prefer_top=15,
            use_existing_list=True,
        )

        if not out_mp4.exists():
            raise FileNotFoundError(f"video not generated: {out_mp4}")

        if args.drive and args.drive_order == "after_video":
            print("[drive] uploading after video ...", flush=True)
            drive_upload(
                drive_parent_id=str(args.drive_parent_id),
                market_upper=market_upper,
                ymd=ymd,
                slot=slot,
                out_mp4=out_mp4,
                images_dir_path=images_dir_path,
                payload_path=(payload if UPLOAD_PAYLOAD_TO_DRIVE else None),
                upload_mode=str(args.drive_upload),
                images_mode=str(args.drive_images_mode),
                workers=int(args.drive_workers),
            )

    # 5) YouTube upload + playlist
    if not args.skip_upload:
        if not out_mp4.exists():
            raise FileNotFoundError(f"video not generated: {out_mp4}")

        cmd = [
            py,
            "scripts/youtube_pipeline_safe.py",
            "--video",
            str(out_mp4),
            "--token",
            str(token_path),
            "--market",
            market_upper,
            "--ymd",
            ymd,
            "--slot",
            slot,
            "--playlist-map",
            str(playlist_map_path),
            "--privacy",
            str(args.privacy),
        ]
        if args.skip_playlist:
            cmd += ["--skip-playlist"]
        run_cmd(cmd, cwd=REPO_ROOT)

        if args.drive and args.drive_order == "after_youtube":
            print("[drive] uploading after youtube ...", flush=True)
            drive_upload(
                drive_parent_id=str(args.drive_parent_id),
                market_upper=market_upper,
                ymd=ymd,
                slot=slot,
                out_mp4=out_mp4,
                images_dir_path=images_dir_path,
                payload_path=(payload if UPLOAD_PAYLOAD_TO_DRIVE else None),
                upload_mode=str(args.drive_upload),
                images_mode=str(args.drive_images_mode),
                workers=int(args.drive_workers),
            )

    if args.drive and args.drive_order == "end":
        print("[drive] uploading at end ...", flush=True)
        drive_upload(
            drive_parent_id=str(args.drive_parent_id),
            market_upper=market_upper,
            ymd=ymd,
            slot=slot,
            out_mp4=out_mp4 if out_mp4.exists() else None,
            images_dir_path=images_dir_path if images_dir_path.exists() else None,
            payload_path=(payload if UPLOAD_PAYLOAD_TO_DRIVE else None),
            upload_mode=str(args.drive_upload),
            images_mode=str(args.drive_images_mode),
            workers=int(args.drive_workers),
        )

    summary_print(
        payload=payload,
        images_dir_path=images_dir_path,
        out_mp4=out_mp4,
        skip_video=bool(args.skip_video),
        skip_upload=bool(args.skip_upload),
        drive_enabled=bool(args.drive),
        drive_parent_id=str(args.drive_parent_id),
        drive_order=str(args.drive_order),
        drive_upload_mode=str(args.drive_upload),
        drive_images_mode=str(args.drive_images_mode),
        market_upper=market_upper,
        slot=slot,
    )

    if debug_tree:
        tree(payload.parent, enabled=True, max_depth=int(args.debug_tree_depth), max_items=int(args.debug_tree_max))
        tree(images_dir_path, enabled=True, max_depth=int(args.debug_tree_depth), max_items=int(args.debug_tree_max))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
