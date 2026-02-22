# scripts/run_shorts.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

REPO_ROOT = Path(__file__).resolve().parents[1]

# -----------------------------
# Timezone mapping (market local)
# -----------------------------
MARKET_TZ = {
    "JP": "Asia/Tokyo",
    "KR": "Asia/Seoul",
    "TW": "Asia/Taipei",
    "CN": "Asia/Shanghai",
    "HK": "Asia/Hong_Kong",
    "TH": "Asia/Bangkok",
    # US/CA: choose primary financial center tz
    "US": "America/New_York",
    "CA": "America/Toronto",
    "UK": "Europe/London",
    "AU": "Australia/Sydney",
}


def _env_bool(name: str, default: str = "0") -> bool:
    v = str(os.getenv(name, default)).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _is_ci() -> bool:
    """
    Treat as CI if either is set:
      - GITHUB_ACTIONS=1
      - CI=1
    (This mirrors drive_uploader.py behavior.)
    """
    return _env_bool("GITHUB_ACTIONS", "0") or _env_bool("CI", "0")


def _env_len(name: str) -> int:
    v = os.getenv(name)
    return len(v) if v else 0


def _run(cmd: list[str], *, cwd: Optional[Path] = None) -> None:
    """
    Run subprocess with inherited env.
    Also forces child python to output UTF-8 on Windows cp950 consoles.
    """
    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")

    # Optional safe debug for env propagation (no secret content printed)
    if _env_bool("RUN_SHORTS_ENV_DEBUG", "0"):
        gha = env.get("GITHUB_ACTIONS", "")
        ci = env.get("CI", "")
        allow_int = env.get("GDRIVE_ALLOW_INTERACTIVE", "")
        print(
            "[RUN_ENV_DEBUG]"
            f" GITHUB_ACTIONS={gha!s} CI={ci!s}"
            f" GDRIVE_ALLOW_INTERACTIVE={allow_int!s}"
            f" TOKEN_JSON_B64(len)={_env_len('GDRIVE_TOKEN_JSON_B64')}"
            f" CLIENT_SECRET_JSON_B64(len)={_env_len('GDRIVE_CLIENT_SECRET_JSON_B64')}",
            flush=True,
        )

    print("▶", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=str(cwd) if cwd else None, env=env)


def _normalize_market(m: str) -> Tuple[str, str]:
    """
    Return (market_lower, market_upper)
    """
    ml = (m or "").strip().lower()
    alias = {
        "jpn": "jp",
        "japan": "jp",
        "jpx": "jp",
        "korea": "kr",
        "kor": "kr",
        "taiwan": "tw",
        "thailand": "th",
        "china": "cn",
        "hongkong": "hk",
        "unitedstates": "us",
        "usa": "us",
        "unitedkingdom": "uk",
        "england": "uk",
        "australia": "au",
        "canada": "ca",
    }
    ml = alias.get(ml, ml)
    if not ml:
        raise ValueError("market is required")
    return ml, ml.upper()


def _tz_now(market_upper: str) -> datetime:
    tz_name = MARKET_TZ.get(market_upper, "UTC")
    if ZoneInfo is None:
        return datetime.now()
    return datetime.now(ZoneInfo(tz_name))


def market_today_ymd(market_upper: str) -> str:
    return _tz_now(market_upper).strftime("%Y-%m-%d")


def market_now_hhmm(market_upper: str) -> str:
    return _tz_now(market_upper).strftime("%H:%M")


def _ensure_json_file_from_env(env_name: str, default_path: Path) -> Path:
    """
    If env var exists:
      - treat it as raw JSON string
      - write to default_path
    else:
      - use default_path (must exist on local runs)
    """
    raw = os.getenv(env_name, "").strip()
    if raw:
        obj = json.loads(raw)
        default_path.parent.mkdir(parents=True, exist_ok=True)
        default_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[OK] wrote {env_name} -> {default_path}")
    return default_path


def _payload_path(market_lower: str, ymd: str, slot: str) -> Path:
    return REPO_ROOT / "data" / "cache" / market_lower / ymd / f"{slot}.payload.json"


def _done_path(market_lower: str, ymd: str, slot: str) -> Path:
    return REPO_ROOT / "data" / "cache" / market_lower / ymd / f"{slot}.done.json"


def _latest_payload_fallback(market_lower: str, slot: str) -> Optional[Path]:
    """
    When ymd mismatch happens (timezone / trading day), try:
      data/cache/<market>/<LATEST_YYYY-MM-DD>/<slot>.payload.json
    """
    base = REPO_ROOT / "data" / "cache" / market_lower
    if not base.exists():
        return None

    dirs: list[Path] = []
    for p in base.iterdir():
        if p.is_dir() and len(p.name) == 10 and p.name[4] == "-" and p.name[7] == "-":
            dirs.append(p)
    if not dirs:
        return None

    dirs.sort(key=lambda x: x.name, reverse=True)
    for d in dirs:
        cand = d / f"{slot}.payload.json"
        if cand.exists():
            return cand
    return None


def _images_dir(market_lower: str, ymd: str, slot: str) -> Path:
    return REPO_ROOT / "media" / "images" / market_lower / ymd / slot


def _read_payload_ymd_effective(payload_path: Path) -> str:
    """
    Read ymd_effective (preferred) from payload json.
    Fallback to payload folder name if parsing fails.
    """
    try:
        obj = json.loads(payload_path.read_text(encoding="utf-8"))
        y = str(obj.get("ymd_effective") or obj.get("ymd") or "").strip()
        return y or payload_path.parent.name
    except Exception:
        return payload_path.parent.name


def _latest_images_ymd(market_lower: str, slot: str) -> Optional[str]:
    """
    Find latest date folder under media/images/<market>/ that has <slot>/list.txt.
    Used for holiday/testing scenarios.
    """
    base = REPO_ROOT / "media" / "images" / market_lower
    if not base.exists():
        return None

    dirs: list[str] = []
    for p in base.iterdir():
        if not p.is_dir():
            continue
        name = p.name
        if len(name) != 10 or name[4] != "-" or name[7] != "-":
            continue
        slot_dir = p / slot
        if (slot_dir / "list.txt").exists():
            dirs.append(name)

    if not dirs:
        return None
    dirs.sort(reverse=True)
    return dirs[0]


def _resolve_images_ymd(
    *,
    requested_ymd: str,
    images_ymd_arg: str,
    market_lower: str,
    slot: str,
    payload_path: Path,
) -> str:
    """
    images ymd policy:
      - requested (default): use run_shorts requested ymd
      - payload/auto: use payload ymd_effective/ymd
      - latest: pick latest media/images/<market>/<YYYY-MM-DD>/<slot>/list.txt
      - YYYY-MM-DD: force a specific date folder
    """
    v = str(images_ymd_arg).strip()
    vl = v.lower()

    if vl in ("payload", "auto"):
        return _read_payload_ymd_effective(payload_path)

    if vl == "latest":
        yy = _latest_images_ymd(market_lower, slot)
        return yy or requested_ymd

    if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
        return v

    return requested_ymd


def _post_align_images_dir(
    *,
    images_dir: Path,
    images_ymd: str,
    requested_ymd: str,
    images_ymd_arg: str,
    market_lower: str,
    slot: str,
    payload_path: Path,
) -> Tuple[Path, str]:
    """
    After render_images finishes, images might land in ymd_effective folder (CN).
    If images_dir does not exist, try ONE re-resolve (payload/latest) depending on images_ymd_arg.
    """
    if images_dir.exists():
        return images_dir, images_ymd

    v = str(images_ymd_arg).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
        return images_dir, images_ymd

    vl = v.lower()
    cand_ymd: Optional[str] = None

    if vl in ("payload", "auto"):
        cand_ymd = _read_payload_ymd_effective(payload_path)
    elif vl == "latest":
        cand_ymd = _latest_images_ymd(market_lower, slot)

    if cand_ymd and cand_ymd != images_ymd:
        cand_dir = _images_dir(market_lower, cand_ymd, slot)
        if cand_dir.exists():
            print(f"[images] dir realigned after render -> {cand_dir}")
            return cand_dir, cand_ymd

    return images_dir, images_ymd


def _video_out(market_lower: str, ymd: str, slot: str) -> Path:
    outdir = REPO_ROOT / "media" / "videos" / market_lower
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir / f"{ymd}_{slot}.mp4"


def _import_build_video():
    try:
        if str(REPO_ROOT) not in sys.path:
            sys.path.insert(0, str(REPO_ROOT))
        from scripts.render_video import build_video_from_images  # type: ignore

        return build_video_from_images
    except Exception as e:
        raise RuntimeError(
            "Cannot import scripts.render_video.build_video_from_images(). "
            "Please ensure scripts/render_video.py still defines build_video_from_images()."
        ) from e


def _safe_rm(path: Path) -> None:
    try:
        if path.is_symlink() or path.is_file():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[WARN] could not delete: {path} err={e}")


def _force_clear_recent_done(market_lower: str, slot: str, keep_n: int = 6) -> None:
    """
    Clear recent <slot>.done.json markers to defeat main.py cache-hit.
    This is intentionally lightweight (deletes only done.json, not payload/images).
    """
    base = REPO_ROOT / "data" / "cache" / market_lower
    if not base.exists():
        return

    dirs: list[Path] = []
    for p in base.iterdir():
        if p.is_dir() and len(p.name) == 10 and p.name[4] == "-" and p.name[7] == "-":
            dirs.append(p)

    dirs.sort(key=lambda x: x.name, reverse=True)
    for d in dirs[: max(1, int(keep_n))]:
        _safe_rm(d / f"{slot}.done.json")


# =============================================================================
# Google Drive helpers (using scripts/utils/drive_uploader.py)
# =============================================================================
def _zip_dir_to(zip_path: Path, src_dir: Path) -> Path:
    """
    Zip entire folder into zip_path.
    - Stores paths relative to src_dir
    """
    src_dir = Path(src_dir).resolve()
    zip_path = Path(zip_path).resolve()
    zip_path.parent.mkdir(parents=True, exist_ok=True)

    if zip_path.exists():
        zip_path.unlink()

    with zipfile.ZipFile(str(zip_path), mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(src_dir.rglob("*")):
            if p.is_file():
                arc = p.relative_to(src_dir).as_posix()
                zf.write(str(p), arcname=arc)

    return zip_path


def _drive_upload(
    *,
    drive_parent_id: str,
    market_upper: str,
    ymd: str,
    slot: str,
    out_mp4: Optional[Path],
    images_dir: Optional[Path],
    upload_mode: str,  # video/images/both
    images_mode: str,  # zip/dir
    drive_subdir_policy: str,  # market/ymd/slot
    workers: int = 8,
) -> None:
    """
    Upload artifacts to Google Drive without blocking YouTube (caller decides order).
    Folder strategy:
      parent/
        <MARKET>/
          <YMD>/
            <SLOT>/
              video.mp4
              images.zip OR images/*.png
    """
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    try:
        from scripts.utils.drive_uploader import (  # type: ignore
            get_drive_service,
            ensure_folder,
            upload_dir,
        )
    except Exception as e:
        raise RuntimeError("Cannot import scripts/utils/drive_uploader.py") from e

    parent_id = str(drive_parent_id).strip()
    if not parent_id:
        raise RuntimeError("--drive-parent-id is required when --drive is enabled")

    service = get_drive_service()

    # Create nested folders
    # policy fixed: parent / MARKET / YMD / SLOT
    market_folder = ensure_folder(service, parent_id, market_upper)
    ymd_folder = ensure_folder(service, market_folder, ymd)
    slot_folder = ensure_folder(service, ymd_folder, slot)

    def want(name: str) -> bool:
        return upload_mode in (name, "both")

    # Upload video
    if want("video"):
        if not out_mp4 or not out_mp4.exists():
            raise FileNotFoundError(f"video not found for drive upload: {out_mp4}")
        # upload single file by using upload_dir with exact filename pattern
        n = upload_dir(
            service,
            slot_folder,
            out_mp4.parent,
            pattern=out_mp4.name,
            recursive=False,
            overwrite=True,
            verbose=True,
            concurrent=False,  # single file
        )
        print(f"[drive] uploaded video: {out_mp4.name} (n={n})")

    # Upload images
    if want("images"):
        if not images_dir or not images_dir.exists():
            raise FileNotFoundError(f"images_dir not found for drive upload: {images_dir}")

        if images_mode == "zip":
            # put zip under media/archives/<market>/<ymd>_<slot>_images.zip
            zip_path = REPO_ROOT / "media" / "archives" / market_upper.lower() / f"{ymd}_{slot}_images.zip"
            _zip_dir_to(zip_path, images_dir)
            n = upload_dir(
                service,
                slot_folder,
                zip_path.parent,
                pattern=zip_path.name,
                recursive=False,
                overwrite=True,
                verbose=True,
                concurrent=False,  # single file
            )
            print(f"[drive] uploaded images zip: {zip_path.name} (n={n})")
        else:
            # upload pngs under images_dir (non-recursive + recursive for sectors)
            # 1) root png
            n1 = upload_dir(
                service,
                slot_folder,
                images_dir,
                pattern="*.png",
                recursive=False,
                overwrite=True,
                verbose=False,
                concurrent=True,
                workers=int(workers),
            )
            # 2) sectors png (if exists)
            sec_dir = images_dir / "sectors"
            n2 = 0
            if sec_dir.exists():
                # put into a subfolder "sectors" to keep tidy
                n2 = upload_dir(
                    service,
                    slot_folder,
                    sec_dir,
                    pattern="*.png",
                    recursive=False,
                    overwrite=True,
                    verbose=False,
                    concurrent=True,
                    workers=int(workers),
                    subfolder_name="sectors",
                )
            print(f"[drive] uploaded images: root={n1} sectors={n2}")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="One-shot Shorts pipeline: main.py -> render_images -> render_video -> youtube upload"
    )

    ap.add_argument("--market", required=True, help="jp/kr/tw/th/cn/us/uk/au/ca/hk ...")
    ap.add_argument("--ymd", default="auto", help="YYYY-MM-DD or 'auto' (default auto)")
    ap.add_argument("--slot", default="midday", help="open/midday/close")

    ap.add_argument(
        "--images-ymd",
        default="requested",
        help="images ymd source: requested|payload|latest|YYYY-MM-DD (default requested)",
    )

    ap.add_argument("--asof", default="auto", help='HH:MM or "auto" (default auto)')

    ap.add_argument("--theme", default="dark", help="render_images theme")
    ap.add_argument("--layout", default="", help="render_images layout (optional)")

    # ✅ NEW: allow-nontrading passthrough to main.py guard
    ap.add_argument(
        "--allow-nontrading",
        action="store_true",
        help="Allow running on non-trading days (passthrough to main.py).",
    )

    # video options
    ap.add_argument("--seconds", type=float, default=2.0)
    ap.add_argument("--fps", type=int, default=30)
    ap.add_argument("--crf", type=int, default=18)
    ap.add_argument("--scale", default="1080:1920")
    ap.add_argument("--fade", action="store_true")

    # YouTube
    ap.add_argument("--token", default="secrets/youtube_token.upload.json")
    ap.add_argument("--playlist-map", default="config/youtube_playlists.json")
    ap.add_argument(
        "--privacy",
        default="private",
        help="YouTube privacy: private/unlisted/public (default private)",
    )
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--skip-playlist", action="store_true")

    # step switches (original)
    ap.add_argument("--skip-main", action="store_true")
    ap.add_argument("--skip-images", action="store_true")
    ap.add_argument("--skip-video", action="store_true")

    # New behavior switches
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

    # Google Drive upload switches (NEW)
    ap.add_argument("--drive", action="store_true", help="Upload artifacts to Google Drive (default: off)")
    ap.add_argument("--drive-parent-id", default="", help="Google Drive folder id to store outputs (required if --drive)")
    ap.add_argument(
        "--drive-order",
        default="after_youtube",
        choices=["after_video", "after_youtube", "end"],
        help="When to upload to Drive. Default: after_youtube (so YouTube speed not impacted).",
    )
    ap.add_argument(
        "--drive-upload",
        default="video",
        choices=["video", "images", "both"],
        help="What to upload to Drive. Default: video.",
    )
    ap.add_argument(
        "--drive-images-mode",
        default="zip",
        choices=["zip", "dir"],
        help="If uploading images: zip (fast) or dir (many files). Default: zip.",
    )
    ap.add_argument("--drive-workers", type=int, default=8, help="Drive concurrent workers for images dir mode (default 8)")

    args = ap.parse_args()

    market_lower, market_upper = _normalize_market(args.market)
    slot = str(args.slot).strip() or "midday"

    # auto ymd (market local date)
    if str(args.ymd).strip().lower() in ("", "auto"):
        ymd = market_today_ymd(market_upper)
        print(f"[auto] ymd({market_upper}) = {ymd}")
    else:
        ymd = str(args.ymd).strip()

    # auto asof (market local time)
    if str(args.asof).strip().lower() in ("", "auto"):
        asof = market_now_hhmm(market_upper)
        print(f"[auto] asof({market_upper}) = {asof}")
    else:
        asof = str(args.asof).strip()

    # Default: main + images only (no video, no upload).
    # Use --full to enable video+upload.
    if not args.full:
        args.skip_video = True
        args.skip_upload = True

    py = sys.executable
    is_gha = _env_bool("GITHUB_ACTIONS", "0")

    # materialize env json to files (GitHub Actions)
    if is_gha:
        token_path = _ensure_json_file_from_env("YOUTUBE_TOKEN_JSON", REPO_ROOT / args.token)
        playlist_map_path = _ensure_json_file_from_env(
            "YOUTUBE_PLAYLIST_MAP_JSON", REPO_ROOT / args.playlist_map
        )
    else:
        token_path = (REPO_ROOT / args.token).expanduser().resolve()
        playlist_map_path = (REPO_ROOT / args.playlist_map).expanduser().resolve()

    # 1) main.py -> payload
    payload = _payload_path(market_lower, ymd, slot)

    if args.force:
        print("[force] deleting existing artifacts (best effort) ...")
        # kill cache-hit markers for nearby days (fix cross-day / tz mismatch issues)
        _force_clear_recent_done(market_lower, slot, keep_n=6)

        # best effort delete for requested day
        _safe_rm(payload)
        _safe_rm(_done_path(market_lower, ymd, slot))
        _safe_rm(_images_dir(market_lower, ymd, slot))
        _safe_rm(_video_out(market_lower, ymd, slot))

    if not args.skip_main:
        cmd = [py, "main.py", "--market", market_lower, "--slot", slot]
        if asof and slot != "close":
            cmd += ["--asof", asof]
        # ✅ passthrough
        if args.allow_nontrading:
            cmd += ["--allow-nontrading"]
        _run(cmd, cwd=REPO_ROOT)

    # payload mismatch fallback
    if not payload.exists():
        fb = _latest_payload_fallback(market_lower, slot)
        if fb is None:
            raise FileNotFoundError(f"payload not found: {payload}")
        print(f"[WARN] payload not found for ymd={ymd}. Fallback -> {fb}")
        payload = fb
        ymd = fb.parent.name
        print(f"[auto] ymd realigned -> {ymd}")

        if args.force:
            print(f"[force] ymd realigned; deleting artifacts for {ymd} (best effort) ...")
            _safe_rm(payload)
            _safe_rm(_done_path(market_lower, ymd, slot))
            _safe_rm(_images_dir(market_lower, ymd, slot))
            _safe_rm(_video_out(market_lower, ymd, slot))

            # rerun main to rebuild payload after deletion (unless user skipped main)
            if not args.skip_main:
                cmd = [py, "main.py", "--market", market_lower, "--slot", slot]
                if asof and slot != "close":
                    cmd += ["--asof", asof]
                # ✅ passthrough
                if args.allow_nontrading:
                    cmd += ["--allow-nontrading"]
                _run(cmd, cwd=REPO_ROOT)

            if not payload.exists():
                raise FileNotFoundError(f"payload not found after force rebuild: {payload}")

    # 2) Resolve images_dir BEFORE running render_images (for logs)
    images_ymd = _resolve_images_ymd(
        requested_ymd=ymd,
        images_ymd_arg=str(args.images_ymd),
        market_lower=market_lower,
        slot=slot,
        payload_path=payload,
    )
    images_dir = _images_dir(market_lower, images_ymd, slot)
    print(f"[images] ymd source={args.images_ymd} -> {images_ymd}")

    # If force and images_ymd differs, delete images/video for images_ymd too (best effort)
    if args.force and images_ymd != ymd:
        _safe_rm(_images_dir(market_lower, images_ymd, slot))
        _safe_rm(_video_out(market_lower, images_ymd, slot))

    # 3) render_images_<market>/cli.py
    if not args.skip_images:
        cli_path = REPO_ROOT / "scripts" / f"render_images_{market_lower}" / "cli.py"
        if not cli_path.exists():
            raise FileNotFoundError(f"market cli not found: {cli_path}")

        cmd = [py, str(cli_path), "--payload", str(payload)]
        if args.theme:
            cmd += ["--theme", str(args.theme)]
        if args.layout:
            cmd += ["--layout", str(args.layout)]
        _run(cmd, cwd=REPO_ROOT)

        # Post-align images_dir AFTER render_images (CN ymd_effective case)
        images_dir, images_ymd = _post_align_images_dir(
            images_dir=images_dir,
            images_ymd=images_ymd,
            requested_ymd=ymd,
            images_ymd_arg=str(args.images_ymd),
            market_lower=market_lower,
            slot=slot,
            payload_path=payload,
        )

    if not images_dir.exists():
        raise FileNotFoundError(f"images_dir not found: {images_dir}")

    # 4) render_video
    out_mp4 = _video_out(market_lower, ymd, slot)

    if not args.skip_video:
        build_video_from_images = _import_build_video()
        build_video_from_images(
            images_dir=images_dir,
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

        # Drive upload right after video (optional)
        if args.drive and args.drive_order == "after_video":
            _drive_upload(
                drive_parent_id=str(args.drive_parent_id),
                market_upper=market_upper,
                ymd=ymd,
                slot=slot,
                out_mp4=out_mp4,
                images_dir=images_dir,
                upload_mode=str(args.drive_upload),
                images_mode=str(args.drive_images_mode),
                drive_subdir_policy="market/ymd/slot",
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
        _run(cmd, cwd=REPO_ROOT)

        # Drive upload right after YouTube (default; avoids impacting YT speed)
        if args.drive and args.drive_order == "after_youtube":
            _drive_upload(
                drive_parent_id=str(args.drive_parent_id),
                market_upper=market_upper,
                ymd=ymd,
                slot=slot,
                out_mp4=out_mp4,
                images_dir=images_dir,
                upload_mode=str(args.drive_upload),
                images_mode=str(args.drive_images_mode),
                drive_subdir_policy="market/ymd/slot",
                workers=int(args.drive_workers),
            )

    # "end" means: do Drive at the very end regardless of full/skip-upload
    if args.drive and args.drive_order == "end":
        _drive_upload(
            drive_parent_id=str(args.drive_parent_id),
            market_upper=market_upper,
            ymd=ymd,
            slot=slot,
            out_mp4=(out_mp4 if out_mp4.exists() else None),
            images_dir=images_dir,
            upload_mode=str(args.drive_upload),
            images_mode=str(args.drive_images_mode),
            drive_subdir_policy="market/ymd/slot",
            workers=int(args.drive_workers),
        )

    print("\n[OK] All done.")
    print("payload :", payload)
    print("images  :", images_dir)
    if not args.skip_video:
        print("video   :", out_mp4)
    else:
        print("video   : (skipped)")
    if not args.skip_upload:
        print("upload  : done")
    else:
        print("upload  : (skipped)")
    if args.drive:
        print(f"drive   : {args.drive_order} upload={args.drive_upload} images_mode={args.drive_images_mode}")
    else:
        print("drive   : (skipped)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
