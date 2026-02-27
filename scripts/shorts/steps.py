# scripts/shorts/steps.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Optional, Tuple

from .paths import (
    done_path,
    force_clear_recent_done,
    images_dir,
    latest_payload_fallback,
    payload_path,
    post_align_images_dir,
    resolve_images_ymd,
    safe_rm,
    video_out,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def env_bool(name: str, default: str = "0") -> bool:
    v = str(os.getenv(name, default)).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def env_len(name: str) -> int:
    v = os.getenv(name)
    return len(v) if v else 0


def import_timekit():
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from markets.timekit import market_today_ymd, market_now_hhmm  # type: ignore

    return market_today_ymd, market_now_hhmm


def run_cmd(cmd: list[str], *, cwd: Optional[Path] = None) -> None:
    """
    Stream stdout/stderr live + tail on failure.
    """
    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")

    if env_bool("RUN_SHORTS_ENV_DEBUG", "0"):
        gha = env.get("GITHUB_ACTIONS", "")
        ci = env.get("CI", "")
        allow_int = env.get("GDRIVE_ALLOW_INTERACTIVE", "")
        print(
            "[RUN_ENV_DEBUG]"
            f" GITHUB_ACTIONS={gha!s} CI={ci!s}"
            f" GDRIVE_ALLOW_INTERACTIVE={allow_int!s}"
            f" TOKEN_JSON_B64(len)={env_len('GDRIVE_TOKEN_JSON_B64')}"
            f" CLIENT_SECRET_JSON_B64(len)={env_len('GDRIVE_CLIENT_SECRET_JSON_B64')}"
            f" ROOT_FOLDER_ID(len)={env_len('GDRIVE_ROOT_FOLDER_ID')}",
            flush=True,
        )

    print("▶", " ".join(cmd), flush=True)

    tail_n = int(os.getenv("RUN_SHORTS_TAIL_LINES", "200") or "200")
    tail: list[str] = []

    def _push(line: str) -> None:
        tail.append(line)
        if len(tail) > tail_n:
            tail.pop(0)

    p = subprocess.Popen(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )

    assert p.stdout is not None
    for line in p.stdout:
        print(line, end="", flush=True)
        _push(line.rstrip("\n"))

    rc = p.wait()
    if rc != 0:
        print("\n[RUN_FAIL] command failed:", flush=True)
        print("▶", " ".join(cmd), flush=True)
        print(f"[RUN_FAIL] exit_code={rc}", flush=True)
        print(f"[RUN_FAIL] last {min(len(tail), tail_n)} log lines:", flush=True)
        print("----- tail begin -----", flush=True)
        for x in tail:
            print(x, flush=True)
        print("----- tail end -----", flush=True)
        raise subprocess.CalledProcessError(rc, cmd)


def ensure_json_file_from_env(env_name: str, default_path: Path) -> Path:
    raw = os.getenv(env_name, "").strip()
    if raw:
        obj = json.loads(raw)
        default_path.parent.mkdir(parents=True, exist_ok=True)
        default_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[OK] wrote {env_name} -> {default_path}", flush=True)
    return default_path


def normalize_market(m: str) -> Tuple[str, str]:
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
        "india": "in",
    }
    ml = alias.get(ml, ml)
    if not ml:
        raise ValueError("market is required")
    return ml, ml.upper()


def tree(root: Path, *, enabled: bool, max_depth: int = 5, max_items: int = 300) -> None:
    if not enabled:
        return
    root = Path(root)
    print(f"[debug-tree] {root}", flush=True)
    if not root.exists():
        print("  (missing)", flush=True)
        return

    items_printed = 0
    root_depth = len(root.resolve().parts)

    def _depth(p: Path) -> int:
        return len(p.resolve().parts) - root_depth

    try:
        for p in sorted(root.rglob("*")):
            if items_printed >= max_items:
                print(f"  ... (truncated, max_items={max_items})", flush=True)
                break
            d = _depth(p)
            if d > max_depth:
                continue
            rel = p.relative_to(root).as_posix()
            if p.is_dir():
                print(f"  [D] {rel}/", flush=True)
            else:
                try:
                    sz = p.stat().st_size
                except Exception:
                    sz = -1
                print(f"  [F] {rel} ({sz} bytes)", flush=True)
            items_printed += 1
    except Exception as e:
        print(f"  (tree error: {e})", flush=True)


def zip_dir_to(zip_path: Path, src_dir: Path) -> Path:
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


def import_build_video():
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from scripts.render_video import build_video_from_images  # type: ignore

    return build_video_from_images


def drive_upload(
    *,
    drive_parent_id: str,
    market_upper: str,
    ymd: str,
    slot: str,
    out_mp4: Optional[Path],
    images_dir_path: Optional[Path],
    upload_mode: str,
    images_mode: str,
    workers: int = 8,
) -> None:
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    from scripts.utils.drive_uploader import get_drive_service, ensure_folder, upload_dir  # type: ignore

    parent_id = str(drive_parent_id).strip()
    if not parent_id:
        raise RuntimeError("Drive parent folder id missing (set --drive-parent-id or env GDRIVE_ROOT_FOLDER_ID).")

    market_upper = str(market_upper or "").strip().upper()
    ymd = str(ymd or "").strip()
    slot = str(slot or "").strip().lower() or "midday"

    service = get_drive_service()

    market_folder = ensure_folder(service, parent_id, market_upper)
    latest_folder = ensure_folder(service, market_folder, "Latest")
    slot_folder = ensure_folder(service, latest_folder, slot)

    def want(name: str) -> bool:
        return upload_mode in (name, "both")

    if want("video"):
        if not out_mp4 or not out_mp4.exists():
            raise FileNotFoundError(f"video not found for drive upload: {out_mp4}")

        fixed_mp4 = out_mp4.parent / f"latest_{slot}.mp4"
        shutil.copy2(out_mp4, fixed_mp4)

        n = upload_dir(
            service,
            slot_folder,
            fixed_mp4.parent,
            pattern=fixed_mp4.name,
            recursive=False,
            overwrite=True,
            verbose=True,
            concurrent=False,
        )
        print(f"[drive] uploaded video: {fixed_mp4.name} (n={n})", flush=True)

    if want("images"):
        if not images_dir_path or not images_dir_path.exists():
            raise FileNotFoundError(f"images_dir not found for drive upload: {images_dir_path}")

        if images_mode == "zip":
            zip_path = REPO_ROOT / "media" / "archives" / market_upper.lower() / f"latest_{slot}_images.zip"
            zip_dir_to(zip_path, images_dir_path)

            n = upload_dir(
                service,
                slot_folder,
                zip_path.parent,
                pattern=zip_path.name,
                recursive=False,
                overwrite=True,
                verbose=True,
                concurrent=False,
            )
            print(f"[drive] uploaded images zip: {zip_path.name} (n={n})", flush=True)

            list_txt = images_dir_path / "list.txt"
            if list_txt.exists():
                n2 = upload_dir(
                    service,
                    slot_folder,
                    list_txt.parent,
                    pattern=list_txt.name,
                    recursive=False,
                    overwrite=True,
                    verbose=False,
                    concurrent=False,
                )
                print(f"[drive] uploaded list.txt (n={n2})", flush=True)
        else:
            n1 = upload_dir(
                service,
                slot_folder,
                images_dir_path,
                pattern="*.png",
                recursive=False,
                overwrite=True,
                verbose=False,
                concurrent=True,
                workers=int(workers),
            )
            print(f"[drive] uploaded images: root_pngs={n1}", flush=True)

            sec_dir = images_dir_path / "sectors"
            if sec_dir.exists():
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
                print(f"[drive] uploaded images: sectors_pngs={n2}", flush=True)

    # -------------------------------------------------
    # ✅ Upload latest_meta.json (optional, for dashboard)
    # -------------------------------------------------
    meta_path = REPO_ROOT / "outputs" / "latest_meta.json"
    if meta_path.exists():
        n_meta = upload_dir(
            service,
            slot_folder,
            meta_path.parent,
            pattern=meta_path.name,
            recursive=False,
            overwrite=True,
            verbose=False,
            concurrent=False,
        )
        print(f"[drive] uploaded latest_meta.json (n={n_meta})", flush=True)
    else:
        print("[drive] latest_meta.json not found (skip)", flush=True)


def resolve_payload_and_maybe_realign(
    *,
    market_lower: str,
    ymd: str,
    slot: str,
    force: bool,
    skip_main: bool,
    asof: str,
    debug_tree: bool,
    debug_depth: int,
    debug_max: int,
) -> Tuple[Path, str]:
    py = sys.executable
    p = payload_path(REPO_ROOT, market_lower, ymd, slot)

    if force:
        print("[force] deleting existing artifacts (best effort) ...", flush=True)
        force_clear_recent_done(REPO_ROOT, market_lower, slot, keep_n=6)
        safe_rm(p)
        safe_rm(done_path(REPO_ROOT, market_lower, ymd, slot))
        safe_rm(images_dir(REPO_ROOT, market_lower, ymd, slot))
        safe_rm(video_out(REPO_ROOT, market_lower, ymd, slot))

    if not skip_main:
        cmd = [py, "main.py", "--market", market_lower, "--slot", slot]
        if asof and slot != "close":
            cmd += ["--asof", asof]
        run_cmd(cmd, cwd=REPO_ROOT)

    if p.exists():
        return p, ymd

    print("[WARN] payload not found at expected path.", flush=True)
    if debug_tree:
        tree(REPO_ROOT / "data" / "cache" / market_lower, enabled=True, max_depth=debug_depth, max_items=debug_max)

    fb = latest_payload_fallback(REPO_ROOT, market_lower, slot)
    if fb is None:
        raise FileNotFoundError(f"payload not found: {p}")

    print(f"[WARN] payload not found for ymd={ymd}. Fallback -> {fb}", flush=True)
    ymd2 = fb.parent.name
    print(f"[auto] ymd realigned -> {ymd2}", flush=True)

    if force:
        print(f"[force] ymd realigned; deleting artifacts for {ymd2} (best effort) ...", flush=True)
        safe_rm(fb)
        safe_rm(done_path(REPO_ROOT, market_lower, ymd2, slot))
        safe_rm(images_dir(REPO_ROOT, market_lower, ymd2, slot))
        safe_rm(video_out(REPO_ROOT, market_lower, ymd2, slot))

        if not skip_main:
            cmd = [py, "main.py", "--market", market_lower, "--slot", slot]
            if asof and slot != "close":
                cmd += ["--asof", asof]
            run_cmd(cmd, cwd=REPO_ROOT)

        if not fb.exists():
            if debug_tree:
                tree(REPO_ROOT / "data" / "cache" / market_lower, enabled=True, max_depth=debug_depth, max_items=debug_max)
            raise FileNotFoundError(f"payload not found after force rebuild: {fb}")

    return fb, ymd2


def summary_print(
    *,
    payload: Path,
    images_dir_path: Path,
    out_mp4: Path,
    skip_video: bool,
    skip_upload: bool,
    drive_enabled: bool,
    drive_parent_id: str,
    drive_order: str,
    drive_upload_mode: str,
    drive_images_mode: str,
    market_upper: str,
    slot: str,
) -> None:
    print("\n[OK] All done.", flush=True)
    print("platform:", platform.platform(), flush=True)
    print("payload :", payload, flush=True)
    print("images  :", images_dir_path, flush=True)
    print("video   :", "(skipped)" if skip_video else out_mp4, flush=True)
    print("upload  :", "(skipped)" if skip_upload else "done", flush=True)
    if drive_enabled:
        print("drive   : enabled", flush=True)
        print("drive_parent_id:", (str(drive_parent_id)[:6] + "…" if drive_parent_id else "(missing)"), flush=True)
        print("drive_order:", drive_order, "drive_upload:", drive_upload_mode, "images_mode:", drive_images_mode, flush=True)
        print("drive_folder:", f"{market_upper}/Latest/{slot}", flush=True)
        if drive_upload_mode in ("video", "both"):
            print("drive_fixed_video:", f"latest_{slot}.mp4", flush=True)
        if drive_upload_mode in ("images", "both") and drive_images_mode == "zip":
            print("drive_fixed_images_zip:", f"latest_{slot}_images.zip", flush=True)
        print("drive_meta:", "latest_meta.json (if produced)", flush=True)
    else:
        print("drive   : (disabled)", flush=True)
