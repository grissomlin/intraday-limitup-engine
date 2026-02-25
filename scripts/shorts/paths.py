# scripts/shorts/paths.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import re
import shutil
from pathlib import Path
from typing import Optional, Tuple


def payload_path(repo_root: Path, market_lower: str, ymd: str, slot: str) -> Path:
    return repo_root / "data" / "cache" / market_lower / ymd / f"{slot}.payload.json"


def done_path(repo_root: Path, market_lower: str, ymd: str, slot: str) -> Path:
    return repo_root / "data" / "cache" / market_lower / ymd / f"{slot}.done.json"


def images_dir(repo_root: Path, market_lower: str, ymd: str, slot: str) -> Path:
    return repo_root / "media" / "images" / market_lower / ymd / slot


def video_out(repo_root: Path, market_lower: str, ymd: str, slot: str) -> Path:
    outdir = repo_root / "media" / "videos" / market_lower
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir / f"{ymd}_{slot}.mp4"


def safe_rm(path: Path) -> None:
    try:
        if path.is_symlink() or path.is_file():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[WARN] could not delete: {path} err={e}", flush=True)


def force_clear_recent_done(repo_root: Path, market_lower: str, slot: str, keep_n: int = 6) -> None:
    """
    Clear recent <slot>.done.json markers to defeat main.py cache-hit.
    Lightweight: deletes only done.json.
    """
    base = repo_root / "data" / "cache" / market_lower
    if not base.exists():
        return

    dirs: list[Path] = []
    for p in base.iterdir():
        if p.is_dir() and len(p.name) == 10 and p.name[4] == "-" and p.name[7] == "-":
            dirs.append(p)

    dirs.sort(key=lambda x: x.name, reverse=True)
    for d in dirs[: max(1, int(keep_n))]:
        safe_rm(d / f"{slot}.done.json")


def latest_payload_fallback(repo_root: Path, market_lower: str, slot: str) -> Optional[Path]:
    """
    data/cache/<market>/<LATEST_YYYY-MM-DD>/<slot>.payload.json
    """
    base = repo_root / "data" / "cache" / market_lower
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


def read_payload_ymd_effective(payload_path: Path) -> str:
    try:
        obj = json.loads(payload_path.read_text(encoding="utf-8"))
        y = str(obj.get("ymd_effective") or obj.get("ymd") or "").strip()
        return y or payload_path.parent.name
    except Exception:
        return payload_path.parent.name


def latest_images_ymd(repo_root: Path, market_lower: str, slot: str) -> Optional[str]:
    """
    media/images/<market>/<YYYY-MM-DD>/<slot>/list.txt
    """
    base = repo_root / "media" / "images" / market_lower
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


def resolve_images_ymd(
    *,
    requested_ymd: str,
    images_ymd_arg: str,
    repo_root: Path,
    market_lower: str,
    slot: str,
    payload_path: Path,
) -> str:
    """
    requested|payload|latest|YYYY-MM-DD
    """
    v = str(images_ymd_arg).strip()
    vl = v.lower()

    if vl in ("payload", "auto"):
        return read_payload_ymd_effective(payload_path)

    if vl == "latest":
        yy = latest_images_ymd(repo_root, market_lower, slot)
        return yy or requested_ymd

    if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
        return v

    return requested_ymd


def post_align_images_dir(
    *,
    repo_root: Path,
    images_dir_path: Path,
    images_ymd: str,
    requested_ymd: str,
    images_ymd_arg: str,
    market_lower: str,
    slot: str,
    payload_path: Path,
) -> Tuple[Path, str]:
    """
    If images_dir missing after render_images, re-resolve ONCE for payload/latest.
    """
    if images_dir_path.exists():
        return images_dir_path, images_ymd

    v = str(images_ymd_arg).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
        return images_dir_path, images_ymd

    vl = v.lower()
    cand_ymd: Optional[str] = None

    if vl in ("payload", "auto"):
        cand_ymd = read_payload_ymd_effective(payload_path)
    elif vl == "latest":
        cand_ymd = latest_images_ymd(repo_root, market_lower, slot)

    if cand_ymd and cand_ymd != images_ymd:
        cand_dir = images_dir(repo_root, market_lower, cand_ymd, slot)
        if cand_dir.exists():
            print(f"[images] dir realigned after render -> {cand_dir}", flush=True)
            return cand_dir, cand_ymd

    return images_dir_path, images_ymd
