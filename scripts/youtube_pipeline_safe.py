# scripts/youtube_pipeline_safe.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

# ✅ Ensure repo root in sys.path so "scripts.*" imports work even when executed as a script
THIS = Path(__file__).resolve()
REPO_ROOT = THIS.parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# ✅ Use centralized metadata builder
from scripts.metadata_builder import build_metadata  # noqa: E402


# ===============================
# Utils
# ===============================

def run_capture(cmd: List[str]) -> str:
    """
    Run subprocess but ALWAYS print stdout/stderr.
    If failed, raise readable RuntimeError.
    """
    print("▶", " ".join(cmd))
    p = subprocess.run(cmd, capture_output=True, text=True, check=False)

    out = p.stdout or ""
    err = p.stderr or ""

    if out:
        print(out, end="" if out.endswith("\n") else "\n")
    if err:
        print(err, end="" if err.endswith("\n") else "\n")

    if p.returncode != 0:
        tail_src = err if err.strip() else out
        tail = "\n".join((tail_src.splitlines() or [])[-120:])
        raise RuntimeError(
            f"\n[ERROR] Subprocess failed (exit={p.returncode})\n"
            f"Command: {' '.join(cmd)}\n"
            f"--- last output ---\n{tail}\n"
        )

    return out + "\n" + err


def extract_video_id(output: str) -> str:
    for line in output.splitlines():
        if line.startswith("VIDEO_ID="):
            return line.split("=", 1)[1].strip()
    raise RuntimeError("Cannot find VIDEO_ID=... (youtube_upload.py may have failed)")


def normalize_privacy(v: str) -> str:
    vv = (v or "").strip().lower()
    if vv not in ("private", "unlisted", "public"):
        raise ValueError("privacy must be one of: private / unlisted / public")
    return vv


def sanitize_youtube_text(s: str) -> str:
    """
    Aggressive sanitizer for YouTube Data API text:

    - normalize newlines to LF
    - replace smart quotes / arrows with safe ascii
    - remove BOM / zero-width chars / variation selectors
    - remove control chars
    - remove non-BMP chars (most emoji)
    - remove unicode category "So" (Symbol, other) (includes ⚠ and many symbols)
    """
    if s is None:
        return ""

    # normalize newlines
    s = str(s).replace("\r\n", "\n").replace("\r", "\n")

    repl = {
        "\u201c": '"',   # “
        "\u201d": '"',   # ”
        "\u2018": "'",   # ‘
        "\u2019": "'",   # ’
        "\u2013": "-",   # –
        "\u2014": "-",   # —
        "\u2212": "-",   # −
        "\u00a0": " ",   # NBSP
        "\u200b": "",    # zero width space
        "\u200c": "",    # zero width non-joiner
        "\u200d": "",    # zero width joiner
        "\ufeff": "",    # BOM
        "\u2028": "\n",  # line separator
        "\u2029": "\n",  # paragraph separator
        "\u2192": "->",  # →
        "\ufe0f": "",    # VS16
        "\ufe0e": "",    # VS15
    }
    for a, b in repl.items():
        s = s.replace(a, b)

    cleaned: List[str] = []
    for ch in s:
        o = ord(ch)

        if ch in ("\n", "\t"):
            cleaned.append(ch)
            continue

        # drop C0 controls + DEL
        if o < 32 or o == 127:
            continue

        # drop surrogate range explicitly
        if 0xD800 <= o <= 0xDFFF:
            continue

        # drop non-BMP (emoji etc.)
        if o > 0xFFFF:
            continue

        # drop Symbol, Other (So)
        try:
            cat = unicodedata.category(ch)
            if cat == "So":
                continue
        except Exception:
            pass

        cleaned.append(ch)

    s2 = "".join(cleaned).strip()

    # collapse excessive blank lines
    while "\n\n\n" in s2:
        s2 = s2.replace("\n\n\n", "\n\n")

    return s2


def sanitize_tags(tags: List[str]) -> List[str]:
    """Keep tags short and safe; remove commas/newlines."""
    out: List[str] = []
    for t in (tags or []):
        tt = sanitize_youtube_text(str(t))
        tt = tt.replace(",", " ").replace("\n", " ").replace("\t", " ").strip()
        while "  " in tt:
            tt = tt.replace("  ", " ")
        if not tt:
            continue
        if len(tt) > 80:
            tt = tt[:80].rstrip()
        out.append(tt)
    return out


def to_ps_arg_text(s: str) -> str:
    """
    IMPORTANT:
    When calling a subprocess with an argv list on Windows, embedding literal newlines
    inside an argument can be fragile (PowerShell/argparse edge cases).

    We convert LF newlines into PowerShell-friendly escape sequence `n
    so the *command line* remains a single-line argument while YouTube gets real newlines.

    Example:
      "line1\\n\\nline2" -> "line1`n`nline2"
    """
    if s is None:
        return ""
    s = str(s).replace("\r\n", "\n").replace("\r", "\n")
    return s.replace("\n", "`n")


def write_latest_meta(*, market: str, ymd: str, slot: str, video_id: str, privacy: str) -> Path:
    """
    Write outputs/latest_meta.json for Drive dashboard to read.

    This file will be uploaded to Drive:
      {MARKET}/Latest/{slot}/latest_meta.json

    Schema (stable):
      - market, ymd, slot
      - youtube_video_id, privacy
      - youtube_url
      - updated_utc
    """
    Path("outputs").mkdir(exist_ok=True)

    vid = str(video_id or "").strip()
    obj = {
        "market": str(market or "").strip().upper(),
        "ymd": str(ymd or "").strip(),
        "slot": str(slot or "").strip().lower(),
        "youtube_video_id": vid,
        "privacy": str(privacy or "").strip().lower(),
        "youtube_url": f"https://www.youtube.com/watch?v={vid}" if vid else "",
        "updated_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }

    out_path = Path("outputs/latest_meta.json")
    out_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved: {out_path.as_posix()}")
    return out_path


# ===============================
# Environment-aware loaders
# ===============================

def resolve_token_path(default_path: str) -> str:
    """
    If env YOUTUBE_TOKEN_JSON exists, write it to a temp file and use it.
    Else use local file path.
    """
    env_json = os.getenv("YOUTUBE_TOKEN_JSON", "").strip()
    if env_json:
        print("[INFO] Using YOUTUBE_TOKEN_JSON from environment")
        Path("secrets").mkdir(exist_ok=True)
        token_path = Path("secrets/youtube_token.from_env.json")
        token_path.write_text(env_json, encoding="utf-8")
        return str(token_path.resolve())

    return str(Path(default_path).expanduser().resolve())


def resolve_playlist_map(default_path: str) -> str:
    """
    If env YOUTUBE_PLAYLIST_MAP_JSON exists, write it to a temp file and use it.
    Else use local file path.
    """
    env_json = os.getenv("YOUTUBE_PLAYLIST_MAP_JSON", "").strip()
    if env_json:
        print("[INFO] Using YOUTUBE_PLAYLIST_MAP_JSON from environment")
        Path("config").mkdir(exist_ok=True)
        map_path = Path("config/youtube_playlists.from_env.json")
        obj = json.loads(env_json)
        map_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(map_path.resolve())

    return str(Path(default_path).expanduser().resolve())


# ===============================
# Playlist map
# ===============================

def load_playlist_id(market: str, playlist_map_path: str) -> str:
    p = Path(playlist_map_path)
    if not p.exists():
        raise FileNotFoundError(f"playlist map not found: {p.resolve()}")

    data = json.loads(p.read_text(encoding="utf-8"))
    if market not in data:
        raise ValueError(f"playlist map missing market: {market}")

    pid = (data[market] or {}).get("playlist_id")
    if not pid:
        raise ValueError(f"playlist_id empty for market: {market}")

    return str(pid)


# ===============================
# Main
# ===============================

def main():
    ap = argparse.ArgumentParser(description="YouTube pipeline (env-aware, safe args)")
    ap.add_argument("--video", required=True)
    ap.add_argument("--token", default="secrets/youtube_token.upload.json")

    ap.add_argument("--market", required=True)
    ap.add_argument("--ymd", required=True)
    ap.add_argument("--slot", default="midday")

    ap.add_argument("--playlist-map", default="config/youtube_playlists.json")
    ap.add_argument("--playlist-id", default="")
    ap.add_argument("--skip-playlist", action="store_true")

    ap.add_argument("--privacy", default="unlisted", choices=["private", "unlisted", "public"])

    args = ap.parse_args()
    py = sys.executable  # use same python

    video = str(Path(args.video).expanduser().resolve())
    if not Path(video).exists():
        raise FileNotFoundError(f"Video not found: {video}")

    token = resolve_token_path(args.token)
    if not Path(token).exists():
        raise FileNotFoundError(f"Token file not found: {token}")

    playlist_map_path = resolve_playlist_map(args.playlist_map)

    meta: Dict[str, Any] = build_metadata(args.market, args.ymd, args.slot)

    # sanitize metadata to avoid invalidDescription + weird unicode issues
    meta_title = sanitize_youtube_text(str(meta.get("title", "")))
    meta_desc_raw = sanitize_youtube_text(str(meta.get("description", "")))
    meta_tags = sanitize_tags([str(x) for x in (meta.get("tags") or [])])
    privacy = normalize_privacy(args.privacy)

    # ✅ KEY FIX: turn multiline description into PowerShell-safe single argv string using `n
    meta_desc = to_ps_arg_text(meta_desc_raw)

    # debug visibility
    try:
        print(
            f"[PIPE_DEBUG] title_len={len(meta_title)} "
            f"desc_len={len(meta_desc_raw)} newline_count={meta_desc_raw.count(chr(10))} "
            f"desc_arg_len={len(meta_desc)}"
        )
    except Exception:
        pass

    # 1) Upload
    out = run_capture([
        py,
        "scripts/youtube_upload.py",
        "--video", video,
        "--token", token,
        "--title", meta_title,
        "--desc", meta_desc,
        "--tags", ",".join(meta_tags),
        "--privacy", privacy,
    ])

    video_id = extract_video_id(out)
    print(f"\n[OK] video_id captured: {video_id}")

    for line in out.splitlines():
        if line.strip().startswith("privacy"):
            print("[INFO] upload returned:", line.strip())

    # 2) Add to playlist
    if not args.skip_playlist:
        if args.playlist_id.strip():
            playlist_id = args.playlist_id.strip()
        else:
            playlist_id = load_playlist_id(str(meta.get("market") or "").strip(), playlist_map_path)

        run_capture([
            py,
            "scripts/youtube_add_to_playlist.py",
            "--token", token,
            "--video-id", video_id,
            "--playlist-id", playlist_id,
        ])

    # 3) Save outputs for downstream steps (Drive dashboard, etc.)
    Path("outputs").mkdir(exist_ok=True)
    Path("outputs/last_video_id.txt").write_text(video_id, encoding="utf-8")
    write_latest_meta(
        market=args.market,
        ymd=args.ymd,
        slot=args.slot,
        video_id=video_id,
        privacy=privacy,
    )

    print("\n[OK] Pipeline completed.")
    print("Saved: outputs/last_video_id.txt")
    print("Saved: outputs/latest_meta.json")


if __name__ == "__main__":
    main()
