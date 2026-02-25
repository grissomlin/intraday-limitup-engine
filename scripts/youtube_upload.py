# scripts/youtube_upload.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import base64
import os
import tempfile
from pathlib import Path
from typing import List, Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]


def _normalize_privacy(s: str) -> str:
    v = (s or "").strip().lower()
    if v not in ("private", "unlisted", "public"):
        raise ValueError(f"Invalid privacy: {s!r} (must be private/unlisted/public)")
    return v


def _normalize_description(desc: str) -> str:
    """
    Normalize description before sending to YouTube API.

    Problem we solve:
      - In youtube_pipeline_safe.py we convert multiline text into PowerShell-safe `n
        (so argv remains single-line). That means this uploader may receive literal
        backtick sequences like:
            "...line1`n`nline2..."
        We MUST convert them back into real newlines.

    Also keep prior normalization:
      - Ensure LF newlines (no CR)
      - Remove NUL
      - Trim excessive trailing whitespace
    """
    s = str(desc or "")

    # ✅ KEY FIX: PowerShell-style newline escapes -> real LF
    # Handle both `r`n and `n (order matters: convert CRLF escape first)
    s = s.replace("`r`n", "\n").replace("`n", "\n")

    # Standard newline normalization
    s = s.replace("\r\n", "\n").replace("\r", "\n")

    # Remove NUL
    s = s.replace("\x00", "")

    # Avoid massive trailing whitespace per line + trim ends
    s = "\n".join([line.rstrip() for line in s.split("\n")]).strip()

    return s


def _write_temp_token_json(json_text: str) -> Path:
    tmp = tempfile.NamedTemporaryFile(
        delete=False,
        suffix=".json",
        prefix="yt_token_",
    )
    tmp.write(json_text.encode("utf-8"))
    tmp.flush()
    tmp.close()
    return Path(tmp.name)


def _load_token_from_env_try(var_name: str) -> Optional[Path]:
    """
    Try to load token.json content from env var (base64-encoded JSON),
    write to temp file, validate + refresh, then return temp path if ok.
    """
    raw_b64 = (os.getenv(var_name) or "").strip()
    if not raw_b64:
        print(f"[INFO] {var_name} not set")
        return None

    print(f"[INFO] Trying {var_name} ...")

    try:
        decoded_json = base64.b64decode(raw_b64).decode("utf-8")
    except Exception as e:
        print(f"[FAIL] {var_name} base64 decode error: {e}")
        return None

    token_path = _write_temp_token_json(decoded_json)

    try:
        creds = Credentials.from_authorized_user_file(str(token_path), scopes=YOUTUBE_SCOPES)

        # Attempt refresh if needed
        if creds and creds.expired and creds.refresh_token:
            print("[INFO] refreshing token...")
            creds.refresh(Request())

        if not creds or not creds.valid:
            print(f"[FAIL] {var_name} invalid credentials (creds.valid=False)")
            return None

        print(f"[PASS] {var_name} works (token_path={token_path})")
        return token_path

    except Exception as e:
        print(f"[FAIL] {var_name} credential parse/refresh error: {e}")
        return None


def _resolve_token_path(user_token_path: Path) -> Path:
    """
    Resolve which token.json to use, in this priority:
      1) user provided token file path (if exists)
      2) env secrets: GDRIVE_TOKEN_JSON_B64
      3) env secrets: GDRIVE_TOKEN_B64
    """
    if user_token_path.exists():
        print(f"[INFO] Using token file: {user_token_path}")
        return user_token_path

    print(f"[WARN] Token file not found: {user_token_path}")
    print("[INFO] Trying GitHub Secrets env vars...")

    for var in ("GDRIVE_TOKEN_JSON_B64", "GDRIVE_TOKEN_B64"):
        p = _load_token_from_env_try(var)
        if p:
            return p

    raise FileNotFoundError(
        "No valid token found. Checked:\n"
        f"  - file: {user_token_path}\n"
        "  - env: GDRIVE_TOKEN_JSON_B64\n"
        "  - env: GDRIVE_TOKEN_B64\n"
    )


def upload_video(
    *,
    token_path: Path,
    video_path: Path,
    title: str,
    description: str,
    tags: List[str],
    category_id: str,
    privacy_status: str,
) -> dict:
    if not video_path.exists():
        raise FileNotFoundError(f"Video not found: {video_path}")

    privacy_status = _normalize_privacy(privacy_status)

    # Resolve token path (file -> env fallback)
    token_path = _resolve_token_path(token_path)

    # debug: how many literal `n exist before normalize
    try:
        raw = str(description or "")
        print(f"[DEBUG] raw_desc_len={len(raw)} backtick_n_count={raw.count('`n')}")
    except Exception:
        pass

    description = _normalize_description(description)

    # DEBUG: clearly print what we send
    newline_count = description.count("\n")
    has_cr = "\r" in description
    print(f"[DEBUG] privacy_status -> {privacy_status}")
    print(f"[DEBUG] desc_len={len(description)} newline_count={newline_count} has_cr={has_cr}")

    creds = Credentials.from_authorized_user_file(str(token_path), scopes=YOUTUBE_SCOPES)

    # Optional: refresh here too (defensive)
    if creds and creds.expired and creds.refresh_token:
        print("[INFO] refreshing token (defensive)...")
        creds.refresh(Request())

    youtube = build("youtube", "v3", credentials=creds)

    body = {
        "snippet": {
            "title": str(title),
            "description": description,
            "tags": list(tags or []),
            "categoryId": str(category_id),
        },
        "status": {
            "privacyStatus": privacy_status,  # private | unlisted | public
            "selfDeclaredMadeForKids": False,
        },
    }

    # ✅ NON-resumable upload (more stable for description/newlines)
    media = MediaFileUpload(
        str(video_path),
        mimetype="video/mp4",
        resumable=False,
    )

    response = youtube.videos().insert(
        part="snippet,status",
        body=body,
        media_body=media,
    ).execute()

    return response


def main():
    ap = argparse.ArgumentParser(description="Upload MP4 to YouTube using existing token.json (NON-resumable, stable)")
    ap.add_argument("--video", required=True, help="Path to .mp4")
    ap.add_argument("--token", default="token.json", help="Path to token.json (default: ./token.json)")
    ap.add_argument("--title", default="Intraday Limitup Dashboard", help="Video title")
    ap.add_argument("--desc", default="Auto-generated video upload test.", help="Video description")
    ap.add_argument("--tags", default="stocks,tw,limitup,quant", help="Comma separated tags")
    ap.add_argument("--category", default="28", help="YouTube categoryId (default 28 Science & Technology)")
    ap.add_argument("--privacy", default="unlisted", choices=["private", "unlisted", "public"], help="Privacy status")
    ap.add_argument(
        "--out-id",
        default="",
        help="Optional: write uploaded video_id to this file (e.g. outputs/last_video_id.txt)",
    )

    args = ap.parse_args()

    token_path = Path(args.token).expanduser().resolve()
    video_path = Path(args.video).expanduser().resolve()
    tags = [t.strip() for t in str(args.tags).split(",") if t.strip()]

    resp = upload_video(
        token_path=token_path,
        video_path=video_path,
        title=str(args.title),
        description=str(args.desc),
        tags=tags,
        category_id=str(args.category),
        privacy_status=str(args.privacy),
    )

    video_id = resp.get("id")
    status = (resp.get("status") or {})
    privacy = status.get("privacyStatus")

    print("\n[OK] Upload success!")
    print("video_id =", video_id)
    print("privacy  =", privacy)

    if video_id:
        print("URL = https://www.youtube.com/watch?v=" + video_id)
        print("VIDEO_ID=" + video_id)

        if args.out_id:
            out_path = Path(args.out_id).expanduser().resolve()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(video_id, encoding="utf-8")
            print("[OK] Wrote video_id to:", out_path)
    else:
        raise RuntimeError("Upload completed but response has no video id.")


if __name__ == "__main__":
    main()
