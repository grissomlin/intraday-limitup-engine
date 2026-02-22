# scripts/youtube_upload.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

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
    if not token_path.exists():
        raise FileNotFoundError(f"Token json not found: {token_path}")

    if not video_path.exists():
        raise FileNotFoundError(f"Video not found: {video_path}")

    privacy_status = _normalize_privacy(privacy_status)

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