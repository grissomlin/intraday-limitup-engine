# scripts/youtube_create_playlist.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from pathlib import Path

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

YOUTUBE_SCOPES = [
    "https://www.googleapis.com/auth/youtube",  # 建 playlist 需要這個
]

def create_playlist(*, token_path: Path, name: str, desc: str, privacy: str) -> dict:
    if not token_path.exists():
        raise FileNotFoundError(f"找不到 token：{token_path}")

    creds = Credentials.from_authorized_user_file(str(token_path), scopes=YOUTUBE_SCOPES)
    youtube = build("youtube", "v3", credentials=creds)

    body = {
        "snippet": {
            "title": name,
            "description": desc,
        },
        "status": {
            "privacyStatus": privacy,  # public | unlisted | private
        },
    }

    resp = youtube.playlists().insert(
        part="snippet,status",
        body=body,
    ).execute()

    return resp

def main() -> int:
    ap = argparse.ArgumentParser(description="Create a YouTube playlist")
    ap.add_argument("--token", required=True, help="Path to token json (youtube scope)")
    ap.add_argument("--name", required=True, help="Playlist name")
    ap.add_argument("--desc", default="", help="Playlist description")
    ap.add_argument("--privacy", default="public", choices=["public", "unlisted", "private"])
    args = ap.parse_args()

    resp = create_playlist(
        token_path=Path(args.token).expanduser().resolve(),
        name=args.name,
        desc=args.desc,
        privacy=args.privacy,
    )

    playlist_id = resp.get("id")
    print("✅ Playlist created!")
    print("name       =", args.name)
    print("playlistId =", playlist_id)
    # 給你後面寫入 youtube_playlists.json 用
    print("PLAYLIST_ID=" + (playlist_id or ""))

    return 0

if __name__ == "__main__":
    raise SystemExit(main())