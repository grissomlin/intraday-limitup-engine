# scripts/youtube_add_to_playlist.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
from pathlib import Path

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube"]


def main():
    ap = argparse.ArgumentParser(description="Add video to playlist based on metadata.json or direct playlist-id")
    ap.add_argument("--token", required=True)
    ap.add_argument("--video-id", required=True)

    # kept for backward compatibility (optional)
    ap.add_argument("--metadata", default="")
    ap.add_argument("--playlist-map", default="")

    # direct mode (what you are using now)
    ap.add_argument("--playlist-id", default="", help="Direct playlist id")
    ap.add_argument("--dry-run", action="store_true", help="Print target playlist only")
    args = ap.parse_args()

    token_path = Path(args.token).expanduser().resolve()
    if not token_path.exists():
        raise FileNotFoundError(f"Token file not found: {token_path}")

    playlist_id = (args.playlist_id or "").strip()
    playlist_name = "(direct playlist-id)"

    # Optional: if user still wants metadata/map mode
    if not playlist_id:
        if not args.metadata:
            raise ValueError("Missing --playlist-id (or provide --metadata + --playlist-map)")

        meta = json.loads(Path(args.metadata).read_text(encoding="utf-8"))
        market = meta.get("market")
        if not market:
            raise ValueError("metadata.json missing market")

        if not args.playlist_map:
            raise ValueError("Missing --playlist-map for metadata mode")

        playlists = json.loads(Path(args.playlist_map).read_text(encoding="utf-8"))
        if market not in playlists:
            raise ValueError(f"playlist map has no market: {market}")

        pl = playlists[market]
        playlist_id = pl["playlist_id"]
        playlist_name = pl.get("name") or playlist_name

    if args.dry_run:
        print("[DRY-RUN] video_id =", args.video_id)
        print("[DRY-RUN] playlistId =", playlist_id)
        print("[DRY-RUN] playlistName =", playlist_name)
        return

    creds = Credentials.from_authorized_user_file(str(token_path), scopes=YOUTUBE_SCOPES)
    youtube = build("youtube", "v3", credentials=creds)

    body = {
        "snippet": {
            "playlistId": playlist_id,
            "resourceId": {
                "kind": "youtube#video",
                "videoId": args.video_id,
            },
        }
    }

    resp = youtube.playlistItems().insert(part="snippet", body=body).execute()

    # NOTE: Avoid emoji here due to Windows cp950 console issues.
    print("[OK] Added to playlist")
    print("playlistId     =", playlist_id)
    print("playlistName   =", playlist_name)
    print("playlistItemId =", resp.get("id"))


if __name__ == "__main__":
    main()