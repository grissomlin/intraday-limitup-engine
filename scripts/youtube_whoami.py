# scripts/youtube_whoami.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from pathlib import Path

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube.readonly",
]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--token", default="token.json")
    ap.add_argument("--video-id", default="")
    args = ap.parse_args()

    token_path = Path(args.token).resolve()
    creds = Credentials.from_authorized_user_file(str(token_path), scopes=SCOPES)
    yt = build("youtube", "v3", credentials=creds)

    # who am I
    me = yt.channels().list(part="snippet,contentDetails", mine=True).execute()
    items = me.get("items", [])
    if not items:
        print("❌ 這個 token 看不到任何 channel（可能 scope 不夠或帳號沒有 YouTube channel）")
        return

    ch = items[0]
    print("=== Token 對應的上傳頻道 ===")
    print("channelId:", ch["id"])
    print("title    :", ch["snippet"]["title"])

    if args.video_id:
        v = yt.videos().list(part="snippet,status", id=args.video_id).execute()
        vi = (v.get("items") or [])
        if not vi:
            print("\n❌ 查不到這個 video_id（可能不是這個 token 上傳的 / 或權限不足）")
            return
        vi = vi[0]
        print("\n=== 影片資訊 ===")
        print("videoId  :", args.video_id)
        print("channelId:", vi["snippet"]["channelId"])
        print("title    :", vi["snippet"]["title"])
        print("privacy  :", vi["status"]["privacyStatus"])

if __name__ == "__main__":
    main()
