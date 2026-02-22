# scripts/youtube_publish.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from pathlib import Path

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

YOUTUBE_SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
]

CONFIRM_WORD = "PUBLISH_TO_PUBLIC"

def main():
    ap = argparse.ArgumentParser(description="Publish an existing YouTube video (set privacyStatus=public) with a hard confirm switch.")
    ap.add_argument("--token", required=True, help="Path to YouTube token json (upload+youtube scope)")
    ap.add_argument("--video-id", required=True, help="YouTube video id to publish")
    ap.add_argument("--title", default="", help="Optional: keep/update title (recommended to pass the current title)")
    ap.add_argument("--category", default="28", help="categoryId (needed when updating snippet in some cases)")
    ap.add_argument("--confirm", required=True, help=f"Must be exactly: {CONFIRM_WORD}")
    args = ap.parse_args()

    if args.confirm != CONFIRM_WORD:
        raise ValueError(f"❌ confirm mismatch. You must pass: --confirm {CONFIRM_WORD}")

    token_path = Path(args.token).expanduser().resolve()
    if not token_path.exists():
        raise FileNotFoundError(f"找不到 token：{token_path}")

    creds = Credentials.from_authorized_user_file(str(token_path), scopes=YOUTUBE_SCOPES)
    youtube = build("youtube", "v3", credentials=creds)

    # 只更新 status.privacyStatus（盡量不動 snippet，降低需求）
    body = {
        "id": args.video_id,
        "status": {
            "privacyStatus": "public",
            "selfDeclaredMadeForKids": False,
        },
    }

    # 有些情境會要求 snippet（依 API / 帳號狀態而異），提供可選參數讓你補
    if args.title:
        body["snippet"] = {
            "title": args.title,
            "categoryId": str(args.category),
        }
        part = "status,snippet"
    else:
        part = "status"

    resp = youtube.videos().update(part=part, body=body).execute()
    print("✅ Published! video_id =", resp.get("id"))

if __name__ == "__main__":
    main()
