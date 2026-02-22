# scripts/tiktok_upload.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import sys
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlencode, urlparse, parse_qs

import requests
from dotenv import load_dotenv

load_dotenv()  # load .env at repo root (or current working dir)


# =========================
# TikTok endpoints (v2)
# =========================
AUTH_URL = "https://www.tiktok.com/v2/auth/authorize/"
TOKEN_URL = "https://open.tiktokapis.com/v2/oauth/token/"

# Content Posting API (Upload to inbox / draft-like flow)
INIT_INBOX_URL = "https://open.tiktokapis.com/v2/post/publish/inbox/video/init/"


# =========================
# Helpers
# =========================
def _require_env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise RuntimeError(f"❌ 缺少環境變數 {name}。請在 repo 根目錄的 .env 裡填好。")
    return v


def build_auth_url(*, client_key: str, redirect_uri: str, scopes: List[str], state: str) -> str:
    params = {
        "client_key": client_key,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        # TikTok Login Kit v2 uses comma-separated scopes
        "scope": ",".join(scopes),
        "state": state,
    }
    return f"{AUTH_URL}?{urlencode(params)}"


def extract_code_from_redirect(redirected_full_url: str) -> str:
    """
    You paste the final redirected URL, e.g.
    https://grissomlin.github.io/?code=XXXX&state=demo
    """
    u = urlparse(redirected_full_url.strip())
    qs = parse_qs(u.query)
    code = (qs.get("code") or [""])[0]
    if not code:
        raise RuntimeError("找不到 code。請確認你貼的是授權後跳轉回來的完整網址（含 ?code=...）。")
    return code


def exchange_code_for_token(*, client_key: str, client_secret: str, redirect_uri: str, code: str) -> dict:
    """
    Exchange authorization code for access token.
    NOTE: Token response shape can differ by app settings/version.
    We'll print it for you to inspect.
    """
    data = {
        "client_key": client_key,
        "client_secret": client_secret,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": redirect_uri,
    }
    resp = requests.post(TOKEN_URL, data=data, timeout=60)
    try:
        j = resp.json()
    except Exception:
        raise RuntimeError(f"Token response not JSON: {resp.status_code} {resp.text[:300]}")
    if resp.status_code >= 400:
        raise RuntimeError(f"Token exchange failed: {resp.status_code} {j}")
    return j


def pick_access_token(token_json: dict) -> str:
    """
    Try common layouts:
      - {"access_token": "..."}
      - {"data": {"access_token": "..."}}
      - {"data": {"access_token": "...", ...}, "error": {...}}
    """
    tok = token_json.get("access_token")
    if tok:
        return str(tok)

    data = token_json.get("data") or {}
    tok2 = data.get("access_token")
    if tok2:
        return str(tok2)

    raise RuntimeError("找不到 access_token。請把 token_json 印出的內容貼我（記得遮住敏感資訊）我幫你對欄位。")


def init_inbox_video_upload(*, access_token: str, video_size: int) -> dict:
    """
    POST /v2/post/publish/inbox/video/init/
    scope: video.upload
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=UTF-8",
    }
    body = {
        "source_info": {
            "source": "FILE_UPLOAD",
            "video_size": int(video_size),
        }
    }
    resp = requests.post(INIT_INBOX_URL, headers=headers, json=body, timeout=60)
    try:
        j = resp.json()
    except Exception:
        raise RuntimeError(f"init response not JSON: {resp.status_code} {resp.text[:300]}")
    if resp.status_code >= 400:
        raise RuntimeError(f"init upload failed: {resp.status_code} {j}")
    return j


def pick_upload_url(init_json: dict) -> str:
    """
    Common layout:
      {"data": {"upload_url": "...", ...}}
    """
    data = init_json.get("data") or {}
    u = data.get("upload_url") or init_json.get("upload_url")
    if not u:
        raise RuntimeError(f"找不到 upload_url。init_json={init_json}")
    return str(u)


def put_upload_single(*, upload_url: str, video_path: Path) -> None:
    """
    Upload whole file via a single PUT with Content-Range.
    For large files, you should implement chunked upload.
    """
    data = video_path.read_bytes()
    size = len(data)
    mime = mimetypes.guess_type(str(video_path))[0] or "video/mp4"

    headers = {
        "Content-Type": mime,
        "Content-Length": str(size),
        "Content-Range": f"bytes 0-{size-1}/{size}",
    }
    resp = requests.put(upload_url, headers=headers, data=data, timeout=600)
    if resp.status_code >= 400:
        raise RuntimeError(f"upload PUT failed: {resp.status_code} {resp.text[:500]}")


def default_video_path(*, ymd: str, slot: str) -> Path:
    """
    Match your render_video.py default:
      media/videos/{ymd}_{slot}.mp4
    """
    return (Path("media") / "videos" / f"{ymd}_{slot}.mp4").resolve()


# =========================
# CLI
# =========================
def main():
    ap = argparse.ArgumentParser(description="TikTok Sandbox upload demo (OAuth + inbox video upload init + PUT).")
    ap.add_argument("--ymd", default="", help="YYYY-MM-DD, e.g. 2026-01-19 (optional)")
    ap.add_argument("--slot", default="midday", help="slot, e.g. midday/eod")
    ap.add_argument("--video", default="", help="mp4 path (optional; overrides --ymd/--slot)")
    ap.add_argument(
        "--redirect-url",
        default="",
        help="Paste the FULL redirected URL after you click authorize (contains ?code=...).",
    )
    ap.add_argument("--state", default="demo", help="OAuth state param (default demo)")
    ap.add_argument(
        "--scopes",
        default="user.info.basic,video.upload",
        help="Comma-separated scopes (default user.info.basic,video.upload)",
    )
    ap.add_argument(
        "--print-auth-url",
        action="store_true",
        help="Only print the authorization URL (step 1) and exit.",
    )

    args = ap.parse_args()

    # Load secrets from .env
    client_key = _require_env("TIKTOK_CLIENT_KEY")
    client_secret = _require_env("TIKTOK_CLIENT_SECRET")
    redirect_uri = _require_env("TIKTOK_REDIRECT_URI")

    scopes = [s.strip() for s in args.scopes.split(",") if s.strip()]
    if not scopes:
        raise RuntimeError("scopes 不能為空。")

    # Step 1: print auth url
    auth_url = build_auth_url(client_key=client_key, redirect_uri=redirect_uri, scopes=scopes, state=args.state)

    if args.print_auth_url or not args.redirect_url:
        print("=== Step 1) 打開這個網址完成授權 ===")
        print(auth_url)
        print("\n=== Step 2) 授權後跳轉回 redirect_uri，把完整網址貼回來 ===")
        print("例如：")
        print("  python -m scripts.tiktok_upload --redirect-url \"https://grissomlin.github.io/?code=XXXX&state=demo\" --ymd 2026-01-19 --slot midday")
        print("或指定影片：")
        print("  python -m scripts.tiktok_upload --redirect-url \"<PASTE_FULL_URL>\" --video \"media/videos/2026-01-19_midday.mp4\"")
        return

    # Decide video path
    if args.video.strip():
        video_path = Path(args.video).expanduser().resolve()
    elif args.ymd.strip():
        video_path = default_video_path(ymd=args.ymd.strip(), slot=args.slot.strip())
    else:
        raise RuntimeError("請提供 --video 或 --ymd（用預設 media/videos/{ymd}_{slot}.mp4）。")

    if not video_path.exists():
        raise RuntimeError(f"找不到影片：{video_path}")

    # Step 2: exchange code -> token
    code = extract_code_from_redirect(args.redirect_url)
    token_json = exchange_code_for_token(
        client_key=client_key,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        code=code,
    )
    print("token_json =", json.dumps(token_json, ensure_ascii=False, indent=2))

    access_token = pick_access_token(token_json)

    # Step 3: init upload
    init_json = init_inbox_video_upload(access_token=access_token, video_size=video_path.stat().st_size)
    print("init_json =", json.dumps(init_json, ensure_ascii=False, indent=2))

    upload_url = pick_upload_url(init_json)

    # Step 4: PUT upload
    print(f"Uploading: {video_path.name} ({video_path.stat().st_size} bytes)")
    put_upload_single(upload_url=upload_url, video_path=video_path)

    print("✅ Upload done.")
    print("📌 接下來請到 TikTok App / Studio 的『收件匣/草稿』完成後續發佈流程（inbox upload）。")


if __name__ == "__main__":
    main()
