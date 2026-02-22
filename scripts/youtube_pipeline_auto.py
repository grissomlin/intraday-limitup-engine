# scripts/youtube_pipeline_auto.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional


def run_capture(cmd: List[str]) -> str:
    print("▶", " ".join(cmd))
    p = subprocess.run(cmd, capture_output=True, text=True, check=True)
    # 把原輸出照樣印出來（你才看得到進度）
    if p.stdout:
        print(p.stdout, end="" if p.stdout.endswith("\n") else "\n")
    if p.stderr:
        print(p.stderr, end="" if p.stderr.endswith("\n") else "\n")
    return (p.stdout or "") + "\n" + (p.stderr or "")


def extract_video_id(output: str) -> str:
    for line in output.splitlines():
        if line.startswith("VIDEO_ID="):
            return line.split("=", 1)[1].strip()
    raise RuntimeError("找不到 VIDEO_ID=...，請確認 youtube_upload.py 有印出 VIDEO_ID=")


def load_metadata(path: str) -> Dict[str, Any]:
    p = Path(path).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"找不到 metadata.json：{p}")
    return json.loads(p.read_text(encoding="utf-8"))


def _safe_str(x: Any) -> str:
    return str(x).strip() if x is not None else ""


def _csv_tags(tags: Any) -> str:
    # meta["tags"] could be list[str] or comma-separated string
    if tags is None:
        return ""
    if isinstance(tags, list):
        return ",".join([_safe_str(t) for t in tags if _safe_str(t)])
    return ",".join([t.strip() for t in str(tags).split(",") if t.strip()])


def build_fallback_metadata(
    *,
    market: str,
    ymd: str,
    slot: str,
    title_prefix: str = "",
    desc_prefix: str = "",
    hashtags: str = "",
) -> Dict[str, Any]:
    """
    最小可用的 fallback metadata。
    - 你可以之後再把 title/desc 做得更像你想要的「統一生成規則」
    """
    m = (market or "").strip().upper()
    slot_label = slot or "midday"

    # very small, safe defaults
    title_main = f"{m}｜Market Movers速報｜{ymd} {slot_label}"
    if m == "JP":
        title_main = f"JP｜日本株 異動速報｜{ymd} {slot_label}"
    elif m == "KR":
        title_main = f"KR｜한국 주식 급등락 속보｜{ymd} {slot_label}"
    elif m == "TH":
        title_main = f"TH｜หุ้นไทย ด่วนเคลื่อนไหว｜{ymd} {slot_label}"
    elif m == "TW":
        title_main = f"TW｜台股異動速報｜{ymd} {slot_label}"
    elif m == "CN":
        title_main = f"CN｜A股异动速报｜{ymd} {slot_label}"
    elif m in ("US", "CA", "UK", "AU"):
        title_main = f"{m}｜Market Movers速報｜{ymd} {slot_label}"

    title = (title_prefix.strip() + " " + title_main).strip()

    desc = desc_prefix.strip()
    if not desc:
        if m == "JP":
            desc = "日本株式市場の強い値動きをショートでまとめます。"
        elif m == "KR":
            desc = "한국 주식시장의 강한 변동 종목을 쇼츠로 요약합니다."
        elif m == "TH":
            desc = "สรุปหุ้นไทยที่มีความเคลื่อนไหวแรงในรูปแบบ Shorts"
        elif m == "TW":
            desc = "每日整理台股強勢異動個股（Shorts）。"
        elif m == "CN":
            desc = "每日整理A股市场强势异动个股（Shorts）。"
        else:
            desc = "Daily market movers summarized in Shorts."

    if hashtags.strip():
        desc = desc.rstrip() + "\n" + hashtags.strip()

    # basic tags
    tags = [m, "Shorts", "Market Movers"]
    if m == "JP":
        tags = ["JP", "日本株", "異動速報", "Shorts"]
    elif m == "KR":
        tags = ["KR", "한국", "급등락", "속보", "Shorts"]
    elif m == "TH":
        tags = ["TH", "หุ้นไทย", "Shorts"]
    elif m == "TW":
        tags = ["TW", "台股", "異動速報", "Shorts"]
    elif m == "CN":
        tags = ["CN", "A股", "异动速报", "Shorts"]

    return {"market": m, "title": title, "description": desc, "tags": tags}


def main():
    ap = argparse.ArgumentParser(
        description="Auto pipeline (NO AI): upload unlisted -> add to playlist (optional)"
    )
    ap.add_argument("--video", required=True, help="Path to mp4")
    ap.add_argument("--token", default="secrets/youtube_token.upload.json", help="Path to token json")

    # metadata usage
    ap.add_argument(
        "--metadata",
        default="outputs/metadata.json",
        help="metadata json path (market/title/description/tags). If missing, can fallback with --market/--ymd/--slot",
    )
    ap.add_argument(
        "--no-metadata",
        action="store_true",
        help="Do not read metadata.json; use fallback metadata from args instead",
    )

    ap.add_argument("--market", default="", help="Fallback market code, e.g. JP/US/TW/KR/TH/CN/UK/AU/CA")
    ap.add_argument("--ymd", default="", help="Fallback YYYY-MM-DD for title")
    ap.add_argument("--slot", default="midday", help="Fallback slot label for title")
    ap.add_argument("--title-prefix", default="", help="Optional prefix before generated title")
    ap.add_argument("--desc-prefix", default="", help="Optional description prefix line")
    ap.add_argument("--hashtags", default="", help="Optional hashtags lines appended to description")

    # playlist
    ap.add_argument("--playlist-map", default="configs/youtube_playlists.json", help="market->playlist mapping")
    ap.add_argument("--skip-playlist", action="store_true", help="Upload only, do not add to playlist")

    args = ap.parse_args()

    video = str(Path(args.video).expanduser().resolve())
    token = str(Path(args.token).expanduser().resolve())

    # 1) metadata (NO AI)
    meta: Dict[str, Any]
    if not args.no_metadata:
        try:
            meta = load_metadata(args.metadata)
        except Exception as e:
            print(f"⚠️ metadata load failed: {e}")
            meta = {}
    else:
        meta = {}

    if not meta:
        # Fallback requires market+ymd (recommend), otherwise playlist mapping will fail
        m = (args.market or "").strip().upper()
        if not m:
            raise ValueError("你選了 --no-metadata 或 metadata 讀不到，請加上 --market JP/US/... 才能繼續。")

        ymd = (args.ymd or "").strip()
        if not ymd:
            raise ValueError("fallback 模式需要 --ymd YYYY-MM-DD（用在 title）。")

        meta = build_fallback_metadata(
            market=m,
            ymd=ymd,
            slot=str(args.slot or "midday"),
            title_prefix=str(args.title_prefix or ""),
            desc_prefix=str(args.desc_prefix or ""),
            hashtags=str(args.hashtags or ""),
        )

    # normalize fields
    market = _safe_str(meta.get("market")).upper()
    title = _safe_str(meta.get("title"))
    desc = _safe_str(meta.get("description") or meta.get("desc"))
    tags_csv = _csv_tags(meta.get("tags"))

    if not title:
        raise ValueError("metadata 缺少 title")
    if not desc:
        desc = ""  # allow empty
    if not tags_csv:
        tags_csv = ",".join([t for t in [market, "Shorts"] if t])

    # 2) upload (ALWAYS unlisted)
    out = run_capture(
        [
            "python",
            "scripts/youtube_upload.py",
            "--video",
            video,
            "--token",
            token,
            "--title",
            title,
            "--desc",
            desc,
            "--tags",
            tags_csv,
            "--privacy",
            "unlisted",
        ]
    )
    video_id = extract_video_id(out)
    print(f"\n✅ video_id captured: {video_id}")

    # 3) add to playlist
    if not args.skip_playlist:
        if not market:
            raise ValueError("metadata 缺少 market，無法對應 playlist。請在 metadata.json 補上 market。")

        run_capture(
            [
                "python",
                "scripts/youtube_add_to_playlist.py",
                "--token",
                token,
                "--video-id",
                video_id,
                "--metadata",
                str(Path(args.metadata).expanduser().resolve() if not args.no_metadata else Path("outputs/metadata.json").resolve()),
                "--playlist-map",
                str(Path(args.playlist_map).expanduser().resolve()),
            ]
        )

    # 4) persist last id (optional)
    Path("outputs/last_video_id.txt").parent.mkdir(parents=True, exist_ok=True)
    Path("outputs/last_video_id.txt").write_text(video_id, encoding="utf-8")
    print("\n✅ Done. Saved outputs/last_video_id.txt")


if __name__ == "__main__":
    main()