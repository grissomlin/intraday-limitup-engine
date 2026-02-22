# scripts/render_video_with_typing.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import os
import random
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

# 直接重用你現有的 render_video.py（不用改它）
from scripts.render_video import build_video_from_images  # noqa


# -----------------------------
# ffmpeg helpers
# -----------------------------
def _run(cmd: List[str]) -> None:
    print("[typing] $", " ".join(cmd))
    subprocess.run(cmd, check=True)


def _ensure_ffmpeg() -> None:
    try:
        subprocess.run(["ffmpeg", "-version"], check=True, capture_output=True, text=True)
        subprocess.run(["ffprobe", "-version"], check=True, capture_output=True, text=True)
    except Exception as e:
        raise RuntimeError(
            "找不到 ffmpeg/ffprobe。請先安裝並加入 PATH。\n"
            "  - PowerShell: winget install Gyan.FFmpeg\n"
        ) from e


def _probe_duration_seconds(video_path: Path) -> float:
    # 用 ffprobe 取影片時長（秒）
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(video_path),
    ]
    out = subprocess.check_output(cmd, text=True).strip()
    try:
        return float(out)
    except Exception:
        raise RuntimeError(f"ffprobe 解析失敗: {out!r}")


# -----------------------------
# Quote pool
# -----------------------------
def _read_lines_from_txt(path: Path) -> List[str]:
    if not path.exists():
        return []
    lines = []
    for ln in path.read_text(encoding="utf-8").splitlines():
        s = ln.strip()
        if s:
            lines.append(s)
    return lines


def load_quote_pool(
    packs_dir: Path,
    pack_files: List[str],
) -> List[str]:
    pool: List[str] = []
    for fn in pack_files:
        p = packs_dir / fn
        pool.extend(_read_lines_from_txt(p))
    # 去重但保留順序（避免很多重複句）
    seen = set()
    uniq = []
    for x in pool:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


# -----------------------------
# ASS typing subtitle generator
# -----------------------------
def _sec_to_ass_time(t: float) -> str:
    # ASS 時間格式: H:MM:SS.cc（cc=1/100秒）
    if t < 0:
        t = 0.0
    cs = int(round(t * 100))
    h = cs // (3600 * 100)
    cs -= h * 3600 * 100
    m = cs // (60 * 100)
    cs -= m * 60 * 100
    s = cs // 100
    cs -= s * 100
    return f"{h}:{m:02d}:{s:02d}.{cs:02d}"


def _escape_ass_text(s: str) -> str:
    # ASS 需要逃逸: \ { } 以及換行
    s = s.replace("\\", r"\\")
    s = s.replace("{", r"\{").replace("}", r"\}")
    s = s.replace("\n", r"\N")
    return s


def _build_karaoke_text(text: str, char_step_s: float) -> str:
    """
    用 ASS karaoke \k 做「打字機」：
    {\k05}白{\k05}日... 每個字延遲顯示
    """
    cs = max(1, int(round(char_step_s * 100)))  # centiseconds
    parts = []
    for ch in text:
        ch2 = _escape_ass_text(ch)
        parts.append(f"{{\\k{cs}}}{ch2}")
    return "".join(parts)


def write_typing_ass(
    out_ass: Path,
    *,
    duration_s: float,
    pool: List[str],
    seed: Optional[int],
    quote_every_s: float,
    char_step_s: float,
    hold_s: float,
    font: str,
    font_size: int,
    margin_v: int,
    align: int,
    max_chars: int,
) -> None:
    """
    - 不管圖片怎麼轉場：只看影片 duration_s
    - 每 quote_every_s 秒換一句（或等一句打完後留 hold_s 再換）
    """
    if not pool:
        raise RuntimeError("字幕池是空的：請確認 subtitles/public_domain 下面 txt 有內容。")

    rnd = random.Random(seed)

    # 讓每句不要太長（避免手機看不到/太慢打字）
    def pick_quote() -> str:
        for _ in range(20):
            q = rnd.choice(pool)
            q = q.strip()
            if q and len(q) <= max_chars:
                return q
        # 實在挑不到就硬拿一個並截斷
        q = rnd.choice(pool).strip()
        return q[:max_chars]

    # ASS header
    out_ass.parent.mkdir(parents=True, exist_ok=True)
    header = f"""[Script Info]
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920
WrapStyle: 2
ScaledBorderAndShadow: yes

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,{font},{font_size},&H00FFFFFF,&H000000FF,&H00000000,&H64000000,0,0,0,0,100,100,0,0,1,3,0,{align},64,64,{margin_v},1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""
    lines: List[str] = [header]

    t = 0.0
    # 我們用「一句一段」：開始 -> 結束
    # 一句的總時間 = max(打字時間, quote_every_s) + hold_s
    while t < duration_s - 0.05:
        quote = pick_quote()
        typing_time = len(quote) * char_step_s
        block_time = max(typing_time, quote_every_s) + hold_s

        start = t
        end = min(duration_s, t + block_time)

        # 產生 karaoke 文字（打字效果）
        ktext = _build_karaoke_text(quote, char_step_s)

        # Dialogue 行
        s_start = _sec_to_ass_time(start)
        s_end = _sec_to_ass_time(end)
        lines.append(f"Dialogue: 0,{s_start},{s_end},Default,,0,0,0,,{ktext}\n")

        t += block_time

    out_ass.write_text("".join(lines), encoding="utf-8")


def burn_ass_to_video(
    *,
    in_mp4: Path,
    ass_path: Path,
    out_mp4: Path,
) -> None:
    out_mp4.parent.mkdir(parents=True, exist_ok=True)

    # Windows 路徑要注意：ffmpeg subtitles filter 吃的是字串
    # 直接用完整路徑通常可行
    vf = f"subtitles={ass_path.as_posix()}"
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(in_mp4),
        "-vf",
        vf,
        "-c:v",
        "libx264",
        "-crf",
        "18",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "copy",
        str(out_mp4),
    ]
    _run(cmd)


def main():
    ap = argparse.ArgumentParser(description="Render video then burn typing subtitles from public_domain packs.")
    ap.add_argument("--ymd", required=True, help="YYYY-MM-DD, e.g. 2026-01-22")
    ap.add_argument("--slot", default="midday", help="slot folder name, e.g. midday/eod")

    # 影片參數（沿用你 render_video 的概念）
    ap.add_argument("--seconds", type=float, default=2.0, help="seconds per image (default 2.0)")
    ap.add_argument("--fps", type=int, default=30, help="output fps (default 30)")
    ap.add_argument("--crf", type=int, default=18, help="x264 crf (default 18)")
    ap.add_argument("--scale", default="1080:1920", help="force scale like 1080:1920")
    ap.add_argument("--fade", action="store_true", help="add simple fade-in/out for whole video")
    ap.add_argument("--ext", default="png", choices=["png", "jpg", "jpeg"], help="image extension")
    ap.add_argument("--prefer-top", type=int, default=15, help="overview prefer top N")
    ap.add_argument("--out", default="", help="output mp4 path (optional)")

    # 字幕來源
    ap.add_argument("--packs-dir", default="subtitles/public_domain", help="字幕素材資料夾")
    ap.add_argument(
        "--packs",
        default="tang_lines.txt,songci_lines.txt,shijing_lines.txt,nantang_lihouzhu_lines.txt",
        help="逗號分隔檔名（在 packs-dir 內）",
    )

    # 打字機字幕規則（完全不管圖片）
    ap.add_argument("--seed", type=int, default=0, help="固定隨機種子（0=用 ymd 當 seed）")
    ap.add_argument("--quote-every", type=float, default=3.0, help="至少每幾秒換一句（default 3.0）")
    ap.add_argument("--char-step", type=float, default=0.06, help="每字顯示間隔秒（default 0.06）")
    ap.add_argument("--hold", type=float, default=0.6, help="一句打完後停留秒數（default 0.6）")

    # 字幕樣式
    ap.add_argument("--font", default="Microsoft JhengHei", help="字幕字型（Windows 建議 Microsoft JhengHei）")
    ap.add_argument("--font-size", type=int, default=64, help="字幕字體大小（1080x1920 建議 56~72）")
    ap.add_argument("--margin-v", type=int, default=140, help="字幕底部距離（越大越往上）")
    ap.add_argument("--align", type=int, default=2, help="ASS 對齊：2=底部置中，8=頂部置中，5=正中")
    ap.add_argument("--max-chars", type=int, default=16, help="每句最長字數（避免太長）")

    args = ap.parse_args()

    _ensure_ffmpeg()

    # 1) 先用你現有的圖片輸出資料夾產 base video
    images_dir = Path("media") / "images" / args.ymd / args.slot
    if not images_dir.exists():
        raise RuntimeError(f"找不到圖片資料夾：{images_dir}（先跑 scripts.render_images.cli 生圖）")

    base_mp4 = Path(args.out) if args.out else (Path("media") / "videos" / f"{args.ymd}_{args.slot}.mp4")

    build_video_from_images(
        images_dir=images_dir,
        out_mp4=base_mp4,
        seconds_per_image=args.seconds,
        fps=args.fps,
        crf=args.crf,
        force_scale=(args.scale.strip() if args.scale.strip() else None),
        fade=args.fade,
        ext=args.ext,
        prefer_top=args.prefer_top,
        use_existing_list=True,
    )

    # 2) 取影片總秒數，生成「打字機字幕」(ASS)
    dur = _probe_duration_seconds(base_mp4)

    packs_dir = Path(args.packs_dir)
    pack_files = [x.strip() for x in (args.packs or "").split(",") if x.strip()]
    pool = load_quote_pool(packs_dir, pack_files)

    seed = args.seed if args.seed != 0 else abs(hash(args.ymd)) % (2**31)

    ass_path = base_mp4.with_suffix(".typing.ass")
    write_typing_ass(
        ass_path,
        duration_s=dur,
        pool=pool,
        seed=seed,
        quote_every_s=args.quote_every,
        char_step_s=args.char_step,
        hold_s=args.hold,
        font=args.font,
        font_size=args.font_size,
        margin_v=args.margin_v,
        align=args.align,
        max_chars=args.max_chars,
    )

    # 3) 燒字幕進影片（輸出 *_sub.mp4）
    out_mp4 = base_mp4.with_name(base_mp4.stem + "_sub.mp4")
    burn_ass_to_video(in_mp4=base_mp4, ass_path=ass_path, out_mp4=out_mp4)

    print(f"✅ base video -> {base_mp4}")
    print(f"✅ ass -> {ass_path}")
    print(f"✅ subtitled video -> {out_mp4}")


if __name__ == "__main__":
    main()
