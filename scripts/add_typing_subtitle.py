# scripts/add_typing_subtitle.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import os
import random
from pathlib import Path
from typing import List

try:
    # moviepy 2.x
    from moviepy import VideoFileClip, CompositeVideoClip
except Exception:
    # moviepy 1.x
    from moviepy.editor import VideoFileClip, CompositeVideoClip

from effects.typing import make_typing_overlay
from effects.marquee import make_marquee_overlay


# -------------------------
# MoviePy compat helpers
# -------------------------
def _with_duration(clip, dur: float):
    if hasattr(clip, "set_duration"):
        return clip.set_duration(dur)
    return clip.with_duration(dur)


# -------------------------
# Paths / picking
# -------------------------
def find_latest_video(videos_dir: Path = Path("media/videos")) -> Path:
    if not videos_dir.exists():
        raise RuntimeError(f"找不到 videos 目錄：{videos_dir.resolve()}")
    mp4s = sorted(videos_dir.glob("*.mp4"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not mp4s:
        raise RuntimeError(f"{videos_dir.resolve()} 底下沒有 mp4")
    return mp4s[0]


def resolve_subtitle_dir() -> Path:
    p = os.getenv("SUBTITLE_DIR", "").strip()
    if p:
        return Path(p)
    return Path("media/subtitles/public_domain")


def list_subtitle_files(subtitle_dir: Path) -> List[Path]:
    if not subtitle_dir.exists():
        raise RuntimeError(f"字幕資料夾不存在：{subtitle_dir.resolve()}")
    files = sorted(subtitle_dir.glob("*.txt"))
    files = [p for p in files if p.name.lower() not in {"requirements.txt"}]
    return files


def _read_all_lines(subtitle_dir: Path) -> List[str]:
    files = list_subtitle_files(subtitle_dir)
    if not files:
        raise RuntimeError(f"找不到字幕檔（*.txt）：{subtitle_dir.resolve()}")

    all_lines: List[str] = []
    for f in files:
        try:
            lines = [x.strip() for x in f.read_text(encoding="utf-8").splitlines() if x.strip()]
            all_lines.extend(lines)
        except Exception:
            continue
    if not all_lines:
        raise RuntimeError(f"字幕資料夾沒有可用文字：{subtitle_dir.resolve()}")
    return all_lines


def pick_n_lines(
    subtitle_dir: Path,
    n: int,
    *,
    min_len: int = 4,
    max_len: int = 30,
    unique: bool = True,
    seed: int | None = None,
) -> List[str]:
    rng = random.Random(seed)
    all_lines = _read_all_lines(subtitle_dir)

    pool = [s for s in all_lines if min_len <= len(s) <= max_len]
    if not pool:
        pool = all_lines[:]

    if unique:
        rng.shuffle(pool)
        return pool[: max(1, min(n, len(pool)))]
    return [rng.choice(pool) for _ in range(max(1, n))]


# -------------------------
# Main
# -------------------------
def main():
    ap = argparse.ArgumentParser()

    # input/output
    ap.add_argument("--in", dest="in_mp4", default="", help="input mp4 (default: latest in media/videos)")
    ap.add_argument("--out", dest="out_mp4", default="", help="output mp4 (default: add _typed suffix)")
    ap.add_argument("--subtitle-dir", default="", help="default: env SUBTITLE_DIR or media/subtitles/public_domain")

    # typing overlay lines
    ap.add_argument("--text", default="", help="force single subtitle text (skip random pick)")
    ap.add_argument("--n-lines", type=int, default=50, help="how many random lines (default: 10)")
    ap.add_argument("--min-len", type=int, default=4)
    ap.add_argument("--max-len", type=int, default=30)
    ap.add_argument("--no-unique", action="store_true", help="allow duplicates")
    ap.add_argument("--seed", type=int, default=0, help="random seed (0 = random)")

    # typing behavior
    ap.add_argument("--pos", default="center", choices=["center", "bottom"], help="subtitle position")
    ap.add_argument("--start", type=float, default=0.2, help="start time of first line")
    ap.add_argument("--chars-per-sec", type=float, default=12.0, help="typing speed")
    ap.add_argument("--hold", type=float, default=1.2, help="hold seconds per line")
    ap.add_argument("--gap", type=float, default=0.35, help="gap seconds between lines")
    ap.add_argument(
        "--use-fixed-line-seconds",
        action="store_true",
        help="use fixed seconds per line instead of chars-per-sec",
    )
    ap.add_argument("--line-seconds", type=float, default=2.8, help="typing seconds per line when fixed")

    # marquee (horizontal R->L) — ✅ 單條穩定版
    ap.add_argument("--no-ad", action="store_true", help="關閉跑馬燈（預設：啟用）")
    ap.add_argument("--ad", default="", help="marquee text; if empty, use built-in pool")
    ap.add_argument("--ad-speed", type=float, default=220.0, help="pixels/sec (default: 220)")
    ap.add_argument("--ad-alpha", type=int, default=200)
    ap.add_argument("--ad-emoji", default="strip", choices=["strip", "replace", "keep"])
    ap.add_argument("--ad-font-ratio", type=float, default=0.035, help="font size ratio (default: 0.035)")
    ap.add_argument("--ad-y-margin", type=float, default=0.20, help="top/bottom reserved ratio (default: 0.20)")
    ap.add_argument("--ad-y-seed", type=int, default=424242, help="seed for marquee y/text behavior")

    # cycle behavior
    ap.add_argument("--ad-cycle-seconds", type=float, default=2.0, help="fallback cycle seconds (unused if auto-cycle on)")
    ap.add_argument("--ad-auto-cycle", default="on", choices=["on", "off"], help="auto cycle so each sentence runs full R->L (default: on)")
    ap.add_argument("--ad-random-text", action="store_true", help="random text per cycle (stable within cycle)")

    # ✅ 保留區（中間放其他文字/唐詩宋詞區）
    ap.add_argument("--ad-typing-area-start", type=float, default=0.40, help="reserve area start ratio (default: 0.40)")
    ap.add_argument("--ad-typing-area-end", type=float, default=0.70, help="reserve area end ratio (default: 0.70)")
    ap.add_argument("--ad-min-distance-from-typing", type=float, default=0.03, help="extra padding ratio around reserve band (default: 0.03)")

    # ✅ 上下出現規則
    ap.add_argument("--ad-side-mode", default="alt", choices=["alt", "rand"], help="alt: alternate above/below each cycle; rand: random (default: alt)")
    ap.add_argument("--ad-y-jitter", type=float, default=1.0, help="y randomness in each region (1.0=full, 0.5=more centered)")

    args = ap.parse_args()

    # input
    in_mp4 = Path(args.in_mp4) if args.in_mp4 else find_latest_video()
    if not in_mp4.exists():
        raise RuntimeError(f"找不到輸入影片：{in_mp4}")

    base = VideoFileClip(str(in_mp4))
    fps = int(getattr(base, "fps", 0) or 30)

    subtitle_dir = Path(args.subtitle_dir) if args.subtitle_dir else resolve_subtitle_dir()
    seed = None if int(args.seed) == 0 else int(args.seed)

    # 1) typing lines
    if args.text.strip():
        lines: List[str] = [args.text.strip()]
        print(f"🧷 Subtitle forced: {lines[0]}")
    else:
        n = max(1, int(args.n_lines))
        lines = pick_n_lines(
            subtitle_dir,
            n,
            min_len=int(args.min_len),
            max_len=int(args.max_len),
            unique=(not args.no_unique),
            seed=seed,
        )
        print(f"🎴 Picked {len(lines)} lines from {subtitle_dir.resolve()}:")
        for i, s in enumerate(lines, 1):
            print(f"  {i:02d}. {s}")

    # 2) layers
    layers = [base]

    typing = make_typing_overlay(
        base,
        lines,
        start=float(args.start),
        chars_per_sec=float(args.chars_per_sec),
        use_chars_per_sec=(not bool(args.use_fixed_line_seconds)),
        type_seconds_per_line=float(args.line_seconds),
        hold_seconds_per_line=float(args.hold),
        gap_seconds=float(args.gap),
        position=str(args.pos),
    )
    layers.append(typing)

    # 3) marquee — ✅ 永遠單條，且每次在保留區上方/下方切換 + 高度不固定
    marquee_enabled = not bool(args.no_ad)
    if marquee_enabled:
        ad_pool = [
            "【廣告位】招商合作 / 聯繫我",
            "徵人啟事：誠徵甲方爸爸，待遇優厚，速洽本欄。",
            "尋人公告：急尋義父一名，走失已久，望見者速告。",
            "公益捐助：本欄接受捐助，咖啡一杯，溫暖一生。",
            "友情廣告：本跑馬燈提供廣告位，徵求各路英雄豪傑。",
            "徵才啟事：誠聘跑馬燈維護員，待遇優，速洽。",
            "失物招領：丟失一顆真心，拾得者請歸還。",
            "社區公告：跑馬燈廣告欄，歡迎刊登各類啟事。",
        ]

        marquee_text = args.ad.strip() if args.ad.strip() else ad_pool

        auto_cycle = (str(args.ad_auto_cycle).lower() == "on")

        marquee_row = make_marquee_overlay(
            base,
            marquee_text,
            path_mode="r2l",
            speed=float(args.ad_speed),
            margin_x=30,
            alpha=int(args.ad_alpha),
            font_size_ratio=float(args.ad_font_ratio),
            pad=12,
            emoji_mode=str(args.ad_emoji),

            # text cycling
            cycle_seconds=float(args.ad_cycle_seconds),
            auto_cycle_seconds=bool(auto_cycle),
            random_cycle=bool(args.ad_random_text),
            text_offset=0,

            # y safe margin
            y_margin_ratio=float(args.ad_y_margin),
            y_seed=int(args.ad_y_seed),

            # reserve band (middle area)
            reserve_on=True,
            reserve_y0_ratio=float(args.ad_typing_area_start),
            reserve_y1_ratio=float(args.ad_typing_area_end),
            reserve_padding_ratio=float(args.ad_min_distance_from_typing),

            # appear above/below reserve, with random y each cycle
            side_mode=str(args.ad_side_mode),
            y_jitter_ratio=float(args.ad_y_jitter),
        )

        layers.append(marquee_row)

        print("📢 Horizontal marquee (R->L) ON (Single-row stable mode)")
        print(f"   speed={args.ad_speed} alpha={args.ad_alpha} emoji={args.ad_emoji} font_ratio={args.ad_font_ratio}")
        print(f"   reserve={args.ad_typing_area_start}-{args.ad_typing_area_end} pad={args.ad_min_distance_from_typing} side={args.ad_side_mode} y_jitter={args.ad_y_jitter}")
        print(f"   auto_cycle={'on' if auto_cycle else 'off'} fallback_cycle_seconds={args.ad_cycle_seconds}")
        print(f"   text={'(custom)' if args.ad.strip() else '(pool)'}")

    else:
        print("📢 Horizontal marquee (R->L) OFF")

    # 4) composite + write
    final = CompositeVideoClip(layers)
    final = _with_duration(final, float(base.duration))

    out_mp4 = Path(args.out_mp4) if args.out_mp4 else in_mp4.with_name(in_mp4.stem + "_typed.mp4")
    out_mp4.parent.mkdir(parents=True, exist_ok=True)

    print(f"🎬 input  = {in_mp4}")
    print(f"🎞️ output = {out_mp4}")
    final.write_videofile(str(out_mp4), fps=fps, codec="libx264", audio_codec="aac")
    print("✅ done:", out_mp4)


if __name__ == "__main__":
    main()
