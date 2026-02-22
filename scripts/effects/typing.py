# effects/typing.py
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Tuple

import numpy as np
from PIL import Image, ImageDraw
from moviepy import VideoClip

from .utils import load_font, with_mask, with_position


@dataclass
class Segment:
    kind: str          # "type" | "hold" | "gap"
    text: str
    t0: float
    t1: float


def _measure(draw: ImageDraw.ImageDraw, text: str, font, stroke_width: int) -> Tuple[int, int]:
    bbox = draw.textbbox((0, 0), text, font=font, stroke_width=stroke_width)
    return int(bbox[2] - bbox[0]), int(bbox[3] - bbox[1])


def _pick_segment(segments: List[Segment], t: float) -> Segment:
    for seg in segments:
        if seg.t0 <= t < seg.t1:
            return seg
    return segments[-1]


def make_typing_overlay(
    base_clip,
    lines: List[str] | str,
    *,
    # timing
    start: float = 0.2,              # 開始時間（第一句）
    type_seconds_per_line: float = 2.8,
    hold_seconds_per_line: float = 1.2,
    gap_seconds: float = 0.35,
    # typing speed（如果你想「固定每句時間」就用 type_seconds_per_line；如果想固定 cps 就調這個）
    chars_per_sec: float = 12.0,
    use_chars_per_sec: bool = True,  # True: 用 chars_per_sec 控制; False: 用 type_seconds_per_line 控制
    # style
    position: str = "center",        # "center" | "bottom"
    bottom_margin_ratio: float = 0.08,
    font_size_ratio: float = 0.06,
    stroke_width: int = 3,
):
    if isinstance(lines, str):
        lines_list = [lines]
    else:
        lines_list = [x.strip() for x in lines if x and x.strip()]

    W, H = int(base_clip.w), int(base_clip.h)
    font = load_font(max(28, int(H * font_size_ratio)))

    # build timeline
    segments: List[Segment] = []
    t_cursor = float(start)

    for s in lines_list:
        s = s.strip()
        if not s:
            continue

        if use_chars_per_sec:
            typing_dur = len(s) / max(1e-6, float(chars_per_sec))
        else:
            typing_dur = float(type_seconds_per_line)

        segments.append(Segment("type", s, t_cursor, t_cursor + typing_dur))
        t_cursor += typing_dur

        segments.append(Segment("hold", s, t_cursor, t_cursor + float(hold_seconds_per_line)))
        t_cursor += float(hold_seconds_per_line)

        segments.append(Segment("gap", "", t_cursor, t_cursor + float(gap_seconds)))
        t_cursor += float(gap_seconds)

    total_dur = min(float(base_clip.duration), t_cursor)

    x_center = W // 2
    y_baseline_bottom = int(H * (1.0 - bottom_margin_ratio))

    def render_rgba(t: float):
        img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        d = ImageDraw.Draw(img)

        if t < segments[0].t0:
            return np.array(img, dtype=np.uint8)

        seg = _pick_segment(segments, t)
        if seg.kind == "gap" or not seg.text:
            return np.array(img, dtype=np.uint8)

        s_full = seg.text

        if seg.kind == "type":
            prog = (t - seg.t0) / max(1e-6, (seg.t1 - seg.t0))
            n = max(0, min(len(s_full), int(math.floor(len(s_full) * prog + 1e-9))))
            s = s_full[:n]
        else:
            s = s_full

        if not s:
            return np.array(img, dtype=np.uint8)

        tw, th = _measure(d, s, font, stroke_width)

        x = int(x_center - tw // 2)
        if position.lower() == "bottom":
            y = int(y_baseline_bottom - th)
        else:
            y = int((H - th) // 2)

        d.text(
            (x, y),
            s,
            font=font,
            fill=(255, 255, 255, 255),
            stroke_width=stroke_width,
            stroke_fill=(0, 0, 0, 255),
        )
        return np.array(img, dtype=np.uint8)

    clip = VideoClip(lambda t: render_rgba(t)[:, :, :3], duration=total_dur)
    mask = VideoClip(lambda t: render_rgba(t)[:, :, 3].astype("float32") / 255.0, duration=total_dur)
    mask.ismask = True

    clip = with_mask(clip, mask)
    clip = with_position(clip, ("center", "center"))
    return clip
