# scripts/subtitle_effects.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import math
from pathlib import Path
from typing import Tuple

import numpy as np
from PIL import Image, ImageDraw, ImageFont
from moviepy import VideoClip, VideoFileClip

# -------------------------
# Font helper
# -------------------------
def load_font(font_size: int) -> ImageFont.FreeTypeFont:
    """
    Windows: 用 env SUBTITLE_FONT 指定
    PowerShell: $env:SUBTITLE_FONT="C:\Windows\Fonts\msjh.ttc"
    """
    font_path = os.getenv("SUBTITLE_FONT", "").strip()
    candidates = []
    if font_path:
        candidates.append(font_path)

    candidates += [
        r"C:\Windows\Fonts\msjh.ttc",
        r"C:\Windows\Fonts\msjhbd.ttc",
        r"C:\Windows\Fonts\mingliu.ttc",
        r"C:\Windows\Fonts\simsun.ttc",
        r"C:\Windows\Fonts\simhei.ttf",
    ]

    for fp in candidates:
        try:
            if fp and Path(fp).exists():
                return ImageFont.truetype(fp, font_size)
        except Exception:
            pass

    return ImageFont.load_default()


def _with_position(clip: VideoClip, pos):
    if hasattr(clip, "set_position"):
        return clip.set_position(pos)  # moviepy 1.x
    return clip.with_position(pos)    # moviepy 2.x


def _with_duration(clip: VideoClip, dur: float):
    if hasattr(clip, "set_duration"):
        return clip.set_duration(dur)  # moviepy 1.x
    return clip.with_duration(dur)     # moviepy 2.x


# -------------------------
# Effect 1: typing subtitle
# -------------------------
def make_typing_overlay(
    base_clip: VideoFileClip,
    text: str,
    *,
    start: float = 0.2,
    chars_per_sec: float = 12.0,
    hold_after_done: float = 1.0,
    bottom_margin_ratio: float = 0.08,
    font_size_ratio: float = 0.05,
    stroke_width: int = 3,
) -> VideoClip:
    W, H = base_clip.w, base_clip.h
    font_size = max(28, int(H * font_size_ratio))
    font = load_font(font_size)

    typing_dur = len(text) / max(1e-6, chars_per_sec)
    total_dur = min(base_clip.duration, start + typing_dur + hold_after_done)

    y = int(H * (1.0 - bottom_margin_ratio))
    x_center = W // 2

    def draw_frame(t: float):
        img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        d = ImageDraw.Draw(img)

        if t < start:
            return np.array(img)

        k = int((t - start) * chars_per_sec)
        k = max(0, min(len(text), k))
        s = text[:k] if k < len(text) else text

        bbox = d.textbbox((0, 0), s, font=font, stroke_width=stroke_width)
        tw = bbox[2] - bbox[0]
        th = bbox[3] - bbox[1]

        x = x_center - tw // 2
        y0 = y - th

        d.text(
            (x, y0),
            s,
            font=font,
            fill=(255, 255, 255, 255),
            stroke_width=stroke_width,
            stroke_fill=(0, 0, 0, 255),
        )
        return np.array(img)

    clip = VideoClip(draw_frame, duration=total_dur)
    clip = _with_position(clip, ("center", "center"))
    return clip


# -------------------------
# (Optional) Effect 2: marquee (斜向跑馬燈 + 文字旋轉)
# 你之後要加跑馬燈就用這段；目前 add_typing_subtitle.py 可以不使用
# -------------------------
def render_text_rgba(text: str, font: ImageFont.FreeTypeFont, rgba=(255, 255, 255, 220), margin=12) -> Image.Image:
    dummy = Image.new("RGBA", (10, 10), (0, 0, 0, 0))
    d = ImageDraw.Draw(dummy)
    bbox = d.textbbox((0, 0), text, font=font)
    w = max(1, int(bbox[2] - bbox[0] + margin * 2))
    h = max(1, int(bbox[3] - bbox[1] + margin * 2))
    im = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    dd = ImageDraw.Draw(im)
    dd.text((margin, margin), text, font=font, fill=rgba)
    return im


def rotate_overlay(im: Image.Image, angle_deg: float) -> Image.Image:
    return im.rotate(angle_deg, resample=Image.Resampling.BICUBIC, expand=True)


def dir_to_layout_tilt_angle(direction: str) -> float:
    # 可讀角度（避免 180 翻字）
    if direction == "LL2UR":   # /
        return -45.0
    if direction == "UR2LL":   # \
        return 45.0
    return 0.0


def dir_to_path_angle(direction: str) -> float:
    # 螢幕座標：x右正、y下正
    if direction == "LL2UR":
        return -45.0
    if direction == "UR2LL":
        return 135.0
    return 180.0


def marquee_position_diag(t: float, overlay_size: Tuple[int, int], video_size: Tuple[int, int], angle_deg: float, speed: float):
    ow, oh = overlay_size
    W, H = video_size

    rad = math.radians(angle_deg)
    dx = math.cos(rad)
    dy = math.sin(rad)

    path_len = (W + H + ow + oh) * 1.3
    cx, cy = W / 2, H / 2
    s = (t * speed) % path_len

    x = cx - 0.5 * path_len * dx + s * dx - ow / 2
    y = cy - 0.5 * path_len * dy + s * dy - oh / 2
    return x, y
