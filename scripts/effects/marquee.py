# effects/marquee.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import math
import re
from pathlib import Path
from typing import List, Sequence, Union, Optional, Tuple

import numpy as np
from PIL import Image, ImageDraw, ImageFont

try:
    from moviepy import VideoClip, VideoFileClip
except Exception:
    from moviepy.editor import VideoClip, VideoFileClip


# -------------------------
# MoviePy compat
# -------------------------
def _with_position(clip: VideoClip, pos):
    if hasattr(clip, "set_position"):
        return clip.set_position(pos)
    return clip.with_position(pos)


def _with_duration(clip: VideoClip, dur: float):
    if hasattr(clip, "set_duration"):
        return clip.set_duration(dur)
    return clip.with_duration(dur)


def _with_mask(clip: VideoClip, mask: VideoClip):
    if hasattr(clip, "set_mask"):
        return clip.set_mask(mask)
    return clip.with_mask(mask)


# -------------------------
# Font
# -------------------------
def load_font(font_size: int) -> ImageFont.FreeTypeFont:
    fp = os.getenv("SUBTITLE_FONT", "").strip()
    candidates: List[str] = []
    if fp:
        candidates.append(fp)

    candidates += [
        r"C:\Windows\Fonts\msjh.ttc",
        r"C:\Windows\Fonts\msjhbd.ttc",
        r"C:\Windows\Fonts\mingliu.ttc",
        r"C:\Windows\Fonts\seguiemj.ttf",
        r"C:\Windows\Fonts\SegoeUI.ttf",
    ]

    for c in candidates:
        try:
            if c and Path(c).exists():
                return ImageFont.truetype(c, font_size)
        except Exception:
            pass

    return ImageFont.load_default()


# -------------------------
# Emoji handling
# -------------------------
_EMOJI_RE = re.compile(
    "["
    "\U0001F300-\U0001FAFF"
    "\U00002700-\U000027BF"
    "\U00002600-\U000026FF"
    "]+",
    flags=re.UNICODE,
)


def sanitize_text(s: str, mode: str = "strip") -> str:
    if mode == "keep":
        return s
    if mode == "replace":
        return _EMOJI_RE.sub("[]", s)
    return _EMOJI_RE.sub("", s)


# -------------------------
# Geometry helpers
# -------------------------
def dir_to_path_angle(direction: str) -> float:
    if direction == "LL2UR":
        return -45.0
    if direction == "UR2LL":
        return 135.0
    return 180.0


def marquee_position_r2l(
    frame_w: int,
    t: float,
    speed: float,
    overlay_w: int,
    y: float,
    margin_x: int = 20,
) -> Tuple[float, float]:
    start_x = frame_w + margin_x
    end_x = -overlay_w - margin_x
    dist = start_x - end_x
    s = (t * speed) % dist
    x = start_x - s
    return x, y


def _stable_u01(k: int, seed: int) -> float:
    x = (k * 1103515245 + seed) & 0x7fffffff
    return (x % 1000000) / 1000000.0


# -------------------------
# Render text to RGBA
# -------------------------
def render_text_rgba(
    text: str,
    font: ImageFont.FreeTypeFont,
    rgba: tuple[int, int, int, int],
    pad: int,
) -> Image.Image:
    dummy = Image.new("RGBA", (10, 10), (0, 0, 0, 0))
    d = ImageDraw.Draw(dummy)
    bbox = d.textbbox((0, 0), text, font=font)
    w = max(1, int(bbox[2] - bbox[0] + pad * 2))
    h = max(1, int(bbox[3] - bbox[1] + pad * 2))

    im = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    dd = ImageDraw.Draw(im)
    dd.text((pad, pad), text, font=font, fill=rgba)
    return im


# -------------------------
# Public API
# -------------------------
def make_marquee_overlay(
    base_clip: VideoFileClip,
    text: Union[str, Sequence[str]],
    *,
    # path
    path_mode: str = "r2l",
    speed: float = 220.0,
    margin_x: int = 30,

    # look
    alpha: int = 200,
    font_size_ratio: float = 0.035,
    pad: int = 12,
    emoji_mode: str = "strip",

    # cycling
    cycle_seconds: float = 2.0,           # fallback
    auto_cycle_seconds: bool = True,      # 讓每句跑完整趟再換
    random_cycle: bool = False,           # 是否每 cycle 隨機選句
    text_offset: int = 0,                 # 外部可用來錯開句子（可不管）

    # y placement
    y_margin_ratio: float = 0.20,         # 上下安全邊界
    y_seed: int = 424242,

    # ✅ 保留區（你說中間那段要放其他文字/唐詩宋詞）
    reserve_on: bool = True,
    reserve_y0_ratio: float = 0.40,       # 保留區開始（越小越靠上）
    reserve_y1_ratio: float = 0.70,       # 保留區結束（越大越靠下）
    reserve_padding_ratio: float = 0.03,  # 保留區上下再多留一點距離

    # ✅ 上下出現規則
    # "alt": 上下交替（每次都不同）
    # "rand": 每次隨機上/下
    side_mode: str = "alt",               # "alt" or "rand"

    # ✅ 高度隨機幅度（在上半區/下半區內亂數）
    y_jitter_ratio: float = 1.0,          # 1.0 = 全區亂數；0.5 = 更集中
) -> VideoClip:
    if path_mode != "r2l":
        raise ValueError("目前此簡化版只支援 path_mode='r2l'")

    W, H = int(base_clip.w), int(base_clip.h)
    font_size = max(18, int(H * float(font_size_ratio)))
    font = load_font(font_size)

    if isinstance(text, str):
        pool = [text]
    else:
        pool = [str(x) for x in text if str(x).strip()]
        if not pool:
            pool = [""]

    def _cycle_seconds_effective(overlay_w: int) -> float:
        dist = float(W + int(overlay_w) + 2 * int(margin_x))
        return max(0.3, dist / max(1e-6, float(speed)))

    def _pick_text(cidx: int) -> str:
        if len(pool) == 1:
            return pool[0]
        if random_cycle:
            r = (cidx * 1103515245 + 12345 + int(text_offset) * 1013) & 0x7fffffff
            return pool[r % len(pool)]
        return pool[(cidx + int(text_offset)) % len(pool)]

    def _safe_range(oh: int) -> Tuple[float, float]:
        m = float(y_margin_ratio)
        m = max(0.0, min(0.49, m))
        y_min = H * m
        y_max = H * (1.0 - m) - oh
        if y_max < y_min:
            y_min = (H - oh) / 2
            y_max = y_min
        return y_min, y_max

    def _pick_side(cidx: int) -> int:
        # 0=上方, 1=下方
        if str(side_mode).lower() == "rand":
            u = _stable_u01(cidx, seed=int(y_seed) + 777)
            return 0 if u < 0.5 else 1
        # alt
        return cidx % 2

    def _pick_y(cidx: int, oh: int) -> float:
        y_min, y_max = _safe_range(oh)
        if y_max <= y_min:
            return y_min

        # reserve band
        if bool(reserve_on):
            ry0 = float(reserve_y0_ratio) * H
            ry1 = float(reserve_y1_ratio) * H
            pad_px = float(reserve_padding_ratio) * H
            band_top = ry0 - pad_px
            band_bot = ry1 + pad_px
        else:
            band_top = band_bot = None  # type: ignore

        side = _pick_side(cidx)

        # 可用區間：上方區 or 下方區
        if reserve_on:
            # 上方區：y ∈ [y_min, band_top - oh]
            top_a = y_min
            top_b = min(y_max, band_top - oh)
            # 下方區：y ∈ [band_bot, y_max]
            bot_a = max(y_min, band_bot)
            bot_b = y_max

            # 如果某邊空了就退回全域亂數
            def pick_in(a: float, b: float, seed_add: int) -> Optional[float]:
                if b <= a:
                    return None
                u = _stable_u01(cidx, seed=int(y_seed) + seed_add)
                jr = max(0.05, min(1.0, float(y_jitter_ratio)))
                u2 = 0.5 + (u - 0.5) * jr
                return a + (b - a) * u2

            if side == 0:
                y = pick_in(top_a, top_b, seed_add=101)
                if y is None:
                    y = pick_in(bot_a, bot_b, seed_add=202)
                if y is None:
                    # fallback
                    y = y_min + (y_max - y_min) * _stable_u01(cidx, seed=int(y_seed) + 303)
            else:
                y = pick_in(bot_a, bot_b, seed_add=202)
                if y is None:
                    y = pick_in(top_a, top_b, seed_add=101)
                if y is None:
                    y = y_min + (y_max - y_min) * _stable_u01(cidx, seed=int(y_seed) + 303)

        else:
            u = _stable_u01(cidx, seed=int(y_seed) + 404)
            jr = max(0.05, min(1.0, float(y_jitter_ratio)))
            u2 = 0.5 + (u - 0.5) * jr
            y = y_min + (y_max - y_min) * u2

        return max(y_min, min(y_max, float(y)))

    # cache: overlay render by cycle
    cache_key: Optional[tuple] = None
    cache_overlay: Optional[Image.Image] = None
    cache_ow: int = 0

    # ✅ frame memo: avoid double render for rgb/mask
    last_t: Optional[float] = None
    last_rgba: Optional[np.ndarray] = None

    def render_rgba(t: float) -> np.ndarray:
        nonlocal cache_key, cache_overlay, cache_ow, last_t, last_rgba

        if last_t is not None and abs(t - last_t) < 1e-9 and last_rgba is not None:
            return last_rgba

        canvas = Image.new("RGBA", (W, H), (0, 0, 0, 0))

        # estimate cycle
        cs0 = _cycle_seconds_effective(cache_ow if cache_ow > 0 else int(W * 0.6)) if auto_cycle_seconds else max(1e-6, float(cycle_seconds))
        cidx0 = int(t / cs0)
        s0 = sanitize_text(_pick_text(cidx0), emoji_mode).strip()
        if not s0:
            arr = np.array(canvas, dtype=np.uint8)
            last_t, last_rgba = t, arr
            return arr

        base_rgba = (255, 255, 255, max(0, min(255, int(alpha))))
        ov0 = render_text_rgba(s0, font, base_rgba, pad=pad)
        ow0, _ = ov0.size

        cs = _cycle_seconds_effective(ow0) if auto_cycle_seconds else max(1e-6, float(cycle_seconds))
        cidx = int(t / cs)

        s = sanitize_text(_pick_text(cidx), emoji_mode).strip()
        if not s:
            arr = np.array(canvas, dtype=np.uint8)
            last_t, last_rgba = t, arr
            return arr

        cycle_id = (cidx, s)
        if cache_overlay is None or cache_key != cycle_id:
            ov = render_text_rgba(s, font, base_rgba, pad=pad)
            cache_overlay = ov
            cache_key = cycle_id
            cache_ow = int(ov.size[0])

        ov = cache_overlay
        ow, oh = ov.size

        y = _pick_y(cidx, oh)
        x, y = marquee_position_r2l(W, t, float(speed), ow, y, margin_x=int(margin_x))
        x_i = int(round(x))
        y_i = int(round(y))

        # crop + composite
        x1 = max(0, x_i)
        y1 = max(0, y_i)
        x2 = min(W, x_i + ow)
        y2 = min(H, y_i + oh)
        if x2 > x1 and y2 > y1:
            crop = ov.crop((x1 - x_i, y1 - y_i, x2 - x_i, y2 - y_i))
            canvas.alpha_composite(crop, (x1, y1))

        arr = np.array(canvas, dtype=np.uint8)
        last_t, last_rgba = t, arr
        return arr

    def frame_rgb(t: float) -> np.ndarray:
        return render_rgba(t)[:, :, :3]

    def frame_mask(t: float) -> np.ndarray:
        return (render_rgba(t)[:, :, 3].astype(np.float32) / 255.0)

    dur = float(base_clip.duration)
    overlay = VideoClip(frame_rgb, duration=dur)

    try:
        mask = VideoClip(frame_mask, duration=dur, ismask=True)
    except TypeError:
        mask = VideoClip(frame_mask, duration=dur)
        mask.ismask = True

    overlay = _with_mask(overlay, mask)
    overlay = _with_position(overlay, ("center", "center"))
    overlay = _with_duration(overlay, dur)
    return overlay
