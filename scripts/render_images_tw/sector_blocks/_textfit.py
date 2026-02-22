# scripts/render_images_tw/sector_blocks/_textfit.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class TextFitter:
    """
    Helper for pixel-aware text measuring and ellipsis fitting.
    """
    fig: any
    ax: any
    fg: str

    _renderer: Optional[any] = None

    def ensure_renderer(self) -> any:
        if self._renderer is None:
            self.fig.canvas.draw()
            self._renderer = self.fig.canvas.get_renderer()
        return self._renderer

    def text_width_px_left(self, s: str, x: float, y: float, fontsize: int, weight: str = "bold") -> float:
        renderer = self.ensure_renderer()
        t = self.ax.text(x, y, s, ha="left", va="center", fontsize=fontsize, color=self.fg, weight=weight, alpha=0.0)
        bb = t.get_window_extent(renderer=renderer)
        t.remove()
        return float(bb.width)

    def fit_left_fontsize(
        self,
        text: str,
        x_left: float,
        x_right: float,
        y: float,
        base_fs: int,
        min_fs: int = 20,
        weight: str = "bold",
    ) -> int:
        renderer = self.ensure_renderer()
        p0 = self.ax.transData.transform((x_left, y))
        p1 = self.ax.transData.transform((x_right, y))
        avail = max(1.0, (p1[0] - p0[0]))

        fs = int(base_fs)
        while fs > int(min_fs):
            if self.text_width_px_left(text, x_left, y, fs, weight=weight) <= avail:
                return fs
            fs -= 1
        return int(min_fs)

    def ellipsis_fit(
        self,
        text: str,
        x_left: float,
        x_right: float,
        y: float,
        fontsize: int,
        weight: str = "medium",
    ) -> str:
        s = str(text).strip() if text is not None else ""
        if not s:
            return ""

        renderer = self.ensure_renderer()
        t = self.ax.text(x_left, y, s, ha="left", va="center", fontsize=fontsize, color=self.fg, weight=weight, alpha=0.0)

        p0 = self.ax.transData.transform((x_left, y))
        p1 = self.ax.transData.transform((x_right, y))
        avail = max(1.0, (p1[0] - p0[0]))

        def ok(ss: str) -> bool:
            t.set_text(ss)
            bb = t.get_window_extent(renderer=renderer)
            return bb.width <= avail

        if ok(s):
            t.remove()
            return s

        ell = "…"
        lo, hi = 0, len(s)
        best = ell
        while lo <= hi:
            mid = (lo + hi) // 2
            cand = s[:mid].rstrip()
            cand = (cand + ell) if cand else ell
            if ok(cand):
                best = cand
                lo = mid + 1
            else:
                hi = mid - 1

        t.remove()
        return best
