# scripts/render_images/disclaimer_mpl.py
# -*- coding: utf-8 -*-
"""
Render a final "Disclaimer" page as a PNG image (Matplotlib).

Key points:
- Pixel-perfect canvas: width_px x height_px, fixed dpi=100 (ignore external dpi but accept it for compatibility)
- Auto language by market:
    TW/HK -> zh_TW + en
    CN    -> zh_CN + en
    US    -> en
    JP    -> ja + en
    KR    -> ko + en
- No right-edge clipping: wrap text to a fixed [left, right] margin using renderer-measured pixel width
- Footer: two lines, all left-aligned (no left/right split to avoid overlap)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Tuple, Optional, List

import matplotlib.pyplot as plt
from matplotlib import font_manager as fm


# =============================================================================
# Market -> languages
# =============================================================================
_MARKET_LANGS: Dict[str, Tuple[str, ...]] = {
    "TW": ("zh_TW", "en"),
    "HK": ("zh_TW", "en"),
    "CN": ("zh_CN", "en"),
    "US": ("en",),
    "JP": ("ja", "en"),
    "KR": ("ko", "en"),
}


# =============================================================================
# Market helpers
# =============================================================================
def _normalize_market(market: str) -> str:
    m = (market or "TW").strip().upper()
    alias = {
        "TWN": "TW",
        "TAIWAN": "TW",
        "HKG": "HK",
        "HKEX": "HK",
        "CHN": "CN",
        "CHINA": "CN",
        "USA": "US",
        "NASDAQ": "US",
        "NYSE": "US",
        "JPN": "JP",
        "JAPAN": "JP",
        "KOR": "KR",
        "KOREA": "KR",
    }
    return alias.get(m, m)


def _get_langs_for_market(market: str) -> Tuple[str, ...]:
    m = _normalize_market(market)
    return _MARKET_LANGS.get(m, ("en",))


def _market_display(market: str) -> str:
    m = _normalize_market(market)
    display = {
        "TW": "Taiwan (TW)",
        "HK": "Hong Kong (HK)",
        "CN": "China (CN)",
        "US": "United States (US)",
        "JP": "Japan (JP)",
        "KR": "Korea (KR)",
    }
    return display.get(m, m)


# =============================================================================
# Disclaimer texts
# =============================================================================
def _text_pack(lang: str) -> Tuple[str, str, str]:
    lang = (lang or "en").strip()

    if lang == "zh_TW":
        headline = "免責聲明 / Disclaimer"
        body = (
            "本內容僅為市場資訊整理與數據視覺化展示，不構成任何投資建議、推薦、邀約或保證。\n"
            "證券及其他金融商品價格具有波動風險，投資前請自行審慎評估並承擔相關風險。"
        )
        extra = (
            "本內容所使用之市場數據主要來源於第三方公開資料服務（如 yfinance），並於盤中特定時間點進行擷取，非收盤後最終結算資料。\n"
            "各市場之交易制度、漲跌幅限制及適用規範可能隨上市別、期間或個別情況有所差異。\n"
            "受資料更新頻率與交易狀態變化影響，圖中資訊可能與最終結果存在差異，僅供參考。"
        )
        return headline, body, extra

    if lang == "zh_CN":
        headline = "免责声明 / Disclaimer"
        body = (
            "本内容仅为市场信息整理与数据可视化展示，不构成任何投资建议、推荐、邀约或保证。\n"
            "证券及其他金融产品价格具有波动风险，投资前请自行审慎评估并承担相关风险。"
        )
        extra = (
            "本内容所使用的市场数据主要来源于第三方公开数据服务（如 yfinance），并在盘中特定时间点采集，非收盘后的最终结算数据。\n"
            "各市场的交易制度、涨跌幅限制及适用规则可能因上市状态、期间或个别情形而不同。\n"
            "受数据更新频率与交易状态变化影响，图中信息可能与最终结果存在差异，仅供参考。"
        )
        return headline, body, extra

    if lang == "ja":
        headline = "免責事項 / Disclaimer"
        body = (
            "本コンテンツは情報提供およびデータ可視化を目的としたものであり、"
            "投資助言、勧誘、推奨を行うものではありません。\n"
            "金融商品には価格変動リスクがあります。投資判断はご自身の責任にて行ってください。"
        )
        extra = (
            "本プロジェクトは第三者の公開データサービス（例：yfinance）を情報源とし、"
            "取引時間中の特定時点で取得されたデータを用いています（終値確定データではありません）。\n"
            "市場ごとに取引制度や値幅制限等が異なる場合があります。\n"
            "データ更新遅延や市場状況により、表示内容が最終確定値と異なることがあります。"
        )
        return headline, body, extra

    if lang == "ko":
        headline = "면책조항 / Disclaimer"
        body = (
            "본 콘텐츠는 정보 제공 및 데이터 시각화를 목적으로 하며 "
            "투자 자문, 권유 또는 추천이 아닙니다.\n"
            "금융 상품에는 가격 변동에 따른 위험이 있으므로 "
            "투자 판단은 본인의 책임 하에 이루어져야 합니다."
        )
        extra = (
            "본 콘텐츠의 시장 데이터는 제3자 공개 데이터 서비스(예: yfinance)를 기반으로 하며 "
            "장중 특정 시점에 수집된 자료입니다(장 마감 후 확정 데이터가 아님).\n"
            "시장별 제도, 가격 제한 및 적용 조건은 상장 상태/기간 등에 따라 달라질 수 있습니다.\n"
            "데이터 갱신 지연 및 거래 상태 변화로 인해 최종 확정 값과 다를 수 있습니다."
        )
        return headline, body, extra

    # English default
    headline = "Disclaimer"
    body = (
        "This content is provided for informational and data visualization purposes only and does not constitute "
        "investment advice, solicitation, or recommendation.\n"
        "Financial markets involve risk. Please conduct your own due diligence before making any investment decisions."
    )
    extra = (
        "Market data used here is primarily sourced from third-party public data services (e.g., yfinance).\n\n"
        "Intraday snapshot (not EOD).\n\n"
        "Market mechanisms, price limits, and regulations may vary by exchange, listing status, or time period.\n"
        "Due to update latency and intraday movements, the information shown may differ from final settlement data."
    )
    return headline, body, extra


# =============================================================================
# Font loading (CJK-safe)
# =============================================================================
def _try_register_font_file(font_path: Path) -> Optional[str]:
    try:
        if font_path.exists() and font_path.is_file():
            fm.fontManager.addfont(str(font_path))
            prop = fm.FontProperties(fname=str(font_path))
            return prop.get_name()
    except Exception:
        return None
    return None


def _pick_font_family() -> str:
    env_fp = os.getenv("SUBTITLE_FONT", "").strip()
    if env_fp:
        name = _try_register_font_file(Path(env_fp))
        if name:
            return name

    # common Noto CJK
    candidates = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        r"C:\Windows\Fonts\NotoSansCJK-Regular.ttc",
        r"C:\Windows\Fonts\msjh.ttc",
        r"C:\Windows\Fonts\msjhl.ttc",
        r"C:\Windows\Fonts\malgun.ttf",
        r"C:\Windows\Fonts\YuGothR.ttc",
    ]
    for c in candidates:
        name = _try_register_font_file(Path(c))
        if name:
            return name

    return "DejaVu Sans"


# =============================================================================
# Wrapping by pixel width (enforce right margin)
# =============================================================================
def _wrap_paragraph_to_px(ax, text: str, fontsize: int, max_px: float) -> str:
    """
    Wrap ONE paragraph to max pixel width.
    - If has spaces: wrap by words
    - Else (CJK): wrap by characters
    """
    if not text:
        return ""

    fig = ax.figure
    renderer = fig.canvas.get_renderer()

    def measure(s: str) -> float:
        t = ax.text(0, 0, s, fontsize=fontsize, alpha=0.0, transform=ax.transAxes)
        fig.canvas.draw()
        w = t.get_window_extent(renderer=renderer).width
        t.remove()
        return w

    has_spaces = (" " in text.strip())
    lines: List[str] = []

    if has_spaces:
        words = text.split()
        cur = ""
        for w in words:
            cand = (cur + " " + w).strip() if cur else w
            if measure(cand) <= max_px:
                cur = cand
            else:
                if cur:
                    lines.append(cur)
                    cur = w
                else:
                    # single word too long -> hard split
                    tmp = ""
                    for ch in w:
                        cand2 = tmp + ch
                        if measure(cand2) <= max_px:
                            tmp = cand2
                        else:
                            if tmp:
                                lines.append(tmp)
                            tmp = ch
                    cur = tmp
        if cur:
            lines.append(cur)
    else:
        cur = ""
        for ch in text:
            if ch == "\n":
                lines.append(cur)
                cur = ""
                continue
            cand = cur + ch
            if measure(cand) <= max_px:
                cur = cand
            else:
                if cur:
                    lines.append(cur)
                cur = ch
        if cur:
            lines.append(cur)

    return "\n".join(lines)


def _wrap_text_to_px(ax, text: str, fontsize: int, max_px: float) -> str:
    """
    Wrap a multi-line text: respect existing newlines.
    """
    parts = text.split("\n")
    wrapped_parts: List[str] = []
    for p in parts:
        p = p.rstrip()
        if p == "":
            wrapped_parts.append("")
        else:
            wrapped_parts.append(_wrap_paragraph_to_px(ax, p, fontsize, max_px))
    return "\n".join(wrapped_parts)


# =============================================================================
# Draw block with bbox measurement (and our wrapping)
# =============================================================================
def _draw_block(ax, x, y, text, *, fs, color, weight=None, gap=0.012, max_px=None) -> float:
    fig = ax.figure
    fig.canvas.draw()

    if max_px is not None:
        text = _wrap_text_to_px(ax, text, fs, max_px)

    t = ax.text(
        x, y, text,
        ha="left", va="top",
        fontsize=fs, color=color,
        fontweight=weight,
        transform=ax.transAxes,
    )

    fig.canvas.draw()
    bbox = ax.transAxes.inverted().transform_bbox(
        t.get_window_extent(fig.canvas.get_renderer())
    )
    return y - bbox.height - gap


# =============================================================================
# Public API (must accept dpi/title/footer for CLI compatibility)
# =============================================================================
def render_disclaimer_page(
    out_path: str | Path,
    *,
    market: str = "TW",
    theme: str = "dark",
    width_px: int = 1080,
    height_px: int = 1920,
    dpi: int = 100,                 # ✅ accept (CLI passes it). We'll keep output dpi fixed to 100 anyway.
    title: Optional[str] = None,    # ✅ accept (CLI passes it)
    footer: Optional[str] = None,   # ✅ accept (CLI passes it)
) -> Path:
    """
    Render disclaimer page PNG.

    Notes:
    - Output is pixel-perfect width_px x height_px using fixed dpi=100.
    - `dpi` param is accepted for compatibility, but output uses dpi=100 to match other images.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    theme = (theme or "dark").strip().lower()
    dark = theme != "light"

    if dark:
        bg = "#0f0f1e"
        fg = "#ffffff"
        muted = "#a0a0a0"
        divider = "#2d2d44"
        accent = "#ff5555"
    else:
        bg = "#ffffff"
        fg = "#111111"
        muted = "#555555"
        divider = "#dddddd"
        accent = "#b22222"

    font_family = _pick_font_family()
    plt.rcParams["font.sans-serif"] = [font_family]
    plt.rcParams["axes.unicode_minus"] = False

    # fixed dpi for pixel-perfect
    fixed_dpi = 100
    fig = plt.figure(figsize=(width_px / fixed_dpi, height_px / fixed_dpi), dpi=fixed_dpi, facecolor=bg)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.axis("off")
    ax.set_facecolor(bg)

    # margins (symmetry)
    left = 0.06
    right = 0.94

    # usable pixel width for wrapping
    fig.canvas.draw()
    text_max_px = (right - left) * fig.bbox.width

    langs = _get_langs_for_market(market)
    market_disp = _market_display(market)

    # title
    if not title:
        headline0, _, _ = _text_pack(langs[0])
        title = headline0

    y = 0.95
    y = _draw_block(ax, left, y, title, fs=40, color=accent, weight="bold", max_px=text_max_px)
    ax.plot([left, right], [y + 0.004, y + 0.004], color=divider, lw=2, transform=ax.transAxes)
    y -= 0.01

    label_map = {
        "zh_TW": "中文（繁體）",
        "zh_CN": "中文（简体）",
        "en": "English",
        "ja": "日本語",
        "ko": "한국어",
    }

    # font sizes tuned to match your reference look
    fs_label = 18
    fs_body_zh = 26
    fs_extra_zh = 20
    fs_body_en = 26
    fs_extra_en = 20

    for i, lc in enumerate(langs):
        _, body, extra = _text_pack(lc)
        label = label_map.get(lc, lc)

        y = _draw_block(ax, left, y, label, fs=fs_label, color=muted, weight="bold", gap=0.006, max_px=text_max_px)

        if lc == "en":
            y = _draw_block(ax, left, y, body, fs=fs_body_en, color=fg, gap=0.012, max_px=text_max_px)

            parts = [p.strip() for p in extra.split("\n\n") if p.strip()]
            for p in parts:
                is_hi = ("Intraday snapshot" in p)
                y = _draw_block(
                    ax, left, y, p,
                    fs=fs_extra_en,
                    color=accent if is_hi else muted,
                    weight="bold" if is_hi else None,
                    gap=0.010,
                    max_px=text_max_px,
                )
        else:
            y = _draw_block(ax, left, y, body, fs=fs_body_zh, color=fg, gap=0.012, max_px=text_max_px)
            y = _draw_block(ax, left, y, extra, fs=fs_extra_zh, color=muted, gap=0.016, max_px=text_max_px)

        if i < len(langs) - 1:
            ax.plot([left, right], [y + 0.006, y + 0.006], color=divider, lw=1, alpha=0.6, transform=ax.transAxes)
            y -= 0.012

    # footer (two lines, all left)
    ax.plot([left, right], [0.09, 0.09], color=divider, lw=1, transform=ax.transAxes)

    if footer is None:
        # keep it simple & stable (no overlap)
        footer1 = f"Market: {market_disp} | Data: yfinance (3rd-party)"
        footer2 = "Intraday snapshot (not EOD) | For reference only."
    else:
        # if user provides footer, still split into 2 lines to avoid overflow
        f = footer.strip()
        if "\n" in f:
            parts = [x.strip() for x in f.splitlines() if x.strip()]
            footer1 = parts[0] if parts else ""
            footer2 = parts[1] if len(parts) > 1 else ""
        else:
            footer1 = f
            footer2 = ""

    ax.text(left, 0.055, footer1, fontsize=15, color=muted, ha="left", va="bottom", transform=ax.transAxes)
    if footer2:
        ax.text(left, 0.035, footer2, fontsize=15, color=accent, weight="bold", ha="left", va="bottom", transform=ax.transAxes)

    fig.savefig(str(out_path), dpi=fixed_dpi, facecolor=bg)
    plt.close(fig)
    return out_path


if __name__ == "__main__":
    out_dir = Path("_debug_disclaimer")
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / "overview_disclaimer.png"
    render_disclaimer_page(p, market="TW", theme="dark")
    print("written:", p)
