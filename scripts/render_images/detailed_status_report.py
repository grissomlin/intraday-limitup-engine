# scripts/render_images/detailed_status_report.py
# -*- coding: utf-8 -*-
"""
詳細漲停明細報告 (for X / Debug)
- 完整列出所有漲停個股的詳細資訊
- 顯示「鎖1」產業的具體個股
- 包含價格數據用於驗證判斷是否正確

✅ 本版修正：
- locked 狀態一律輸出「鎖{streak}連」，不再出現裸「鎖」
  （streak 缺值/0 時 fallback 成 1，避免出現「鎖」與「鎖1連」混用）
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Tuple
from pathlib import Path
from datetime import datetime

# -------------------------
# Helper functions
# -------------------------


def _hhmm(asof: str) -> str:
    """從 ISO 字串提取時間 HH:MM"""
    if not asof:
        return ""
    if "T" in asof:
        return asof.split("T", 1)[1][:5]
    return asof[:5]


def _sym_no_suffix(sym: str) -> str:
    """移除股票代號的後綴（如 .TW）"""
    return (sym or "").strip().split(".")[0]


def _format_price(price: Any) -> str:
    """格式化價格，保留兩位小數"""
    try:
        return f"{float(price):.2f}"
    except Exception:
        return "N/A"


def _format_percent(ret: Any) -> str:
    """格式化百分比"""
    try:
        return f"{float(ret) * 100:+.2f}%"
    except Exception:
        return "N/A"


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _locked_text(row: Dict[str, Any]) -> str:
    """
    ✅ 統一 locked 狀態文字：
    - 一律輸出「鎖{max(streak,1)}連」
    """
    streak = _safe_int(row.get("streak", 0) or 0, 0)
    if streak <= 0:
        streak = 1
    return f"鎖{streak}連"


def _get_limitup_status_text(row: Dict[str, Any]) -> str:
    """取得漲停狀態文字描述（保留，但目前主要報告未使用它組字）"""
    status = str(row.get("limitup_status", "")).lower()
    streak = _safe_int(row.get("streak", 0) or 0, 0)
    streak_prev = _safe_int(row.get("streak_prev", 0) or 0, 0)

    if status == "locked":
        return _locked_text(row)
    elif status == "touch_only":
        # 這裡的舊格式是「觸昨X」，但主報告目前用「昨X 觸」
        if streak_prev > 0:
            return f"觸昨{streak_prev}"
        return "觸昨無"
    elif status == "no_limit_theme":
        return "題材"
    return ""


def _get_yesterday_status(stock: Dict[str, Any]) -> str:
    """取得昨日狀態文字"""
    streak_prev = _safe_int(stock.get("streak_prev", 0) or 0, 0)
    if streak_prev > 0:
        return f"昨{streak_prev}"
    return "昨無"


# -------------------------
# 主要報告生成函數
# -------------------------


def build_detailed_status_report(
    payload: Dict[str, Any],
    *,
    max_sectors: int = 20,
    include_price_details: bool = True,
    include_debug_info: bool = True,
    max_chars: int = 2800,  # X 限制
) -> Tuple[str, str]:
    """
    產生詳細的漲停明細報告

    Returns:
        Tuple[str, str]: (短版用於X, 完整版用於debug)
    """
    payload = dict(payload or {})

    # 基本資訊
    ymd = str(payload.get("ymd_effective") or payload.get("ymd") or "")
    asof = str(payload.get("asof") or payload.get("generated_at") or "")
    hhmm = _hhmm(asof)

    limitup: List[Dict[str, Any]] = list(payload.get("limitup") or [])
    sector_summary: List[Dict[str, Any]] = list(payload.get("sector_summary") or [])

    # 分類個股
    def _status(r):
        return str(r.get("limitup_status", "")).lower()

    locked = [r for r in limitup if _status(r) == "locked"]
    touch = [r for r in limitup if _status(r) == "touch_only"]
    theme = [r for r in limitup if _status(r) == "no_limit_theme"]

    # 按產業分組鎖死個股
    locked_by_sector: Dict[str, List[Dict[str, Any]]] = {}
    for stock in locked:
        sector = str(stock.get("sector", "未分類")).strip()
        locked_by_sector.setdefault(sector, []).append(stock)

    # 找出「鎖1」產業（只有一家 locked 的產業）
    single_lock_sectors = []
    for sector, stocks in locked_by_sector.items():
        if len(stocks) == 1:
            single_lock_sectors.append((sector, stocks[0]))

    # 建立報告
    lines_short = []  # 短版 (for X)
    lines_full = []   # 完整版 (for debug)

    # 標題
    title = f"📈 台股漲停明細 {ymd}"
    if hhmm:
        title += f" 截至 {hhmm}"

    lines_short.append(title)
    lines_short.append("=" * len(title))
    lines_full.append(title)
    lines_full.append("=" * len(title))

    # 統計摘要
    summary = f"漲停鎖死: {len(locked)} | 觸及未鎖: {len(touch)} | 題材: {len(theme)}"
    lines_short.append(summary)
    lines_full.append(summary)

    # 鎖1產業明細
    if single_lock_sectors:
        lines_short.append("")
        lines_short.append("🔒【鎖1產業明細】")
        lines_full.append("")
        lines_full.append("🔒【鎖1產業明細 - 只有一家漲停的產業】")

        for sector, stock in sorted(single_lock_sectors, key=lambda x: x[0]):
            name = str(stock.get("name", "")).strip()
            sym = _sym_no_suffix(str(stock.get("symbol", "")).strip())
            ret = _format_percent(stock.get("ret"))
            yesterday = _get_yesterday_status(stock)

            status_text = _locked_text(stock)  # ✅ 統一
            line = f"- {sector}: {name}({sym})｜{yesterday} {status_text} {ret}"
            lines_short.append(line)

            # 完整版加入價格詳情
            full_line = f"- {sector}: {name}({sym})｜{yesterday} {status_text} {ret}"

            if include_price_details:
                prev = _format_price(stock.get("prev_close"))
                limit = _format_price(stock.get("limit_up_price"))
                close = _format_price(stock.get("close"))
                full_line += f" [前收:{prev} 漲停:{limit} 收盤:{close}]"

            lines_full.append(full_line)

    # 鎖死個股完整列表（按產業分組）
    lines_short.append("")
    lines_short.append("🔒【所有鎖死個股】")
    lines_full.append("")
    lines_full.append("🔒【所有鎖死個股 - 詳細明細】")

    for sector, stocks in sorted(locked_by_sector.items()):
        if len(stocks) == 1 and single_lock_sectors:
            continue  # 已在鎖1部分列出

        sector_header = f"鎖{len(stocks)}"
        lines_short.append(sector_header)
        lines_full.append(sector_header)

        for stock in sorted(stocks, key=lambda x: _safe_int(x.get("streak", 0) or 0, 0), reverse=True):
            name = str(stock.get("name", "")).strip()
            sym = _sym_no_suffix(str(stock.get("symbol", "")).strip())
            ret = _format_percent(stock.get("ret"))
            yesterday = _get_yesterday_status(stock)

            status_text = _locked_text(stock)  # ✅ 統一
            line_short = f"  - {name}({sym})｜{yesterday} {status_text} {ret}"
            lines_short.append(line_short)

            # 完整版
            line_full = f"  - {name}({sym})｜{yesterday} {status_text} {ret}"
            if include_price_details:
                prev = _format_price(stock.get("prev_close"))
                limit = _format_price(stock.get("limit_up_price"))
                close = _format_price(stock.get("close"))
                high = _format_price(stock.get("high"))

                price_info = f"前收:{prev} 漲停:{limit} 收盤:{close}"
                if include_debug_info:
                    # 計算差價，驗證是否真的鎖死
                    try:
                        close_val = float(stock.get("close", 0))
                        limit_val = float(stock.get("limit_up_price", 0))
                        diff = close_val - limit_val
                        price_info += f" 差:{diff:+.3f}"
                    except Exception:
                        pass

                line_full += f" [{price_info}]"

            lines_full.append(line_full)

    # 觸及未鎖個股（含詳細價格資訊）
    if touch:
        lines_short.append("")
        lines_short.append("⚠️【觸及未鎖個股】")
        lines_full.append("")
        lines_full.append("⚠️【觸及未鎖個股 - 詳細價格分析】")

        for stock in sorted(touch, key=lambda x: float(x.get("ret", 0) or 0), reverse=True):
            name = str(stock.get("name", "")).strip()
            sym = _sym_no_suffix(str(stock.get("symbol", "")).strip())
            streak_prev = _safe_int(stock.get("streak_prev", 0) or 0, 0)
            ret = _format_percent(stock.get("ret"))

            yesterday = f"昨{streak_prev}" if streak_prev > 0 else "昨無"
            line_short = f"- {name}({sym})｜{yesterday} 觸 {ret}"
            lines_short.append(line_short)

            # 完整版：詳細價格資訊
            line_full = f"- {name}({sym})｜{yesterday} 觸 {ret}"

            if include_price_details:
                prev = _format_price(stock.get("prev_close"))
                limit = _format_price(stock.get("limit_up_price"))
                high = _format_price(stock.get("high"))
                close = _format_price(stock.get("close"))

                # 計算與漲停價的差距
                try:
                    close_val = float(stock.get("close", 0))
                    limit_val = float(stock.get("limit_up_price", 0))
                    diff = close_val - limit_val
                    diff_pct = (diff / limit_val * 100) if limit_val > 0 else 0

                    price_info = f"前收:{prev} 漲停:{limit} 最高:{high} 收盤:{close}"
                    price_info += f" 差:{diff:+.3f} ({diff_pct:+.3f}%)"

                    # 標記可能誤判的個股
                    if abs(diff_pct) < 0.01:  # 相差不到 0.01%
                        price_info += " ⚠️(可能誤判)"

                    line_full += f" [{price_info}]"
                except Exception:
                    line_full += f" [前收:{prev} 漲停:{limit} 最高:{high} 收盤:{close}]"

            lines_full.append(line_full)

    # 題材個股
    if theme:
        lines_short.append("")
        lines_short.append("🎯【題材/無漲跌幅個股】")
        lines_full.append("")
        lines_full.append("🎯【題材/無漲跌幅個股】")

        for stock in theme:
            name = str(stock.get("name", "")).strip()
            sym = _sym_no_suffix(str(stock.get("symbol", "")).strip())
            ret = _format_percent(stock.get("ret"))
            yesterday = _get_yesterday_status(stock)
            lines_short.append(f"- {name}({sym})｜{yesterday} 題材 {ret}")
            lines_full.append(f"- {name}({sym})｜{yesterday} 題材 {ret}")

    # 產業統計摘要
    if sector_summary:
        lines_short.append("")
        lines_short.append("📊【產業統計 Top 15】")
        lines_full.append("")
        lines_full.append("📊【產業統計 Top 15】")

        # 只取有鎖死個股的產業
        locked_sectors = [s for s in sector_summary if int(s.get("locked_cnt", 0) or 0) > 0]
        locked_sectors = sorted(
            locked_sectors,
            key=lambda x: int(x.get("locked_cnt", 0) or 0),
            reverse=True
        )[:15]

        for i, srow in enumerate(locked_sectors, 1):
            sector = str(srow.get("sector", "未分類")).strip()
            locked_cnt = int(srow.get("locked_cnt", 0) or 0)
            touch_cnt = int(srow.get("touch_cnt", 0) or 0)
            theme_cnt = int(srow.get("no_limit_cnt", 0) or 0)

            line = f"{i:2d}. {sector}｜鎖{locked_cnt} 觸{touch_cnt} 題{theme_cnt}"
            lines_short.append(line)
            lines_full.append(line)

    # 底部資訊
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines_short.append("")
    lines_short.append(f"生成時間: {timestamp}")
    lines_short.append("#台股 #漲停 #盤中快訊")

    lines_full.append("")
    lines_full.append(f"生成時間: {timestamp}")
    lines_full.append("資料來源: yfinance | 免責聲明: 非投資建議")

    # 組合成文字
    short_text = "\n".join(lines_short).strip()
    full_text = "\n".join(lines_full).strip()

    # 確保短版不超過字數限制
    if len(short_text) > max_chars:
        short_text = short_text[: max_chars - 100] + "\n...\n(完整報告請見詳細版)"

    return short_text, full_text


# -------------------------
# 檔案寫入函數
# -------------------------


def write_detailed_status_reports(
    payload: Dict[str, Any],
    out_dir: Path,
    *,
    short_filename: str = "x_post.txt",
    full_filename: str = "detailed_status.txt",
    **kwargs,
) -> Tuple[Path, Path]:
    """
    寫入詳細漲停報告

    Returns:
        Tuple[Path, Path]: (短版檔案路徑, 完整版檔案路徑)
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # 生成報告
    short_text, full_text = build_detailed_status_report(payload, **kwargs)

    # 寫入檔案
    short_path = out_dir / short_filename
    full_path = out_dir / full_filename

    short_path.write_text(short_text, encoding="utf-8")
    full_path.write_text(full_text, encoding="utf-8")

    return short_path, full_path


# -------------------------
# 用於直接執行的函數
# -------------------------


def generate_for_x_post(
    payload: Dict[str, Any],
    out_dir: Path,
    **kwargs,
) -> str:
    """
    專門為 X 貼文生成報告
    """
    short_text, _ = build_detailed_status_report(payload, **kwargs)

    # 優化格式，適合 X
    lines = short_text.split("\n")
    optimized_lines = []

    for line in lines:
        # 移除過長的產業統計（如果有的話）
        if "｜鎖" in line and "觸" in line and "題" in line:
            # 簡化產業統計行
            parts = line.split("｜")
            if len(parts) >= 2:
                sector = parts[0].strip()
                counts = parts[1].strip()
                optimized_lines.append(f"{sector} {counts}")
        else:
            optimized_lines.append(line)

    optimized_text = "\n".join(optimized_lines)
    char_count = len(optimized_text)

    if char_count > 2800:
        optimized_text = optimized_text[:2750] + "\n...\n(完整報告請見詳細版)"

    return optimized_text
