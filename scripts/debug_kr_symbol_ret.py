# scripts/debug_kr_symbol_ret.py
# -*- coding: utf-8 -*-
"""
Debug KR single symbol returns:
- 从DB (sqlite) 读取并计算收益数据
- 从payload JSON 读取快照数据
- 比较两边的数据

用法:
  python scripts/debug_kr_symbol_ret.py --symbol 005930 --ymd 2026-01-29 --days 12 ^
    --db markets/kr/kr_stock_warehouse.db ^
    --payload data/cache/kr/2026-01-29/close.payload.json

如果省略 --db，会尝试使用 markets/kr/kr_stock_warehouse.db
如果省略 --payload，只会打印数据库端的数据

也可以查询多个股票:
  python scripts/debug_kr_symbol_ret.py --symbols "005930,000660,035420" --ymd 2026-02-01 --days 5
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import warnings
warnings.filterwarnings('ignore')


# 韩国股市的涨停限制（主板15%，科斯达克30%）
KR_RET_TH_MAIN = 0.15  # 主板15%
KR_RET_TH_KOSDAQ = 0.30  # 科斯达克30%


def _f(x: Any) -> float:
    try:
        if x is None:
            return float("nan")
        return float(x)
    except Exception:
        return float("nan")


def _i(x: Any) -> int:
    try:
        return int(x)
    except Exception:
        return 0


def _s(x: Any) -> str:
    return str(x) if x is not None else ""


def _load_payload(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    return json.loads(p.read_text(encoding="utf-8"))


def _pick_universe(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    # 根据你的pipeline惯例
    for k in ("open_limit_watchlist", "snapshot_open", "snapshot_main", "snapshot_all", "snapshot"):
        v = payload.get(k)
        if isinstance(v, list) and v:
            return v
    return []


def _find_symbol_in_payload_rows(rows: List[Dict[str, Any]], symbol: str) -> Dict[str, Any]:
    symu = (symbol or "").strip()
    for r in rows:
        # 韩国股票代码通常是6位数字，可能不带后缀
        row_symbol = str(r.get("symbol") or "").strip()
        # 直接比较或移除后缀比较
        if row_symbol == symu or row_symbol.replace(".KS", "") == symu or row_symbol.replace(".KQ", "") == symu:
            return r
    return {}


def _db_default_path() -> Path:
    env = (os.getenv("KR_DB_PATH") or "").strip()
    if env:
        return Path(env)
    return Path("markets/kr/kr_stock_warehouse.db")


def _get_market_from_symbol(symbol: str, conn: sqlite3.Connection) -> Tuple[str, str]:
    """根据股票代码判断市场，返回市场类型和完整的symbol"""
    symbol_clean = symbol.strip()
    
    # 如果已经有后缀，直接使用
    if '.' in symbol_clean:
        suffix = symbol_clean.split('.')[-1].upper()
        if suffix == 'KS':
            return "main", symbol_clean
        elif suffix == 'KQ':
            return "kosdaq", symbol_clean
        else:
            return "main", symbol_clean  # 默认
    
    # 如果没有后缀，从stock_info表中查找
    cursor = conn.cursor()
    
    # 尝试查找带.KS后缀的
    cursor.execute("SELECT symbol, market_detail FROM stock_info WHERE symbol = ?", (symbol_clean + '.KS',))
    row = cursor.fetchone()
    if row:
        symbol_with_suffix, market_detail = row
        market = "main" if market_detail == 'KOSPI' else "kosdaq"
        return market, symbol_with_suffix
    
    # 尝试查找带.KQ后缀的
    cursor.execute("SELECT symbol, market_detail FROM stock_info WHERE symbol = ?", (symbol_clean + '.KQ',))
    row = cursor.fetchone()
    if row:
        symbol_with_suffix, market_detail = row
        market = "kosdaq" if market_detail == 'KOSDAQ' else "main"
        return market, symbol_with_suffix
    
    # 尝试模糊查找
    cursor.execute("SELECT symbol, market_detail FROM stock_info WHERE symbol LIKE ?", (symbol_clean + '.%',))
    row = cursor.fetchone()
    if row:
        symbol_with_suffix, market_detail = row
        market = "main" if market_detail == 'KOSPI' else "kosdaq"
        return market, symbol_with_suffix
    
    # 如果找不到，尝试直接使用原始symbol（可能不带后缀）
    return "main", symbol_clean


def _get_limit_rate_for_market(market: str) -> float:
    """根据市场类型获取涨停限制"""
    if market == "kosdaq":
        return KR_RET_TH_KOSDAQ
    else:  # main or default
        return KR_RET_TH_MAIN


def _query_db_recent_days(
    db_path: Path,
    symbol: str,
    ymd: str,
    days: int,
) -> Tuple[pd.DataFrame, str, str, float]:
    """
    查询最近N个交易日的价格数据
    
    返回: (DataFrame, 市场类型, 完整symbol, 涨停阈值)
    """
    if not db_path.exists():
        raise FileNotFoundError(f"数据库不存在: {db_path}")

    conn = sqlite3.connect(str(db_path))
    
    try:
        # 获取市场信息和完整symbol
        market, symbol_with_suffix = _get_market_from_symbol(symbol, conn)
        limit_rate = _get_limit_rate_for_market(market)
        
        print(f"[DB] 股票: {symbol} -> {symbol_with_suffix} ({market}), 涨停限制: {limit_rate*100}%")
        
        # 查询价格数据
        query = """
        SELECT date, open, high, low, close, volume
        FROM stock_prices
        WHERE symbol = ? AND date <= ?
        ORDER BY date DESC
        LIMIT ?
        """
        
        df = pd.read_sql_query(
            query,
            conn,
            params=(symbol_with_suffix, ymd, int(days)),
        )
        
        if df.empty:
            print(f"[DB] 警告: 未找到 {symbol_with_suffix} 的数据")
            return pd.DataFrame(), market, symbol_with_suffix, limit_rate
        
        # 重命名列以便统一处理
        df = df.rename(columns={
            'date': 'date',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume'
        })
        
        # 转换为时间正序
        df = df.sort_values('date').reset_index(drop=True)
        
        # 转换为数值类型
        for c in ['open', 'high', 'low', 'close', 'volume']:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce')
        
        # 计算收益
        df['prev_close'] = df['close'].shift(1)
        
        df['ret_close'] = (df['close'] / df['prev_close']) - 1.0
        df['ret_high'] = (df['high'] / df['prev_close']) - 1.0
        
        # 使用韩国市场的涨停阈值
        df['hit_close_limit'] = ((df['ret_close'] >= limit_rate) & df['ret_close'].notna()).astype(int)
        df['touch_limit'] = ((df['ret_high'] >= limit_rate) & (df['hit_close_limit'] == 0) & df['ret_high'].notna()).astype(int)
        
        # 计算连板数
        streak = []
        cur = 0
        for v in df['hit_close_limit'].tolist():
            if v == 1:
                cur += 1
            else:
                cur = 0
            streak.append(cur)
        df['streak_close'] = streak
        df['streak_prev'] = df['streak_close'].apply(lambda x: max(0, int(x) - 1))
        
        return df, market, symbol_with_suffix, limit_rate
        
    finally:
        conn.close()


def _fmt_pct(x: Any, digits: int = 2) -> str:
    v = _f(x)
    if pd.isna(v):
        return "NA"
    return f"{v*100:+.{digits}f}%"


def _print_payload_snapshot(payload_row: Dict[str, Any], symbol: str) -> None:
    if not payload_row:
        print(f"[JSON] symbol={symbol}: 在payload中未找到")
        return
    
    # 列出可能的字段
    fields = [
        "bar_date",
        "symbol",
        "name",
        "sector",
        "prev_close",
        "open",
        "high",
        "low",
        "close",
        "ret",
        "ret_high",
        "hit_limit",
        "touch_limit",
        "streak",
        "streak_prev",
        "limit_rate",  # 韩国可能有的字段
        "market",      # 市场类型
    ]
    
    print("\n[JSON] payload行 (关键字段)")
    for k in fields:
        if k in payload_row:
            v = payload_row.get(k)
            if k in ('ret', 'ret_high', 'limit_rate'):
                if isinstance(v, (int, float)):
                    print(f"  {k:15s} = {_fmt_pct(v)}")
                else:
                    print(f"  {k:15s} = {v}")
            else:
                print(f"  {k:15s} = {v}")
    
    # 额外信息
    if 'market_detail' in payload_row:
        print(f"  {'market_detail':15s} = {payload_row.get('market_detail')}")
    
    print("")


def debug_single_symbol(args, symbol: str):
    """调试单个股票"""
    print(f"\n{'='*80}")
    print(f"调试股票: {symbol}")
    print(f"{'='*80}")
    
    ymd = args.ymd.strip()
    days = int(args.days)
    db_path = Path(args.db) if args.db else _db_default_path()
    
    # ---- 数据库端 ----
    print(f"[DB] 数据库: {db_path}")
    df, market, symbol_with_suffix, limit_rate = _query_db_recent_days(
        db_path=db_path, 
        symbol=symbol, 
        ymd=ymd, 
        days=days
    )
    
    if df.empty:
        print(f"[DB] symbol={symbol} <= {ymd}: 无数据")
        return
    
    # 显示表格
    df_show = df.copy()
    
    # 格式化百分比
    df_show["ret_close_pct"] = df_show["ret_close"].apply(lambda x: _fmt_pct(x, 2))
    df_show["ret_high_pct"] = df_show["ret_high"].apply(lambda x: _fmt_pct(x, 2))
    
    # 格式化价格
    df_show["prev_close_fmt"] = df_show["prev_close"].map(lambda x: "NA" if pd.isna(x) else f"{x:,.0f}")
    
    for c in ["open", "high", "low", "close"]:
        df_show[f"{c}_fmt"] = df_show[c].map(lambda x: "NA" if pd.isna(x) else f"{x:,.0f}")
    
    # 显示关键信息
    display_cols = [
        'date', 
        'prev_close_fmt', 
        'open_fmt', 'high_fmt', 'low_fmt', 'close_fmt',
        'ret_close_pct', 'ret_high_pct',
        'hit_close_limit', 'touch_limit', 'streak_close'
    ]
    
    # 重命名显示列
    df_display = df_show[display_cols].copy()
    df_display.columns = [
        '日期', '前收', '开盘', '最高', '最低', '收盘', 
        '收盘涨幅', '最高涨幅', '涨停', '触板', '连板'
    ]
    
    print(f"\n[DB] {symbol_with_suffix} ({market}) 最近 {len(df)} 个交易日数据")
    print(f"涨停限制: {limit_rate*100:.1f}%")
    print("-" * 120)
    print(df_display.to_string(index=False))
    
    # 最后一天的摘要
    last = df.iloc[-1].to_dict()
    
    print(f"\n[DB] 最后交易日摘要 ({last['date']})")
    print(f"  日期           = {last['date']}")
    print(f"  前收盘价       = {last['prev_close']:,.0f}")
    print(f"  收盘价         = {last['close']:,.0f}")
    print(f"  收盘收益       = {_fmt_pct(last['ret_close'])}")
    print(f"  最高收益       = {_fmt_pct(last['ret_high'])}")
    print(f"  是否涨停       = {'是' if last['hit_close_limit'] == 1 else '否'} (>= {limit_rate*100:.1f}%)")
    print(f"  触及涨停       = {'是' if last['touch_limit'] == 1 else '否'}")
    print(f"  连板数         = {int(last['streak_close'])}")
    print(f"  前一日连板数   = {int(last['streak_prev'])}")
    
    # 计算统计信息
    if len(df) > 1:
        print(f"\n[DB] 统计信息 (最近 {len(df)} 个交易日):")
        print(f"  涨停天数       = {df['hit_close_limit'].sum()}")
        print(f"  触板天数       = {df['touch_limit'].sum()}")
        print(f"  平均涨幅       = {_fmt_pct(df['ret_close'].mean())}")
        print(f"  最大涨幅       = {_fmt_pct(df['ret_close'].max())}")
        print(f"  最小涨幅       = {_fmt_pct(df['ret_close'].min())}")
    
    # ---- JSON端 ----
    if args.payload:
        payload = _load_payload(args.payload)
        if payload:
            rows = _pick_universe(payload)
            payload_row = _find_symbol_in_payload_rows(rows, symbol)
            
            print(f"\n[JSON] payload: {args.payload}")
            _print_payload_snapshot(payload_row, symbol)
            
            # 对比数据库最后一天和JSON数据
            if payload_row:
                print("[对比] 数据库最后一天 vs JSON (同日期检查)")
                db_last = df.iloc[-1]
                db_date = str(db_last["date"])
                
                js_date = str(payload_row.get("bar_date") or payload.get("ymd_effective") or payload.get("ymd") or "")
                print(f"  数据库日期     = {db_date}")
                print(f"  JSON bar_date/ymd = {js_date}")
                
                # JSON中的收益数据
                js_ret = _f(payload_row.get("ret"))
                js_ret_high = _f(payload_row.get("ret_high"))
                js_hit_limit = _i(payload_row.get("hit_limit") or payload_row.get("hit_close_limit"))
                js_touch = _i(payload_row.get("touch_limit") or payload_row.get("touch_limit"))
                js_streak = _i(payload_row.get("streak"))
                js_streak_prev = _i(payload_row.get("streak_prev"))
                
                print(f"  数据库 收盘收益 = {_fmt_pct(db_last['ret_close'])} | JSON ret      = {_fmt_pct(js_ret)}")
                print(f"  数据库 最高收益 = {_fmt_pct(db_last['ret_high'])} | JSON ret_high = {_fmt_pct(js_ret_high)}")
                print(f"  数据库 是否涨停 = {int(db_last['hit_close_limit'])} | JSON hit_limit = {js_hit_limit}")
                print(f"  数据库 触及涨停 = {int(db_last['touch_limit'])} | JSON touch_limit = {js_touch}")
                print(f"  数据库 连板数   = {int(db_last['streak_close'])} | JSON streak    = {js_streak}")
                print(f"  数据库 前日连板 = {int(db_last['streak_prev'])} | JSON streak_prev= {js_streak_prev}")
                
                # 差异提示
                if db_date != js_date:
                    print(f"\n[警告] 日期不匹配! 这可能是因为数据源不同步或处理延迟")
    elif args.payload:
        print(f"[JSON] 提供了payload路径但为空? path={args.payload}")


def debug_multiple_symbols(args, symbols: List[str]):
    """调试多个股票，显示简要信息"""
    print(f"\n{'='*80}")
    print(f"调试多个股票: {', '.join(symbols)}")
    print(f"{'='*80}")
    
    ymd = args.ymd.strip()
    days = int(args.days)
    db_path = Path(args.db) if args.db else _db_default_path()
    
    all_results = []
    
    for symbol in symbols:
        df, market, symbol_with_suffix, limit_rate = _query_db_recent_days(
            db_path=db_path, 
            symbol=symbol, 
            ymd=ymd, 
            days=days
        )
        
        if df.empty:
            all_results.append({
                'symbol': symbol,
                'symbol_full': symbol_with_suffix,
                'market': market,
                'limit_rate': limit_rate,
                'last_date': '无数据',
                'last_close': 'NA',
                'last_ret': 'NA',
                'hit_limit': 'NA',
                'streak': 'NA',
                'has_data': False
            })
            continue
        
        last = df.iloc[-1].to_dict()
        
        all_results.append({
            'symbol': symbol,
            'symbol_full': symbol_with_suffix,
            'market': market,
            'limit_rate': limit_rate,
            'last_date': last['date'],
            'last_close': f"{last['close']:,.0f}" if not pd.isna(last['close']) else 'NA',
            'last_ret': _fmt_pct(last['ret_close']),
            'hit_limit': '是' if last['hit_close_limit'] == 1 else '否',
            'touch_limit': '是' if last['touch_limit'] == 1 else '否',
            'streak': int(last['streak_close']),
            'days_up': df['hit_close_limit'].sum(),
            'has_data': True
        })
    
    # 创建DataFrame显示
    if all_results:
        df_results = pd.DataFrame(all_results)
        
        # 显示列
        display_cols = ['symbol', 'symbol_full', 'market', 'limit_rate', 
                       'last_date', 'last_close', 'last_ret', 
                       'hit_limit', 'touch_limit', 'streak', 'days_up']
        
        df_display = df_results[display_cols].copy()
        df_display.columns = ['代码', '完整代码', '市场', '涨停%', '最后日期', 
                             '收盘价', '涨幅', '涨停', '触板', '连板', '涨停天数']
        
        print(f"\n[汇总] 多个股票简要信息 (最近 {days} 个交易日截至 {ymd})")
        print("-" * 120)
        print(df_display.to_string(index=False))
        
        # 统计信息
        valid_results = [r for r in all_results if r['has_data']]
        if valid_results:
            print(f"\n[统计] 共 {len(valid_results)} 个股票有数据:")
            print(f"  涨停股票数: {sum(1 for r in valid_results if r['hit_limit'] == '是')}")
            print(f"  触板股票数: {sum(1 for r in valid_results if r['touch_limit'] == '是')}")
            print(f"  平均连板数: {sum(r['streak'] for r in valid_results) / len(valid_results):.1f}")


def main():
    parser = argparse.ArgumentParser(description="调试韩国股票收益数据")
    parser.add_argument("--symbol", help="单个股票代码 (例如: 005930)")
    parser.add_argument("--symbols", help="多个股票代码，用逗号分隔 (例如: '005930,000660,035420')")
    parser.add_argument("--ymd", required=True, help="截止日期，例如: 2026-01-31")
    parser.add_argument("--days", type=int, default=10, help="最近交易日数 (默认: 10)")
    parser.add_argument("--db", default=None, help="数据库路径")
    parser.add_argument("--payload", default=None, help="payload JSON路径")
    
    args = parser.parse_args()
    
    if not args.symbol and not args.symbols:
        print("错误: 必须指定 --symbol 或 --symbols 参数")
        return
    
    # 处理单个或多个股票
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(',') if s.strip()]
        debug_multiple_symbols(args, symbols)
        
        # 询问是否要查看详细数据
        print("\n" + "="*80)
        detail = input("是否要查看某个股票的详细数据? (输入股票代码或按回车跳过): ").strip()
        if detail:
            args.symbol = detail
            debug_single_symbol(args, detail)
    else:
        # 单个股票
        debug_single_symbol(args, args.symbol)
    
    print("\n完成.")


if __name__ == "__main__":
    main()