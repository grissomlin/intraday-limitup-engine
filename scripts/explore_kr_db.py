# scripts/explore_kr_db.py
# -*- coding: utf-8 -*-
"""
探索韩国数据库结构，查看有哪些表和字段
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import List, Dict, Any


def get_table_schema(db_path: Path) -> Dict[str, List[Dict[str, Any]]]:
    """获取数据库中所有表的架构信息"""
    if not db_path.exists():
        raise FileNotFoundError(f"数据库文件不存在: {db_path}")
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    # 获取所有表名
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    
    schema = {}
    
    for table in tables:
        # 获取表结构
        cursor.execute(f"PRAGMA table_info('{table}');")
        columns = cursor.fetchall()
        
        # 转换为字典列表
        column_info = []
        for col in columns:
            column_info.append({
                'cid': col[0],          # 列ID
                'name': col[1],         # 列名
                'type': col[2],         # 数据类型
                'notnull': col[3],      # 是否允许NULL
                'default': col[4],      # 默认值
                'pk': col[5]            # 是否为主键
            })
        
        schema[table] = column_info
    
    conn.close()
    return schema


def get_table_sample_data(db_path: Path, table_name: str, limit: int = 5) -> List[Dict[str, Any]]:
    """获取表的样本数据"""
    if not db_path.exists():
        raise FileNotFoundError(f"数据库文件不存在: {db_path}")
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        sample_data = []
        for row in rows:
            sample_data.append(dict(zip(columns, row)))
    except Exception as e:
        print(f"读取表 {table_name} 时出错: {e}")
        sample_data = []
    
    conn.close()
    return sample_data


def print_schema_summary(schema: Dict[str, List[Dict[str, Any]]]):
    """打印数据库架构摘要"""
    print(f"数据库包含 {len(schema)} 个表:")
    print("=" * 80)
    
    for table_name, columns in schema.items():
        print(f"\n表名: {table_name}")
        print(f"列数: {len(columns)}")
        print("-" * 60)
        
        for col in columns:
            pk_marker = " (PK)" if col['pk'] else ""
            notnull_marker = " NOT NULL" if col['notnull'] else ""
            default_marker = f" DEFAULT {col['default']}" if col['default'] else ""
            
            print(f"  {col['name']:20s} {col['type']:15s}{pk_marker}{notnull_marker}{default_marker}")
        
        # 检查是否有日期和价格相关字段
        date_cols = [c['name'] for c in columns if 'date' in c['name'].lower()]
        price_cols = [c['name'] for c in columns if any(word in c['name'].lower() 
                                                        for word in ['price', 'close', 'open', 'high', 'low'])]
        
        if date_cols:
            print(f"  日期字段: {', '.join(date_cols)}")
        if price_cols:
            print(f"  价格字段: {', '.join(price_cols)}")


def main():
    parser = argparse.ArgumentParser(description="探索韩国数据库结构")
    parser.add_argument("--db", default="markets/kr/kr_stock_warehouse.db", 
                       help="数据库路径 (默认: markets/kr/kr_stock_warehouse.db)")
    parser.add_argument("--table", help="指定查看特定表的结构")
    parser.add_argument("--sample", action="store_true", help="显示样本数据")
    parser.add_argument("--limit", type=int, default=5, help="样本数据行数 (默认: 5)")
    
    args = parser.parse_args()
    
    db_path = Path(args.db)
    
    if not db_path.exists():
        print(f"错误: 数据库文件不存在: {db_path}")
        print("请检查路径是否正确，或者使用 --db 参数指定正确的路径")
        return
    
    print(f"正在分析数据库: {db_path.absolute()}")
    print("=" * 80)
    
    # 获取所有表的架构
    schema = get_table_schema(db_path)
    
    if args.table:
        # 查看特定表
        if args.table in schema:
            print(f"\n表 '{args.table}' 的架构:")
            print("-" * 60)
            
            for col in schema[args.table]:
                pk_marker = " (PK)" if col['pk'] else ""
                notnull_marker = " NOT NULL" if col['notnull'] else ""
                default_marker = f" DEFAULT {col['default']}" if col['default'] else ""
                
                print(f"  {col['name']:20s} {col['type']:15s}{pk_marker}{notnull_marker}{default_marker}")
            
            if args.sample:
                print(f"\n表 '{args.table}' 的样本数据 (前 {args.limit} 行):")
                print("-" * 60)
                
                sample_data = get_table_sample_data(db_path, args.table, args.limit)
                
                if sample_data:
                    # 打印表头
                    headers = list(sample_data[0].keys())
                    print("  " + " | ".join(f"{h:15s}" for h in headers))
                    print("  " + "-" * (len(headers) * 16))
                    
                    # 打印数据行
                    for row in sample_data:
                        values = []
                        for h in headers:
                            val = row[h]
                            if val is None:
                                val_str = "NULL"
                            elif isinstance(val, (int, float)):
                                val_str = str(val)
                            else:
                                val_str = str(val)[:14]  # 截断长字符串
                            values.append(f"{val_str:15s}")
                        print("  " + " | ".join(values))
                else:
                    print("  无数据")
        else:
            print(f"错误: 表 '{args.table}' 不存在")
            print(f"可用的表: {', '.join(schema.keys())}")
    else:
        # 打印所有表的摘要
        print_schema_summary(schema)
        
        # 如果有价格相关的表，特别提示
        for table_name, columns in schema.items():
            column_names = [c['name'].lower() for c in columns]
            if any(word in ' '.join(column_names) for word in ['price', 'close', 'open', 'high', 'low', 'volume']):
                print(f"\n提示: 表 '{table_name}' 看起来包含价格数据")
                print("      您可能需要检查这个表来调试股票收益")


if __name__ == "__main__":
    main()