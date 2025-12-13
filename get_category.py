#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MoneyForward カテゴリ取得コマンド

カテゴリ名から大カテゴリID・中カテゴリIDを取得する独立したコマンドです。

使用例:
    python get_category.py "コンビニ"
    python get_category.py "給料" --is_income
    python get_category.py "外食" --json category.json
"""

import sys
import argparse
import json
from moneyforward_api import session_from_cookie_file
from moneyforward_utils import get_middle_category_impl


def save_json(fn, obj):
    """JSONファイルに保存"""
    with open(fn, 'w') as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='MoneyForward カテゴリ名から大カテゴリID・中カテゴリIDを取得'
    )
    
    parser.add_argument(
        'category_name',
        help='検索する中カテゴリ名（部分一致）'
    )
    
    parser.add_argument(
        '-c', '--mf_cookies',
        default='mf_cookies.pkl',
        help='MoneyForward クッキーファイルのパス（デフォルト: mf_cookies.pkl）'
    )
    
    parser.add_argument(
        '--cache_csv',
        default='cache_search_categories.csv',
        help='カテゴリキャッシュCSVファイルのパス（デフォルト: cache_search_categories.csv）'
    )
    
    parser.add_argument(
        '--force_update',
        action='store_true',
        help='キャッシュを無視してAPIから再取得'
    )
    
    parser.add_argument(
        '--is_income',
        action='store_true',
        help='収入カテゴリから検索（指定しない場合は全カテゴリから検索）'
    )
    
    parser.add_argument(
        '--json',
        metavar='FILE',
        help='結果をJSONファイルに出力'
    )
    
    args = parser.parse_args(argv)
    
    # セッション作成とカテゴリ取得
    try:
        with session_from_cookie_file(args.mf_cookies) as s:
            is_income_filter = True if args.is_income else None
            
            large_id, middle_id = get_middle_category_impl(
                s,
                args.cache_csv,
                args.force_update,
                args.category_name,
                is_income=is_income_filter
            )
            
            # 結果出力
            if args.json:
                result = {
                    'category_name': args.category_name,
                    'large_category_id': large_id,
                    'middle_category_id': middle_id
                }
                save_json(args.json, result)
            else:
                print(f"{large_id} {middle_id}")
            
            return 0
    
    except ValueError as e:
        print(f"エラー: {e}", file=sys.stderr)
        return 1
    except FileNotFoundError as e:
        print(f"ファイルが見つかりません: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"予期しないエラー: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
