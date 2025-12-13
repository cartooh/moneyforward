#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import argparse
import logging
from pprint import pprint

logger = logging.getLogger(__name__)

formatter = '%(levelname)s : %(asctime)s : %(message)s'
logging.basicConfig(level=logging.INFO, format=formatter)

# Import API functions
from moneyforward_api import request_user_asset_acts, session_from_cookie_file


def get_categories_form_user_asset_acts(user_asset_acts):
    """user_asset_actsレスポンスからカテゴリ辞書を取得"""
    large = { int(k):v for k, v in user_asset_acts['large'].items()}
    large[0] = '-'
    middle = { int(k):v for k, v in user_asset_acts['middle'].items()}
    middle[0] = '-'
    return large, middle


def append_row_form_user_asset_acts(rows, user_asset_acts, list_header):
    """user_asset_actsから行データを抽出"""
    large, middle = get_categories_form_user_asset_acts(user_asset_acts)
    
    for act in user_asset_acts['user_asset_acts']:
        row = []
        for h in list_header:
            if h in act:
                row.append(act[h])
                continue
            
            if h == 'large_category':
                row.append(large[act['large_category_id']])
                continue
            
            if h == 'middle_category':
                row.append(middle[act['middle_category_id']])
                continue
            
            if '.' in h:
                node = act
                for k in h.split('.'):
                    if node is None:
                        node = '_'
                        break
                    if k not in node:
                        logger.warning('Not found key: %s' % h)
                        node = '?'
                        break
                    node = node[k]
                row.append(node)
                continue
            
            raise ValueError("Not found key: %s" % h)
        rows.append(row)


def output_rows(rows, list_header, output_format='list', csv_file=None):
    """行データを出力"""
    if output_format == 'list':
        print(*list_header)
        for row in rows:
            print(*row)
    elif output_format == 'csv':
        import csv
        with open(csv_file, 'wt', encoding='utf_8_sig') as f:
            writer = csv.writer(f, lineterminator="\n")
            writer.writerow(list_header)
            for row in rows:
                writer.writerow(row)


def save_json(fn, obj):
    """JSON保存"""
    import json
    with open(fn, 'w') as f:
        json.dump(obj, f)


def main(argv=None):
    parser = argparse.ArgumentParser(description='MoneyForward user_asset_acts API client')
    parser.add_argument('-c', '--mf_cookies', default='mf_cookies.pkl',
                        help='Cookie file path (default: mf_cookies.pkl)')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Enable debug mode')
    
    # Output format
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument('--json', help='Output as JSON file')
    output_group.add_argument('--csv', help='Output as CSV file')
    output_group.add_argument('--list', action='store_true', help='Output as list (default)')
    
    # API parameters
    parser.add_argument('--offset', type=int, help='Offset for pagination')
    parser.add_argument('--size', type=int, help='Number of items to fetch')
    parser.add_argument('--is_new', type=int, choices=[0, 1], help='Only new items')
    parser.add_argument('--is_old', type=int, choices=[0, 1], help='Only old items')
    parser.add_argument('--is_continuous', type=int, choices=[0, 1], 
                        help='Continuous mode (1: from -> "1990-01-01")')
    parser.add_argument('--select_category', type=int, help='Select large category ID')
    parser.add_argument('--base_date', type=str, help='Base date for aggregation')
    parser.add_argument('--keyword', type=str, help='Keyword search')
    
    # List format options
    default_list_header = 'id is_transfer is_income is_target updated_at content amount large_category_id large_category middle_category_id middle_category account.service.service_name sub_account.sub_type sub_account.sub_name'.split()
    parser.add_argument('--list_header', type=str, nargs='+', default=default_list_header,
                        help='List header columns')
    
    args = parser.parse_args(argv)
    
    if args.debug:
        import http.client
        http.client.HTTPConnection.debuglevel = 2
    
    # Call API
    with session_from_cookie_file(args.mf_cookies) as s:
        if args.csv or args.list:
            rows = []
            MAX_SIZE = 500
            size = args.size
            
            if size and size > MAX_SIZE:
                offset = args.offset or 0
                original_size = size
                while size > 0:
                    logger.info('get_user_asset_acts: size = %d' % size)
                    # Fetch with pagination
                    user_asset_acts = request_user_asset_acts(
                        s,
                        offset=offset,
                        size=min(size, MAX_SIZE),
                        is_new=args.is_new,
                        is_old=args.is_old,
                        is_continuous=args.is_continuous,
                        select_category=args.select_category,
                        base_date=args.base_date,
                        keyword=args.keyword
                    )
                    append_row_form_user_asset_acts(rows, user_asset_acts, args.list_header)
                    offset += MAX_SIZE
                    size -= MAX_SIZE
                    
                    logger.info('total_count: %d' % user_asset_acts['total_count'])
                    if user_asset_acts['total_count'] <= 0:
                        break
            else:
                user_asset_acts = request_user_asset_acts(
                    s,
                    offset=args.offset,
                    size=args.size,
                    is_new=args.is_new,
                    is_old=args.is_old,
                    is_continuous=args.is_continuous,
                    select_category=args.select_category,
                    base_date=args.base_date,
                    keyword=args.keyword
                )
                append_row_form_user_asset_acts(rows, user_asset_acts, args.list_header)
            
            # Output
            if args.csv:
                output_rows(rows, args.list_header, output_format='csv', csv_file=args.csv)
            else:
                output_rows(rows, args.list_header, output_format='list')
        else:
            # JSON or pprint output
            user_asset_acts = request_user_asset_acts(
                s,
                offset=args.offset,
                size=args.size,
                is_new=args.is_new,
                is_old=args.is_old,
                is_continuous=args.is_continuous,
                select_category=args.select_category,
                base_date=args.base_date,
                keyword=args.keyword
            )
            
            if args.json:
                save_json(args.json, user_asset_acts)
            else:
                pprint(user_asset_acts)


if __name__ == '__main__':
    main()
