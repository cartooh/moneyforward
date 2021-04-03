#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import argparse
import pickle
import json
import csv
import logging
from pprint import pprint
import requests
import pandas as pd

logger = logging.getLogger(__name__)

formatter = '%(levelname)s : %(asctime)s : %(message)s'
logging.basicConfig(level=logging.INFO, format=formatter)

def save_json(fn, obj):
    with open(fn, 'w') as f:
        json.dump(obj, f)


def request_category(s):
    return s.get("https://moneyforward.com/sp/category").json()


def update_params(name, params, args, default=None):
    value = getattr(args, name)
    if value:
        params[name] = value
    elif default:
        params[name] = default
    return params


def get_category(s, args):
    category = request_category(s)
    if args.json:
        save_json(args.json, category)
        return
    pprint(category)


def request_large_categories(s):
    large_categories = s.get("https://moneyforward.com/sp2/large_categories").json()
    return large_categories['large_categories']


def save_large_categories_csv(fn, large_categories):
    with open(fn, 'wt', encoding='utf_8_sig') as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow("large_category_id large_category_name middle_category_id middle_category_name user_category".split())
        for large_category in large_categories:
            for middle_category in large_category['middle_categories']:
                writer.writerow([large_category['id'], large_category['name'], 
                    middle_category['id'], middle_category['name'], middle_category['user_category']])


def get_large_categories(s, args):
    large_categories = request_large_categories(s)
    if args.json:
        save_json(args.json, large_categories)
        return
       
    if args.csv:
        save_large_categories_csv(args.csv, large_categories)
        return
    
    pprint(large_categories)


def search_category(s, args):
    if not os.path.exists(args.cache_csv) or args.force_update:
        large_categories = request_large_categories(s)
        save_large_categories_csv(args.cache_csv, large_categories)
    df = pd.read_csv(args.cache_csv)
    if args.large:
        df = df[df['large_category_name'].str.contains(args.large, na=False)]
    if args.middle:
        df = df[df['middle_category_name'].str.contains(args.middle, na=False)]
    
    print(*df.columns.tolist())
    for index, row in df.iterrows():
        print(*row.tolist())

def get_categories_form_user_asset_acts(user_asset_acts):
    large = { int(k):v for k, v in user_asset_acts['large'].items()}
    large[0] = '-'
    middle = { int(k):v for k, v in user_asset_acts['middle'].items()}
    middle[0] = '-'
    return large, middle


def append_row_form_user_asset_acts(rows, user_asset_acts, args):
    large, middle = get_categories_form_user_asset_acts(user_asset_acts)
    
    for act in user_asset_acts['user_asset_acts']:
        row = []
        for h in args.list_header:
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


def output_rows_for_user_asset_acts(rows, args):
    if args.list:
        for row in rows:
            print(*row)
    elif args.csv:
        with open(args.csv, 'wt', encoding='utf_8_sig') as f:
            writer = csv.writer(f, lineterminator="\n")
            writer.writerow(args.list_header)
            for row in rows:
                writer.writerow(row)
    else:
        raise ValueError("Invalid args.list or args.csv")


def create_user_asset_acts_params(args):
    params = {}
    update_params('offset', params, args)
    update_params('size', params, args)
    update_params('is_new', params, args)
    update_params('is_old', params, args)
    update_params('is_continuous', params, args)
    update_params('select_category', params, args)
    update_params('base_date', params, args)
    update_params('keyword', params, args)
    return params


def request_user_asset_acts(s, params={}):
    user_asset_acts = s.get("https://moneyforward.com/sp2/user_asset_acts", params=params).json()
    if 'messages' in user_asset_acts:
        pprint(user_asset_acts)
        raise ValueError(user_asset_acts['messages'])
    
    if 'error' in user_asset_acts:
        pprint(user_asset_acts)
        raise ValueError(user_asset_acts['error'])
    
    return user_asset_acts

def get_user_asset_acts(s, args):
    params = create_user_asset_acts_params(args)
    
    if args.csv or args.list:
        rows = []
        MAX_SIZE = 500
        if 'size' in params and params['size'] > MAX_SIZE:
            if 'offset' not in params:
                params['offset'] = 0
            size = params['size']
            while size > 0:
                logger.info('get_user_asset_acts: size = %d' % size)
                params['size'] = min(size, MAX_SIZE)
                user_asset_acts = request_user_asset_acts(s, params)
                append_row_form_user_asset_acts(rows, user_asset_acts, args)
                params['offset'] += MAX_SIZE
                size -= MAX_SIZE
                
                logger.info('total_count: %d' % user_asset_acts['total_count'])
                if user_asset_acts['total_count'] <= 0:
                    break
        else:
            user_asset_acts = request_user_asset_acts(s, params)
            append_row_form_user_asset_acts(rows, user_asset_acts, args)
        output_rows_for_user_asset_acts(rows, args)
        return
    
    user_asset_acts = request_user_asset_acts(s, params)
    if args.json:
        save_json(args.json, user_asset_acts)
        return
    
    pprint(user_asset_acts)


def update_filter_flags(df, base_flags, column_name, match_values, not_match_values):
    if match_values:
        flags = False
        for val in match_values:
            flags |= df[column_name].str.contains(val, na=False)
    elif not_match_values:
        flags = True
        for val in not_match_values:
            flags &= ~df[column_name].str.contains(val, na=True)
    else:
        flags = True
    return base_flags & flags


def filter_csv(s, args):
    df = pd.read_csv(args.input_csv)
    if args.query:
        result = df.query(args.query, engine='python')
    elif args.pattern:
        flags = df['content'].str.contains(args.pattern, na=False) ^ args.reverse
        
        flags = update_filter_flags(df, flags, 'middle_category', args.match_middle_categories, args.not_match_middle_categories)
        flags = update_filter_flags(df, flags, 'large_category', args.match_large_categories, args.not_match_large_categories)
        flags = update_filter_flags(df, flags, 'account.service.service_name', args.match_service_name, args.not_match_service_name)
        flags = update_filter_flags(df, flags, 'sub_account.sub_type', args.match_sub_account, args.not_match_sub_account)
        
        result = df.loc[flags]
    else:
        raise ValueError("invalid args")
    
    if args.columns:
        result = result[args.columns]
    
    if args.list:
        for index, row in result.iterrows():
            print(*row.tolist())
    elif args.output_csv:
        result.to_csv(args.output_csv, encoding='utf_8_sig', index=False)
    elif args.update_category:
        large_category_id, middle_category_id = args.update_category
        request_transactions_category_bulk_updates(s, large_category_id, middle_category_id, result['id'].tolist())
    else:
        print(result)


def request_transactions_category_bulk_updates(s, large_category_id, middle_category_id, ids):
    url = 'https://moneyforward.com/sp2/transactions_category_bulk_updates'
    params = dict(
      middle_category_id=middle_category_id,
      large_category_id=large_category_id,
      ids=ids
    )
    r = s.put(url, json.dumps(params), headers={'Content-Type': 'application/json'})
    if r.status_code != requests.codes.ok:
        print(r.status_code, r.text)


def transactions_category_bulk_updates(s, args):
    request_transactions_category_bulk_updates(s, args.large_category_id, args.middle_category_id, args.ids)


parser = argparse.ArgumentParser()
parser.add_argument('-c', '--mf_cookies', default='mf_cookies.pkl')
subparsers = parser.add_subparsers(dest='cmd', required=True)

parser_category = subparsers.add_parser('category')
group_category = parser_category.add_mutually_exclusive_group()
group_category.add_argument('--json')
parser_category.set_defaults(func=get_category)

parser_large_categories = subparsers.add_parser('large_categories')
group_large_categories = parser_large_categories.add_mutually_exclusive_group()
group_large_categories.add_argument('--csv')
group_large_categories.add_argument('--json')
parser_large_categories.set_defaults(func=get_large_categories)


parser_search_category = subparsers.add_parser('search_category')
parser_search_category.add_argument('--cache_csv', default='cache_search_categories.csv')
parser_search_category.add_argument('--force_update', action='store_true')
parser_search_category.add_argument('-l', '--large')
parser_search_category.add_argument('-m', '--middle')
parser_search_category.set_defaults(func=search_category)


parser_user_asset_acts = subparsers.add_parser('user_asset_acts')
group_user_asset_acts = parser_user_asset_acts.add_mutually_exclusive_group()
group_user_asset_acts.add_argument('--csv')
group_user_asset_acts.add_argument('--json')
group_user_asset_acts.add_argument('--list', action='store_true')
parser_user_asset_acts.add_argument('--offset', type=int)
parser_user_asset_acts.add_argument('--size', type=int)
parser_user_asset_acts.add_argument('--is_new', type=int, choices=[0, 1]) # only new
parser_user_asset_acts.add_argument('--is_old', type=int, choices=[0, 1]) # only old
parser_user_asset_acts.add_argument('--is_continuous', type=int, choices=[0, 1]) # 1: from -> "1990-01-01"
parser_user_asset_acts.add_argument('--select_category', type=int) # select large category id
parser_user_asset_acts.add_argument('--base_date', type=str) # 集計日基準？
parser_user_asset_acts.add_argument('--keyword', type=str)
user_asset_acts_list_header = 'id is_transfer is_income is_target updated_at content amount large_category_id large_category middle_category_id middle_category account.service.service_name sub_account.sub_type sub_account.sub_name'.split()
parser_user_asset_acts.add_argument('--list_header', type=str, nargs='+', default=user_asset_acts_list_header)
parser_user_asset_acts.set_defaults(func=get_user_asset_acts)


parser_filter = subparsers.add_parser('filter_csv')
parser_filter.set_defaults(func=filter_csv)
parser_filter.add_argument('input_csv')
parser_filter.add_argument('--columns', type=str, nargs='+')

group = parser_filter.add_mutually_exclusive_group()
group.add_argument('--list', action='store_true')
group.add_argument('--output_csv')
group.add_argument('--update_category', type=int, nargs=2, metavar=('large_category_id', 'middle_category_id'))


group = parser_filter.add_mutually_exclusive_group(required=True)
group.add_argument('-q', '--query', help='ex) content.notnull() and content.str.match(\'セブン\') and middle_category != \'コンビニ\'')
group.add_argument('-p', '--pattern')

group_filter_pattern = parser_filter.add_argument_group('group_filter_pattern')
group_filter_pattern.add_argument('-r', '--reverse', action='store_true')

group = group_filter_pattern.add_mutually_exclusive_group()
group.add_argument('-m', '--match_middle_categories', nargs='+', metavar='category')
group.add_argument('-M', '--not_match_middle_categories', nargs='+', metavar='category')

group = group_filter_pattern.add_mutually_exclusive_group()
group.add_argument('-l', '--match_large_categories', nargs='+', metavar='category')
group.add_argument('-L', '--not_match_large_categories', nargs='+', metavar='category')

group = group_filter_pattern.add_mutually_exclusive_group()
group.add_argument('-s', '--match_service_name', nargs='+', metavar='service_name')
group.add_argument('-S', '--not_match_service_name', nargs='+', metavar='service_name')

group = group_filter_pattern.add_mutually_exclusive_group()
group.add_argument('-t', '--match_sub_account', nargs='+', metavar='sub_account')
group.add_argument('-T', '--not_match_sub_account', nargs='+', metavar='sub_account')


parser_transactions_category_bulk_updates = subparsers.add_parser('transactions_category_bulk_updates')
parser_transactions_category_bulk_updates.set_defaults(func=transactions_category_bulk_updates)
parser_transactions_category_bulk_updates.add_argument('-m', '--middle_category_id', type=int, required=True)
parser_transactions_category_bulk_updates.add_argument('-l', '--large_category_id', type=int, required=True)
parser_transactions_category_bulk_updates.add_argument('-i', '--ids', type=int, nargs='+', required=True)


args = parser.parse_args()
with requests.session() as s:
    with open(args.mf_cookies, 'rb') as f:
        s.cookies = pickle.load(f)
    
    args.func(s, args)


