#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import pickle
import json
import csv
import logging
import datetime
from pprint import pprint
import requests
import pandas as pd
import dateutil.parser
from datetime import timedelta, datetime
from collections import defaultdict
import sqlite3
import sqlalchemy
from contextlib import closing
from bs4 import BeautifulSoup
from time import sleep
from random import uniform
from tqdm import tqdm

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


def upsert(frame, name: str, unique_index_label, con):
    pandas_sql = pd.io.sql.pandasSQL_builder(con)
    
    if isinstance(frame, pd.Series):
        frame = frame.to_frame()
    elif not isinstance(frame, pd.DataFrame):
        raise NotImplementedError(
            "'frame' argument should be either a Series or a DataFrame"
        )
    
    # table = pd.io.sql.SQLiteTable(name, pandas_sql, frame=frame, index=False, if_exists='append')
    table = pd.io.sql.SQLiteTable(name, pandas_sql, frame=frame, index=False, if_exists='append', keys=unique_index_label)
    table.create()
    # pandas_sql.execute('CREATE UNIQUE INDEX IF NOT EXISTS "{0}_{1}" ON "{0}" ("{1}");'.format(table.name, unique_index_label))
    
    def _execute_insert(self, conn, keys, data_iter):
        wld = "?"  # wildcard char
        escape = pd.io.sql._get_valid_sqlite_name

        bracketed_names = [escape(str(column)) for column in keys]
        col_names = ",".join(bracketed_names)
        wildcards = ",".join([wld] * len(bracketed_names))
        insert_statement = (
            f"INSERT OR REPLACE INTO {escape(self.name)} ({col_names}) VALUES ({wildcards})"
        )
        data_list = list(data_iter)
        conn.executemany(insert_statement, data_list)
    
    table.insert(method=_execute_insert)


def get_large_categories(s, args):
    large_categories = request_large_categories(s)
    if args.json:
        save_json(args.json, large_categories)
        return
       
    if args.csv:
        save_large_categories_csv(args.csv, large_categories)
        return
    
    if args.sqlite:
        large_category_list = []
        middle_category_list = []
        for large_category in large_categories:
            large_category_list.append(dict(id=large_category['id'], name=large_category['name']))
            for middle_category in large_category['middle_categories']:
                middle_category_list.append(dict(large_category_id=large_category['id'], id=middle_category['id'], name=middle_category['name']))
        
        large_category_df = pd.DataFrame(large_category_list)
        middle_category_df = pd.DataFrame(middle_category_list)
        
        with closing(sqlite3.connect(args.sqlite)) as con:
            upsert(large_category_df, 'large_categories', 'id', con)
            upsert(middle_category_df, 'middle_categories', 'id', con)
        return
    
    pprint(large_categories)


def request_account_summaries(s):
    return s.get("https://moneyforward.com/sp2/account_summaries").json()


def save_account_summaries_csv(fn, account_summaries, args):
    raise ValueError('not supoort')
    with open(fn, 'wt', encoding='utf_8_sig') as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow("large_category_id large_category_name middle_category_id middle_category_name user_category".split())
        for large_category in large_categories:
            for middle_category in large_category['middle_categories']:
                writer.writerow([large_category['id'], large_category['name'], 
                    middle_category['id'], middle_category['name'], middle_category['user_category']])


def traverse(output, base, node, skip=()):
    if isinstance(node, list):
        # print("list", list)
        for idx, val in enumerate(node):
            # print("list", idx, val)
            traverse(output, base + "[%d]" % idx, val, skip=skip)
    elif isinstance(node, dict):
        if len(base) > 0:
            base += "."
        for key, val in node.items():
            # print("dict", key, type(val), val)
            if key in skip:
                # print("skip", key, skip)
                continue
            traverse(output, base + key, val, skip=skip)
    else:
        # print("set", base, node)
        output[base] = node


def get_account_summaries_list(account_summaries, args):
    def ext(output_list, account):
        list_key1 = 'sub_accounts'
        list_key2 = 'user_asset_det_summaries'
        
        account_ref = {}
        traverse(account_ref, '', account, (list_key1, list_key2,))
        
        if list_key1 not in account or not account[list_key1]:
            output_list.append(account_ref)
            return
        
        for sub_accounts in account[list_key1]:
            account_ref1 = account_ref.copy()
            traverse(account_ref1, list_key1, sub_accounts, (list_key2,))
            
            if list_key2 not in sub_accounts or not sub_accounts[list_key2]:
                output_list.append(account_ref1)
                continue
            
            for user_asset_det_summaries in sub_accounts[list_key2]:
                account_ref2 = account_ref1.copy()
                traverse(account_ref2, list_key1 + "." + list_key2, user_asset_det_summaries, ())
                output_list.append(account_ref2)
    
    accounts = []
    for account in account_summaries['accounts']:
        ext(accounts, account)
    df = pd.DataFrame(accounts)
    
    if getattr(args, 'service_category_id', None):
        df = df[df['service_category_id'] == args.service_category_id]
    
    if getattr(args, 'name', None):
        df = df[df['name'].str.contains(args.name)]
    
    return df


def get_account_summaries(s, args):
    account_summaries = request_account_summaries(s)
    if args.json:
        save_json(args.json, account_summaries)
        return
    
    if args.csv:
        df = get_account_summaries_list(account_summaries, args)
        df.to_csv(args.csv, encoding='utf-8-sig', index=False)
        return
        
    if args.list:
        df = get_account_summaries_list(account_summaries, args)
        df = df[args.list_header]
        
        print(*df.columns.tolist())
        for index, row in df.iterrows():
            print(*row.tolist())
        return
    
    pprint(account_summaries)


def request_service_detail(s, args):
    params = {}
    if args.sub_account_id_hash:
        params['sub_account_id_hash'] = args.sub_account_id_hash
    
    if args.range:
        params['range'] = str(args.range)
    
    service_detail = s.get("https://moneyforward.com/sp/service_detail/{}".format(args.account_id_hash), params=params)
    return service_detail.json()


def get_service_detail(s, args):
    service_detail = request_service_detail(s, args)
    if args.json:
        save_json(args.json, service_detail)
        return
    
    pprint(service_detail)


def request_accounts(s, args):
    params = {}
    if args.sub_account_id_hash:
        params['sub_account_id_hash'] = args.sub_account_id_hash
    
    accounts = s.get("https://moneyforward.com/sp2/accounts/{}".format(args.id), params=params)
    return accounts.json()


def get_accounts(s, args):
    accounts = request_accounts(s, args)
    if args.json:
        save_json(args.json, accounts)
        return
    
    pprint(accounts)


def request_liabilities(s):
    return s.get("https://moneyforward.com/sp2/liabilities").json()


def get_liabilities(s, args):
    liabilities = request_liabilities(s)
    if args.json:
        save_json(args.json, liabilities)
        return
    
    pprint(liabilities)


def request_smartphone_asset(s):
    return s.get("https://moneyforward.com/smartphone_asset").json()


def get_smartphone_asset(s, args):
    smartphone_asset = request_smartphone_asset(s)
    if args.json:
        save_json(args.json, smartphone_asset)
        return
    
    pprint(smartphone_asset)


def request_cf_sum_by_sub_account(s, args):
    params = {}
    if args.sub_account_id_hash:
        params['sub_account_id_hash'] = args.sub_account_id_hash
    
    if args.year_offset:
        params['year_offset'] = str(args.year_offset)
    
    cf_sum_by_sub_account = s.get("https://moneyforward.com/sp/cf_sum_by_sub_account", params=params)
    return cf_sum_by_sub_account.json()


def get_cf_sum_by_sub_account(s, args):
    cf_sum_by_sub_account = request_cf_sum_by_sub_account(s, args)
    if args.json:
        save_json(args.json, cf_sum_by_sub_account)
        return
    
    pprint(cf_sum_by_sub_account)


def request_cf_term_data_by_sub_account(s, sub_account_id_hash, date_from=None, date_to=None):
    params = dict(sub_account_id_hash=sub_account_id_hash)
    if date_from:
        params['from'] = date_from.strftime('%Y-%m-%d')
    if date_to:
        params['to'] = date_to.strftime('%Y-%m-%d')
    
    cf_term_data_by_sub_account = s.get("https://moneyforward.com/sp/cf_term_data_by_sub_account", params=params)
    return cf_term_data_by_sub_account.json()


def copy_dict_from_attr(dst, src, name, converter=lambda x: x):
    if hasattr(src, name) and getattr(src, name):
        dst[name] = converter(getattr(src, name))


def get_term_data_list(cf_term_data_by_sub_account, s=None, large=None, middle=None):
    user_asset_acts = []
    for e in cf_term_data_by_sub_account['user_asset_acts']:
        other = [k for k in e.keys() if k != 'user_asset_act']
        if other:
            print("other", other)
        
        user_asset_act_ref = {}
        traverse(user_asset_act_ref, '', e['user_asset_act'])
        user_asset_acts.append(user_asset_act_ref)
    
    if s:
        large, middle = get_categories_form_session(s)
    
    if large and middle:
        for i in range(len(user_asset_acts)):
            user_asset_acts[i]['large_category'] = large[user_asset_acts[i]['large_category_id']]
            user_asset_acts[i]['middle_category'] = middle[user_asset_acts[i]['middle_category_id']]
    
    for i, e in enumerate(user_asset_acts):
        user_asset_acts[i]['date'] = datetime.fromisoformat(e['recognized_at']).strftime("%y/%m/%d")
        user_asset_acts[i]['year'] = datetime.fromisoformat(e['recognized_at']).strftime("CY%y")
        user_asset_acts[i]['month'] = datetime.fromisoformat(e['recognized_at']).strftime("%y'%m")
    
    return pd.DataFrame(user_asset_acts)


def get_term_data_by_sub_account(s, args):
    cf_term_data_by_sub_account = request_cf_term_data_by_sub_account(s, args.sub_account_id_hash, args.date_from, args.date_to)
    if args.json:
        save_json(args.json, cf_term_data_by_sub_account)
        return
    
    if args.csv:
        df = get_term_data_list(cf_term_data_by_sub_account, s=s)
        df.to_csv(args.csv, encoding='utf-8-sig', index=False)
        return
        
    if args.list:
        df = get_term_data_list(cf_term_data_by_sub_account, s=s)
        # df = df[args.list_header]
        
        print(*df.columns.tolist())
        for index, row in df.iterrows():
            print(*row.tolist())
        return
    
    pprint(cf_term_data_by_sub_account)


def xrange(start, stop, step):
    while start <= stop:
        yield start
        start += step

def request_term_data(s, args):
    large, middle = get_categories_form_session(s)
    df = get_account_summaries_list(request_account_summaries(s), args)
    
    sub_account_id_hash_list = df['sub_accounts.sub_account_id_hash'].unique()
    
    def gen_prams(sub_account_id_hash_list, args):
        for sub_account_id_hash in tqdm(sub_account_id_hash_list, desc='sub_account'):
            params = dict(sub_account_id_hash=sub_account_id_hash,
                          date_from=args.date_from,
                          date_to=args.date_to)
            if not args.date_from or not args.date_to:
                yield params
                continue
            
            date_from_list = list(xrange(args.date_from, args.date_to, timedelta(days=365)))
            for date_from in tqdm(date_from_list, leave=False, desc='date'):
                params['date_from'] = date_from
                params['date_to'] = min(date_from + timedelta(days=364), args.date_to)
                yield params
    
    term_data_list = []
    for params in gen_prams(sub_account_id_hash_list, args):
        cf_term_data = request_cf_term_data_by_sub_account(s, **params)
        df = get_term_data_list(cf_term_data, large=large, middle=middle)
        term_data_list.append(df)
        sleep(uniform(0.1, 1))
    
    return pd.concat(term_data_list)


def get_term_data(s, args):
    term_data_list = request_term_data(s, args)
    
    if args.csv:
        if args.csv_header:
            if args.ignore_KeyError:
                for c in set(args.csv_header) - set(term_data_list.columns):
                    term_data_list[c] = None
            term_data_list = term_data_list[args.csv_header]
        term_data_list.to_csv(args.csv, encoding='utf-8-sig', index=False)
        return
    
    if args.sqlite:
        if args.sqlite_header:
            select_header = [x.split("=", 2)[0] for x in args.sqlite_header]
            for c in set(select_header) - set(term_data_list.columns):
                term_data_list[c] = None
            term_data_list = term_data_list[select_header]
            
            rename_header = dict(x.split("=", 2) for x in args.sqlite_header if x.find('=') != -1)
            if rename_header:
                term_data_list=term_data_list.rename(columns=rename_header)
            
        with closing(sqlite3.connect(args.sqlite)) as con:
            upsert(term_data_list, 'user_asset_act', 'id', con)
        return
    
    print(*term_data_list.columns.tolist())
    for index, row in term_data_list.iterrows():
        print(*row.tolist())


def add_dummy_data_to_user_asset_act(s, args):
    with closing(sqlite3.connect(args.sqlite)) as con:
        df = pd.read_sql('SELECT * FROM user_asset_act WHERE id > 0 AND content = ?', con, params=(args.content,))
        df['service_category_id'] = args.service_category_id
        df['id'] = df['id'] * -10
        upsert(df, 'user_asset_act', 'id', con)
        df['id'] -= 1
        df['amount'] = - df['amount']
        upsert(df, 'user_asset_act', 'id', con)


def add_dummy_offset_data_to_user_asset_act(s, args):
    with closing(sqlite3.connect(args.sqlite)) as con:
        placeholder = ','.join('?' * len(args.service_category_ids))
        df = pd.read_sql('SELECT * FROM user_asset_act WHERE id > 0 AND service_category_id IN (%s)'  % placeholder, con, params=args.service_category_ids)
        df['service_category_id'] = - df['service_category_id']
        df['id'] = - df['id']
        df['amount'] = - df['amount']
        upsert(df, 'user_asset_act', 'id', con)


def get_csrf_token(s):
    res = s.get("https://moneyforward.com/cf")
    soup = BeautifulSoup(res.content, "html.parser")
    return soup.find("meta", {'name': 'csrf-token'})['content']


def request_update_user_asset_act(s, csrf_token, id_, 
        large_category_id=None, middle_category_id=None, is_target=None, memo=None,
        partner_account_id_hash=None, partner_sub_account_id_hash=None, partner_act_id=None):
    url = 'https://moneyforward.com/cf/update'
    headers = {
        'X-CSRF-Token': csrf_token,
        'X-Requested-With': 'XMLHttpRequest',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    }
    params = { 'user_asset_act[id]': id_ }
    if large_category_id:
        params['user_asset_act[large_category_id]'] = large_category_id
    if middle_category_id:
        params['user_asset_act[middle_category_id]'] = middle_category_id
    if is_target is not None:
        params['user_asset_act[is_target]'] = is_target
    if memo:
        params['user_asset_act[memo]'] = memo
    if partner_account_id_hash:
        params['user_asset_act[partner_account_id_hash]'] = partner_account_id_hash
    if partner_sub_account_id_hash:
        params['user_asset_act[partner_sub_account_id_hash]'] = partner_sub_account_id_hash
    if partner_act_id:
        params['user_asset_act[partner_act_id]'] = partner_act_id
    
    r = s.put(url, params, headers=headers)
    is_ok = r.status_code == 200
    if not is_ok:
        print(r.status_code, r.text)


def request_update_change_type(s, csrf_token, id_, change_type):
    url = 'https://moneyforward.com/cf/update'
    headers = {
        'X-CSRF-Token': csrf_token,
        'X-Requested-With': 'XMLHttpRequest',
    }
    params = { 'id': id_, 'change_type': change_type }
    r = s.put(url, params, headers=headers)
    is_ok = r.status_code == 200
    if not is_ok:
        print(r.status_code, r.text)


def get_ids():
    ids = set()
    for line in sys.stdin.readlines():
        line = line.strip()
        if not line:
            continue
        ids.add(int(line))
    return ids


def update_user_asset_act(s, args):
    csrf_token = get_csrf_token(s)
    
    ids = args.ids
    if not args.ids:
        ids = get_ids()
    
    for id_ in ids:
        request_update_user_asset_act(s, csrf_token, id_,
            args.large_category_id, args.middle_category_id,
            args.is_target, args.memo)


def update_user_asset_act(s, args):
    csrf_token = get_csrf_token(s)
    
    ids = args.ids
    if not args.ids:
        ids = get_ids()
    
    for id_ in ids:
        request_update_user_asset_act(s, csrf_token, id_,
            args.large_category_id, args.middle_category_id,
            args.is_target, args.memo,
            args.partner_account_id_hash,
            args.partner_sub_account_id_hash,
            args.partner_act_id)


def update_enable_transfer(s, args):
    csrf_token = get_csrf_token(s)
    
    ids = args.ids
    if not args.ids:
        ids = get_ids()
    
    for id_ in ids:
        request_update_change_type(s, csrf_token, id_, 'enable_transfer')


def update_disable_transfer(s, args):
    csrf_token = get_csrf_token(s)
    
    ids = args.ids
    if not args.ids:
        ids = get_ids()
    
    for id_ in ids:
        request_update_change_type(s, csrf_token, id_, 'disable_transfer')

def search_category_sub(s, cache_csv, force_update, large=None, middle=None):
    if not os.path.exists(cache_csv) or force_update:
        large_categories = request_large_categories(s)
        save_large_categories_csv(cache_csv, large_categories)
    
    df = pd.read_csv(cache_csv)
    if large:
        df = df[df['large_category_name'].str.contains(large, na=False)]
    if middle:
        df = df[df['middle_category_name'].str.contains(middle, na=False)]
    
    return df

def search_category(s, args):
    df = search_category_sub(s, args.cache_csv, args.force_update, args.large, args.middle)
    print(*df.columns.tolist())
    for index, row in df.iterrows():
        print(*row.tolist())

def get_categories_form_user_asset_acts(user_asset_acts):
    large = { int(k):v for k, v in user_asset_acts['large'].items()}
    large[0] = '-'
    middle = { int(k):v for k, v in user_asset_acts['middle'].items()}
    middle[0] = '-'
    return large, middle


def get_categories_form_session(s):
    user_asset_acts = request_user_asset_acts(s, params=dict(size=1))
    return get_categories_form_user_asset_acts(user_asset_acts)


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


def update_filter_flags(df, base_flags, column_name, match_values, not_match_values, is_null=False, is_not_null=False):
    if is_null:
        flags = df[column_name].isnull()
    elif is_not_null:
        flags = df[column_name].notnull()
    elif match_values:
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


def filter_db(s, args):
    category_id = None
    if args.update_category_name:
        category_df = search_category_sub(s, 
            args.cache_category_csv,
            args.force_category_update,
            middle=args.update_category_name)
        if len(category_df) == 0:
            raise ValueError(f"Not Found Category Name: {args.update_category_name}")
        if len(category_df) > 1:
            print(*category_df.columns.tolist())
            for index, row in category_df.iterrows():
                print(*row.tolist())
            raise ValueError(f"Not Unique Category Name: {args.update_category_name}")
        category_id = (int(category_df.iloc[0].large_category_id), int(category_df.iloc[0].middle_category_id), )
    
    column_name_for_service_name = 'account.service.service_name'
    column_name_for_sub_type = 'sub_account.sub_type'
    
    if args.csv:
        df = pd.read_csv(args.csv)
    elif args.sqlite:
        column_name_for_service_name = 'service_name'
        column_name_for_sub_type = 'sub_type'
        with closing(sqlite3.connect(args.sqlite)) as con:
            df = pd.read_sql(f'SELECT * FROM {args.sqlite_table}', con)
    else:
        raise ValueError("invalid args")
    
    if args.query:
        result = df.query(args.query, engine='python')
    elif args.pattern:
        flags = df['content'].str.contains(args.pattern, na=False) ^ args.reverse
        
        flags = update_filter_flags(df, flags, 'middle_category', args.match_middle_categories, args.not_match_middle_categories)
        flags = update_filter_flags(df, flags, 'large_category', args.match_large_categories, args.not_match_large_categories)
        flags = update_filter_flags(df, flags, column_name_for_service_name, args.match_service_name, args.not_match_service_name)
        flags = update_filter_flags(df, flags, column_name_for_sub_type, args.match_sub_account, args.not_match_sub_account)
        
        flags = update_filter_flags(df, flags, 'memo', args.match_memo, args.not_match_memo, args.null_memo, args.not_null_memo)
        
        result = df.loc[flags]
    else:
        raise ValueError("invalid args")
    
    if args.columns:
        result = result[args.columns]
    
    if args.list:
        print(*result.columns.tolist())
        for index, row in result.iterrows():
            print(*row.tolist())
    elif args.output_csv:
        result.to_csv(args.output_csv, encoding='utf_8_sig', index=False)
    elif args.update_category:
        large_category_id, middle_category_id = args.update_category
        request_transactions_category_bulk_updates(s, large_category_id, middle_category_id, result['id'].tolist(), args.sqlite, args.sqlite_table)
    elif category_id:
        large_category_id, middle_category_id = category_id[0], category_id[1]
        request_transactions_category_bulk_updates(s, large_category_id, middle_category_id, result['id'].tolist(), args.sqlite, args.sqlite_table)
    else:
        print(result)


def request_transactions_category_bulk_updates(s, large_category_id, middle_category_id, ids, sqlite=None, sqlite_table=None):
    if not ids:
        print("ids is empty")
        return 
    
    url = 'https://moneyforward.com/sp2/transactions_category_bulk_updates'
    n = 100
    for i in range(0, len(ids), n):
        params = dict(
          middle_category_id=middle_category_id,
          large_category_id=large_category_id,
          ids=ids[i:i + n]
        )
        r = s.put(url, json.dumps(params), headers={'Content-Type': 'application/json'})
        if r.status_code != requests.codes.ok:
            print(r.status_code, r.text)
    
    if not (sqlite and sqlite_table):
        return
    large, middle = get_categories_form_session(s)
    if not (large and middle):
        return
    """
    param = dict(
        middle_category_id=middle_category_id,
        large_category_id=large_category_id,
        middle_category=middle[middle_category_id],
        large_category=large[large_category_id],
        ids=",".join(str(x) for x in ids),
    )
    """
    param = [large_category_id, large[large_category_id],
             middle_category_id, middle[middle_category_id],
             *ids]
    #print(param)
    with closing(sqlite3.connect(sqlite)) as con:
        cur = con.cursor()
        try:
            """
            cur.execute(f"UPDATE {sqlite_table} SET "
                        + "large_category_id = :large_category_id, "
                        + "large_category = :large_category, "
                        + "middle_category_id = :middle_category_id, "
                        + "middle_category = :middle_category "
                        + "WHERE id IN (:ids)", param)
            """
            #con.set_trace_callback(print)
            cur.execute(f"UPDATE {sqlite_table} SET "
                        + "large_category_id = ?, large_category = ?, "
                        + "middle_category_id = ?, middle_category = ? "
                        + f"WHERE id IN ({','.join('?' * len(ids))})", param)
            
        except sqlite3.Error as e:
            print("error", e.args[0])
            print(e)
        con.commit()
    """
    with closing(sqlite3.connect(sqlite)) as con:
        cur = con.cursor()
        for a in cur.execute(f"SELECT id, middle_category_id, middle_category FROM {sqlite_table} WHERE id IN({','.join('?' * len(ids))})", ids):
            print(a)
    """

def transactions_category_bulk_updates(s, args):
    ids = args.ids
    if ids is None:
        data = sys.stdin.readlines()
        ids = [int(x) for line in data for x in line.strip().split() if x.isdecimal()]
        if not ids:
            raise ValueError('ids not specified')
    request_transactions_category_bulk_updates(s, args.large_category_id, args.middle_category_id, ids)


def bulk_update_category(s, args):
    stream = sys.stdin
    if args.input_file:
        stream = open(args.input_file)
    
    data = defaultdict(lambda: defaultdict(set))
    for line in stream.readlines():
        line = line.strip()
        if not line:
            continue
        row = line.split(args.delimiter)
        l = int(row[args.column_large_category_id])
        m = int(row[args.column_middle_category_id])
        i = int(row[args.column_id])
        data[l][m].add(i)
    
    for l, md in data.items():
        for m, ids in md.items():
            print('large_category_id', l, 'middle_category_id', m)
            print(ids)
            print()
            
    try:
        if not input("\nReally quit? (y/n)> ").lower().startswith('y'):
            sys.exit(1)
        print("execute")
        
        for l, md in data.items():
            for m, ids in md.items():
                print('large_category_id', l, 'middle_category_id', m)
                request_transactions_category_bulk_updates(s, l, m, list(ids))

    except KeyboardInterrupt:
        print("Ok ok, quitting")
        sys.exit(1)
    

setattr(argparse._ActionsContainer, '__enter__', lambda self: self)
setattr(argparse._ActionsContainer, '__exit__', lambda self, exc_type, exc_value, traceback: None)

def add_standard_output_group(subparser, csv=True, lst=False):
    group = subparser.add_mutually_exclusive_group()
    group.add_argument('--json')
    if csv:
        group.add_argument('--csv')
    if lst:
        group.add_argument('--list', action='store_true')
    return group

def add_parser(subparsers, name, func):
    subparser = subparsers.add_parser(name)
    subparser.set_defaults(func=func)
    return subparser

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--mf_cookies', default='mf_cookies.pkl')
parser.add_argument('-d', '--debug', action='store_true')

subparsers = parser.add_subparsers(dest='cmd', required=True)


with add_parser(subparsers, 'category', func=get_category) as subparser:
    add_standard_output_group(subparser, csv=False)


with add_parser(subparsers, 'large_categories', func=get_large_categories) as subparser:
    group = add_standard_output_group(subparser)
    group.add_argument('--sqlite')


with add_parser(subparsers, 'account_summaries', func=get_account_summaries) as subparser:
    add_standard_output_group(subparser, lst=True)
    list_header = 'service_category_id account_id_hash show_path sub_accounts.sub_account_id_hash name sub_accounts.sub_type sub_accounts.sub_name sub_accounts.user_asset_det_summaries.asset_subclass_name sub_accounts.user_asset_det_summaries.asset_subclass_unit'.split()
    subparser.add_argument('--list_header', type=str, nargs='+', default=list_header)
    subparser.add_argument('-c', '--service_category_id', type=int)
    subparser.add_argument('-n', '--name')

with add_parser(subparsers, 'liabilities', func=get_liabilities) as subparser:
    add_standard_output_group(subparser)

with add_parser(subparsers, 'smartphone_asset', func=get_smartphone_asset) as subparser:
    add_standard_output_group(subparser)

with add_parser(subparsers, 'service_detail', func=get_service_detail) as subparser:
    subparser.add_argument('account_id_hash')
    subparser.add_argument('-s', '--sub_account_id_hash')
    subparser.add_argument('-r', '--range', type=int)
    add_standard_output_group(subparser)


with add_parser(subparsers, 'accounts', func=get_accounts) as subparser:
    subparser.add_argument('id')
    subparser.add_argument('-s', '--sub_account_id_hash')
    add_standard_output_group(subparser)


with add_parser(subparsers, 'cf_sum_by_sub_account', func=get_cf_sum_by_sub_account) as subparser:
    subparser.add_argument('sub_account_id_hash')
    subparser.add_argument('-y', '--year_offset', type=int)
    add_standard_output_group(subparser)


with add_parser(subparsers, 'cf_term_data_by_sub_account', func=get_term_data_by_sub_account) as subparser:
    subparser.add_argument('sub_account_id_hash')
    subparser.add_argument('-f', '--date_from', type=dateutil.parser.parse)
    subparser.add_argument('-t', '--date_to', type=dateutil.parser.parse)
    add_standard_output_group(subparser, lst=True)


with add_parser(subparsers, 'cf_term_data', func=get_term_data) as subparser:
    subparser.add_argument('-f', '--date_from', type=dateutil.parser.parse)
    subparser.add_argument('-t', '--date_to', type=dateutil.parser.parse)
    subparser.add_argument('-c', '--service_category_id', type=int)
    subparser.add_argument('-n', '--name')
    with subparser.add_mutually_exclusive_group() as group:
        group.add_argument('--csv')
        group.add_argument('--sqlite')
    subparser.add_argument('--csv_header', nargs='+')
    sqlite_header = """id date year month account_id sub_account_id is_transfer is_income
                       orig_content=content orig_amount=amount currency jpyrate memo 
                       large_category_id middle_category_id large_category middle_category
                       is_target partner_account_id partner_sub_account_id partner_act_id
                       created_at recognized_at updated_at sub_account_id_hash transfer_type
                       account.account.service_id=service_id
                       account.account.service_category_id=service_category_id
                       account.account.disp_name=disp_name
                       account.account.service.service.service_name=service_name
                       sub_account.sub_account.sub_name=sub_name
                       sub_account.sub_account.sub_type=sub_type
                       sub_account.sub_account.sub_number=sub_number
                       partner_account.partner_account.service_id=partner_account_service_id
                       partner_account.partner_account.service_category_id=partner_account_service_category_id
                       partner_account.partner_account.disp_name=partner_account_disp_name
                       partner_account.partner_account.memo=partner_account_memo
                       partner_account.partner_account.display_name=partner_account_display_name
                       partner_sub_account.partner_sub_account.sub_name=partner_account_sub_name
                       partner_sub_account.partner_sub_account.sub_type=partner_account_sub_type
                       partner_sub_account.partner_sub_account.sub_number=partner_account_sub_number
                       partner_sub_account.partner_sub_account.service_category_id=partner_sub_account_service_category_id
                       partner_sub_account.partner_sub_account.is_dummy=partner_sub_account_is_dummy
                       partner_act.partner_act.orig_content=partner_act_content
                       partner_act.partner_act.orig_amount=partner_act_amount
                       partner_act.partner_act.currency=partner_act_currency
                       partner_act.partner_act.jpyrate=partner_act_jpyrate
                       partner_act.partner_act.memo=partner_act_memo
                       partner_act.partner_act.large_category_id=partner_act_large_category_id
                       partner_act.partner_act.middle_category_id=partner_act_middle_category_id
                       partner_act.partner_act.sub_account_id_hash=partner_act_sub_account_id_hash
                       partner_act.partner_act.partner_sub_account_id_hash=partner_act_partner_sub_account_id_hash
                       """.split()
    subparser.add_argument('--sqlite_header', nargs='+', default=sqlite_header)
    subparser.add_argument('-i', '--ignore_KeyError', action='store_true')


with subparsers.add_parser('add_dummy_data_to_user_asset_act') as subparser:
    subparser.set_defaults(func=add_dummy_data_to_user_asset_act)
    subparser.add_argument('sqlite')
    subparser.add_argument('content', type=str)
    subparser.add_argument('service_category_id', type=int)


with subparsers.add_parser('add_dummy_offset_data_to_user_asset_act') as subparser:
    subparser.set_defaults(func=add_dummy_offset_data_to_user_asset_act)
    subparser.add_argument('sqlite')
    subparser.add_argument('service_category_ids', type=int, nargs='+')


with subparsers.add_parser('update_user_asset_act') as subparser:
    subparser.set_defaults(func=update_user_asset_act)
    subparser.add_argument('ids', type=int, nargs='*')
    subparser.add_argument('-l', '--large_category_id', type=int)
    subparser.add_argument('-m', '--middle_category_id', type=int)
    subparser.add_argument('-t', '--is_target', choices={0, 1})
    subparser.add_argument('-M', '--memo')
    subparser.add_argument('--partner_account_id_hash')
    subparser.add_argument('--partner_sub_account_id_hash')
    subparser.add_argument('--partner_act_id')


with add_parser(subparsers, 'update_enable_transfer', func=update_enable_transfer) as subparser:
    subparser.add_argument('ids', type=int, nargs='*')


with add_parser(subparsers, 'update_disable_transfer', func=update_disable_transfer) as subparser:
    subparser.add_argument('ids', type=int, nargs='*')


with subparsers.add_parser('search_category') as subparser:
    subparser.add_argument('--cache_csv', default='cache_search_categories.csv')
    subparser.add_argument('--force_update', action='store_true')
    subparser.add_argument('-l', '--large')
    subparser.add_argument('-m', '--middle')
    subparser.set_defaults(func=search_category)


with subparsers.add_parser('user_asset_acts') as subparser:
    group = subparser.add_mutually_exclusive_group()
    group.add_argument('--csv')
    group.add_argument('--json')
    group.add_argument('--list', action='store_true')
    
    subparser.add_argument('--offset', type=int)
    subparser.add_argument('--size', type=int)
    subparser.add_argument('--is_new', type=int, choices=[0, 1]) # only new
    subparser.add_argument('--is_old', type=int, choices=[0, 1]) # only old
    subparser.add_argument('--is_continuous', type=int, choices=[0, 1]) # 1: from -> "1990-01-01"
    subparser.add_argument('--select_category', type=int) # select large category id
    subparser.add_argument('--base_date', type=str) # 集計日基準？
    subparser.add_argument('--keyword', type=str)
    
    user_asset_acts_list_header = 'id is_transfer is_income is_target updated_at content amount large_category_id large_category middle_category_id middle_category account.service.service_name sub_account.sub_type sub_account.sub_name'.split()
    subparser.add_argument('--list_header', type=str, nargs='+', default=user_asset_acts_list_header)
    subparser.set_defaults(func=get_user_asset_acts)


with subparsers.add_parser('filter_db') as subparser:
    subparser.set_defaults(func=filter_db)
    with subparser.add_mutually_exclusive_group(required=True) as group:
        group.add_argument('--csv')
        group.add_argument('--sqlite', metavar='cf_term_data.db')
    subparser.add_argument('--sqlite_table', default='user_asset_act')
    subparser.add_argument('--columns', type=str, nargs='+')

    with subparser.add_mutually_exclusive_group() as group:
        group.add_argument('--list', action='store_true')
        group.add_argument('--output_csv')
        group.add_argument('-u', '--update_category_name')
        group.add_argument('-U', '--update_category', type=int, nargs=2, metavar=('large_category_id', 'middle_category_id'))

    with subparser.add_mutually_exclusive_group(required=True) as group:
        group.add_argument('-q', '--query', help='ex) content.notnull() and content.str.match(\'セブン\') and middle_category != \'コンビニ\'')
        group.add_argument('-p', '--pattern', help='ex) ".*" / "^タイムズ" ')

    with subparser.add_argument_group('group_filter_pattern') as group_filter_pattern:
        group_filter_pattern.add_argument('-r', '--reverse', action='store_true')

        with group_filter_pattern.add_mutually_exclusive_group() as group:
            group.add_argument('--null_memo', action='store_true')
            group.add_argument('--not_null_memo', action='store_true')
            group.add_argument('--match_memo', nargs='+', metavar='memo')
            group.add_argument('--not_match_memo', nargs='+', metavar='memo')

        with group_filter_pattern.add_mutually_exclusive_group() as group:
            group.add_argument('-m', '--match_middle_categories', nargs='+', metavar='category')
            group.add_argument('-M', '--not_match_middle_categories', nargs='+', metavar='category')

        with group_filter_pattern.add_mutually_exclusive_group() as group:
            group.add_argument('-l', '--match_large_categories', nargs='+', metavar='category')
            group.add_argument('-L', '--not_match_large_categories', nargs='+', metavar='category')

        with group_filter_pattern.add_mutually_exclusive_group() as group:
            group.add_argument('-s', '--match_service_name', nargs='+', metavar='service_name')
            group.add_argument('-S', '--not_match_service_name', nargs='+', metavar='service_name')

        with group_filter_pattern.add_mutually_exclusive_group() as group:
            group.add_argument('-t', '--match_sub_account', nargs='+', metavar='sub_account')
            group.add_argument('-T', '--not_match_sub_account', nargs='+', metavar='sub_account')
    
    subparser.add_argument('--cache_category_csv', default='cache_search_categories.csv')
    subparser.add_argument('--force_category_update', action='store_true')


with subparsers.add_parser('transactions_category_bulk_updates') as subparser:
    subparser.set_defaults(func=transactions_category_bulk_updates)
    subparser.add_argument('-m', '--middle_category_id', type=int, required=True)
    subparser.add_argument('-l', '--large_category_id', type=int, required=True)
    subparser.add_argument('-i', '--ids', type=int, nargs='+')


with add_parser(subparsers, 'bulk_update_category', func=bulk_update_category) as subparser:
    subparser.add_argument('-f', '--input_file')
    subparser.add_argument('-d', '--delimiter')
    subparser.add_argument('-m', '--column_middle_category_id', type=int, required=True)
    subparser.add_argument('-l', '--column_large_category_id', type=int, required=True)
    subparser.add_argument('-i', '--column_id', type=int, required=True)


args = parser.parse_args()

if args.debug:
    import http.client
    http.client.HTTPConnection.debuglevel = 2

with requests.session() as s:
    with open(args.mf_cookies, 'rb') as f:
        s.cookies = pickle.load(f)
    
    args.func(s, args)


