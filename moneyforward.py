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
from pprint import pprint, pformat
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
from contextlib import contextmanager
import numpy as np

logger = logging.getLogger(__name__)

formatter = '%(levelname)s : %(asctime)s : %(message)s'
logging.basicConfig(level=logging.INFO, format=formatter)

def save_json(fn, obj):
    with open(fn, 'w') as f:
        json.dump(obj, f)

# move HTTP request helpers to separate module
from moneyforward_api import *



def update_params(name, params, args, default=None):
    value = getattr(args, name)
    if value:
        params[name] = value
    elif default:
        params[name] = default
    return params


# CLI wrapper functions for moneyforward_api functions
def request_service_detail_from_args(s, args):
    """argsからサービス詳細を取得（CLIラッパー）"""
    return request_service_detail(
        s,
        account_id_hash=args.account_id_hash,
        sub_account_id_hash=args.sub_account_id_hash,
        range_value=getattr(args, 'range', None)
    )


def request_accounts_from_args(s, args):
    """argsからアカウント情報を取得（CLIラッパー）"""
    return request_accounts(
        s,
        account_id=args.id,
        sub_account_id_hash=args.sub_account_id_hash
    )


def request_cf_sum_by_sub_account_from_args(s, args):
    """argsからキャッシュフロー集計を取得（CLIラッパー）"""
    return request_cf_sum_by_sub_account(
        s,
        sub_account_id_hash=args.sub_account_id_hash,
        year_offset=args.year_offset
    )


def get_category(s, args):
    category = request_category(s)
    if args.json:
        save_json(args.json, category)
        return
    pprint(category)


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
    
    if getattr(args, 'sub_type', None):
        df = df[df['sub_accounts.sub_type'].str.contains(args.sub_type)]
    
    return df


def get_account_summaries(s, args):
    account_summaries = request_account_summaries(s, args.default_group)
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
        if args.unique_list:
            df = df.drop_duplicates()
        
        print(*df.columns.tolist())
        for index, row in df.iterrows():
            print(*row.tolist())
        return
    
    pprint(account_summaries)


def get_service_detail(s, args):
    service_detail = request_service_detail_from_args(s, args)
    if args.json:
        save_json(args.json, service_detail)
        return
    
    pprint(service_detail)


def get_accounts(s, args):
    accounts = request_accounts_from_args(s, args)
    if args.json:
        save_json(args.json, accounts)
        return
    
    pprint(accounts)


def get_liabilities(s, args):
    liabilities = request_liabilities(s)
    if args.json:
        save_json(args.json, liabilities)
        return
    
    pprint(liabilities)


def get_smartphone_asset(s, args):
    smartphone_asset = request_smartphone_asset(s)
    if args.json:
        save_json(args.json, smartphone_asset)
        return
    
    pprint(smartphone_asset)


def get_cf_sum_by_sub_account(s, args):
    cf_sum_by_sub_account = request_cf_sum_by_sub_account_from_args(s, args)
    if args.json:
        save_json(args.json, cf_sum_by_sub_account)
        return
    
    pprint(cf_sum_by_sub_account)



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
    
    if args.csv or args.list:
        df = get_term_data_list(cf_term_data_by_sub_account, s=s)
        if args.columns:
            df = df[args.columns]
        
        
        if args.csv:
            df.to_csv(args.csv, encoding='utf-8-sig', index=False)
            return
        
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
    with change_default_group(s):
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


def read_ids_from_stdin():
    ids = set()
    for line in sys.stdin.readlines():
        line = line.strip()
        if not line:
            continue
        ids.add(int(line))
    return ids


def get_ids(args):
    if args.ids:
        return args.ids
    print("Please Input IDs.") 
    return read_ids_from_stdin()


def request_bulk_update_user_asset_act(s, ids, 
        large_category_id=None, middle_category_id=None, is_target=None, memo=None,
        partner_account_id_hash=None, partner_sub_account_id_hash=None, partner_act_id=None,
        sqlite=None, sqlite_table=None):
    
    csrf_token = get_csrf_token(s)
    
    for id_ in ids:
        request_update_user_asset_act(s, csrf_token, id_,
            large_category_id=large_category_id,
            middle_category_id=middle_category_id,
            is_target=is_target, memo=memo, 
            partner_account_id_hash=partner_account_id_hash, 
            partner_sub_account_id_hash=partner_sub_account_id_hash, 
            partner_act_id=partner_act_id,
        )
    
    if sqlite and sqlite_table:
        request_update_sqlite_db(s, ids, sqlite, sqlite_table)


def update_user_asset_act(s, args):
    if args.category_name and (args.large_category_id or args.middle_category_id):
        print("Error: Can't use -c with -l or -m.")
        sys.exit(-1)
    
    if args.category_name:
        large_category_id, middle_category_id = category_id = get_middle_category(s, args, args.category_name)
        print(f'{large_category_id=}, {middle_category_id=}')
    else:
        large_category_id, middle_category_id = args.large_category_id, args.middle_category_id
        
    ids = get_ids(args)
    
    request_bulk_update_user_asset_act(s, ids,
        large_category_id=large_category_id,
        middle_category_id=middle_category_id,
        is_target=args.is_target, memo=args.memo, 
        partner_account_id_hash=args.partner_account_id_hash, 
        partner_sub_account_id_hash=args.partner_sub_account_id_hash, 
        partner_act_id=args.partner_act_id,
        sqlite=args.sqlite, sqlite_table=args.sqlite_table,
    )


def update_change_transfer_type(s, args, is_transfer, ids=None):
    csrf_token = get_csrf_token(s)
    ids = ids or get_ids(args)
    change_type = 'enable_transfer' if is_transfer else 'disable_transfer'
    
    for id_ in ids:
        request_update_change_type(s, csrf_token, id_, change_type)

def update_enable_transfer(s, args):
    update_change_transfer_type(s, args, True)


def update_disable_transfer(s, args):
    update_change_transfer_type(s, args, False)



def change_transfer(s, args):
    request_change_transfer(s, str(args.id), 
        partner_account_id_hash=args.partner_account_id_hash or "0",
        partner_sub_account_id_hash=args.partner_sub_account_id_hash or "0",
        partner_act_id=args.partner_act_id)


def clear_transfer(s, args):
    request_clear_transfer(s, str(args.id))


def search_category_sub(s, cache_csv, force_update, large=None, middle=None, is_income=None):
    if not os.path.exists(cache_csv) or force_update:
        large_categories = request_large_categories(s)
        save_large_categories_csv(cache_csv, large_categories)
    
    df = pd.read_csv(cache_csv)
    if large:
        df = df[df['large_category_name'].str.contains(large, na=False)]
    if middle:
        df = df[df['middle_category_name'].str.contains(middle, na=False)]
    if is_income is not None:
        if is_income:
            df = df[df['large_category_id'] == 1]
        else:
            df = df[df['large_category_id'] != 1]
    
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


def convert_user_asset_act_to_dict(user_asset_act, large, middle):
    if not 'user_asset_act' in user_asset_act:
        pprint(user_asset_act)
        raise ValueError('Not Found user_asset_act')
    
    user_asset_act_dict = {}
    traverse(user_asset_act_dict, '', user_asset_act['user_asset_act'])
    
    user_asset_act_dict['large_category'] = large[user_asset_act_dict['large_category_id']]
    user_asset_act_dict['middle_category'] = middle[user_asset_act_dict['middle_category_id']]
    
    recognized_at = user_asset_act_dict['recognized_at']
    user_asset_act_dict['date'] = datetime.fromisoformat(recognized_at).strftime("%y/%m/%d")
    user_asset_act_dict['year'] = datetime.fromisoformat(recognized_at).strftime("CY%y")
    user_asset_act_dict['month'] = datetime.fromisoformat(recognized_at).strftime("%y'%m")
    
    return user_asset_act_dict


def request_user_asset_acts_by_ids(s, ids):
    large, middle = get_categories_form_session(s)
    user_asset_acts = []
    for id in tqdm(ids):
        user_asset_act = request_user_asset_act_by_id(s, id)
        user_asset_act_dict = convert_user_asset_act_to_dict(user_asset_act, large, middle)
        #pprint(user_asset_act_dict)
        df = pd.DataFrame([user_asset_act_dict])
        user_asset_acts.append(df)
        sleep(uniform(0.1, 1))
    return pd.concat(user_asset_acts)


def get_user_asset_act_by_id(s, args):
    user_asset_act = request_user_asset_act_by_id(s, args.id)
    pprint(user_asset_act)


def get_user_asset_acts_by_ids(s, args):
    user_asset_acts = request_user_asset_acts_by_ids(s, args.ids)
    if args.columns:
        user_asset_acts = user_asset_acts[args.columns]
    print(*user_asset_acts.columns.tolist(), sep=args.sep)
    for index, row in user_asset_acts.iterrows():
        print(*row.tolist(), sep=args.sep)


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


def get_sub_account_groups(s, args):
    sub_account_groups = request_sub_account_groups(s)
    if args.json:
        save_json(args.json, sub_account_groups)
        return
    
    pprint(sub_account_groups)


def change_group(s, args):
    sub_account_groups = request_sub_account_groups(s)
    current_group_id_hash = sub_account_groups['current_group_id_hash']
    print(f"{current_group_id_hash=}")
    
    group_id_hash = "0"
    
    if args.group_name:
        group_name = args.group_name
        group_lists = sub_account_groups['sub_account_groups']['sub_account_group']['group_lists']
        groups = [gl['group_list'] for gl in group_lists]
        df = pd.DataFrame(groups)
        df = df[df.group_name.str.contains(group_name)]
        
        if len(df) == 0:
            raise ValueError(f"Not Found Category Name: {group_name}")
        if len(df) > 1:
            df = df['group_id_hash group_name'.split()]
            print(*df.columns.tolist())
            for index, row in df.iterrows():
                print(*row.tolist())
            raise ValueError(f"Not Unique Category Name: {group_name}")
        
        g = df.iloc[0]
        group_id_hash = g.group_id_hash
        group_name    = g.group_name
        
        print(f"{group_id_hash=}, {group_name=}")
    
    if args.group_id_hash:
        group_id_hash = args.group_id_hash
    
    request_change_group(s, group_id_hash)


def convert_manual_user_asset_act_partner_source_list(manual_user_asset_act_partner_sources, 
      partner_candidate_acts=None,
      service_category_id=None,
      content=None, amount=None,
      updated_at=None,
      **kwargs):
    
    data = [x['sub_account'] for x in manual_user_asset_act_partner_sources['manual_user_asset_act_partner_sources']]
    if partner_candidate_acts:
        data = [ { **d,  **x['partner_candidate_act']} for d in data if d['partner_candidate_acts'] for x in d['partner_candidate_acts']]
        for i in range(len(data)):
          del data[i]['partner_candidate_acts']
    
    df = pd.DataFrame(data)
    
    columns = 'sub_name sub_type sub_number account_id_hash sub_account_id_hash account_disp_name account_service_name'.split()
    for c in columns:
        if c in kwargs and kwargs[c] is not None:
            #print(f"{c=}, {kwargs[c]=}, {df[c]=}")
            df = df[df[c].str.contains(kwargs[c], na=True)]
    if service_category_id:
        df = df[df.service_category_id == service_category_id]
    if 'content' in df.columns and content:
        df = df[df.content.str.contains(content, na=True)]
    if 'amount' in df.columns and amount:
        df = df[np.isclose(df.amount, amount)]
    if 'updated_at' in df.columns and updated_at:
        df = df[pd.to_datetime(df.updated_at).dt.date == updated_at.date()]
    
    return df

def get_manual_user_asset_act_partner_sources(s, args):
    manual_user_asset_act_partner_sources = request_manual_user_asset_act_partner_sources(s, args.act_id)
    if args.json:
        save_json(args.json, manual_user_asset_act_partner_sources)
        return
    
    if not args.list:
        pprint(manual_user_asset_act_partner_sources)
        return
    
    df = convert_manual_user_asset_act_partner_source_list(manual_user_asset_act_partner_sources, **vars(args))
    print(*df.columns.tolist())
    for index, row in df.iterrows():
        print(*row.tolist())
    return 
    



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

def get_middle_category(s, args, category_name, is_income=None):
    category_df = search_category_sub(s, 
        args.cache_category_csv,
        args.force_category_update,
        middle=category_name,
        is_income=is_income)
    if len(category_df) == 0:
        raise ValueError(f"Not Found Category Name: {category_name}")
    if len(category_df) > 1:
        print(*category_df.columns.tolist())
        for index, row in category_df.iterrows():
            print(*row.tolist())
        raise ValueError(f"Not Unique Category Name: {category_name}")
    category_id = (int(category_df.iloc[0].large_category_id), int(category_df.iloc[0].middle_category_id), )
    return category_id


def filter_db(s, args):
    category_id = None
    if args.update_category_name:
        category_id = get_middle_category(s, args, args.update_category_name, is_income=args.is_income)
    elif args.update_category:
        category_id = args.update_category
    
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
        df = df.query(args.query, engine='python')
    
    if args.patterns is not None:
        flags = df.index == np.nan
        for pat in args.patterns:
            flags |= df['content'].str.contains(pat, na=False)
    else:
        flags = df.index != np.nan
    
    if args.exclude_patterns is not None:
        for ep in args.exclude_patterns:
            flags &= ~df['content'].str.contains(ep, na=False)
    
    flags = update_filter_flags(df, flags, 'middle_category', args.match_middle_categories, args.not_match_middle_categories)
    flags = update_filter_flags(df, flags, 'large_category', args.match_large_categories, args.not_match_large_categories)
    flags = update_filter_flags(df, flags, column_name_for_service_name, args.match_service_name, args.not_match_service_name)
    flags = update_filter_flags(df, flags, column_name_for_sub_type, args.match_sub_account, args.not_match_sub_account)
    
    flags = update_filter_flags(df, flags, 'memo', args.match_memo, args.not_match_memo, args.null_memo, args.not_null_memo)
    
    if args.date_from or args.date_to:
        print(f"date: {args.date_from and args.date_from.strftime('%y/%m/%d')} - {args.date_to and args.date_to.strftime('%y/%m/%d')}")
        dt = pd.to_datetime(df['date'], format='%y/%m/%d')
    if args.date_from:
        flags &= dt >= args.date_from
    if args.date_to:
        flags &= dt <= args.date_to
    
    if args.ignore_invalid_data:
        flags &= df['id'] > 0
    
    if args.is_income is not None:
        flags &= df['is_income'] == args.is_income
    if args.is_transfer is not None:
        flags &= df['is_transfer'] == args.is_transfer
    if args.lt is not None:
        flags &= df['amount'] < args.lt
    if args.le is not None:
        flags &= df['amount'] <= args.le
    if args.gt is not None:
        flags &= df['amount'] > args.gt
    if args.ge is not None:
        flags &= df['amount'] >= args.ge
    
    result = df.loc[flags ^ args.reverse]
    
    if args.columns:
        result = result[args.columns]
    
    if args.sort:
        result = result.sort_values(args.sort)
    
    if args.list:
        print(*result.columns.tolist())
        for index, row in result.iterrows():
            print(*row.tolist())
    elif args.output_csv:
        result.to_csv(args.output_csv, encoding='utf_8_sig', index=False)
    elif category_id:
        large_category_id, middle_category_id = category_id[0], category_id[1]
        request_transactions_category_bulk_updates_with_update_db(s, large_category_id, middle_category_id, result['id'].tolist(), args.sqlite, args.sqlite_table)
    elif args.update_sqlite_db:
        update_sqlite_db(s, args, ids=result['id'].tolist())
    elif args.list_id:
        print()
        print(" ".join(str(x) for x in result['id'].tolist()))
        print()
    elif args.update_transfer is not None:
        ids = result['id'].tolist()
        update_change_transfer_type(s, args, args.update_transfer, ids=ids)
        if args.sqlite:
            update_sqlite_db(s, args, ids=ids)
    elif args.update_partner_account is not None:
        ids = result['id'].tolist()
        partner_account_id_hash, partner_sub_account_id_hash = args.update_partner_account
        update_change_transfer_type(s, args, True, ids=ids)
        request_bulk_update_user_asset_act(s, ids=ids, 
            partner_account_id_hash=partner_account_id_hash,
            partner_sub_account_id_hash=partner_sub_account_id_hash,
            sqlite=args.sqlite, sqlite_table=args.sqlite_table,
        )
    else:
        print(result)


def update_sqlite_db(s, args, ids=None):
    ids = ids or get_ids(args)
    pretty = False
    if hasattr(args, 'pretty'):
        pretty = args.pretty

    if args.sqlite and args.sqlite_table:
        request_update_sqlite_db(s, ids, args.sqlite, args.sqlite_table, pretty=pretty)
    else:
        raise ValueError("invalid args #{args.sqlite=}, {args.sqlite_table=}")


def request_update_sqlite_db(s, ids, sqlite, sqlite_table, pretty=False):
    large, middle = get_categories_form_session(s)
    if not (large and middle):
        raise ValueError("failed: get_categories_form_session")
    
    with closing(sqlite3.connect(sqlite)) as con:
        cur = con.cursor()
        con.set_trace_callback(tqdm.write)
        
        for id in tqdm(ids):
            user_asset_act = request_user_asset_act_by_id(s, id)
            #tqdm.write(f"{user_asset_act=}")
            user_asset_act_dict = convert_user_asset_act_to_dict(user_asset_act, large, middle)
            if pretty:
                tqdm.write(pformat(user_asset_act))
            param = dict(id=id)
            names = '''middle_category_id middle_category large_category_id large_category memo
                       is_transfer
                       partner_account.disp_name=partner_account_disp_name
                       partner_account.display_name=partner_account_display_name
                       partner_account.memo=partner_account_memo
                       partner_sub_account.sub_name=partner_account_sub_name
                       partner_sub_account.sub_type=partner_account_sub_type
                       partner_sub_account.sub_number=partner_account_sub_number
                       transfer_type is_target partner_account_id partner_sub_account_id partner_act_id'''.split()
            for n in names:
                if n.find('=') == -1:
                    k = n
                else:
                    k, n = n.split("=", 2)
                if k in user_asset_act_dict:
                    param[n] = user_asset_act_dict[k]

            try:
                cur.execute(f"UPDATE {sqlite_table} SET "
                            + ", ".join(f"{n} = :{n}" for n in param.keys() if n != 'id')
                            + " WHERE id == :id", param)
            except sqlite3.Error as e:
                print("error", e.args[0])
            sleep(uniform(0.1, 1))
        con.commit()


def request_transactions_category_bulk_updates_with_update_db(s, large_category_id, middle_category_id, ids, sqlite=None, sqlite_table=None):
    request_transactions_category_bulk_updates(s, large_category_id, middle_category_id, ids)
    
    if sqlite and sqlite_table:
        request_update_sqlite_db(s, ids, sqlite, sqlite_table)


def transactions_category_bulk_updates(s, args):
    category_id = None
    if args.category_name:
        category_id = get_middle_category(s, args, args.category_name)
    elif args.category_id:
        category_id = args.category_id
    large_category_id, middle_category_id = category_id
    print(f'{large_category_id=}, {middle_category_id=}')
    
    ids = args.ids
    if ids is None:
        data = sys.stdin.readlines()
        ids = [int(x) for line in data for x in line.strip().split() if x.isdecimal()]
        if not ids:
            raise ValueError('ids not specified')
    request_transactions_category_bulk_updates_with_update_db(s, large_category_id, middle_category_id, ids,
                                               sqlite=args.sqlite, sqlite_table=args.sqlite_table)


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
                request_transactions_category_bulk_updates_with_update_db(s, l, m, list(ids))

    except KeyboardInterrupt:
        print("Ok ok, quitting")
        sys.exit(1)
    
def bulk_update_category2(s, args):
    stream = sys.stdin
    if args.input_file:
        stream = open(args.input_file)
    else:
      print(f"Please Input. ({args.delimiter=}, column_category_name={args.column_category_name=}, {args.column_id=})")
    
    data = defaultdict(set)
    for line in stream.readlines():
        line = line.strip()
        if not line:
            continue
        
        try:
            row = line.split(args.delimiter)
            i = row[args.column_id]
            c = row[args.column_category_name]
            
            if i.strip() == "":
                print(f"Not Found ID. Skip! {line}")
                continue
            i = int(i)
            
            if c.strip() == "":
                print(f"Not Found ID. Skip! {line}")
                continue
            
            category_id = get_middle_category(s, args, c)
            data[category_id].add(i)
            
        except Exception as e:
            print(e)
            print(f"Parse Error. Skip! {line}")
            continue
    
    print("--------")
    for (l, m), ids in data.items():
        print('large_category_id', l, 'middle_category_id', m)
        print(ids)
        print()
    print("--------")
    
    if not input("\nReally quit? (y/N)> ").lower().startswith('y'):
        sys.exit(1)
    print("execute")
    
    for (l, m), ids in data.items():
        print('large_category_id', l, 'middle_category_id', m)
        request_transactions_category_bulk_updates_with_update_db(s, l, m, list(ids),
            sqlite=args.sqlite, sqlite_table=args.sqlite_table)

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
parser.add_argument('--cache_category_csv', default='cache_search_categories.csv') # いろいろなコマンドで使うので共通化
parser.add_argument('--force_category_update', action='store_true') # いろいろなコマンドで使うので共通化


subparsers = parser.add_subparsers(dest='cmd', required=True)


with add_parser(subparsers, 'category', func=get_category) as subparser:
    add_standard_output_group(subparser, csv=False)


with add_parser(subparsers, 'large_categories', func=get_large_categories) as subparser:
    group = add_standard_output_group(subparser)
    group.add_argument('--sqlite')


with add_parser(subparsers, 'account_summaries', func=get_account_summaries) as subparser:
    add_standard_output_group(subparser, lst=True)
    list_header = 'service_category_id account_id_hash sub_accounts.sub_account_id_hash name sub_accounts.sub_type sub_accounts.sub_name sub_accounts.sub_number sub_accounts.user_asset_det_summaries.asset_subclass_name sub_accounts.user_asset_det_summaries.asset_subclass_unit'.split()
    subparser.add_argument('--list_header', type=str, nargs='+', default=list_header)
    subparser.add_argument('-c', '--service_category_id', type=int)
    subparser.add_argument('-n', '--name')
    subparser.add_argument('-t', '--sub_type')
    subparser.add_argument('-u', '--unique_list', action='store_true')
    subparser.add_argument('-d', '--default_group', action='store_true')

with add_parser(subparsers, 'liabilities', func=get_liabilities) as subparser:
    add_standard_output_group(subparser)

with add_parser(subparsers, 'smartphone_asset', func=get_smartphone_asset) as subparser:
    add_standard_output_group(subparser)


with add_parser(subparsers, 'sub_account_groups', func=get_sub_account_groups) as subparser:
    add_standard_output_group(subparser)


with add_parser(subparsers, 'change_group', func=change_group) as subparser:
    with subparser.add_mutually_exclusive_group() as group:
        group.add_argument('-i', '--group_id_hash')
        group.add_argument('-n', '--group_name')


with add_parser(subparsers, 'manual_user_asset_act_partner_sources', func=get_manual_user_asset_act_partner_sources) as subparser:
    subparser.add_argument('act_id', type=int)
    subparser.add_argument('-n', '--sub_name')
    subparser.add_argument('-t', '--sub_type')
    subparser.add_argument('-N', '--sub_number')
    subparser.add_argument('-S', '--service_category_id', type=int)
    subparser.add_argument('--account_id_hash')
    subparser.add_argument('--sub_account_id_hash')
    subparser.add_argument('-d', '--account_disp_name')
    subparser.add_argument('-s', '--account_service_name')
    subparser.add_argument('-C', '--content')
    subparser.add_argument('-a', '--amount', type=float)
    subparser.add_argument('-D', '--updated_at', type=dateutil.parser.parse)
    subparser.add_argument('-c', '--partner_candidate_acts', action='store_true')
    add_standard_output_group(subparser, lst=True)


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
    subparser.add_argument('--columns', nargs='+')
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
    subparser.add_argument('-c', '--category_name')
    subparser.add_argument('-l', '--large_category_id', type=int)
    subparser.add_argument('-m', '--middle_category_id', type=int)
    subparser.add_argument('-t', '--is_target', choices={0, 1})
    subparser.add_argument('-M', '--memo')
    subparser.add_argument('--partner_account_id_hash')
    subparser.add_argument('--partner_sub_account_id_hash')
    subparser.add_argument('--partner_act_id')
    subparser.add_argument('-s', '--sqlite', metavar='cf_term_data.db')
    subparser.add_argument('--sqlite_table', default='user_asset_act')


with add_parser(subparsers, 'update_enable_transfer', func=update_enable_transfer) as subparser:
    subparser.add_argument('ids', type=int, nargs='*')


with add_parser(subparsers, 'update_disable_transfer', func=update_disable_transfer) as subparser:
    subparser.add_argument('ids', type=int, nargs='*')


with add_parser(subparsers, 'change_transfer', func=change_transfer) as subparser:
    subparser.add_argument('id', type=int)
    subparser.add_argument('-a', '--partner_account_id_hash')
    subparser.add_argument('-s', '--partner_sub_account_id_hash')
    subparser.add_argument('-A', '--partner_act_id')


with add_parser(subparsers, 'clear_transfer', func=clear_transfer) as subparser:
    subparser.add_argument('id', type=int)


with subparsers.add_parser('search_category') as subparser:
    subparser.add_argument('--cache_csv', default='cache_search_categories.csv')
    subparser.add_argument('--force_update', action='store_true')
    subparser.add_argument('-l', '--large')
    subparser.add_argument('-m', '--middle')
    subparser.set_defaults(func=search_category)


with add_parser(subparsers, 'user_asset_act_by_id', func=get_user_asset_act_by_id) as subparser:
    subparser.add_argument('id', type=int)


with add_parser(subparsers, 'user_asset_acts_by_ids', func=get_user_asset_acts_by_ids) as subparser:
    subparser.add_argument('ids', type=int, nargs='+')
    subparser.add_argument('-c', '--columns', nargs='+')
    subparser.add_argument('-s', '--sep')


with subparsers.add_parser('user_asset_acts') as subparser:
    add_standard_output_group(subparser, lst=True)
    
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


with add_parser(subparsers, 'update_sqlite_db', func=update_sqlite_db) as subparser:
    subparser.add_argument('ids', type=int, nargs='*')
    subparser.add_argument('-s', '--sqlite', required=True, metavar='cf_term_data.db')
    subparser.add_argument('--sqlite_table', default='user_asset_act')
    subparser.add_argument('--pretty', action='store_true')


with subparsers.add_parser('filter_db') as subparser:
    subparser.set_defaults(func=filter_db)
    with subparser.add_mutually_exclusive_group(required=True) as group:
        group.add_argument('--csv')
        group.add_argument('--sqlite', metavar='cf_term_data.db')
    subparser.add_argument('--sqlite_table', default='user_asset_act')
    subparser.add_argument('--columns', type=str, nargs='+')
    subparser.add_argument('--sort', metavar='column', nargs='+')

    with subparser.add_mutually_exclusive_group() as group:
        group.add_argument('--list', action='store_true')
        group.add_argument('--output_csv')
        group.add_argument('-u', '--update_category_name')
        group.add_argument('-U', '--update_category', type=int, nargs=2, metavar=('large_category_id', 'middle_category_id'))
        group.add_argument('-d', '--update_sqlite_db', action='store_true')
        group.add_argument('--list_id', action='store_true')
        group.add_argument('--update_transfer', type=int, choices={0, 1})
        group.add_argument('--update_partner_account', nargs=2, metavar=('account_id_hash', 'sub_account_id_hash'))

    subparser.add_argument('-q', '--query', help='ex) content.notnull() and content.str.match(\'セブン\') and middle_category != \'コンビニ\'')
    
    with subparser.add_argument_group('group_filter_pattern') as group_filter_pattern:
        group_filter_pattern.add_argument('-r', '--reverse', action='store_true')
        group_filter_pattern.add_argument('-p', '--patterns', nargs='+', metavar='pattern', help='ex) ".*" / "^タイムズ" ')
        group_filter_pattern.add_argument('-E', '--exclude_patterns', nargs='+', metavar='pattern')
        group_filter_pattern.add_argument('-i', '--ignore_invalid_data', action='store_true')
        group_filter_pattern.add_argument('--is_income', type=int, choices={0, 1})
        group_filter_pattern.add_argument('--is_transfer', type=int, choices={0, 1})
        group_filter_pattern.add_argument('-b', '--date_from', type=dateutil.parser.parse)
        group_filter_pattern.add_argument('-e', '--date_to', type=dateutil.parser.parse)
        
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

        with group_filter_pattern.add_mutually_exclusive_group() as group:
            group.add_argument('--lt', type=int, metavar='amount', help='less then [amount]')
            group.add_argument('--le', type=int, metavar='amount', help='less then or equal to [amount]')
            group.add_argument('--gt', type=int, metavar='amount', help='greater then [amount]')
            group.add_argument('--ge', type=int, metavar='amount', help='greater then or equal to [amount]')


with subparsers.add_parser('transactions_category_bulk_updates') as subparser:
    subparser.set_defaults(func=transactions_category_bulk_updates)
    with subparser.add_mutually_exclusive_group(required=True) as category_group:
        category_group.add_argument('-c', '--category_name')
        category_group.add_argument('-p', '--category_id', nargs=2, type=int, metavar=('LARGE_CATEGORY_ID', 'MIDDLE_CATEGORY_ID'))
    #subparser.add_argument('-m', '--middle_category_id', type=int, required=True)
    #subparser.add_argument('-l', '--large_category_id', type=int, required=True)
    subparser.add_argument('-i', '--ids', type=int, nargs='+')
    subparser.add_argument('-s', '--sqlite', metavar='cf_term_data.db')
    subparser.add_argument('--sqlite_table', default='user_asset_act')


with add_parser(subparsers, 'bulk_update_category', func=bulk_update_category) as subparser:
    subparser.add_argument('-f', '--input_file')
    subparser.add_argument('-d', '--delimiter')
    subparser.add_argument('-m', '--column_middle_category_id', type=int, required=True)
    subparser.add_argument('-l', '--column_large_category_id', type=int, required=True)
    subparser.add_argument('-i', '--column_id', type=int, required=True)

with add_parser(subparsers, 'bulk_update_category2', func=bulk_update_category2) as subparser:
    subparser.add_argument('-f', '--input_file')
    subparser.add_argument('-d', '--delimiter', default=":", nargs='?', const=None)
    subparser.add_argument('-i', '--column_id', type=int, default=0)
    subparser.add_argument('-c', '--column_category_name', type=int, default=1)
    
    subparser.add_argument('-s', '--sqlite', metavar='cf_term_data.db')
    subparser.add_argument('--sqlite_table', default='user_asset_act')


def main(argv=None):
    args = parser.parse_args(argv)

    if args.debug:
        import http.client
        http.client.HTTPConnection.debuglevel = 2

    with session_from_cookie_file(args.mf_cookies) as s:
        args.func(s, args)


if __name__ == '__main__':
    main()


