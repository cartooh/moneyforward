#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import datetime
import pandas as pd
import dateutil.parser
from datetime import timedelta, datetime
import sqlite3
from contextlib import closing
from time import sleep
from random import uniform
from tqdm import tqdm

logger = logging.getLogger(__name__)

formatter = '%(levelname)s : %(asctime)s : %(message)s'
logging.basicConfig(level=logging.INFO, format=formatter)

# move HTTP request helpers to separate module
from moneyforward_api import *

# move shared utilities to separate module
from moneyforward_utils import traverse, get_categories_form_session


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


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--mf_cookies', default='mf_cookies.pkl')
    parser.add_argument('-d', '--debug', action='store_true')
    
    parser.add_argument('-f', '--date_from', type=dateutil.parser.parse)
    parser.add_argument('-t', '--date_to', type=dateutil.parser.parse)
    parser.add_argument('-C', '--service_category_id', type=int)
    parser.add_argument('-N', '--name')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--csv')
    group.add_argument('--sqlite')
    parser.add_argument('--csv_header', nargs='+')
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
    parser.add_argument('--sqlite_header', nargs='+', default=sqlite_header)
    parser.add_argument('-i', '--ignore_KeyError', action='store_true')
    
    args = parser.parse_args(argv)

    if args.debug:
        import http.client
        http.client.HTTPConnection.debuglevel = 2

    with session_from_cookie_file(args.mf_cookies) as s:
        get_term_data(s, args)


if __name__ == '__main__':
    main()