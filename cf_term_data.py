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
import os
from openpyxl import load_workbook, Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

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


def parse_header(header_list):
    select_header = []
    rename_header = {}
    dtypes_dict = {}
    
    for item in header_list:
        # まず : で型を分離
        if ':' in item:
            name_part, dtype = item.rsplit(':', 1)  # 右からsplitして最後の:を型とする
        else:
            name_part = item
            dtype = None
        
        # 次に = でnameとaliasを分離
        if '=' in name_part:
            name, alias = name_part.split('=', 1)
            rename_header[name] = alias
            select_header.append(name)
            if dtype:
                dtypes_dict[alias] = dtype  # エイリアス後の型はaliasに適用
            else:
                dtypes_dict[alias] = 'object'
        else:
            name = name_part
            select_header.append(name)
            if dtype:
                dtypes_dict[name] = dtype
            else:
                dtypes_dict[name] = 'object'
    
    return select_header, rename_header, dtypes_dict


def upsert_to_excel(df, sheet_name, excel_file, unique_index_label):
    """
    ユニークインデックスを用いて差分更新を行い、DataFrameをExcelシートにアップサートします。


    この関数はExcelファイルに対してアップサート操作を行います。
    ファイルやシートが存在しない場合は新規作成します。
    既存データがある場合は、unique_index_label列を使って一致する行に差分を反映し、
    新規データ（列・行）は既存の書式やカスタム列・行を維持したまま末尾に追加します。
    既存データにしかない列や行はそのまま残ります。

    引数:
        df (pd.DataFrame): アップサートするDataFrame。空であってはなりません。
        sheet_name (str): Excelシート名。
        excel_file (str): Excelファイルのパス。
        unique_index_label (str): 更新時に使うユニークインデックスとなる列名。dfおよび既存シート（存在する場合）に必須。

    例外:
        ValueError: dfが空、unique_index_labelが空、または既存シートにunique_index_label列がない場合。
        PermissionError: ファイルがロックされている場合（ユーザー入力によるリトライ処理あり）。

    動作:
        - 新規ファイル/シート: dfを書き込み作成。
        - 既存シートでスキーマ一致: 差分更新・追加。
        - スキーマ不一致（列・行）: シート全体を再書き込み。
        - 空df: エラー。
        - 既存シートにunique_index_label列がない: エラー。
    """
    if df.empty:
        raise ValueError("DataFrame must not be empty")
    if not unique_index_label:
        raise ValueError("unique_index_label must be provided and not empty")
    
    if os.path.exists(excel_file):
        # 既存のファイルを読み込む
        wb = load_workbook(excel_file)
        if sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            # 既存データを読み込む
            existing_data = []
            row_numbers = []
            for row_idx, row in enumerate(ws.iter_rows(values_only=True), 1):
                if row_idx == 1:
                    headers = list(row)
                else:
                    existing_data.append(row)
                    row_numbers.append(row_idx)

            if existing_data:
                existing_df = pd.DataFrame(existing_data, columns=headers)
                existing_df['excel_row'] = row_numbers
            else:
                existing_df = pd.DataFrame()
                existing_df['excel_row'] = []
            
            # 既存データがある場合（ヘッダーあり）、unique_index_label の列チェック
            if headers and unique_index_label not in headers:
                raise ValueError(f"unique_index_label '{unique_index_label}' column not found in existing sheet '{sheet_name}'")
        else:
            # シートが存在しない場合
            ws = wb.create_sheet(sheet_name)
            existing_df = pd.DataFrame()
            existing_df['excel_row'] = []
            headers = []
    else:
        # 新規作成
        wb = Workbook()
        ws = wb.active
        ws.title = sheet_name
        existing_df = pd.DataFrame()
        existing_df['excel_row'] = []
        headers = []
    
    # ヘッダーが同じかどうか確認し、新しい列に対応
    new_columns = [col for col in df.columns if col not in headers]
    missing_columns = [col for col in headers if col not in df.columns]
    # 行の変更をチェック
    if unique_index_label and unique_index_label in existing_df.columns and unique_index_label in df.columns:
        existing_ids = set(existing_df[unique_index_label])
        new_ids = set(df[unique_index_label])
        missing_rows = existing_ids - new_ids
        new_rows = new_ids - existing_ids
        existing_df_indexed = existing_df.set_index(unique_index_label)
        df_indexed = df.set_index(unique_index_label)
        common_indices = existing_df_indexed.index.intersection(df_indexed.index)
        # 変更された行を特定
        # common_indicesがpandas.Index型かset型かで空判定を分岐
        is_empty = False
        if hasattr(common_indices, 'empty'):
            is_empty = common_indices.empty
        else:
            is_empty = len(common_indices) == 0
        if not is_empty:
            common_cols = existing_df_indexed.columns.intersection(df_indexed.columns).drop('excel_row', errors='ignore')
            existing_for_cmp = existing_df_indexed[common_cols]
            df_for_cmp = df_indexed[common_cols]
            changed_mask = (existing_for_cmp.loc[common_indices] != df_for_cmp.loc[common_indices]).any(axis=1)
            changed_indices = common_indices[changed_mask]
        else:
            changed_indices = common_indices
    else:
        missing_rows = set()
        new_rows = set()
        if unique_index_label and unique_index_label in df.columns:
            new_rows = set(df[unique_index_label])
        common_indices = set()
        changed_indices = set()
        existing_df_indexed = None
        df_indexed = None
    
    if new_columns or missing_columns or missing_rows or new_rows:
        # スキーマ不一致時、差分で対応（既存データを維持）
        # 新規列を既存列の後に追加
        existing_col_count = len(headers)
        col_map = {col: i+1 for i, col in enumerate(headers)}
        for col in df.columns:
            if col not in headers:
                existing_col_count += 1
                ws.insert_cols(existing_col_count)
                ws.cell(row=1, column=existing_col_count, value=col)
                col_map[col] = existing_col_count
        
        # 欠損列は削除せず残す
        # 欠損行は削除せず残す
        
        # 新規行を追加
        if new_rows:
            start_row = ws.max_row + 1
            for idx in new_rows:
                row_data = df[df[unique_index_label] == idx].iloc[0]
                for col_name, value in row_data.items():
                    if col_name in col_map:
                        ws.cell(row=start_row, column=col_map[col_name], value=value)
                start_row += 1
        
        # 変更行を更新
        is_empty = False
        if hasattr(common_indices, 'empty'):
            is_empty = common_indices.empty
        else:
            is_empty = len(common_indices) == 0
        if not is_empty:
            for idx in common_indices:
                if idx in changed_indices:
                    row_num = existing_df_indexed.loc[idx, 'excel_row']
                    row_data = df[df[unique_index_label] == idx].iloc[0]
                    for col_name, value in row_data.items():
                        if col_name in col_map:
                            ws.cell(row=row_num, column=col_map[col_name], value=value)
    else:
        # 列が一致する場合、差分更新
        # ユニークインデックスがある場合
        if unique_index_label in existing_df.columns and unique_index_label in df.columns:
            # インデックスを設定
            existing_df_indexed = existing_df.set_index(unique_index_label)
            df_indexed = df.set_index(unique_index_label)
            
            # 共通のインデックス
            common_indices = existing_df_indexed.index.intersection(df_indexed.index)
            
            # 変更された行を特定（共通インデックスで値が異なる行）
            if not common_indices.empty:
                # 共通の列で比較
                common_cols = existing_df_indexed.columns.intersection(df_indexed.columns).drop('excel_row', errors='ignore')
                existing_for_cmp = existing_df_indexed[common_cols]
                df_for_cmp = df_indexed[common_cols]
                changed_mask = (existing_for_cmp.loc[common_indices] != df_for_cmp.loc[common_indices]).any(axis=1)
                changed_indices = common_indices[changed_mask]
                
                # 変更された行のExcel行番号を取得
                changed_rows = existing_df_indexed.loc[changed_indices, 'excel_row']
                
                # 変更されたセルを更新
                for idx in changed_indices:
                    row_num = changed_rows.loc[idx]
                    row_data = pd.Series([idx] + list(df_indexed.loc[idx]), index=[unique_index_label] + list(df_indexed.columns))
                    for col_idx, value in enumerate(row_data, 1):
                        ws.cell(row=row_num, column=col_idx, value=value)
            
            # 新規行を追加
            new_indices = df_indexed.index.difference(existing_df_indexed.index)
            is_empty = False
            if hasattr(common_indices, 'empty'):
                is_empty = common_indices.empty
            else:
                is_empty = len(common_indices) == 0
            if not is_empty:
                # 共通の列で比較
                common_cols = existing_df_indexed.columns.intersection(df_indexed.columns).drop('excel_row', errors='ignore')
                existing_for_cmp = existing_df_indexed[common_cols]
                df_for_cmp = df_indexed[common_cols]
                changed_mask = (existing_for_cmp.loc[common_indices] != df_for_cmp.loc[common_indices]).any(axis=1)
                changed_indices = common_indices[changed_mask]
            
                # 変更された行のExcel行番号を取得
                changed_rows = existing_df_indexed.loc[changed_indices, 'excel_row']
            
                # 変更されたセルを更新
                for idx in changed_indices:
                    row_num = changed_rows.loc[idx]
                    row_data = pd.Series([idx] + list(df_indexed.loc[idx]), index=[unique_index_label] + list(df_indexed.columns))
                    for col_idx, value in enumerate(row_data, 1):
                        ws.cell(row=row_num, column=col_idx, value=value)
                        ws.cell(row=r, column=c, value=value)
    
    # 保存
    while True:
        try:
            wb.save(excel_file)
            break
        except PermissionError:
            print(f"PermissionError: {excel_file} が開いている可能性があります。Excelファイルを閉じてEnterキーを押してください。")
            input()


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
            select_header, rename_header, dtypes_dict = parse_header(args.sqlite_header)
            for c in set(select_header) - set(term_data_list.columns):
                term_data_list[c] = None
            term_data_list = term_data_list[select_header]
            
            if rename_header:
                term_data_list = term_data_list.rename(columns=rename_header)
            
            # dtypesを適用
            term_data_list = term_data_list.astype(dtypes_dict)
            
        with closing(sqlite3.connect(args.sqlite)) as con:
            upsert(term_data_list, 'user_asset_act', 'id', con)
        return
    
    if args.excel:
        if args.excel_header:
            select_header, rename_header, dtypes_dict = parse_header(args.excel_header)
            for c in set(select_header) - set(term_data_list.columns):
                term_data_list[c] = None
            term_data_list = term_data_list[select_header]
            
            if rename_header:
                term_data_list = term_data_list.rename(columns=rename_header)
            
            # dtypesを適用
            term_data_list = term_data_list.astype(dtypes_dict)
            
        upsert_to_excel(term_data_list, 'user_asset_act', args.excel, 'id')
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
    group.add_argument('--excel')
    parser.add_argument('--csv_header', nargs='+')
    sqlite_header = """id:str date year month account_id:str sub_account_id:str is_transfer is_income
                       orig_content=content orig_amount=amount currency jpyrate memo 
                       large_category_id middle_category_id large_category middle_category
                       is_target partner_account_id:str partner_sub_account_id:str partner_act_id:str
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
    parser.add_argument('--excel_header', nargs='+', default=sqlite_header)  # 同じデフォルトを使用
    parser.add_argument('-i', '--ignore_KeyError', action='store_true')
    
    args = parser.parse_args(argv)

    if args.debug:
        import http.client
        http.client.HTTPConnection.debuglevel = 2

    with session_from_cookie_file(args.mf_cookies) as s:
        get_term_data(s, args)


if __name__ == '__main__':
    main()