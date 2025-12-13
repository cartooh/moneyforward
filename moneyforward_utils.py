#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MoneyForward 共通ユーティリティモジュール

カテゴリ検索、CSV保存など、API層とCLI層の両方から利用される
ビジネスロジックを提供します。このモジュールの関数は args に依存せず、
具体的な引数のみを受け取ります。
"""

import os
import csv
import pandas as pd
from moneyforward_api import request_large_categories


def save_large_categories_csv(fn, large_categories):
    """
    大カテゴリ・中カテゴリ情報をCSVファイルに保存
    
    Args:
        fn (str): 出力CSVファイルパス
        large_categories (list): 大カテゴリリスト（APIレスポンス形式）
    """
    with open(fn, 'wt', encoding='utf_8_sig') as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow("large_category_id large_category_name middle_category_id middle_category_name user_category".split())
        for large_category in large_categories:
            for middle_category in large_category['middle_categories']:
                writer.writerow([
                    large_category['id'],
                    large_category['name'],
                    middle_category['id'],
                    middle_category['name'],
                    middle_category['user_category']
                ])


def search_category_sub(s, cache_csv, force_update, large=None, middle=None, is_income=None):
    """
    カテゴリを検索してDataFrameで返す
    
    Args:
        s (requests.Session): 認証済みセッション
        cache_csv (str): キャッシュCSVファイルパス
        force_update (bool): 強制的にAPIから再取得するかどうか
        large (str, optional): 大カテゴリ名の検索文字列
        middle (str, optional): 中カテゴリ名の検索文字列
        is_income (bool, optional): True=収入のみ, False=収入以外, None=全て
    
    Returns:
        pd.DataFrame: フィルタ済みカテゴリ情報
    """
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


def get_middle_category_impl(s, cache_csv, force_update, category_name, is_income=None):
    """
    カテゴリ名から大カテゴリID・中カテゴリIDを取得
    
    Args:
        s (requests.Session): 認証済みセッション
        cache_csv (str): キャッシュCSVファイルパス
        force_update (bool): 強制的にAPIから再取得するかどうか
        category_name (str): 中カテゴリ名（部分一致）
        is_income (bool, optional): True=収入カテゴリから検索, False=支出カテゴリから検索
    
    Returns:
        tuple: (large_category_id, middle_category_id)
    
    Raises:
        ValueError: カテゴリが見つからない、または一意でない場合
    """
    category_df = search_category_sub(
        s,
        cache_csv,
        force_update,
        middle=category_name,
        is_income=is_income
    )
    
    if len(category_df) == 0:
        raise ValueError(f"Not Found Category Name: {category_name}")
    
    if len(category_df) > 1:
        raise ValueError(f"Not Unique Category Name: {category_name}")
    
    return (
        int(category_df.iloc[0].large_category_id),
        int(category_df.iloc[0].middle_category_id)
    )
