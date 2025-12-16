#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import requests
from bs4 import BeautifulSoup
from contextlib import contextmanager
from pprint import pprint
import pickle


def request_category(s):
    """カテゴリ情報を取得
    
    Args:
        s: requests.Session
    
    Returns:
        dict: カテゴリ情報
    """
    return s.get("https://moneyforward.com/sp/category").json()


def request_large_categories(s):
    """大カテゴリ一覧を取得
    
    Args:
        s: requests.Session
    
    Returns:
        list: 大カテゴリのリスト
    """
    large_categories = s.get("https://moneyforward.com/sp2/large_categories").json()
    return large_categories['large_categories']


@contextmanager
def change_default_group(s):
    """デフォルトグループを一時的に変更するコンテキストマネージャ
    
    現在のグループを保存し、デフォルトグループに切り替え、
    処理終了後に元のグループに戻す。
    
    Args:
        s: requests.Session
    
    Yields:
        None
    """
    sub_account_groups = request_sub_account_groups(s)
    if 'current_group_id_hash' not in sub_account_groups:
        print(f"Not Found current_group_id_hash in sub_account_groups: {sub_account_groups}")
    current_group_id_hash = sub_account_groups['current_group_id_hash']
    request_change_group(s)
    
    try:
        yield
    finally:
        request_change_group(s, current_group_id_hash)


def request_account_summaries(s, default_group=False):
    """アカウントサマリー一覧を取得
    
    Args:
        s: requests.Session
        default_group: Trueの場合、デフォルトグループで取得
    
    Returns:
        dict: アカウントサマリー情報
    """
    if default_group:
        with change_default_group(s):
            return request_account_summaries(s)
    
    return s.get("https://moneyforward.com/sp2/account_summaries").json()


def request_service_detail(s, account_id_hash, sub_account_id_hash=None, range_value=None):
    """サービス詳細を取得
    
    Args:
        s: requests.Session
        account_id_hash: アカウントIDハッシュ
        sub_account_id_hash: サブアカウントIDハッシュ (optional)
        range_value: 範囲指定 (optional)
    """
    params = {}
    if sub_account_id_hash:
        params['sub_account_id_hash'] = sub_account_id_hash
    if range_value is not None:
        params['range'] = str(range_value)
    
    service_detail = s.get(f"https://moneyforward.com/sp/service_detail/{account_id_hash}", params=params)
    return service_detail.json()


def request_accounts(s, account_id, sub_account_id_hash=None):
    """アカウント情報を取得
    
    Args:
        s: requests.Session
        account_id: アカウントID
        sub_account_id_hash: サブアカウントIDハッシュ (optional)
    """
    params = {}
    if sub_account_id_hash:
        params['sub_account_id_hash'] = sub_account_id_hash
    
    accounts = s.get(f"https://moneyforward.com/sp2/accounts/{account_id}", params=params)
    return accounts.json()


def request_liabilities(s):
    """負債情報を取得
    
    Args:
        s: requests.Session
    
    Returns:
        dict: 負債情報
    """
    return s.get("https://moneyforward.com/sp2/liabilities").json()


def request_smartphone_asset(s):
    """資産情報を取得（スマートフォン版）
    
    Args:
        s: requests.Session
    
    Returns:
        dict: 資産情報（スマートフォン版）
    """
    return s.get("https://moneyforward.com/smartphone_asset").json()


def request_cf_sum_by_sub_account(s, sub_account_id_hash=None, year_offset=None):
    """サブアカウント別キャッシュフロー集計を取得
    
    Args:
        s: requests.Session
        sub_account_id_hash: サブアカウントIDハッシュ (optional)
        year_offset: 年オフセット (optional)
    """
    params = {}
    if sub_account_id_hash:
        params['sub_account_id_hash'] = sub_account_id_hash
    if year_offset is not None:
        params['year_offset'] = str(year_offset)
    
    cf_sum_by_sub_account = s.get("https://moneyforward.com/sp/cf_sum_by_sub_account", params=params)
    return cf_sum_by_sub_account.json()


def request_cf_term_data_by_sub_account(s, sub_account_id_hash, date_from=None, date_to=None):
    """サブアカウント別期間キャッシュフローデータを取得
    
    Args:
        s: requests.Session
        sub_account_id_hash: サブアカウントIDハッシュ
        date_from: 開始日 (datetime, optional)
        date_to: 終了日 (datetime, optional)
    
    Returns:
        dict: 期間キャッシュフローデータ
    """
    params = dict(sub_account_id_hash=sub_account_id_hash)
    if date_from:
        params['from'] = date_from.strftime('%Y-%m-%d')
    if date_to:
        params['to'] = date_to.strftime('%Y-%m-%d')

    cf_term_data_by_sub_account = s.get("https://moneyforward.com/sp/cf_term_data_by_sub_account", params=params)
    return cf_term_data_by_sub_account.json()


def get_csrf_token(s):
    """CSRFトークンを取得
    
    HTMLページからCSRFトークンのメタタグを抽出する。
    
    Args:
        s: requests.Session
    
    Returns:
        str: CSRFトークン
    
    Raises:
        AssertionError: CSRFトークンが見つからない場合
    """
    res = s.get("https://moneyforward.com/cf")
    soup = BeautifulSoup(res.content, "html.parser")
    metas = soup.find_all("meta", {'name': 'csrf-token'})
    
    for meta in metas:
        if 'content' in meta.attrs:
            return meta['content']
            
    # Debug info if not found
    if metas:
        print(f"Found {len(metas)} csrf-token meta tags, but none had content attribute.")
        for m in metas:
            print(f" - {m}")
    else:
        print("No csrf-token meta tag found.")
        
    raise ValueError("CSRF token not found")


def request_update_user_asset_act(s, csrf_token, id_, 
        large_category_id=None, middle_category_id=None, is_target=None, memo=None,
        partner_account_id_hash=None, partner_sub_account_id_hash=None, partner_act_id=None):
    """ユーザー資産取引（明細）を更新
    
    取引のカテゴリ、メモ、振替先などを変更する。
    
    Args:
        s: requests.Session
        csrf_token: CSRFトークン
        id_: 取引ID
        large_category_id: 大カテゴリID (optional)
        middle_category_id: 中カテゴリID (optional)
        is_target: 計算対象フラグ (optional)
        memo: メモ (optional)
        partner_account_id_hash: 振替先アカウントIDハッシュ (optional)
        partner_sub_account_id_hash: 振替先サブアカウントIDハッシュ (optional)
        partner_act_id: 振替先取引ID (optional)
    """
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
    """取引タイプを変更
    
    振替の有効化/無効化など、取引タイプを変更する。
    
    Args:
        s: requests.Session
        csrf_token: CSRFトークン
        id_: 取引ID
        change_type: 変更タイプ ('enable_transfer', 'disable_transfer'等)
    """
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


def request_change_transfer(s, id, partner_account_id_hash="0", partner_sub_account_id_hash="0", partner_act_id=None):
    """振替設定を変更
    
    指定した取引の振替先を設定する。
    
    Args:
        s: requests.Session
        id: 取引ID
        partner_account_id_hash: 振替先アカウントIDハッシュ (デフォルト: "0")
        partner_sub_account_id_hash: 振替先サブアカウントIDハッシュ (デフォルト: "0")
        partner_act_id: 振替先取引ID (optional)
    """
    url = 'https://moneyforward.com/sp/change_transfer'
    params = dict(id=id, partner_account_id_hash=partner_account_id_hash, partner_sub_account_id_hash=partner_sub_account_id_hash)
    if partner_act_id is not None:
        params['partner_act_id'] = partner_act_id
    r = s.post(url, json.dumps(params), headers={'Content-Type': 'application/json'})
    if r.status_code != requests.codes.ok:
        print(r.status_code, r.text)


def request_clear_transfer(s, id):
    """振替設定をクリア
    
    指定した取引の振替設定を解除する。
    
    Args:
        s: requests.Session
        id: 取引ID
    """
    url = 'https://moneyforward.com/sp/clear_transfer'
    params = dict(id=id)
    r = s.post(url, json.dumps(params), headers={'Content-Type': 'application/json'})
    if r.status_code != requests.codes.ok:
        print(r.status_code, r.text)


def request_user_asset_act_by_id(s, id):
    """IDを指定して取引を取得
    
    Args:
        s: requests.Session
        id: 取引ID
    
    Returns:
        dict: 取引情報
    """
    user_asset_act = s.get(f"https://moneyforward.com/sp2/user_asset_acts/{id}").json()
    return user_asset_act


def request_user_asset_acts(s, 
    offset=None, size=None, is_new=None, is_old=None, 
    is_continuous=None, select_category=None, base_date=None, keyword=None):
    """ユーザー資産取引一覧を取得
    
    具体的なパラメータを受け取り、取引一覧API呼び出しを行う。
    エラーレスポンスの場合は例外を発生させる。
    
    Args:
        s: requests.Session
        offset: オフセット (optional)
        size: 取得件数 (optional)
        is_new: 新規フラグ (optional)
        is_old: 過去フラグ (optional)
        is_continuous: 連続フラグ (optional)
        select_category: カテゴリ選択 (optional)
        base_date: 基準日 (optional)
        keyword: キーワード検索 (optional)
    
    Returns:
        dict: 取引一覧情報
    
    Raises:
        ValueError: APIがエラーを返した場合
    """
    params = {}
    if offset is not None:
        params['offset'] = offset
    if size is not None:
        params['size'] = size
    if is_new is not None:
        params['is_new'] = is_new
    if is_old is not None:
        params['is_old'] = is_old
    if is_continuous is not None:
        params['is_continuous'] = is_continuous
    if select_category is not None:
        params['select_category'] = select_category
    if base_date is not None:
        params['base_date'] = base_date
    if keyword is not None:
        params['keyword'] = keyword
    
    user_asset_acts = s.get("https://moneyforward.com/sp2/user_asset_acts", params=params).json()
    if 'messages' in user_asset_acts:
        pprint(user_asset_acts)
        raise ValueError(user_asset_acts['messages'])
    
    if 'error' in user_asset_acts:
        pprint(user_asset_acts)
        raise ValueError(user_asset_acts['error'])
    
    return user_asset_acts


def request_sub_account_groups(s):
    """サブアカウントグループ一覧を取得
    
    Args:
        s: requests.Session
    
    Returns:
        dict: サブアカウントグループ情報
    """
    return s.get("https://moneyforward.com/sp/sub_account_groups").json()


def request_change_group(s, group_id_hash="0"):
    """グループを変更
    
    表示するグループを切り替える。
    "0"を指定するとデフォルトグループに戻る。
    
    Args:
        s: requests.Session
        group_id_hash: グループIDハッシュ (デフォルト: "0")
    """
    url = 'https://moneyforward.com/sp/change_group'
    params = dict(group_id_hash=group_id_hash)
    r = s.post(url, json.dumps(params), headers={'Content-Type': 'application/json'})
    if r.status_code != requests.codes.ok:
        print(r.status_code, r.text)


def request_manual_user_asset_act_partner_sources(s, act_id):
    """手動取引の振替候補を取得
    
    指定した手動取引に対する振替候補（サブアカウント一覧）を取得する。
    
    Args:
        s: requests.Session
        act_id: 取引ID
    
    Returns:
        dict: 振替候補情報
    """
    params = dict(act_id=act_id)
    url = "https://moneyforward.com/sp/manual_user_asset_act_partner_sources"
    return s.get(url, params=params).json()


def request_transactions_category_bulk_updates(s, large_category_id, middle_category_id, ids):
    """複数取引のカテゴリを一括更新
    
    指定した複数の取引IDに対して、カテゴリを一括で変更する。
    負のIDは自動的にフィルタリングされる。
    100件ずつに分割してリクエストを送信する。
    
    Args:
        s: requests.Session
        large_category_id: 大カテゴリID
        middle_category_id: 中カテゴリID
        ids: 取引IDのリスト
    """
    if any(id < 0 for id in ids):
        print("Filtered invalid ids")
        ids = [id for id in ids if id > 0]
        
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

@contextmanager
def session_from_cookie_file(cookie_file='mf_cookies.pkl'):
    """クッキーファイルから認証済みセッションを作成
    
    pickle化されたクッキーファイルを読み込み、
    認証済みのrequests.Sessionを作成するコンテキストマネージャ。
    
    Args:
        cookie_file: クッキーファイルのパス (デフォルト: 'mf_cookies.pkl')
    
    Yields:
        requests.Session: 認証済みセッション
    
    Note:
        pickleファイルは信頼できるソースからのみ読み込むこと。
    """
    s = requests.Session()
    try:
        with open(cookie_file, 'rb') as f:
            s.cookies = pickle.load(f)
        yield s
    finally:
        s.close()
