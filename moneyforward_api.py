#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import requests
from bs4 import BeautifulSoup
from contextlib import contextmanager
from pprint import pprint


def request_category(s):
    return s.get("https://moneyforward.com/sp/category").json()


def request_large_categories(s):
    large_categories = s.get("https://moneyforward.com/sp2/large_categories").json()
    return large_categories['large_categories']


@contextmanager
def change_default_group(s):
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
    if default_group:
        with change_default_group(s):
            return request_account_summaries(s)
    
    return s.get("https://moneyforward.com/sp2/account_summaries").json()


def request_service_detail(s, args):
    params = {}
    if getattr(args, 'sub_account_id_hash', None):
        params['sub_account_id_hash'] = args.sub_account_id_hash

    if getattr(args, 'range', None):
        params['range'] = str(args.range)

    service_detail = s.get("https://moneyforward.com/sp/service_detail/{}".format(args.account_id_hash), params=params)
    return service_detail.json()


def request_accounts(s, args):
    params = {}
    if getattr(args, 'sub_account_id_hash', None):
        params['sub_account_id_hash'] = args.sub_account_id_hash

    accounts = s.get("https://moneyforward.com/sp2/accounts/{}".format(args.id), params=params)
    return accounts.json()


def request_liabilities(s):
    return s.get("https://moneyforward.com/sp2/liabilities").json()


def request_smartphone_asset(s):
    return s.get("https://moneyforward.com/smartphone_asset").json()


def request_cf_sum_by_sub_account(s, args):
    params = {}
    if getattr(args, 'sub_account_id_hash', None):
        params['sub_account_id_hash'] = args.sub_account_id_hash

    if getattr(args, 'year_offset', None):
        params['year_offset'] = str(args.year_offset)

    cf_sum_by_sub_account = s.get("https://moneyforward.com/sp/cf_sum_by_sub_account", params=params)
    return cf_sum_by_sub_account.json()


def request_cf_term_data_by_sub_account(s, sub_account_id_hash, date_from=None, date_to=None):
    params = dict(sub_account_id_hash=sub_account_id_hash)
    if date_from:
        params['from'] = date_from.strftime('%Y-%m-%d')
    if date_to:
        params['to'] = date_to.strftime('%Y-%m-%d')

    cf_term_data_by_sub_account = s.get("https://moneyforward.com/sp/cf_term_data_by_sub_account", params=params)
    return cf_term_data_by_sub_account.json()


def get_csrf_token(s):
    res = s.get("https://moneyforward.com/cf")
    soup = BeautifulSoup(res.content, "html.parser")
    meta = soup.find("meta", {'name': 'csrf-token'})
    assert meta, "CSRF token meta tag not found"
    assert 'content' in meta, "CSRF token meta tag has no content attribute"
    return meta['content']


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


def request_change_transfer(s, id, partner_account_id_hash="0", partner_sub_account_id_hash="0", partner_act_id=None):
    url = 'https://moneyforward.com/sp/change_transfer'
    params = dict(id=id, partner_account_id_hash=partner_account_id_hash, partner_sub_account_id_hash=partner_sub_account_id_hash)
    if partner_act_id is not None:
        params['partner_act_id'] = partner_act_id
    r = s.post(url, json.dumps(params), headers={'Content-Type': 'application/json'})
    if r.status_code != requests.codes.ok:
        print(r.status_code, r.text)


def request_clear_transfer(s, id):
    url = 'https://moneyforward.com/sp/clear_transfer'
    params = dict(id=id)
    r = s.post(url, json.dumps(params), headers={'Content-Type': 'application/json'})
    if r.status_code != requests.codes.ok:
        print(r.status_code, r.text)


def request_user_asset_act_by_id(s, id):
    user_asset_act = s.get(f"https://moneyforward.com/sp2/user_asset_acts/{id}").json()
    return user_asset_act


def request_user_asset_acts(s, params={}):
    user_asset_acts = s.get("https://moneyforward.com/sp2/user_asset_acts", params=params).json()
    if 'messages' in user_asset_acts:
        pprint(user_asset_acts)
        raise ValueError(user_asset_acts['messages'])
    
    if 'error' in user_asset_acts:
        pprint(user_asset_acts)
        raise ValueError(user_asset_acts['error'])
    
    return user_asset_acts


def request_sub_account_groups(s):
    return s.get("https://moneyforward.com/sp/sub_account_groups").json()


def request_change_group(s, group_id_hash="0"):
    url = 'https://moneyforward.com/sp/change_group'
    params = dict(group_id_hash=group_id_hash)
    r = s.post(url, json.dumps(params), headers={'Content-Type': 'application/json'})
    if r.status_code != requests.codes.ok:
        print(r.status_code, r.text)


def request_manual_user_asset_act_partner_sources(s, act_id):
    params = dict(act_id=act_id)
    url = "https://moneyforward.com/sp/manual_user_asset_act_partner_sources"
    return s.get(url, params=params).json()


def request_transactions_category_bulk_updates(s, large_category_id, middle_category_id, ids):
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

