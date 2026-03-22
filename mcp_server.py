#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MoneyForward MCP サーバー

MoneyForwardの取引データをClaude Desktop (dispatch) から操作するためのMCPサーバー。
既存の moneyforward_api.py と moneyforward_utils.py を薄くラップして公開する。

起動方法:
    uv run python mcp_server.py

Claude Desktop 設定 (%%APPDATA%%/Claude/claude_desktop_config.json):
    {
      "mcpServers": {
        "moneyforward": {
          "command": "uv",
          "args": ["run", "--project", "<project_dir>", "python", "<project_dir>/mcp_server.py"],
          "env": {
            "MF_COOKIE_FILE": "<project_dir>/mf_cookies.pkl",
            "MF_CATEGORY_CACHE": "<project_dir>/large_categories.csv"
          }
        }
      }
    }
"""

# MCP は stdio でJSON通信するため、moneyforward_api.py の print() が
# stdout に出るとプロトコルが壊れる。先頭で stderr にリダイレクトする。
import sys
sys.stdout = sys.stderr

import os
import logging
from datetime import datetime, timedelta
from typing import Literal

import pandas as pd
from fastmcp import FastMCP

from moneyforward_api import (
    session_from_cookie_file,
    request_user_asset_acts,
    request_large_categories,
    request_account_summaries,
    request_cf_term_data_by_sub_account,
    request_update_user_asset_act,
    request_transactions_category_bulk_updates,
    get_csrf_token,
)
from moneyforward_utils import (
    search_category_sub,
    get_middle_category_impl,
    append_row_form_user_asset_acts,
    get_categories_form_user_asset_acts,
)

# -----------------------------------------------------------------------
# 設定
# -----------------------------------------------------------------------
COOKIE_FILE = os.environ.get("MF_COOKIE_FILE", "mf_cookies.pkl")
CATEGORY_CACHE = os.environ.get("MF_CATEGORY_CACHE", "large_categories.csv")

logging.basicConfig(level=logging.WARNING, stream=sys.stderr)

# -----------------------------------------------------------------------
# 共通ヘルパー
# -----------------------------------------------------------------------

# get_transactions で取得するフィールド一覧
_ACT_FIELDS = (
    "id is_transfer is_income is_target updated_at content recognized_at amount "
    "large_category_id large_category middle_category_id middle_category memo "
    "account.service.service_name sub_account.sub_type sub_account.sub_name"
).split()


def _check_session(s) -> None:
    """セッションが有効かどうかを確認する。
    MoneyForward はセッション切れ時に /sign_in へリダイレクトするため、
    簡単なAPIを叩いてリダイレクトが発生した場合は ValueError を返す。
    """
    res = s.get("https://moneyforward.com/sp2/user_asset_acts", params={"size": 1})
    if "sign_in" in res.url:
        raise ValueError(
            "Session expired. Please run start_mf_session.py to refresh mf_cookies.pkl"
        )


def _fetch_all_transactions(
    s,
    keyword: str | None = None,
    base_date: str | None = None,
    select_category: int | None = None,
    max_size: int = 1000,
    exclude_transfers: bool = True,
    is_income: bool | None = None,
) -> tuple[list[dict], int]:
    """内部: 取引を最大 max_size 件まで取得してフラット辞書のリストで返す。"""
    PAGE = 500
    rows = []
    total_count = 0

    for offset in range(0, max_size, PAGE):
        fetch = min(PAGE, max_size - offset)
        data = request_user_asset_acts(
            s,
            offset=offset,
            size=fetch,
            keyword=keyword,
            base_date=base_date,
            select_category=select_category,
        )
        if offset == 0:
            total_count = data.get("total_count", 0)

        batch: list[list] = []
        append_row_form_user_asset_acts(batch, data, _ACT_FIELDS)
        acts = [dict(zip(_ACT_FIELDS, row)) for row in batch]

        if exclude_transfers:
            acts = [a for a in acts if not a.get("is_transfer")]
        if is_income is True:
            acts = [a for a in acts if a.get("is_income")]
        elif is_income is False:
            acts = [a for a in acts if not a.get("is_income")]

        rows.extend(acts)

        # これ以上取得できない場合は終了
        if len(batch) < fetch:
            break

    return rows, total_count


# -----------------------------------------------------------------------
# FastMCP サーバー定義
# -----------------------------------------------------------------------
mcp = FastMCP("MoneyForward")


# === カテゴリ操作 ===

@mcp.tool()
def list_categories(
    large: str | None = None,
    middle: str | None = None,
    is_income: bool | None = None,
) -> list[dict]:
    """カテゴリ一覧を取得する。

    large_categories.csv のキャッシュから読み込む（MoneyForward APIへの通信不要）。
    カテゴリIDを調べてから set_transaction_category を呼ぶ際に利用する。

    Args:
        large: 大カテゴリ名の部分一致フィルタ（例: "食費"）
        middle: 中カテゴリ名の部分一致フィルタ（例: "外食"）
        is_income: True=収入カテゴリのみ, False=支出カテゴリのみ, None=全て
    """
    with session_from_cookie_file(COOKIE_FILE) as s:
        df = search_category_sub(
            s, CATEGORY_CACHE, force_update=False,
            large=large, middle=middle, is_income=is_income
        )
    return df.to_dict(orient="records")


@mcp.tool()
def find_category_by_name(
    category_name: str,
    is_income: bool | None = None,
) -> dict:
    """カテゴリ名から large_category_id と middle_category_id を返す。

    set_transaction_category を呼ぶ前にこのツールでIDを確認することを推奨。
    一意に特定できない場合はエラーと候補一覧を返す。

    Args:
        category_name: 中カテゴリ名（部分一致）
        is_income: True=収入カテゴリから検索, False=支出カテゴリから検索
    """
    with session_from_cookie_file(COOKIE_FILE) as s:
        try:
            large_id, middle_id = get_middle_category_impl(
                s, CATEGORY_CACHE, force_update=False,
                category_name=category_name, is_income=is_income
            )
        except ValueError as e:
            # 候補一覧も付けて返す
            df = search_category_sub(
                s, CATEGORY_CACHE, force_update=False, middle=category_name
            )
            return {
                "error": str(e),
                "candidates": df.to_dict(orient="records"),
            }

    return {
        "large_category_id": large_id,
        "middle_category_id": middle_id,
    }


# === 取引取得 ===

@mcp.tool()
def get_transactions(
    offset: int = 0,
    size: int = 50,
    keyword: str | None = None,
    base_date: str | None = None,
    select_category: int | None = None,
    is_income: bool | None = None,
    exclude_transfers: bool = True,
) -> dict:
    """取引一覧を取得する。

    Args:
        offset: 取得開始位置
        size: 取得件数（最大1000）
        keyword: キーワード検索
        base_date: 基準日 "YYYY-MM-DD" 形式
        select_category: カテゴリIDでフィルタ（0=未分類）
        is_income: True=収入のみ, False=支出のみ, None=全て
        exclude_transfers: True=振替取引を除外（デフォルトTrue）
    """
    with session_from_cookie_file(COOKIE_FILE) as s:
        acts, total_count = _fetch_all_transactions(
            s,
            keyword=keyword,
            base_date=base_date,
            select_category=select_category,
            max_size=min(size, 1000),
            exclude_transfers=exclude_transfers,
            is_income=is_income,
        )
    return {"transactions": acts, "total_count": total_count, "fetched_count": len(acts)}


@mcp.tool()
def get_uncategorized_transactions(
    size: int = 200,
    exclude_transfers: bool = True,
    exclude_income: bool = False,
) -> dict:
    """未分類（大カテゴリ未設定）の取引一覧を取得する。

    カテゴリ管理タスクの起点として使う。
    取得後に find_category_by_name でIDを調べ、bulk_set_category で一括更新する。

    Args:
        size: 最大取得件数（デフォルト200）
        exclude_transfers: True=振替取引を除外
        exclude_income: True=収入取引を除外（支出のみ）
    """
    with session_from_cookie_file(COOKIE_FILE) as s:
        acts, total_count = _fetch_all_transactions(
            s,
            select_category=0,
            max_size=min(size, 1000),
            exclude_transfers=exclude_transfers,
            is_income=False if exclude_income else None,
        )
    return {"transactions": acts, "total_count": total_count, "fetched_count": len(acts)}


@mcp.tool()
def get_account_summaries(sub_type: str | None = None) -> list[dict]:
    """口座一覧と現在残高を取得する。

    残高検証に使う sub_account_id_hash を調べる際に利用する。
    銀行口座のみ取得したい場合は sub_type="銀行口座" を指定。

    Args:
        sub_type: サブアカウントタイプでフィルタ（例: "銀行口座", "証券"）
    """
    with session_from_cookie_file(COOKIE_FILE) as s:
        raw = request_account_summaries(s)

    accounts = []
    for account in raw.get("accounts", []):
        service_name = account.get("service", {}).get("service_name", "")
        service_category_id = account.get("service_category_id")
        for sub in account.get("sub_accounts", []):
            entry = {
                "sub_account_id_hash": sub.get("sub_account_id_hash"),
                "service_name": service_name,
                "service_category_id": service_category_id,
                "sub_name": sub.get("sub_name", ""),
                "sub_type": sub.get("sub_type", ""),
                "disp_name": sub.get("disp_name", ""),
            }
            # 残高情報
            for det in sub.get("user_asset_det_summaries", []):
                entry["balance"] = det.get("value")
                entry["currency"] = det.get("currency_code", "JPY")
                break  # 通常は1件

            if sub_type and sub_type not in entry.get("sub_type", ""):
                continue
            accounts.append(entry)

    return accounts


@mcp.tool()
def get_transactions_by_account(
    sub_account_id_hash: str,
    date_from: str,
    date_to: str,
) -> dict:
    """口座を指定して取引一覧と期首・期末残高を取得する。

    残高整合性検証の主要な情報源。365日を超える期間は内部で分割する。

    Args:
        sub_account_id_hash: 口座ハッシュ（get_account_summaries で取得）
        date_from: 開始日 "YYYY-MM-DD"
        date_to: 終了日 "YYYY-MM-DD"
    """
    dt_from = datetime.strptime(date_from, "%Y-%m-%d")
    dt_to = datetime.strptime(date_to, "%Y-%m-%d")

    all_acts: list[dict] = []
    balance_start = None
    balance_end = None

    # cf_term_data は最大365日ずつに分割
    cursor = dt_from
    while cursor <= dt_to:
        chunk_end = min(cursor + timedelta(days=364), dt_to)

        with session_from_cookie_file(COOKIE_FILE) as s:
            raw = request_cf_term_data_by_sub_account(
                s, sub_account_id_hash, date_from=cursor, date_to=chunk_end
            )

        # 期首残高は最初のチャンクのみ取得
        if balance_start is None:
            balance_start = raw.get("start_balance") or raw.get("balance_start") or raw.get("opening_balance")
        balance_end = raw.get("end_balance") or raw.get("balance_end") or raw.get("closing_balance")

        for e in raw.get("user_asset_acts", []):
            act = e.get("user_asset_act", e)
            all_acts.append({
                "id": act.get("id"),
                "recognized_at": act.get("recognized_at"),
                "content": act.get("content"),
                "amount": act.get("amount"),
                "is_income": act.get("is_income"),
                "is_transfer": act.get("is_transfer"),
                "is_target": act.get("is_target"),
                "large_category_id": act.get("large_category_id"),
                "middle_category_id": act.get("middle_category_id"),
                "memo": act.get("memo"),
            })

        cursor = chunk_end + timedelta(days=1)

    return {
        "sub_account_id_hash": sub_account_id_hash,
        "date_from": date_from,
        "date_to": date_to,
        "balance_start": balance_start,
        "balance_end": balance_end,
        "transaction_count": len(all_acts),
        "transactions": all_acts,
        "note": "balance_start/balance_end が None の場合は cf_term_data API のフィールド名を要確認",
    }


# === 取引更新 ===

@mcp.tool()
def set_transaction_category(
    transaction_id: int,
    large_category_id: int,
    middle_category_id: int,
    memo: str | None = None,
) -> dict:
    """1件の取引のカテゴリを更新する。

    複数件まとめて更新する場合は bulk_set_category を使うと効率的。

    Args:
        transaction_id: 取引ID
        large_category_id: 大カテゴリID（find_category_by_name で取得）
        middle_category_id: 中カテゴリID（find_category_by_name で取得）
        memo: メモ（省略可）
    """
    with session_from_cookie_file(COOKIE_FILE) as s:
        csrf = get_csrf_token(s)
        request_update_user_asset_act(
            s, csrf, transaction_id,
            large_category_id=large_category_id,
            middle_category_id=middle_category_id,
            memo=memo,
        )
    return {"success": True, "transaction_id": transaction_id}


@mcp.tool()
def bulk_set_category(
    transaction_ids: list[int],
    large_category_id: int,
    middle_category_id: int,
) -> dict:
    """複数取引のカテゴリを一括更新する。

    内部で100件ずつバッチ処理する。同じカテゴリに分類できる取引を
    まとめて更新する際に使う。

    Args:
        transaction_ids: 取引IDのリスト
        large_category_id: 大カテゴリID
        middle_category_id: 中カテゴリID
    """
    with session_from_cookie_file(COOKIE_FILE) as s:
        request_transactions_category_bulk_updates(
            s, large_category_id, middle_category_id, transaction_ids
        )
    return {"success": True, "updated_count": len(transaction_ids)}


@mcp.tool()
def set_transaction_memo(transaction_id: int, memo: str) -> dict:
    """取引のメモを更新する（カテゴリは変更しない）。

    Args:
        transaction_id: 取引ID
        memo: 設定するメモ文字列
    """
    with session_from_cookie_file(COOKIE_FILE) as s:
        csrf = get_csrf_token(s)
        request_update_user_asset_act(s, csrf, transaction_id, memo=memo)
    return {"success": True, "transaction_id": transaction_id}


# === 集計 ===

@mcp.tool()
def summarize_transactions(
    date_from: str,
    date_to: str,
    group_by: Literal["month", "large_category", "middle_category", "account"] = "month",
    is_income: bool | None = None,
    exclude_transfers: bool = True,
) -> dict:
    """取引を集計してグループ別合計を返す。

    例: 「2025年の食費を月別に」→ group_by="month" + get_transactions でカテゴリ絞り込み後に呼ぶ

    Args:
        date_from: 集計開始日 "YYYY-MM-DD"
        date_to: 集計終了日 "YYYY-MM-DD"
        group_by: グループキー（"month" / "large_category" / "middle_category" / "account"）
        is_income: True=収入のみ, False=支出のみ, None=両方
        exclude_transfers: True=振替取引を除外
    """
    with session_from_cookie_file(COOKIE_FILE) as s:
        acts, total_count = _fetch_all_transactions(
            s,
            base_date=date_to,  # base_date 以前の取引を取得
            max_size=5000,
            exclude_transfers=exclude_transfers,
            is_income=is_income,
        )

    # date_from〜date_to でフィルタ
    dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
    dt_to = datetime.strptime(date_to, "%Y-%m-%d").date()

    def parse_date(s: str):
        try:
            return datetime.fromisoformat(s).date()
        except Exception:
            return None

    acts = [
        a for a in acts
        if (d := parse_date(a.get("recognized_at", ""))) and dt_from <= d <= dt_to
    ]

    if not acts:
        return {"summary": [], "date_from": date_from, "date_to": date_to, "total_count": 0}

    df = pd.DataFrame(acts)

    group_key_map = {
        "month": lambda df: df["recognized_at"].str[:7],           # "YYYY-MM"
        "large_category": lambda df: df["large_category"],
        "middle_category": lambda df: df["middle_category"],
        "account": lambda df: df["account.service.service_name"],
    }

    if group_by not in group_key_map:
        return {"error": f"Unknown group_by: {group_by}. Must be one of {list(group_key_map)}"}

    df["_group"] = group_key_map[group_by](df)
    grouped = df.groupby("_group").agg(
        total_amount=("amount", "sum"),
        transaction_count=("id", "count"),
    ).reset_index().rename(columns={"_group": "group_key"})

    summary = grouped.sort_values("total_amount").to_dict(orient="records")

    return {
        "date_from": date_from,
        "date_to": date_to,
        "group_by": group_by,
        "summary": summary,
        "total_count": len(acts),
    }




# -----------------------------------------------------------------------
# エントリーポイント
# -----------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run()
