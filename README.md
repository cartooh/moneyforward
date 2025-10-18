# moneyforward

MoneyForward にある家計簿の取得や更新をするツール

## 概要

- 家計簿の取得・検索
- 家計簿のカテゴリの一括変更

## 依存パッケージのインストール

```powersshell
uv sync
```

## 前提

認証に関しては、パスキーは対象外。二段階認証には対応

## コマンドライン

### ユーザ情報の保存

keyring に ユーザ名(例：hoge@gmail.com) に対するパスワードを保存

```powershell
> uv run keyring set moneyforwad hoge@gmail.com
Password for 'hoge@gmail.com' in 'moneyforwad':
```

### Money Forward の認証済みセッションの保存

ユーザ名を指定して、セッション情報を取得。
二段階認証の場合メールで受信したワンタイムパスワード(OTP)を追加入力。
なお、タイミングの問題が解消できず、ブラウザの非表示ができず、一瞬表示されるが、ご愛嬌。

```powershell
> uv run start_mf_session.py hoge@gmail.com
Enter OTP: xxxxxx
Login successful
Save session: mf_cookies.pkl
```

### 大項目・中項目の項目と ID を取得

```powershell
> uv run moneyforward.py large_categories --csv large_categories.csv
> type large_categories.csv
large_category_id,large_category_name,middle_category_id,middle_category_name,user_category
1,収入,1,給与,False
1,収入,2,一時所得,False
1,収入,3,事業・副業,False
1,収入,4,年金,False
1,収入,89,配当所得,False
1,収入,90,不動産所得,False
1,収入,104,不明な入金,False
1,収入,5,その他入金,False
・・・
```

### 大項目・中項目の項目と ID を検索

```powershell
> uv run moneyforward.py search_category -m 雑費
large_category_id large_category_name middle_category_id middle_category_name user_category
18 その他 68 雑費 False

> uv run moneyforward.py search_category -l 特別な支出
large_category_id large_category_name middle_category_id middle_category_name user_category
16 特別な支出 67 家具・家電 False
16 特別な支出 66 住宅・リフォーム False
16 特別な支出 88 その他特別な支出 False

> uv run moneyforward.py search_category -m コンビニ
large_category_id large_category_name middle_category_id middle_category_name user_category
11 食費 5114500 コンビニ True
```

### 内容を指定して家計簿の一覧を保存

```powershell
> uv run moneyforward.py user_asset_acts --keyword ローソン --csv ローソン.csv
```

### 最新の 1000 件の家計簿を保存

```powershell
> uv run moneyforward.py user_asset_acts --size 1000 --csv list1000.csv --is_continuous 1
INFO : 2021-03-15 01:04:00,713 : get_user_asset_acts: size = 1000
INFO : 2021-03-15 01:04:03,513 : total_count: 500
INFO : 2021-03-15 01:04:03,514 : get_user_asset_acts: size = 500
INFO : 2021-03-15 01:04:05,759 : total_count: 500
```

### 保存した家計簿(csv)から変更したい項目を検索

```powershell
> uv run moneyforward.py filter_db --csv .\ローソン.csv -E ポイント利用分 --columns id middle_category content -M コンビニ
                   id middle_category     content
7   1773433095027622751             食料品  ローソン阪急石橋駅前
70  1627937953777188876             食料品   ローソン大阪茶屋町
76  1623861165627016065             食料品  ローソン阪急石橋駅前
```

### 検索した項目の大項目・中項目を更新

```powershell
> uv run moneyforward.py filter_db --csv .\ローソン.csv -E ポイント利用分 --columns id middle_category content -M コンビニ -u コンビニ
```

### 更新した家計簿の中項目を確認

```
> uv run moneyforward.py user_asset_acts --keyword  ローソン阪急石橋駅前 --list --list_header id content middle_category
1773433095027622751 ローソン阪急石橋駅前 コンビニ
1623861165627016065 ローソン阪急石橋駅前 コンビニ

> uv run moneyforward.py user_asset_acts --keyword  ローソン大阪 --list --list_header id content large_category middle_category
1627937953777188876 ローソン大阪茶屋町 食費 コンビニ
1627937953777123340 ローソン大阪茶屋町(ポイント利用分) 収入 キャッシュバック
```

## コマンドライン引数

```powersshell
  > uv run .\moneyforward.py -h
usage: moneyforward.py [-h] [-c MF_COOKIES] [-d] [--cache_category_csv CACHE_CATEGORY_CSV] [--force_category_update]
                       {category,large_categories,account_summaries,liabilities,smartphone_asset,sub_account_groups,change_group,manual_user_asset_act_partner_sources,service_detail,accounts,cf_sum_by_sub_account,cf_term_data_by_sub_account,cf_term_data,add_dummy_data_to_user_asset_act,add_dummy_offset_data_to_user_asset_act,update_user_asset_act,update_enable_transfer,update_disable_transfer,change_transfer,clear_transfer,search_category,user_asset_act_by_id,user_asset_acts_by_ids,user_asset_acts,update_sqlite_db,filter_db,transactions_category_bulk_updates,bulk_update_category,bulk_update_category2} ...

positional arguments:
  {category,large_categories,account_summaries,liabilities,smartphone_asset,sub_account_groups,change_group,manual_user_asset_act_partner_sources,service_detail,accounts,cf_sum_by_sub_account,cf_term_data_by_sub_account,cf_term_data,add_dummy_data_to_user_asset_act,add_dummy_offset_data_to_user_asset_act,update_user_asset_act,update_enable_transfer,update_disable_transfer,change_transfer,clear_transfer,search_category,user_asset_act_by_id,user_asset_acts_by_ids,user_asset_acts,update_sqlite_db,filter_db,transactions_category_bulk_updates,bulk_update_category,bulk_update_category2}

options:
  -h, --help            show this help message and exit
  -c, --mf_cookies MF_COOKIES
  -d, --debug
  --cache_category_csv CACHE_CATEGORY_CSV
  --force_category_update
```

```powersshell
  > uv run moneyforward.py category -h
usage: moneyforward.py category [-h] [--json JSON]

options:
  -h, --help            show this help message and exit
  --json JSON
```

```powersshell
  > uv run moneyforward.py large_categories -h
usage: moneyforward.py large_categories [-h] [--json JSON | --csv CSV | --sqlite SQLITE]

options:
  -h, --help            show this help message and exit
  --json JSON
  --csv CSV
  --sqlite SQLITE
```

```powersshell
  > uv run moneyforward.py search_category -h
usage: moneyforward.py search_category [-h] [--cache_csv CACHE_CSV] [--force_update] [-l LARGE] [-m MIDDLE]

options:
  -h, --help            show this help message and exit
  --cache_csv CACHE_CSV
  --force_update
  -l, --large LARGE
  -m, --middle MIDDLE
```

```powersshell
  > uv run moneyforward.py user_asset_acts -h
usage: moneyforward.py user_asset_acts [-h] [--json JSON | --csv CSV | --list] [--offset OFFSET] [--size SIZE] [--is_new {0,1}]
                                       [--is_old {0,1}] [--is_continuous {0,1}] [--select_category SELECT_CATEGORY]
                                       [--base_date BASE_DATE] [--keyword KEYWORD] [--list_header LIST_HEADER [LIST_HEADER ...]]

options:
  -h, --help            show this help message and exit
  --json JSON
  --csv CSV
  --list
  --offset OFFSET
  --size SIZE
  --is_new {0,1}
  --is_old {0,1}
  --is_continuous {0,1}
  --select_category SELECT_CATEGORY
  --base_date BASE_DATE
  --keyword KEYWORD
  --list_header LIST_HEADER [LIST_HEADER ...]
```

```powersshell
  > uv run moneyforward.py filter_db -h
usage: moneyforward.py filter_db [-h] (--csv CSV | --sqlite cf_term_data.db) [--sqlite_table SQLITE_TABLE]
                                 [--columns COLUMNS [COLUMNS ...]] [--sort column [column ...]] [--list | --output_csv OUTPUT_CSV |
                                 -u UPDATE_CATEGORY_NAME | -U large_category_id middle_category_id | -d | --list_id |
                                 --update_transfer {0,1} | --update_partner_account account_id_hash sub_account_id_hash] [-q QUERY]
                                 [-r] [-p pattern [pattern ...]] [-E pattern [pattern ...]] [-i] [--is_income {0,1}]
                                 [--is_transfer {0,1}] [-b DATE_FROM] [-e DATE_TO] [--null_memo | --not_null_memo |
                                 --match_memo memo [memo ...] | --not_match_memo memo [memo ...]] [-m category [category ...] |
                                 -M category [category ...]] [-l category [category ...] | -L category [category ...]]
                                 [-s service_name [service_name ...] | -S service_name [service_name ...]]
                                 [-t sub_account [sub_account ...] | -T sub_account [sub_account ...]] [--lt amount | --le amount |
                                 --gt amount | --ge amount]

options:
  -h, --help            show this help message and exit
  --csv CSV
  --sqlite cf_term_data.db
  --sqlite_table SQLITE_TABLE
  --columns COLUMNS [COLUMNS ...]
  --sort column [column ...]
  --list
  --output_csv OUTPUT_CSV
  -u, --update_category_name UPDATE_CATEGORY_NAME
  -U, --update_category large_category_id middle_category_id
  -d, --update_sqlite_db
  --list_id
  --update_transfer {0,1}
  --update_partner_account account_id_hash sub_account_id_hash
  -q, --query QUERY     ex) content.notnull() and content.str.match('セブン') and middle_category != 'コンビニ'

group_filter_pattern:
  -r, --reverse
  -p, --patterns pattern [pattern ...]
                        ex) ".*" / "^タイムズ"
  -E, --exclude_patterns pattern [pattern ...]
  -i, --ignore_invalid_data
  --is_income {0,1}
  --is_transfer {0,1}
  -b, --date_from DATE_FROM
  -e, --date_to DATE_TO
  --null_memo
  --not_null_memo
  --match_memo memo [memo ...]
  --not_match_memo memo [memo ...]
  -m, --match_middle_categories category [category ...]
  -M, --not_match_middle_categories category [category ...]
  -l, --match_large_categories category [category ...]
  -L, --not_match_large_categories category [category ...]
  -s, --match_service_name service_name [service_name ...]
  -S, --not_match_service_name service_name [service_name ...]
  -t, --match_sub_account sub_account [sub_account ...]
  -T, --not_match_sub_account sub_account [sub_account ...]
  --lt amount           less then [amount]
  --le amount           less then or equal to [amount]
  --gt amount           greater then [amount]
  --ge amount           greater then or equal to [amount]
```
