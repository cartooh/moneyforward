# moneyforward
=============================================================
MoneyForwardにある家計簿の取得や更新をするツール

概要
-----
- 家計簿の取得・検索
- 家計簿のカテゴリの一括変更

依存パッケージのインストール
----
```
pipenv install
```

コマンドライン
----

- ユーザ情報の保存

  ```sh
  > keyring set moneyforwad hoge@gmail.com
  Password for 'hoge@gmail.com' in 'moneyforwad':
  ```

- Money Forwardの認証済みセッションの保存

  ```shell
  > python start_mf_session.py hoge@gmail.com
  
  DevTools listening on ws://127.0.0.1:22291/devtools/browser/02850e21-30e4-4dea-905d-fe3228503832
  Save session: mf_cookies.pkl
  ```

- 大項目・中項目の項目とIDを取得

  ```
  > python moneyforward.py large_categories --csv large_categories.csv
  > chcp 65001
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

- 大項目・中項目の項目とIDを検索

  ```
  > python moneyforward.py search_category -m 雑費
  large_category_id large_category_name middle_category_id middle_category_name user_category
  18 その他 68 雑費 False
  
  > python moneyforward.py search_category -l 特別な支出
  large_category_id large_category_name middle_category_id middle_category_name user_category
  16 特別な支出 67 家具・家電 False
  16 特別な支出 66 住宅・リフォーム False
  16 特別な支出 88 その他特別な支出 False
  
  > python moneyforward.py search_category -m コンビニ
  large_category_id large_category_name middle_category_id middle_category_name user_category
  11 食費 5114500 コンビニ True
  ```

- 内容を指定して家計簿の一覧を保存

  ```
  > python moneyforward.py user_asset_acts --keyword ファミリーマート --csv ファミリーマート.csv
  ```

- 最新の1000件の家計簿を保存

  ```
  > python moneyforward.py user_asset_acts --size 1000 --csv list1000.csv --is_continuous 1
  INFO : 2021-03-15 01:04:00,713 : get_user_asset_acts: size = 1000
  INFO : 2021-03-15 01:04:03,513 : total_count: 500
  INFO : 2021-03-15 01:04:03,514 : get_user_asset_acts: size = 500
  INFO : 2021-03-15 01:04:05,759 : total_count: 500
  ```

- 保存した家計簿(csv)から変更したい項目を検索

  ```
  >python moneyforward.py filter_csv ファミリーマート.csv -p "うかい|グランフロント" -M コンビニ --columns id content middle_category
              id               content middle_category
  33  8031350080     ファミリーマートグランフロント大阪             食料品
  36  8031350072  ファミリーマートうかいやポートアイランド             食料品
  ```

- 検索した項目の大項目・中項目を更新

  ```
  > python moneyforward.py filter_csv ファミリーマート.csv -p "うかい|グランフロント" -M コンビニ --update_category 11 5114500
  ```

- 更新した家計簿の中項目を確認

  ```
  > python moneyforward.py user_asset_acts --keyword うかいや --list --list_header id content middle_category
  8031350072 ファミリーマートうかいやポートアイランド コンビニ
  ```

  



コマンドライン引数
----

- 
  ```
  >python moneyforward.py -h
  usage: moneyforward.py [-h] [-c MF_COOKIES] {category,large_categories,search_category,user_asset_acts,filter_csv,transactions_category_bulk_updates} ...
  
  positional arguments:
    {category,large_categories,search_category,user_asset_acts,filter_csv,transactions_category_bulk_updates}
  
  optional arguments:
    -h, --help            show this help message and exit
    -c MF_COOKIES, --mf_cookies MF_COOKIES
  ```

- ```
  >python moneyforward.py category -h
  usage: moneyforward.py category [-h] [--json JSON]
  
  optional arguments:
    -h, --help   show this help message and exit
    --json JSON
  ```

- ```
  >python moneyforward.py large_categories -h
  usage: moneyforward.py large_categories [-h] [--csv CSV | --json JSON]
  
  optional arguments:
    -h, --help   show this help message and exit
    --csv CSV
    --json JSON
  ```

- ```
  >python moneyforward.py search_category -h
  usage: moneyforward.py search_category [-h] [--cache_csv CACHE_CSV] [--force_update] [-l LARGE] [-m MIDDLE]
  
  optional arguments:
    -h, --help            show this help message and exit
    --cache_csv CACHE_CSV
    --force_update
    -l LARGE, --large LARGE
    -m MIDDLE, --middle MIDDLE
  ```

- ```
  >python moneyforward.py user_asset_acts -h
  usage: moneyforward.py user_asset_acts [-h] [--csv CSV | --json JSON | --list] [--offset OFFSET] [--size SIZE] [--is_new {0,1}] [--is_old {0,1}] [--is_continuous {0,1}] [--select_category SELECT_CATEGORY] [--base_date BASE_DATE] [--keyword KEYWORD] [--list_header LIST_HEADER [LIST_HEADER ...]]
  
  optional arguments:
    -h, --help            show this help message and exit
    --csv CSV
    --json JSON
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

- ```
  >python moneyforward.py filter_csv -h
  usage: moneyforward.py filter_csv [-h] [--list | --output_csv OUTPUT_CSV | --update_category large_category_id middle_category_id] (-q QUERY | -p PATTERN) [-r] [-m category [category ...] | -M category [category ...]] [-l category [category ...] | -L category [category ...]]
                                    [-s service_name [service_name ...] | -S service_name [service_name ...]] [-t sub_account [sub_account ...] | -T sub_account [sub_account ...]]
                                    input_csv
  
  positional arguments:
    input_csv
  
  optional arguments:
    -h, --help            show this help message and exit
    --list
    --output_csv OUTPUT_CSV
    --update_category large_category_id middle_category_id
    -q QUERY, --query QUERY
                          ex) content.notnull() and content.str.match('セブン') and middle_category != 'コンビニ'
    -p PATTERN, --pattern PATTERN
  
  group_filter_pattern:
    -r, --reverse
    -m category [category ...], --match_middle_categories category [category ...]
    -M category [category ...], --not_match_middle_categories category [category ...]
    -l category [category ...], --match_large_categories category [category ...]
    -L category [category ...], --not_match_large_categories category [category ...]
    -s service_name [service_name ...], --match_service_name service_name [service_name ...]
    -S service_name [service_name ...], --not_match_service_name service_name [service_name ...]
    -t sub_account [sub_account ...], --match_sub_account sub_account [sub_account ...]
    -T sub_account [sub_account ...], --not_match_sub_account sub_account [sub_account ...]
  ```

- ```
  >python moneyforward.py transactions_category_bulk_updates -h
  usage: moneyforward.py transactions_category_bulk_updates [-h] -m MIDDLE_CATEGORY_ID -l LARGE_CATEGORY_ID -i IDS [IDS ...]
  
  optional arguments:
    -h, --help            show this help message and exit
    -m MIDDLE_CATEGORY_ID, --middle_category_id MIDDLE_CATEGORY_ID
    -l LARGE_CATEGORY_ID, --large_category_id LARGE_CATEGORY_ID
    -i IDS [IDS ...], --ids IDS [IDS ...]
  ```
