# MoneyForward Web Application 仕様書

## 1. 概要
既存の `moneyforward_api.py` をバックエンドとして利用し、iPhoneでの操作を想定したモダンなUIを持つWebアプリケーションを構築する。
主に入出金履歴（`user_asset_acts`）の閲覧、検索、および一括編集機能を提供する。
**グラフ表示機能は今回の実装範囲外とする。**

## 2. 技術スタック
*   **Backend**: Python, Flask
*   **Frontend**: HTML5, CSS3 (Tailwind CSS), JavaScript (Vanilla JS)
*   **Icon**: FontAwesome (Free CDN)
*   **Data Source**: 
    *   `moneyforward_api.py`: APIラッパー
    *   `moneyforward_utils.py`: データ変換・ユーティリティ (`traverse`, `convert_user_asset_act_to_dict` 等)
    *   `mf_cookies.pkl`: 認証情報

## 3. ファイル構成
```text
moneyforward/
  ├── moneyforward_api.py   (既存: APIラッパー)
  ├── moneyforward_utils.py (既存: ユーティリティ)
  ├── webapp.py             (新規: Flask Webサーバー)
  ├── templates/            (新規: HTMLテンプレート)
  │   └── index.html        (一覧・検索・編集画面)
  └── static/               (新規: 静的ファイル)
      ├── style.css         (カスタムCSS)
      └── script.js         (フロントエンドロジック)
```

## 4. 画面・機能要件

### 4.1. メイン画面 (一覧表示)
*   **レイアウト**: iPhone縦画面を想定したシングルカラムレイアウト。
*   **ヘッダー**:
    *   検索ボックス（キーワード入力）を常時表示。
    *   「キャンセル」ボタン（検索クリア等）。
    *   右上に「鉛筆マーク」ボタン（編集モード切替）。
*   **リスト表示**:
    *   **グルーピング**: 日付（例：2025年11月30日 (日)）ごとに明細をグループ化して表示。
    *   **各行の構成**:
        *   左側: 大カテゴリに対応するアイコン（FontAwesome）。
        *   中央: 内容（摘要）。
        *   右側: 金額（円マーク付き）。
    *   **無限スクロール**:
        *   初期表示20件。
        *   スクロール最下部到達時に追加で20件を非同期読み込み。
*   **カテゴリ表示**:
    *   大カテゴリIDに基づき、FontAwesomeのアイコンをマッピングして表示（例: 食費→`fa-utensils`）。
    *   アイコンマッピングはフロントエンドまたはバックエンドで定義。

### 4.2. 検索機能
*   **キーワード検索**: ヘッダーの入力欄でリアルタイムまたはEnterで検索実行。
*   **詳細検索（オプション）**:
    *   検索ボックス付近のUI（フィルターアイコン等）から呼び出し。
    *   **設定項目**:
        *   期間（`base_date` 等）。
        *   カテゴリ（大項目のみ）。
        *   フラグ設定:
            *   `is_new` {0, 1}
            *   `is_old` {0, 1}
            *   `is_continuous` {0, 1}
    *   **カテゴリ選択UI**:
        *   全大項目をリスト表示（C案）。
        *   `moneyforward_api.request_large_categories` を使用して取得。

### 4.3. 編集モード（一括編集）
*   **起動**: 右上の「鉛筆マーク」タップでモード移行。
*   **UI変化**:
    *   各行の左端にチェックボックスが出現（アニメーション等でスムーズに）。
    *   画面下部に「変更メニュー」エリア（フッター）が表示される。
*   **操作**:
    *   任意の行を複数選択可能。
    *   フッターの「カテゴリ変更」ボタン押下でカテゴリ選択モーダルを表示。
    *   ヘッダー等の「キャンセル」または「完了」で通常モードへ戻る。

### 4.4. カテゴリ一括変更機能
*   **UI**: 全画面風モーダルウィンドウ。
*   **カテゴリ選択フロー (ドリルダウン形式)**:
    1.  **初期表示**:
        *   検索ボックス（中項目インクリメンタル検索）。
        *   「最近使用したカテゴリ」（LocalStorage履歴）。
        *   大項目一覧リスト。
    2.  **大項目選択後**:
        *   画面が遷移し、ヘッダーに「戻る」ボタンと選択した大項目名を表示。
        *   該当する中項目一覧リストを表示。
    3.  **中項目選択 (確定)**:
        *   APIを実行して更新。
        *   LocalStorageに選択したカテゴリを保存（LRU）。
        *   モーダルを閉じ、Toast通知を表示（「〇件更新しました」）。
        *   **注記**: 画面のリロードは行わない。
*   **「最近使用したカテゴリ」の仕様**:
    *   **保存対象**: ユーザーが選択して更新を実行した「中項目」。
    *   **保存場所**: ブラウザの LocalStorage。
    *   **保存件数**: 最大5件。
    *   **更新ロジック**:
        *   更新実行時に、選択された中項目をリストの先頭に追加。
        *   既にリストに存在する場合は、既存の項目を削除して先頭に追加（順序更新）。
        *   5件を超える場合は、最も古い項目を削除（LRU方式）。
*   **検索挙動**:
    *   検索ボックスに入力時、階層を無視してマッチする中項目をフラットなリストで表示する。
    *   表示形式: `大項目名 > 中項目名`

### 4.5. 詳細・編集機能
*   **起動**: 通常モード（編集モードOFF）にて、一覧の行（明細）をタップすると詳細モーダルを表示。
*   **UI**:
    *   **ヘッダー**: 「入出金明細」タイトル、閉じるボタン。
    *   **表示項目**:
        *   金額（大きく表示）。
        *   大カテゴリ・中カテゴリ（タップで変更可能）。
        *   日付。
        *   内容（摘要）。
        *   口座・保有資産情報。
        *   計算対象フラグ（トグルスイッチ）。
        *   メモ（テキストエリア）。
    *   **振替設定エリア**:
        *   現在の状態（支出/収入/振替）を表示。
        *   「振替」への切り替え、または「振替解除」ボタン。
        *   振替の場合、振替元/振替先の口座情報を表示。
    *   **フッター**: 「保存」ボタン。

*   **振替設定フロー**:
    1.  詳細画面で「振替に変更」または振替情報の編集ボタンをタップ。
    2.  **振替設定サブ画面（またはモーダル）**を表示。
        *   API `request_manual_user_asset_act_partner_sources` をコールして候補を取得。
        *   **推奨候補**: `partner_candidate_acts` がある場合、優先表示（日付・金額一致）。
        *   **全口座リスト**: その他の口座を選択可能。
    3.  口座を選択して「決定」。
    4.  API `request_change_transfer` を実行（即時反映）。
    5.  詳細画面に戻り、表示を更新（振替状態になる）。

*   **振替解除フロー**:
    1.  詳細画面で「振替解除」ボタンをタップ。
    2.  API `request_clear_transfer` を実行（即時反映）。
    3.  詳細画面の表示を更新（支出または収入に戻る）。

*   **その他の更新フロー**:
    1.  カテゴリ、計算対象、メモを編集。
    2.  「保存」ボタンタップ。
    3.  API `get_csrf_token` でトークン取得（セッション内でキャッシュ可）。
    4.  API `request_update_user_asset_act` を実行。
    5.  成功時、モーダルを閉じ、一覧データを更新（再取得またはローカル更新）。

## 5. データ連携
*   **認証**: サーバー上の `mf_cookies.pkl` を読み込んでセッションを確立。
*   **API**: 
    *   参照: `moneyforward_api.py` の `request_user_asset_acts`
    *   更新: `moneyforward_api.py` の `request_transactions_category_bulk_updates`
*   **データ変換**: `moneyforward_utils.py` の `append_row_form_user_asset_acts` を使用してデータを抽出し、JSON形式に変換してフロントエンドに返す。
*   **APIエンドポイント仕様**:
    *   **GET `/api/acts`**: 取引履歴取得
        *   レスポンス構造:
            ```json
            {
              "acts": [
                {
                  "id": "123456789",
                  "is_transfer": 0,
                  "is_income": 0,
                  "is_target": 1,
                  "updated_at": "2025-11-30T12:34:56+09:00",
                  "content": "コンビニ",
                  "amount": "-500",
                  "large_category_id": "1",
                  "large_category": "食費",
                  "middle_category_id": "10",
                  "middle_category": "食料品",
                  "account.service.service_name": "財布",
                  "sub_account.sub_type": "wallet",
                  "sub_account.sub_name": "現金"
                },
                ...
              ],
              "total_count": 100,
              "fetched_count": 20
            }
            ```
            ※ `id`, `amount`, `large_category_id`, `middle_category_id` はJavaScriptでの精度落ちを防ぐため文字列として返す。
            ※ `is_...` フラグは数値 (0/1) で返す。

    *   **POST `/api/bulk_update_category`**: カテゴリ一括更新
        *   リクエストボディ (JSON):
            ```json
            {
              "ids": ["123456789", "987654321"],
              "large_category_id": 1,
              "middle_category_id": 10
            }
            ```
            ※ `ids` は文字列の配列でも可（サーバー側で数値に変換）。
        *   レスポンス:
            ```json
            {
              "status": "success",
              "updated_count": 2
            }
            ```
            ※エラー時は `{"status": "error", "message": "エラーメッセージ"}` を返す。

    *   **GET `/api/act/<id>/partner_sources`**: 振替候補取得
        *   `moneyforward_api.request_manual_user_asset_act_partner_sources` をラップ。
        *   レスポンス: `manual_user_asset_act_partner_sources.json` の内容。

    *   **POST `/api/act/<id>/transfer`**: 振替設定
        *   リクエストボディ:
            ```json
            {
              "partner_account_id_hash": "...",
              "partner_sub_account_id_hash": "...",
              "partner_act_id": "..." (optional)
            }
            ```
        *   `moneyforward_api.request_change_transfer` をラップ。

    *   **DELETE `/api/act/<id>/transfer`**: 振替解除
        *   `moneyforward_api.request_clear_transfer` をラップ。

    *   **PUT `/api/act/<id>`**: 明細更新（カテゴリ、メモ、フラグ）
        *   リクエストボディ:
            ```json
            {
              "large_category_id": "...",
              "middle_category_id": "...",
              "is_target": 1,
              "memo": "..."
            }
            ```
        *   `moneyforward_api.request_update_user_asset_act` をラップ。
        *   サーバー側で `get_csrf_token` を自動処理。

## 6. 制約事項
*   `moneyforward.py` はインポートしない（SQLite依存回避のため）。
