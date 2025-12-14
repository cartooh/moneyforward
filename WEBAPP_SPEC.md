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
    *   画面下部に「変更メニュー」エリアが表示される。
*   **操作**:
    *   任意の行を複数選択可能。
    *   ヘッダー等の「キャンセル」または「完了」で通常モードへ戻る。
    *   **注記**: 具体的な一括編集処理（カテゴリ変更APIの呼び出し等）の実装詳細は別途指示待ち。今回はボタン配置のみ検討。

## 5. データ連携
*   **認証**: サーバー上の `mf_cookies.pkl` を読み込んでセッションを確立。
*   **API**: `moneyforward_api.py` の `request_user_asset_acts` を使用。
*   **データ変換**: `moneyforward_utils.py` の `append_row_form_user_asset_acts` を使用してデータを抽出し、JSON形式に変換してフロントエンドに返す。
*   **APIレスポンス構造 (`/api/acts`)**:
    ```json
    {
      "acts": [
        {
          "id": 123456789,
          "is_transfer": false,
          "is_income": false,
          "is_target": true,
          "updated_at": "2025-11-30T12:34:56+09:00",
          "content": "コンビニ",
          "amount": -500,
          "large_category_id": 1,
          "large_category": "食費",
          "middle_category_id": 10,
          "middle_category": "食料品",
          "account.service.service_name": "財布",
          "sub_account.sub_type": "wallet",
          "sub_account.sub_name": "現金"
        },
        ...
      ],
      "total_count": 100
    }
    ```

## 6. 制約事項
*   `moneyforward.py` はインポートしない（SQLite依存回避のため）。
