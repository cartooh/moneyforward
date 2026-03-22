# MoneyForward MCP サーバー 設計書

## 1. 概要・背景

### 目的
MoneyForwardの取引データをClaude Desktop（dispatch）から自然言語で操作できるようにする。

従来はCLI（`moneyforward.py`）やWebアプリ（`webapp.py`）で操作していたが、
MCP（Model Context Protocol）サーバーを立てることで、Claude自身が文脈に応じて
「未分類を探して分類する」「残高を検証する」といった複合的なタスクを自律的に実行できる。

### 解決する課題

| 課題 | 現状 | MCPで解決 |
|------|------|-----------|
| 未カテゴリ取引の分類 | 手動でWebアプリ操作 | Claudeが一覧取得→分類候補提示→一括更新 |
| カテゴリ整合性確認 | 目視 | Claudeが自動チェック・レポート |
| 月次集計 | CLIコマンド実行 | 自然言語で「先月の食費は？」と聞く |
| 残高整合性確認 | 未実装 | 取引合計と実残高の自動照合 |

---

## 2. アーキテクチャ

```
Claude Desktop (dispatch)
      │
      │ MCP プロトコル（stdio transport）
      │
┌─────▼──────────────────────────────┐
│  mcp_server.py                     │
│  ・FastMCP デコレータでツール定義   │
│  ・セッション管理（mf_cookies.pkl）│
│  ・stdout→stderr リダイレクト      │
└──────┬─────────────────────────────┘
       │ import
┌──────▼──────────────┐  ┌───────────────────────┐
│ moneyforward_api.py │  │ moneyforward_utils.py │
│ ・HTTP API呼び出し  │  │ ・カテゴリ検索        │
│ ・セッション管理    │  │ ・データ変換          │
└──────┬──────────────┘  └───────────────────────┘
       │
┌──────▼──────────────┐
│ MoneyForward API    │
│ https://moneyforward│
│ .com/sp*            │
└─────────────────────┘
```

### 技術選定

**FastMCP を選んだ理由**

| 比較項目 | FastMCP | mcp（低レベルSDK） |
|---------|---------|-----------------|
| コード量 | `@mcp.tool()` デコレータのみ | JSON-RPC・スキーマを手実装 |
| 型ヒントからの自動スキーマ | ○ | × |
| 既存コードとの親和性 | Flask の `@app.route()` と同じ感覚 | - |
| stdio 対応 | `mcp.run()` 一行 | 要実装 |

---

## 3. 重要な設計決定

### 3.1 stdout 汚染問題

MCP は stdio（標準入出力）でJSONをやり取りする。
`moneyforward_api.py` 内の `print()` 呼び出しが stdout に出力されると
MCPプロトコルが壊れ、Claude Desktopが応答不能になる。

**対策**: `mcp_server.py` の先頭で stdout を stderr に差し替える。
```python
import sys
sys.stdout = sys.stderr  # moneyforward_api.py の print() を stderr へ逃がす
```

### 3.2 セッション管理

MoneyForwardはCookieベース認証。`mf_cookies.pkl`に保存されたセッションを使う。

```python
COOKIE_FILE = os.environ.get("MF_COOKIE_FILE", "mf_cookies.pkl")

# 各ツール呼び出しで新しい requests.Session を開く
with session_from_cookie_file(COOKIE_FILE) as s:
    ...
```

**Cookie期限切れ検知**: API応答が `/sign_in` へのリダイレクトになった場合、
`"Session expired - please run start_mf_session.py"` を返す。

### 3.3 `get_transactions` と `get_transactions_by_account` を分けた理由

| | `get_transactions` | `get_transactions_by_account` |
|--|-------------------|------------------------------|
| API | `/sp/cf` | `/sp/cf/term_data` |
| 口座 | 全口座横断 | 1口座限定 |
| 残高スナップショット | なし | **あり（期首・期末）** |
| 用途 | 一般的な取引閲覧・分類 | **残高整合性検証専用** |

残高検証には期首・期末残高が必要で、それを返すAPIが `cf_term_data` のみ。

### 3.4 `set_transaction_memo` を独立ツールにした理由

`request_update_user_asset_act()` はカテゴリとメモを同時更新できるが、
ツールを分けることで Claude に意図を明確に伝えられる。
「カテゴリはそのままでメモだけ追加」という指示が確実に実行される。

### 3.5 振替取引の扱い

残高検証・集計では振替取引（`is_transfer=True`）を除外する。
振替は口座間移動であり、収支の増減を伴わない。含めると二重計上になる。

```python
# 残高検証ロジック
amount_sum = sum(t["amount"] for t in transactions if not t["is_transfer"])
balance_end_calc = balance_start + amount_sum
discrepancy = balance_end_calc - balance_end_actual
```

---

## 4. MCPツール仕様

### カテゴリ操作

#### `list_categories`
```python
def list_categories(
    large: str | None = None,      # 大カテゴリ名で絞り込み
    middle: str | None = None,     # 中カテゴリ名で絞り込み
    is_income: bool | None = None  # 収入/支出で絞り込み
) -> list[dict]
```
- `large_categories.csv` から読む（MoneyForward APIへの通信不要）
- 返り値: `[{large_category_id, large_category, middle_category_id, middle_category}, ...]`
- 利用ソース: `moneyforward_utils.search_category_sub()`

#### `find_category_by_name`
```python
def find_category_by_name(
    category_name: str,            # カテゴリ名（大・中どちらでも）
    is_income: bool | None = None
) -> dict  # {large_category_id, middle_category_id, large_category, middle_category}
```
- カテゴリ名が一意でない場合はエラーを返し候補一覧を示す
- 利用ソース: `moneyforward_utils.get_middle_category_impl()`

---

### 取引取得

#### `get_transactions`
```python
def get_transactions(
    offset: int = 0,
    size: int = 100,               # 内部で500件ずつページネーション
    keyword: str | None = None,
    base_date: str | None = None,  # "YYYY-MM-DD" 形式
    select_category: int | None = None,  # 0=未分類
    is_income: bool | None = None,
    exclude_transfers: bool = True # デフォルトで振替を除外
) -> dict  # {transactions: list[dict], total_count: int}
```
- 利用ソース: `moneyforward_api.request_user_asset_acts()` + `moneyforward_utils.append_row_form_user_asset_acts()`

#### `get_uncategorized_transactions`
```python
def get_uncategorized_transactions(
    size: int = 200,
    exclude_transfers: bool = True,
    exclude_income: bool = False
) -> dict
```
- `select_category=0` で `get_transactions` を呼ぶ便利ラッパー
- 分類タスクの起点として使う

#### `get_account_summaries`
```python
def get_account_summaries(
    sub_type: str | None = None   # "銀行口座" などで絞り込み
) -> list[dict]
# [{sub_account_id_hash, name, sub_name, sub_type, service_name, balance}, ...]
```
- `sub_account_id_hash` の取得が主目的（残高検証の前に呼ぶ）
- 利用ソース: `moneyforward_api.request_account_summaries()`

#### `get_transactions_by_account`
```python
def get_transactions_by_account(
    sub_account_id_hash: str,
    date_from: str,   # "YYYY-MM-DD"
    date_to: str      # "YYYY-MM-DD"（最大365日）
) -> dict
# {transactions: list[dict], balance_start: float, balance_end: float}
```
- 利用ソース: `moneyforward_api.request_cf_term_data_by_sub_account()`
- 365日を超える場合は内部で分割して連結

---

### 取引更新

#### `set_transaction_category`
```python
def set_transaction_category(
    transaction_id: int,
    large_category_id: int,
    middle_category_id: int,
    memo: str | None = None
) -> dict  # {success: bool, transaction_id: int}
```
- 内部で `get_csrf_token()` → `request_update_user_asset_act()` の順に呼ぶ
- CSRF取得のため余分なHTTPリクエストが1回発生する

#### `bulk_set_category`
```python
def bulk_set_category(
    transaction_ids: list[int],
    large_category_id: int,
    middle_category_id: int
) -> dict  # {success: bool, updated_count: int}
```
- 利用ソース: `moneyforward_api.request_transactions_category_bulk_updates()`（内部100件バッチ済み）
- 同カテゴリに分類できる取引をまとめて更新する際に使う

#### `set_transaction_memo`
```python
def set_transaction_memo(
    transaction_id: int,
    memo: str
) -> dict  # {success: bool, transaction_id: int}
```

---

### 集計

#### `summarize_transactions`
```python
def summarize_transactions(
    date_from: str,
    date_to: str,
    group_by: Literal["month", "large_category", "middle_category", "account"] = "month",
    is_income: bool | None = None,
    exclude_transfers: bool = True
) -> dict
# {summary: [{group_key, total_amount, transaction_count}], date_from, date_to}
```
- 内部で全取引をページネーション取得後、pandas で groupby
- 例: 「2025年の食費を月別に」→ `group_by="month"` + カテゴリフィルタ

---

### 残高検証

#### `verify_account_balance`
```python
def verify_account_balance(
    sub_account_id_hash: str,
    date_from: str,
    date_to: str
) -> dict
```
返り値:
```json
{
  "sub_account_id_hash": "...",
  "date_from": "2025-01-01",
  "date_to": "2025-03-31",
  "balance_start": 500000,
  "balance_end_actual": 523500,
  "balance_end_calculated": 521000,
  "discrepancy": -2500,
  "transaction_count": 47,
  "transaction_sum": 21000,
  "is_balanced": false,
  "note": "差異 -2500円: 取引の欠落または重複の可能性があります"
}
```

#### `verify_all_bank_accounts`
```python
def verify_all_bank_accounts(
    date_from: str,
    date_to: str
) -> list[dict]  # verify_account_balance の結果リスト（差異の大きい順）
```
- `get_account_summaries(sub_type="銀行口座")` で口座一覧取得後、全口座を検証

---

## 5. ファイル構成

```
moneyforward/
├── mcp_server.py              ★新規: MCPサーバー本体
├── MCP_SERVER_DESIGN.md       ★新規: 本設計書
│
├── moneyforward_api.py        既存: HTTP APIラッパー（変更なし）
├── moneyforward_utils.py      既存: ビジネスロジック（変更なし）
├── moneyforward.py            既存: CLI
├── webapp.py                  既存: Webアプリ
│
├── pyproject.toml             修正: fastmcp を dependencies に追加
├── mf_cookies.pkl             既存: 認証セッション
└── large_categories.csv       既存: カテゴリキャッシュ
```

---

## 6. Claude Desktop への接続設定

`%APPDATA%\Claude\claude_desktop_config.json` に `mcpServers` ブロックを追加：

```json
{
  "mcpServers": {
    "moneyforward": {
      "command": "uv",
      "args": [
        "run",
        "--project",
        "C:\\Users\\shink\\work\\ClaudeCowork\\moneyforward",
        "python",
        "C:\\Users\\shink\\work\\ClaudeCowork\\moneyforward\\mcp_server.py"
      ],
      "env": {
        "MF_COOKIE_FILE": "C:\\Users\\shink\\work\\ClaudeCowork\\moneyforward\\mf_cookies.pkl",
        "MF_CATEGORY_CACHE": "C:\\Users\\shink\\work\\ClaudeCowork\\moneyforward\\large_categories.csv"
      }
    }
  }
}
```

設定後、Claude Desktop を再起動するとツールが利用可能になる。

---

## 7. 実装ステップ

### Step 1: 依存関係の追加
```bash
uv add fastmcp
```
`pyproject.toml` の `dependencies` に `fastmcp` が追加される。

### Step 2: 読み取り系ツールで mcp_server.py を作成
実装するツール: `list_categories`, `find_category_by_name`, `get_transactions`,
`get_uncategorized_transactions`, `get_account_summaries`

動作確認:
```bash
uv run python mcp_server.py
# → エラーなく起動し、Ctrl+C で停止できればOK
```

### Step 3: 集計ツールの追加
実装するツール: `summarize_transactions`

動作確認: 既知の月に対して集計し、MoneyForwardのWebで確認した値と一致するか確認。

### Step 4: 書き込みツールの追加
実装するツール: `set_transaction_category`, `bulk_set_category`, `set_transaction_memo`

注意: CSRF取得が必要。テスト用の取引に対してのみ実行すること。

### Step 5: 残高検証ツールの追加
実装するツール: `get_transactions_by_account`, `verify_account_balance`, `verify_all_bank_accounts`

事前確認が必要:
```python
# cf_term_data のレスポンス構造を確認するスクリプト
with session_from_cookie_file("mf_cookies.pkl") as s:
    data = request_cf_term_data_by_sub_account(s, <hash>, "2025-01-01", "2025-01-31")
    print(data.keys())  # balance_start 等のフィールド名を確認
```

### Step 6: Claude Desktop への接続
1. `claude_desktop_config.json` を上記設定で更新
2. Claude Desktop を再起動
3. チャットで「MoneyForwardの未分類の取引を教えて」と入力して動作確認

---

## 8. リスクと対策

| リスク | 影響 | 対策 |
|--------|------|------|
| Cookie期限切れ | 全ツールが失敗 | HTTP 302検知→明確なエラーメッセージを返す |
| cf_term_dataの残高フィールド名不明 | 残高検証が実装できない | Step 5 の事前確認スクリプトで確認 |
| 365日制限（cf_term_data） | 長期間の検証不可 | 内部で365日ずつ分割してページネーション |
| print()によるstdout汚染 | MCP通信が壊れる | `sys.stdout = sys.stderr` でリダイレクト |
| 大量取引時の応答遅延 | タイムアウト | ページネーションを500件ずつに制限 |
