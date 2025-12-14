from flask import Flask, render_template, jsonify, request
from moneyforward_api import request_user_asset_acts, request_large_categories, session_from_cookie_file
from moneyforward_utils import append_row_form_user_asset_acts
import os
from datetime import datetime

app = Flask(__name__)
COOKIE_FILE = 'mf_cookies.pkl'

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/acts')
def get_acts():
    offset = request.args.get('offset', default=0, type=int)
    size = request.args.get('size', default=20, type=int)
    keyword = request.args.get('keyword')
    base_date = request.args.get('base_date')
    select_category = request.args.get('select_category', type=int)
    
    # フラグ系パラメータ
    is_new = request.args.get('is_new', type=int)
    is_old = request.args.get('is_old', type=int)
    is_continuous = request.args.get('is_continuous', type=int)

    try:
        with session_from_cookie_file(COOKIE_FILE) as s:
            # API呼び出し
            data = request_user_asset_acts(
                s, 
                offset=offset, 
                size=size, 
                keyword=keyword,
                base_date=base_date,
                select_category=select_category,
                is_new=is_new,
                is_old=is_old,
                is_continuous=is_continuous
            )
            
            # user_asset_acts.py と同じヘッダー定義を使用
            list_header = 'id is_transfer is_income is_target updated_at content amount large_category_id large_category middle_category_id middle_category account.service.service_name sub_account.sub_type sub_account.sub_name'.split()
            
            rows = []
            # 共通関数を使用してデータを抽出
            append_row_form_user_asset_acts(rows, data, list_header)
            
            # リストのリストを辞書のリストに変換してJSONレスポンス用に整形
            acts = [dict(zip(list_header, row)) for row in rows]
            
            return jsonify({
                'acts': acts, 
                'total_count': data.get('total_count', 0)
            })
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/categories')
def get_categories():
    try:
        with session_from_cookie_file(COOKIE_FILE) as s:
            cats = request_large_categories(s)
            return jsonify(cats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
