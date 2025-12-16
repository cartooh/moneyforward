from flask import Flask, render_template, jsonify, request
from moneyforward_api import (
    request_user_asset_acts, 
    request_large_categories, 
    request_transactions_category_bulk_updates, 
    request_manual_user_asset_act_partner_sources,
    request_change_transfer,
    request_clear_transfer,
    request_update_user_asset_act,
    get_csrf_token
)
from moneyforward_utils import append_row_form_user_asset_acts
import os
from datetime import datetime
import pickle
import requests
from contextlib import contextmanager


app = Flask(__name__)
COOKIE_FILE = 'mf_cookies.pkl'
with open(COOKIE_FILE, 'rb') as f:
    app.config['COOKIES_DATA'] = pickle.load(f)


@contextmanager
def session_from_cookies_data(cookies_data):
    s = requests.Session()
    try:
        s.cookies = cookies_data
        yield s
    finally:
        s.close()


@app.route('/')
def index():
    notify()
    return render_template('index.html', now=datetime.now().timestamp())

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

    # 除外フィルタ (カンマ区切りID)
    exclude_large = request.args.get('exclude_large', '')
    exclude_middle = request.args.get('exclude_middle', '')
    
    exclude_large_ids = set(int(x) for x in exclude_large.split(',') if x.isdigit())
    exclude_middle_ids = set(int(x) for x in exclude_middle.split(',') if x.isdigit())

    try:
        with session_from_cookies_data(app.config['COOKIES_DATA']) as s:
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
            all_acts = [dict(zip(list_header, row)) for row in rows]
            
            # フィルタリング実行
            filtered_acts = []
            for act in all_acts:
                lid = act.get('large_category_id')
                mid = act.get('middle_category_id')
                
                if lid in exclude_large_ids:
                    continue
                if mid in exclude_middle_ids:
                    continue
                filtered_acts.append(act)
            
            # 数値の文字列化 (JavaScriptの精度落ち対策)
            # id, amount, category_id 等を文字列に変換
            # is_... フラグは 0/1 のままにしておく (JSで boolean として扱うため)
            stringified_acts = []
            for act in filtered_acts:
                new_act = act.copy()
                for key in ['id', 'amount', 'large_category_id', 'middle_category_id']:
                    if key in new_act and new_act[key] is not None:
                        new_act[key] = str(new_act[key])
                stringified_acts.append(new_act)

            return jsonify({
                'acts': stringified_acts, 
                'total_count': data.get('total_count', 0),
                'fetched_count': len(all_acts) # APIから取得した実際の件数（ページネーション制御用）
            })
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/categories')
def get_categories():
    try:
        with session_from_cookies_data(app.config['COOKIES_DATA']) as s:
            cats = request_large_categories(s)
            return jsonify(cats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/bulk_update_category', methods=['POST'])
def bulk_update_category():
    try:
        data = request.get_json()
        # IDは文字列で来る可能性があるためintに変換
        ids = [int(x) for x in data.get('ids', [])]
        large_category_id = data.get('large_category_id')
        middle_category_id = data.get('middle_category_id')

        if not ids or large_category_id is None or middle_category_id is None:
            return jsonify({'status': 'error', 'message': 'Missing required parameters'}), 400

        with session_from_cookies_data(app.config['COOKIES_DATA']) as s:
            request_transactions_category_bulk_updates(
                s,
                large_category_id=large_category_id,
                middle_category_id=middle_category_id,
                ids=ids
            )
            return jsonify({'status': 'success', 'updated_count': len(ids)})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

def notify():
    try:
        import notifications as nc
        notification = nc.Notification()
        notification.message = "Webapp is running"
        nc.schedule_notification(notification, 1, False)
    except ImportError:
        pass  # No operation for notification in this context

@app.route('/api/act/<id>/partner_sources', methods=['GET'])
def get_partner_sources(id):
    try:
        with session_from_cookies_data(app.config['COOKIES_DATA']) as s:
            data = request_manual_user_asset_act_partner_sources(s, id)
            
            # Stringify IDs to prevent JS precision loss
            if 'manual_user_asset_act_partner_sources' in data:
                for source in data['manual_user_asset_act_partner_sources']:
                    if 'sub_account' in source:
                        sub = source['sub_account']
                        if 'id' in sub:
                            sub['id'] = str(sub['id'])
                        
                        if 'partner_candidate_acts' in sub:
                            for candidate in sub['partner_candidate_acts']:
                                if 'partner_candidate_act' in candidate:
                                    act = candidate['partner_candidate_act']
                                    if 'id' in act:
                                        act['id'] = str(act['id'])
                                    if 'amount' in act:
                                        act['amount'] = str(act['amount'])

            return jsonify(data)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/act/<id>/transfer', methods=['POST'])
def set_transfer(id):
    try:
        data = request.json
        partner_account_id_hash = data.get('partner_account_id_hash')
        partner_sub_account_id_hash = data.get('partner_sub_account_id_hash')
        partner_act_id = data.get('partner_act_id') # Optional
        with session_from_cookies_data(app.config['COOKIES_DATA']) as s:
            request_change_transfer(
                s, 
                id, 
                partner_account_id_hash=partner_account_id_hash, 
                partner_sub_account_id_hash=partner_sub_account_id_hash,
                partner_act_id=partner_act_id
            )
            return jsonify({'status': 'success'})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/act/<id>/transfer', methods=['DELETE'])
def clear_transfer(id):
    try:
        with session_from_cookies_data(app.config['COOKIES_DATA']) as s:
            request_clear_transfer(s, id)
            return jsonify({'status': 'success'})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/act/<id>', methods=['PUT'])
def update_act(id):
    try:
        data = request.json
        large_category_id = data.get('large_category_id')
        middle_category_id = data.get('middle_category_id')
        is_target = data.get('is_target')
        memo = data.get('memo')

        with session_from_cookies_data(app.config['COOKIES_DATA']) as s:
            token = get_csrf_token(s)
            request_update_user_asset_act(
                s, 
                token, 
                id, 
                large_category_id=large_category_id,
                middle_category_id=middle_category_id,
                is_target=is_target,
                memo=memo
            )
            return jsonify({'status': 'success'})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    try:
        import background as bg
        with bg.BackgroundTask() as b:
            app.run()
    except ImportError:
        pass
    app.run(debug=True, host='0.0.0.0', port=5000)
