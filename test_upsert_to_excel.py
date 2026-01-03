import unittest
import pandas as pd
import os
import tempfile
from openpyxl import load_workbook, Workbook

# cf_term_data.py から upsert_to_excel をインポート
from cf_term_data import upsert_to_excel

class TestUpsertToExcel(unittest.TestCase):

    def setUp(self):
        self.df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C'],
            'value': [10, 20, 30]
        })
        self.temp_dir = tempfile.mkdtemp()
        self.excel_file = os.path.join(self.temp_dir, 'test.xlsx')
        self.sheet_name = 'Sheet1'
        self.unique_index = 'id'

    def tearDown(self):
        if os.path.exists(self.excel_file):
            os.remove(self.excel_file)
        os.rmdir(self.temp_dir)

    def _read_excel(self):
        wb = load_workbook(self.excel_file)
        ws = wb[self.sheet_name]
        headers = [cell.value for cell in ws[1]]
        data = [[cell.value for cell in row] for row in ws.iter_rows(min_row=2)]
        return headers, data

    def test_new_file_creation(self):
        """新規作成テスト"""
        upsert_to_excel(self.df, self.sheet_name, self.excel_file, self.unique_index)
        headers, data = self._read_excel()
        self.assertEqual(headers, list(self.df.columns))
        self.assertEqual(data, self.df.values.tolist())

    def test_no_change_on_same_data(self):
        """再取得一致テスト"""
        upsert_to_excel(self.df, self.sheet_name, self.excel_file, self.unique_index)
        headers1, data1 = self._read_excel()
        upsert_to_excel(self.df, self.sheet_name, self.excel_file, self.unique_index)
        headers2, data2 = self._read_excel()
        self.assertEqual(headers1, headers2)
        self.assertEqual(data1, data2)

    def test_update_changed_row(self):
        """変更行更新テスト"""
        upsert_to_excel(self.df, self.sheet_name, self.excel_file, self.unique_index)
        # Excelを変更
        wb = load_workbook(self.excel_file)
        ws = wb[self.sheet_name]
        ws.cell(row=3, column=3, value=999)  # value列の3行目を変更
        wb.save(self.excel_file)
        # 再実行
        upsert_to_excel(self.df, self.sheet_name, self.excel_file, self.unique_index)
        headers, data = self._read_excel()
        self.assertEqual(data[1][2], 20)  # 元に戻っている

    def test_add_new_row(self):
        """新規行追加テスト"""
        upsert_to_excel(self.df, self.sheet_name, self.excel_file, self.unique_index)
        new_df = self.df.copy()
        new_df = pd.concat([new_df, pd.DataFrame({'id': [4], 'name': ['D'], 'value': [40]})], ignore_index=True)
        upsert_to_excel(new_df, self.sheet_name, self.excel_file, self.unique_index)
        headers, data = self._read_excel()
        self.assertEqual(len(data), 4)
        self.assertEqual(data[-1], [4, 'D', 40])

    def test_add_new_column(self):
        """新規列追加テスト"""
        upsert_to_excel(self.df, self.sheet_name, self.excel_file, self.unique_index)
        new_df = self.df.copy()
        new_df['new_col'] = ['X', 'Y', 'Z']
        upsert_to_excel(new_df, self.sheet_name, self.excel_file, self.unique_index)
        headers, data = self._read_excel()
        self.assertIn('new_col', headers)
        self.assertEqual(data[0][3], 'X')

    def test_remove_row(self):
        """行削除対応テスト"""
        upsert_to_excel(self.df, self.sheet_name, self.excel_file, self.unique_index)
        # Excelから行削除 (2行目を削除)
        wb = load_workbook(self.excel_file)
        ws = wb[self.sheet_name]
        ws.delete_rows(3)  # ヘッダー+1行目なので、3行目削除でid=2の行
        wb.save(self.excel_file)
        # 再実行
        upsert_to_excel(self.df, self.sheet_name, self.excel_file, self.unique_index)
        headers, data = self._read_excel()
        self.assertEqual(len(data), 3)
        self.assertEqual(data, self.df.values.tolist())

    def test_no_unique_index(self):
        """ユニークインデックスなしテスト"""
        upsert_to_excel(self.df, self.sheet_name, self.excel_file, None)
        headers, data = self._read_excel()
        self.assertEqual(data, self.df.values.tolist())

    def test_empty_dataframe(self):
        """空DataFrameテスト"""
        empty_df = pd.DataFrame()
        upsert_to_excel(empty_df, self.sheet_name, self.excel_file, self.unique_index)
        # ファイルが存在するか確認
        self.assertTrue(os.path.exists(self.excel_file))

    def test_missing_column(self):
        """欠損列テスト"""
        upsert_to_excel(self.df, self.sheet_name, self.excel_file, self.unique_index)
        new_df = self.df[['id', 'name']]  # value列削除
        upsert_to_excel(new_df, self.sheet_name, self.excel_file, self.unique_index)
        headers, data = self._read_excel()
        self.assertEqual(headers, ['id', 'name'])
        self.assertEqual(data, new_df.values.tolist())

if __name__ == '__main__':
    unittest.main()