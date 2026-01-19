# Snowflake Column Detector

指定されたSnowflake Schemaにある複数のテーブルから、テーブル・カラム情報とサンプル値を取得するPythonスクリプトです。

## 機能

- 指定したSchemaの全テーブルを自動取得
- 各テーブルのカラム情報(カラム名、データ型、NULL許可など)を取得
- 各カラムの複数レコードからサンプル値を取得
- 結果をCSVまたはExcel形式で出力

## 必要なライブラリ

```bash
pip install -r requirements.txt
```

## 使い方

### 1. 環境変数の設定(推奨)

セキュリティのため、環境変数で接続情報を設定することを推奨します:

```bash
export SNOWFLAKE_ACCOUNT='your_account'
export SNOWFLAKE_USER='your_username'
export SNOWFLAKE_PASSWORD='your_password'
export SNOWFLAKE_WAREHOUSE='your_warehouse'
export SNOWFLAKE_DATABASE='your_database'
export SNOWFLAKE_SCHEMA='your_schema'
```

### 2. スクリプトの実行

```bash
python column_detector.py
```

### 3. コード内で直接指定する場合

`column_detector.py`の`main()`関数内の`config`を直接編集:

```python
config = {
    'account': 'your_account',
    'user': 'your_username',
    'password': 'your_password',
    'warehouse': 'your_warehouse',
    'database': 'your_database',
    'schema': 'your_schema'
}
```

## カスタマイズ例

### 特定のテーブルのみを分析

```python
results = detector.analyze_schema(
    sample_size=5, 
    specific_tables=['TABLE1', 'TABLE2', 'TABLE3']
)
```

### サンプル値の件数を変更

```python
results = detector.analyze_schema(sample_size=10)  # 10件のサンプルを取得
```

### Excel形式で出力

```python
detector.export_to_excel(results, 'snowflake_schema_analysis.xlsx')
```

## 出力ファイル

デフォルトでは `snowflake_schema_analysis.csv` というファイルに結果が出力されます。

### 出力内容

| テーブル名 | カラム名 | データ型 | NULL許可 | サンプル値 |
|-----------|---------|---------|---------|-----------|
| USERS | USER_ID | NUMBER | NO | 1, 2, 3, 4, 5 |
| USERS | USER_NAME | VARCHAR | YES | Alice, Bob, Charlie, David, Eve |
| ORDERS | ORDER_ID | NUMBER | NO | 101, 102, 103, 104, 105 |

## クラスの使い方

### プログラムから利用する場合

```python
from column_detector import SnowflakeColumnDetector

# インスタンス化
detector = SnowflakeColumnDetector(
    account='your_account',
    user='your_username',
    password='your_password',
    warehouse='your_warehouse',
    database='your_database',
    schema='your_schema'
)

# 接続
detector.connect()

# テーブル一覧の取得
tables = detector.get_tables()
print(tables)

# 特定テーブルのカラム情報を取得
columns_info = detector.get_columns_info('TABLE_NAME')
print(columns_info)

# 特定カラムのサンプル値を取得
samples = detector.get_sample_values('TABLE_NAME', 'COLUMN_NAME', sample_size=10)
print(samples)

# スキーマ全体の分析
results = detector.analyze_schema(sample_size=5)

# 切断
detector.disconnect()
```

## 注意事項

- 大量のテーブルやカラムがある場合、処理に時間がかかることがあります
- サンプル値の取得は各カラムに対してクエリを実行するため、テーブル数が多い場合は実行時間に注意してください
- パスワードなどの機密情報は環境変数または安全な方法で管理してください

## トラブルシューティング

### 接続エラー

- Snowflakeアカウント情報が正しいか確認
- ネットワーク接続を確認
- ウェアハウスが起動しているか確認

### 権限エラー

- 指定したユーザーがデータベース、スキーマ、テーブルへのアクセス権限を持っているか確認
- `INFORMATION_SCHEMA`へのアクセス権限があるか確認
