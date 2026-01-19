"""
Snowflake Schema Column Detector
指定されたSchemaのテーブルとカラム情報を取得し、サンプル値を表示するスクリプト
"""

import snowflake.connector
import pandas as pd
from typing import List, Dict
import os


class SnowflakeColumnDetector:
    """Snowflakeのテーブル・カラム情報を取得するクラス"""
    
    def __init__(self, account: str, user: str, password: str, 
                 warehouse: str, database: str, schema: str):
        """
        初期化
        
        Parameters:
        -----------
        account : str
            Snowflakeアカウント
        user : str
            ユーザー名
        password : str
            パスワード
        warehouse : str
            ウェアハウス名
        database : str
            データベース名
        schema : str
            スキーマ名
        """
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.connection = None
    
    def connect(self):
        """Snowflakeに接続"""
        try:
            self.connection = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            print(f"✓ Snowflakeに接続しました: {self.database}.{self.schema}")
        except Exception as e:
            print(f"✗ 接続エラー: {e}")
            raise
    
    def disconnect(self):
        """Snowflakeから切断"""
        if self.connection:
            self.connection.close()
            print("✓ Snowflakeから切断しました")
    
    def get_tables(self) -> List[str]:
        """
        指定されたスキーマ内の全テーブルを取得
        
        Returns:
        --------
        List[str]
            テーブル名のリスト
        """
        query = f"""
        SELECT TABLE_NAME
        FROM {self.database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{self.schema}'
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        
        return tables
    
    def get_columns_info(self, table_name: str) -> pd.DataFrame:
        """
        指定されたテーブルのカラム情報を取得
        
        Parameters:
        -----------
        table_name : str
            テーブル名
            
        Returns:
        --------
        pd.DataFrame
            カラム情報(カラム名、データ型、NULL許可など)
        """
        query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE
        FROM {self.database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{self.schema}'
        AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        columns = cursor.fetchall()
        cursor.close()
        
        df = pd.DataFrame(columns, columns=[
            'COLUMN_NAME', 'DATA_TYPE', 'IS_NULLABLE', 
            'COLUMN_DEFAULT', 'CHAR_MAX_LENGTH', 
            'NUMERIC_PRECISION', 'NUMERIC_SCALE'
        ])
        
        return df
    
    def get_sample_values(self, table_name: str, column_name: str, 
                         sample_size: int = 5) -> List:
        """
        指定されたカラムのサンプル値を取得
        
        Parameters:
        -----------
        table_name : str
            テーブル名
        column_name : str
            カラム名
        sample_size : int
            取得するサンプル数(デフォルト: 5)
            
        Returns:
        --------
        List
            サンプル値のリスト
        """
        query = f"""
        SELECT DISTINCT "{column_name}"
        FROM {self.database}.{self.schema}."{table_name}"
        WHERE "{column_name}" IS NOT NULL
        LIMIT {sample_size}
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            samples = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return samples
        except Exception as e:
            return [f"エラー: {str(e)}"]
    
    def analyze_schema(self, sample_size: int = 5, 
                      specific_tables: List[str] = None) -> pd.DataFrame:
        """
        スキーマ全体を分析し、テーブル・カラム・サンプル値を取得
        
        Parameters:
        -----------
        sample_size : int
            各カラムのサンプル値の数(デフォルト: 5)
        specific_tables : List[str], optional
            特定のテーブルのみを分析する場合に指定
            
        Returns:
        --------
        pd.DataFrame
            テーブル、カラム、データ型、サンプル値を含むDataFrame
        """
        if not self.connection:
            self.connect()
        
        # テーブルリストの取得
        if specific_tables:
            tables = specific_tables
        else:
            tables = self.get_tables()
        
        print(f"\n分析対象テーブル数: {len(tables)}")
        
        results = []
        
        for table in tables:
            print(f"\n処理中: {table}")
            
            # カラム情報の取得
            columns_info = self.get_columns_info(table)
            
            for _, col_info in columns_info.iterrows():
                column_name = col_info['COLUMN_NAME']
                data_type = col_info['DATA_TYPE']
                is_nullable = col_info['IS_NULLABLE']
                
                # サンプル値の取得
                samples = self.get_sample_values(table, column_name, sample_size)
                sample_str = ', '.join([str(s) for s in samples[:sample_size]])
                
                results.append({
                    'テーブル名': table,
                    'カラム名': column_name,
                    'データ型': data_type,
                    'NULL許可': is_nullable,
                    'サンプル値': sample_str
                })
                
                print(f"  - {column_name} ({data_type})")
        
        df_results = pd.DataFrame(results)
        return df_results
    
    def export_to_csv(self, df: pd.DataFrame, output_file: str = 'schema_analysis.csv'):
        """
        分析結果をCSVファイルに出力
        
        Parameters:
        -----------
        df : pd.DataFrame
            分析結果のDataFrame
        output_file : str
            出力ファイル名
        """
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n✓ 結果を '{output_file}' に出力しました")
    
    def export_to_excel(self, df: pd.DataFrame, output_file: str = 'schema_analysis.xlsx'):
        """
        分析結果をExcelファイルに出力
        
        Parameters:
        -----------
        df : pd.DataFrame
            分析結果のDataFrame
        output_file : str
            出力ファイル名
        """
        df.to_excel(output_file, index=False, engine='openpyxl')
        print(f"\n✓ 結果を '{output_file}' に出力しました")


def main():
    """
    メイン実行関数
    """
    # 接続情報の設定(環境変数または直接指定)
    config = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT', 'your_account'),
        'user': os.getenv('SNOWFLAKE_USER', 'your_username'),
        'password': os.getenv('SNOWFLAKE_PASSWORD', 'your_password'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'your_warehouse'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'your_database'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'your_schema')
    }
    
    # SnowflakeColumnDetectorのインスタンス化
    detector = SnowflakeColumnDetector(**config)
    
    try:
        # Snowflakeに接続
        detector.connect()
        
        # スキーマの分析(サンプル値を5件取得)
        # 特定のテーブルのみを分析する場合:
        # results = detector.analyze_schema(sample_size=5, specific_tables=['TABLE1', 'TABLE2'])
        results = detector.analyze_schema(sample_size=5)
        
        # 結果の表示
        print("\n" + "="*80)
        print("分析結果")
        print("="*80)
        print(results.to_string())
        
        # CSVファイルに出力
        detector.export_to_csv(results, 'snowflake_schema_analysis.csv')
        
        # Excelファイルに出力(オプション)
        # detector.export_to_excel(results, 'snowflake_schema_analysis.xlsx')
        
    except Exception as e:
        print(f"\nエラーが発生しました: {e}")
    finally:
        # 接続を切断
        detector.disconnect()


if __name__ == "__main__":
    main()
