import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

def csv_data_format():
    """
    データベースに接続し、'CSV_data'テーブルから全てのデータを取得して、
    辞書の配列（リスト）として返します。
    """
    # .envファイルから環境変数を読み込む
    load_dotenv()

    # .envファイルからデータベース接続URLを取得
    database_url = os.getenv("DATABASE_URL")

    # データベースURLが設定されていない場合はエラーメッセージを表示して終了
    if not database_url:
        print("エラー: .envファイルにDATABASE_URLが設定されていません。")
        return []

    # 取得したデータを格納するための空のリストを準備
    results_list = []

    try:
        # データベースエンジンを作成
        engine = create_engine(database_url)

        # 'with'構文を使い、接続の開始と終了を自動で管理
        with engine.connect() as connection:
            print("データベースに接続しました。データを取得しています...")
            
            # 実行するSQLクエリを定義 (text()で囲むことを推奨)
            # idの昇順でデータを取得
            query = text('SELECT id, "件数", "緯度", "経度" FROM "CSV_data" ORDER BY id;')

            # クエリを実行し、結果を受け取る
            result_proxy = connection.execute(query)

            # 結果セットの各行を辞書に変換し、リストに追加
            for row in result_proxy:
                # row._mapping を使うと、カラム名をキーにした辞書ライクなオブジェクトが取得できる
                results_list.append(dict(row._mapping))
            
            print(f"✅ {len(results_list)}件のデータを取得しました。")

    except OperationalError as e:
        print(f"❌ データベースへの接続に失敗しました: {e}")
        print("ヒント: DATABASE_URLが正しいか、ネットワーク接続が有効か確認してください。")
        return [] # エラー時は空のリストを返す
    except Exception as e:
        print(f"❌ 予期せぬエラーが発生しました: {e}")
        return [] # エラー時は空のリストを返す

    # 最終的に取得したデータのリストを返す
    return results_list

# --- このファイルが直接実行された場合のサンプルコード ---
if __name__ == "__main__":
    
    # 関数を呼び出してデータを取得
    example_csv_data = csv_data_format()

    # 取得したデータがあれば表示する
    if example_csv_data:
        print("\n--- 取得したデータの先頭5件 ---")
        # データが多すぎる場合があるので、先頭の5件だけ表示
        for record in example_csv_data[:5]:
            print(record)
        
        print("\n---")
        print(f"（合計{len(example_csv_data)}件中、先頭5件を表示しました）")