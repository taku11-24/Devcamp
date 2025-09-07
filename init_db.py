# init_db.py (Neon/PostgreSQL版)

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import OperationalError

load_dotenv()

# .envからDATABASE_URLを直接取得
DATABASE_URL = os.getenv("DATABASE_URL")

Base = declarative_base()

class User(Base):
    # (このUserクラスの定義は全く変更なし)
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), unique=True)
    email = Column(String(100))

print("データベースに接続しています...")

try:
    engine = create_engine(DATABASE_URL)
    
    connection = engine.connect()
    connection.close()
    print("✅ データベースへの接続に成功しました。")

    print("テーブルを作成しています...")
    Base.metadata.create_all(bind=engine)
    print("✅ テーブルの準備が完了しました。")

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()

    print("データを投入しています...")
    user1 = User(name="Taro Yamada", email="taro@example.com")
    user2 = User(name="Hanako Sato", email="hanako@example.com")
    
    if not db.query(User).filter(User.name == user1.name).first():
        db.add(user1)
    if not db.query(User).filter(User.name == user2.name).first():
        db.add(user2)
    
    db.commit()
    print("✅ データの投入が完了しました。")
    db.close()

except OperationalError as e:
    print("❌ データベースへの接続に失敗しました。")
    print("エラー詳細:", e)
    print("\n--- 確認してください ---")
    print("1. .envファイルのDATABASE_URLは正しくコピーされていますか？")
    print("2. インターネット接続は問題ありませんか？")
    print("--------------------")
except Exception as e:
    print(f"予期せぬエラーが発生しました: {e}")