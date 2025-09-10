from pydantic import BaseModel
# datetime は不要になったため import から削除

# リクエストボディの形式 (変更なし)
class BrakingEventCreate(BaseModel):
    latitude: float
    longitude: float

# レスポンスの形式 (created_at を削除)
class BrakingEvent(BaseModel):
    id: int
    latitude: float
    longitude: float
    # 以下の created_at の行を削除（またはコメントアウト）します
    # created_at: datetime

    class Config:
        orm_mode = True
