from pydantic import BaseModel
from datetime import datetime

# リクエストボディの形式を定義
class BrakingEventCreate(BaseModel):
    latitude: float
    longitude: float

# レスポンスの形式を定義
class BrakingEvent(BaseModel):
    id: int
    latitude: float
    longitude: float
    created_at: datetime

    # SQLAlchemyモデルからPydanticモデルに変換できるように設定
    class Config:
        orm_mode = True