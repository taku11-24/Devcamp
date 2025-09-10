from sqlalchemy import Column, Integer, Float
# DateTime と func は不要になったため import から削除
from database import Base

class BrakingEvent(Base):
    __tablename__ = "BrakingEvents"

    id = Column(Integer, primary_key=True, index=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)