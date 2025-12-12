from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime
from config.database.session import Base

class AccountORM(Base):
    __tablename__ = "account"

    id = Column(Integer, primary_key=True)
    email = Column(String(255))
    nickname = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
