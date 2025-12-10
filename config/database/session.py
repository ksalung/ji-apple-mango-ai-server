import os
import urllib.parse

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

# Uses SQL_* env vars provided (e.g., Supabase): SQL_USER, SQL_PASSWORD, SQL_HOST, SQL_PORT, SQL_DATABASE
# 한국어 주석: Supabase 등에서 제공한 PostgreSQL 접속 정보를 사용하여 SQLAlchemy 엔진을 만듭니다.
password = urllib.parse.quote_plus(os.getenv("SQL_PASSWORD", ""))

DATABASE_URL = (
    f"postgresql+psycopg2://{os.getenv('SQL_USER','postgres')}:{password}"
    f"@{os.getenv('SQL_HOST','localhost')}:{os.getenv('SQL_PORT','5432')}/{os.getenv('SQL_DATABASE','apple_mango')}"
)

engine = create_engine(
    DATABASE_URL,
    echo=os.getenv("SQL_ECHO", "false").lower() == "true",
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db_session():
    return SessionLocal()
