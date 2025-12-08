from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from dotenv import load_dotenv
import urllib.parse

load_dotenv()

# Fetch variables
USER = os.getenv("SQL_USER")
PASSWORD = urllib.parse.quote_plus(os.getenv("SQL_PASSWORD"))
HOST = os.getenv("SQL_HOST")
PORT = os.getenv("SQL_PORT")
DBNAME = os.getenv("SQL_DATABASE")

DATABASE_URL = (
    f"postgresql+psycopg2://{USER}:{PASSWORD}"
    f"@{HOST}:{PORT}/{DBNAME}"
)

engine = create_engine(
    DATABASE_URL,
    echo=True,
    pool_pre_ping=True
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db_session():
    return SessionLocal()
