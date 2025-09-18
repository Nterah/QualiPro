# scripts/test_db.py
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
e = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True, future=True)
with e.begin() as c:
    # ensure the session uses your schema
    c.execute(text("set search_path to pqp, public"))
    print("Connected OK")
    print(c.execute(text("select current_user, current_database()")).all())
    print(c.execute(text("show search_path")).all())

