import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
url = os.getenv("DATABASE_URL")
print("URL:", url)
e = create_engine(url, pool_pre_ping=True, future=True)

with e.begin() as c:
    # hook already sets search_path, but setting again is safe
    c.execute(text("set search_path to pqp, public"))
    print("Connected OK:", c.execute(text("select current_user, current_database()")).all())
    print("search_path:", c.execute(text("show search_path")).all())
