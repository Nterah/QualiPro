from dotenv import load_dotenv
from sqlalchemy import text
from app import create_app, db

load_dotenv()
app = create_app()
with app.app_context():
    # make sure the schema exists
    db.session.execute(text("create schema if not exists pqp"))
    db.session.commit()

    # create tables from your models into schema pqp
    db.create_all()

    # show what got created
    rows = db.session.execute(text("""
        select table_schema, table_name
        from information_schema.tables
        where table_schema='pqp'
        order by 2
    """)).all()
    print("PQP tables:", rows)
