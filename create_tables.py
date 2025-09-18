# create_tables.py
# Run this once to create any missing tables (e.g., pqp_sections)

from sqlalchemy import inspect

# Try to load your Flask app in the most robust way
app = None
db = None

# First, try the factory style: from app import create_app, db
try:
    from app import create_app, db as _db  # type: ignore
    app = create_app()
    db = _db
except Exception as e:
    print("Factory load failed or not present:", e)

# Fallback: non-factory style: from app import app, db
if app is None or db is None:
    try:
        from app import app as _app, db as _db  # type: ignore
        app = _app
        db = _db
    except Exception as e2:
        print("Direct app load failed:", e2)

if app is None or db is None:
    raise SystemExit("ERROR: Could not load Flask app/db from 'app'. Run this from your project root.")

with app.app_context():
    db.create_all()
    tables = inspect(db.engine).get_table_names()
    print("Tables in DB:", tables)
    if "pqp_sections" in tables:
        print("✅ Success: 'pqp_sections' table is present.")
    else:
        print("⚠️ 'pqp_sections' not found. Check that the PQPSection model is defined and imported.")
