# scripts/init_db.py
from app import create_app, db
from app.pqp.pqp_models import PQPSection   # ensures model is registered
from app.pqp.models_import import ImportJob # ensures model is registered

app = create_app()
with app.app_context():
    db.create_all()
    print("db.create_all() done")

