# app/__init__.py
import os
from flask import Flask, jsonify
from sqlalchemy import event, text
from dotenv import load_dotenv
load_dotenv()

from .extensions import db, migrate   # <-- use the shared instance here

def create_app():
    app = Flask(__name__)

    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    app.config["SQLALCHEMY_DATABASE_URI"] = db_url
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    db.init_app(app)
    migrate.init_app(app, db)

    with app.app_context():
        def _set_search_path(dbapi_conn, _rec):
            cur = dbapi_conn.cursor()
            cur.execute("SET search_path TO pqp, public")
            cur.close()
        event.listen(db.engine, "connect", _set_search_path)

        has_org_id = db.session.execute(text("""
            select
              exists(select 1 from information_schema.columns
                       where table_schema='pqp' and table_name='project' and column_name='org_id') and
              exists(select 1 from information_schema.columns
                       where table_schema='pqp' and table_name='checklist_item' and column_name='org_id')
        """)).scalar()
        app.config["HAS_ORG_ID"] = bool(has_org_id)

    @app.get("/api/pqp/health/db")
    def health_db():
        sp = db.session.execute(text("select current_setting('search_path')")).scalar()
        return jsonify(ok=True, search_path=sp, has_org_id=app.config["HAS_ORG_ID"])

    from app.pqp.pqp_routes import pqp_bp, pqp_api_bp
    app.register_blueprint(pqp_bp)
    app.register_blueprint(pqp_api_bp)

    from .routes_root import root_bp
    app.register_blueprint(root_bp)

    return app
