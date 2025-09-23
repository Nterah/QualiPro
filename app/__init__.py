# app/__init__.py
import os
from flask import Flask, jsonify
from sqlalchemy import event, text
from dotenv import load_dotenv

from .extensions import db, migrate  # shared instances

load_dotenv(override=True)

def create_app():
    app = Flask(__name__)

    # ---- DB config ----
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")
    app.config["SQLALCHEMY_DATABASE_URI"] = db_url
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    # Helpful with Supabase pooler
    app.config.setdefault("SQLALCHEMY_ENGINE_OPTIONS", {"pool_pre_ping": True})

    db.init_app(app)
    migrate.init_app(app, db)

    # Ensure every new DB connection uses pqp,public
    with app.app_context():
        def _set_search_path(dbapi_conn, _rec):
            cur = dbapi_conn.cursor()
            cur.execute("SET search_path TO pqp, public")
            cur.close()
        event.listen(db.engine, "connect", _set_search_path)

        # Optional: detect org_id presence without crashing if tables are missing
        try:
            has_org_id = db.session.execute(text("""
                SELECT
                  EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='pqp' AND table_name='project' AND column_name='org_id'
                  )
                  AND
                  EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='pqp' AND table_name='checklist_item' AND column_name='org_id'
                  )
            """)).scalar()
        except Exception:
            has_org_id = False
        app.config["HAS_ORG_ID"] = bool(has_org_id)

    # Lightweight DB health
    @app.get("/api/pqp/health/db")
    def health_db():
        sp = db.session.execute(text("SELECT current_setting('search_path')")).scalar()
        return jsonify(ok=True, search_path=sp, has_org_id=app.config["HAS_ORG_ID"])

    # ---- Blueprints (register ONLY inside the factory) ----
    # app/pqp/__init__.py should NOT register anything at import time.
    from app.pqp import pqp_bp, pqp_api_bp, risk_api_bp  # risk_api_bp may be None if not present

    app.register_blueprint(pqp_bp)
    app.register_blueprint(pqp_api_bp)
    if risk_api_bp:
        app.register_blueprint(risk_api_bp)

    from .routes_root import root_bp
    app.register_blueprint(root_bp)

    return app
