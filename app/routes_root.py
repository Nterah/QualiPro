# app/routes_root.py
from flask import Blueprint, jsonify

# small root blueprint so "/" isn't a 404
root_bp = Blueprint("root", __name__)

@root_bp.get("/")
def home():
    # keep this simple; your real UI lives under /pqp/
    return "App is up. Try /pqp/ or /api/pqp/health/db"

@root_bp.get("/_ping")
def ping():
    return jsonify(ok=True)
