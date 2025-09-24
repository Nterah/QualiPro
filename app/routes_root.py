# app/routes_root.py
from flask import Blueprint, redirect

root_bp = Blueprint("root_bp", __name__)

@root_bp.get("/")
def root():
    # canonical landing page: /pqp  (no trailing slash)
    return redirect("/pqp", code=308)  # 308 keeps the HTTP method
