from flask import Blueprint, request, jsonify, abort
from sqlalchemy import text
from app.extensions import db

bp = Blueprint("risk_api", __name__, url_prefix="/api/pqp/risk")

TABLES = {
    "concept": "pqp.risk_concept",
    "docs":    "pqp.risk_docs",
    "works":   "pqp.risk_works",
}

def _tbl(stage):
    t = TABLES.get(stage)
    if not t: abort(404, f"Unknown stage '{stage}'")
    return t

@bp.get("/<stage>")
def list_risks(stage):
    t = _tbl(stage)
    project = request.args.get("project")
    q = f"select * from {t} where (:p is null or id=:p) order by row_id desc limit 500"
    rows = db.session.execute(text(q), {"p": project}).mappings().all()
    return jsonify(rows)

@bp.post("/<stage>")
def create_risk(stage):
    t = _tbl(stage)
    payload = request.get_json(force=True) or {}
    # minimally require id (project_code) + risk fields
    if not payload.get("id"): abort(400, "id (project_code) is required")
    cols = payload.keys()
    cols_sql = ",".join(cols)
    vals_sql = ",".join([f":{c}" for c in cols])
    q = f"insert into {t} ({cols_sql}) values ({vals_sql}) returning row_id"
    rid = db.session.execute(text(q), payload).scalar()
    db.session.commit()
    return jsonify({"row_id": rid}), 201

@bp.patch("/<stage>/<int:row_id>")
def update_risk(stage, row_id):
    t = _tbl(stage)
    payload = request.get_json(force=True) or {}
    if not payload: return jsonify({"updated": 0})
    sets = ",".join([f"{k}=:{k}" for k in payload.keys()])
    payload["row_id"] = row_id
    q = f"update {t} set {sets} where row_id=:row_id"
    db.session.execute(text(q), payload)
    db.session.commit()
    return jsonify({"updated": 1})

@bp.delete("/<stage>/<int:row_id>")
def delete_risk(stage, row_id):
    t = _tbl(stage)
    db.session.execute(text(f"delete from {t} where row_id=:id"), {"id": row_id})
    db.session.commit()
    return "", 204
