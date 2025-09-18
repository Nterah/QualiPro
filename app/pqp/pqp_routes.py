# app/pqp/pqp_routes.py
import os
import io
import csv
import json
import time
import zipfile
from datetime import date, datetime

def as_bool(val):
    """Convert form values like 'on', 'true', '1' to boolean True, else False."""
    return str(val).lower() in ("1", "true", "on", "yes")

from sqlalchemy import select, update, insert, MetaData, Table, func, text
import re                               # <- needed for the slug helper


from datetime import datetime
from app.pqp.models_import import ImportJob
from app.pqp.ingest.ai_import import parse_workbook_to_payload, commit_payload

# String/date helpers
from datetime import date, datetime

def _as_str(x):
    """Return a clean string for templates: supports None, date, datetime, text, numbers."""
    if x is None:
        return ""
    if isinstance(x, datetime):
        return x.date().isoformat()   # YYYY-MM-DD
    if isinstance(x, date):
        return x.isoformat()          # YYYY-MM-DD
    return str(x).strip()


def normalize_code_full(s: str) -> str:
    # Keep the entire code including suffix like "P700"
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)  # collapse multiple spaces to single
    return s.upper()



from flask import (
    Blueprint, render_template, request, redirect, url_for, flash,
    Response, jsonify, send_file, abort, current_app
)
from werkzeug.utils import secure_filename

# Define the folder where uploads will be saved
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

from sqlalchemy import MetaData, Table, select, func
from sqlalchemy.exc import NoSuchTableError

# from app import db
from app.extensions import db


from app.pqp.pqp_models import Project, PQPDetail, PQPSection
from app.pqp.sections import SECTION_DEFS, DEFAULT_SECTION_TITLES, get_section_columns

from sqlalchemy import text  # needed by the API queries




# --- Section → table/columns mapping (snake_case; matches pqp.sectionX views) ---
SECTION_TABLE = {
    1: "pqp.section1",
    2: "pqp.section2",
    3: "pqp.section31",   # In-place / filing items
    4: "pqp.section41",   # Planning & Design notes
    5: "pqp.section51",   # Documentation & Procurement
    6: "pqp.section61",   # Works & Handover
    7: "pqp.section71",   # Additional services
    8: "pqp.section8",    # Close-out & feedback
}

SECTION_COLS = {
    1: ["project_code","project_description","project_location",
        "client_organisation","primary_contact_name","vat_number",
        "contact_person_designation","invoice_address"],
    2: ["project_code","role","reqd","organisation_responsible",
        "representative_name","email","cell",
        "subconsultant_to_hn","subconsultant_agreement","cpg_partner",
        "cpg_percent","comments"],
    3: ["project_code","item","in_place","date_val","filing_location","notes"],
    4: ["project_code","design_criteria_requirements","planning_design_risks",
        "scope_register_location","design_notes"],
    5: ["project_code","client_tender_doc_requirements","form_of_contract",
        "standard_specs","client_template_date","documentation_risks",
        "tender_phase_notes"],
    6: ["project_code","construction_description","contractor_organisation",
        "contract_number","award_value_incl_vat","award_date","original_order_no",
        "original_date_order","inception_meeting_date","final_payment_cert_date",
        "final_value_incl_vat","commencement_of_works","date_of_ea_instruction",
        "where_instruction_recorded","completion_date","final_approval_date",
        "client_takeover_date","commencement_instruction_date",
        "commencement_instruction_location","construction_phase_risks",
        "construction_phase_notes"],
    7: ["project_code","additional_services_done","notes"],
    8: ["project_code","date_csq_submitted","date_csq_received","csq_rating",
        "location","comments_on_feedback","actual_close_out_date",
        "general_remarks_lessons_learned"],
}



from sqlalchemy import text
from flask import abort

def q(conn, sql, **params):
    """Execute a text SQL and return rows as dictionaries."""
    return conn.execute(text(sql), params).mappings().all()

def fetch_section_rows(conn, project_code: str, section_number: int):
    table = SECTION_TABLE.get(section_number)
    cols  = SECTION_COLS.get(section_number)
    if not table or not cols:
        abort(404, f"Unknown section {section_number}")

    sql = f"""
        SELECT {', '.join(cols)}
        FROM {table}
        WHERE project_code = %(code)s
        ORDER BY 1
    """
    return q(conn, sql, code=project_code)




# ---------- Project ID detection helpers ----------


_CODE_CORE = r"(?:\b\d{3}[A-Z]{2,3}\b)"               # e.g., 291RT, 322IN, 320ST, 101RT
_CODE_SUFFIX = r"(?:\s*(?:P\d{3}))"                    # optional: ' P700', ' P232'
_CODE_FULL_RE = re.compile(rf"{_CODE_CORE}(?:{_CODE_SUFFIX})?", re.IGNORECASE)

def _normalize_project_id(s: str) -> str:
    """
    Returns a normalized project_id.
    - Keeps forms like '291RT P700' intact if present.
    - Otherwise returns the core like '322IN'.
    - Trims repeated whitespace.
    """
    if not s:
        return ""
    s = " ".join(str(s).strip().split())
    m = _CODE_FULL_RE.search(s)
    if not m:
        return ""
    return m.group(0).upper()


def _to_str(v):
    # tidy coercion for headers used in template
    import datetime as dt
    if v is None:
        return ""
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat() if isinstance(v, dt.datetime) else v.strftime("%Y-%m-%d")
    return str(v)

def _normalize_rows(cols, rows):
    out = []
    for r in rows or []:
        if isinstance(r, dict):
            d = {c: r.get(c, "") for c in cols}
        elif isinstance(r, (list, tuple)):
            d = {c: (r[i] if i < len(r) else "") for i, c in enumerate(cols)}
        else:
            continue
        if "id" in cols and "id" not in d:
            d["id"] = ""
        out.append(d)
    return out
# Older JASON-style section 3 loader (if you still use it)
def _load_section3_from_unified(engine, code):
    # returns {'31': [...], '32': [...], '33': [...]} and same-shaped cols
    from sqlalchemy import text
    sql = text("""
        select part::text, row_id, data
        from pqp.section3_unified
        where coalesce(project_code,'') = :code
        order by part::int, row_id
    """)
    parts = {"31": [], "32": [], "33": []}
    cols  = {
      "31": ["id","in_place","date","filing_location","notes"],
      "32": ["id","appointment_review_date","appointment_reviewer","review_comments"],
      "33": ["id","ecsa_stage","date_completed","deliverable_notes","delivered","delivered_to_employer","employer_approved"]
    }
    with engine.begin() as conn:
        for row in conn.execute(sql, {"code": code}):
            part = row.part
            rec  = row.data or {}
            parts.setdefault(part, []).append(rec)
    return cols, parts

from sqlalchemy import text
# Newer section3 loader (if you use the separate tables)
def _load_section3_from_parts(code: str):
    """
    Reads pqp.section31/32/33 and returns:
      section3_cols: {'31':[...], '32':[...], '33':[...]}
      section3_data: {'31':[row dicts], '32':[...], '33':[...]}
    """

    cols = {
        "31": ["id", "item", "in_place", "date_val", "filing_location", "notes"],
        "32": ["id", "appointment_review_date", "appointment_reviewer", "review_comments",
               "appointment_roles", "appointment_date", "expected_duration",
               "original_end_date", "contract_ref_no", "general_comments"],
        "33": ["id", "ecsa_project_stage", "date_completed", "description_of_deliverable",
               "deliverable", "deliverable_accepted", "employer_approved", "comments"],
    }
    data = {"31": [], "32": [], "33": []}

    q31 = db.session.execute(
        text("""select id, item, in_place, date_val, filing_location, notes
                from pqp.section31
                where project_code = :code
                order by id"""),
        {"code": code}
    ).mappings().all()

    q32 = db.session.execute(
        text("""select id, appointment_review_date, appointment_reviewer, review_comments,
                       appointment_roles, appointment_date, expected_duration,
                       original_end_date, contract_ref_no, general_comments
                from pqp.section32
                where project_code = :code
                order by id"""),
        {"code": code}
    ).mappings().all()

    q33 = db.session.execute(
        text("""select id, ecsa_project_stage, date_completed, description_of_deliverable,
                       deliverable, deliverable_accepted, employer_approved, comments
                from pqp.section33
                where project_code = :code
                order by id"""),
        {"code": code}
    ).mappings().all()

    for m in q31: data["31"].append({c: m.get(c) for c in cols["31"]})
    for m in q32: data["32"].append({c: m.get(c) for c in cols["32"]})
    for m in q33: data["33"].append({c: m.get(c) for c in cols["33"]})

    return cols, data



def _detect_project_id_from_context(filename: str, sheetnames: list[str], first_cells_text: str = "") -> str:
    """
    Tries to find a project_id in (1) filename, then (2) sheet names, then (3) a blob of sampled cell text.
    Returns '' if nothing matched.
    """
    # 1) filename
    pid = _normalize_project_id(filename or "")
    if pid:
        return pid

    # 2) sheet names (scan them all, pick the first that matches)
    for nm in sheetnames or []:
        pid = _normalize_project_id(nm or "")
        if pid:
            return pid

    # 3) sampled cell text (if caller provides it)
    pid = _normalize_project_id(first_cells_text or "")
    return pid
# ---------------------------------------------------




# -------------------------- helpers --------------------------
# These helpers are used to manage section rows in the PQPSection model.
def _upsert_section_rows(project_code: str, section_number: int, columns: list, rows: list):
    """
    Ensure PQPSection(project_code, section_number) exists and store rows as JSON list.
    """
    sec = (db.session.query(PQPSection)
           .filter_by(project_code=project_code, section_number=section_number)
           .first())
    if not sec:
        sec = PQPSection(project_code=project_code, section_number=section_number)
        db.session.add(sec)

    # Keep only known columns if provided; otherwise store as-is
    cleaned = []
    for r in rows or []:
        if isinstance(r, dict) and columns:
            cleaned.append({c: r.get(c) for c in columns})
        else:
            cleaned.append(r)

    sec.rows_json = json.dumps(cleaned)          # preferred by the form
    # Optional: also keep columns for the form if your template uses them
    try:
        sec.columns_json = json.dumps(columns or [])
    except Exception:
        pass

    db.session.flush()


# --- Helper: ensure a Project has all PQP sections scaffolded ---
# Ensure the 9 PQP sections exist for a project code
def _ensure_sections(project_code: str) -> None:
    for i in range(1, 10):
        sec = (
            db.session.query(PQPSection)
            .filter_by(project_code=project_code, section_number=i)
            .first()
        )
        if not sec:
            sec = PQPSection(
                project_code=project_code,
                section_number=i,
                rows_json="[]",
            )
            db.session.add(sec)
    db.session.flush()  # no commit here; callers can commit/rollback
        




# ------------------------------------------------------------------------------
# Blueprint
# ------------------------------------------------------------------------------
pqp_bp = Blueprint(
    "pqp",
    __name__,
    template_folder="templates",
    static_folder="static",
    url_prefix="/pqp",
)

# ------------------------------------------------------------------------------
# ProjectRecords reflection (adjust column names if yours differ)
# ------------------------------------------------------------------------------
PR_TABLE = "ProjectRecords"
PR_COL_CODE = "Code"
PR_COL_SHORTDESC = "Short Description"
PR_COL_CLIENT = "Client"
PR_COL_PM = "Project Manager"
PR_COL_ACTIVE = "Active"
PR_COL_STATUS = "Status"


_metadata = MetaData()

# near the top of pqp_routes.py
PR_TABLE  = "ProjectRecords"
PR_SCHEMA = "public"  # <-- change this from 'pqp' to 'public'

def _projectrecords(engine):
    return Table(PR_TABLE, _metadata, schema=PR_SCHEMA, autoload_with=engine)



def _col(t, name: str):
    """
    Return a reflected column by name, tolerating newline/space/hyphen and
    mapping legacy labels to current column names.
    """
    # legacy -> current mappings
    ALIASES = {
        "Appointment Date": ["start_date", "appointment_date"],
        "Close-Out Date":   ["end_date", "close_out_date"],
    }

    # direct & simple normalizations
    candidates = {
        name,
        name.replace("\n", " "),
        name.replace("\n", ""),
        name.replace("-", " "),
        name.replace("–", " "),
    }
    # add alias candidates
    base = name.replace("\n", " ").strip()
    for k, vals in ALIASES.items():
        if base.lower() == k.lower():
            candidates.update(vals)

    for n in candidates:
        if n in t.c:
            return t.c[n]

    # last resort: case-insensitive, ignore spaces/newlines/hyphens/underscores
    def _norm(s: str) -> str:
        return "".join(ch for ch in s.lower() if ch not in {" ", "\n", "-", "–", "_"})
    target = _norm(base)
    for c in t.c.keys():
        if _norm(c) == target:
            return t.c[c]

    raise KeyError(name)


# ------------------------------------------------------------------------------
# PQP Section side-table (by ProjectRecords.Code)
# ------------------------------------------------------------------------------


# --- Helper: ensure a Project has PQP sections scaffolded (all or one) ---
def _ensure_sections_by_code(code: str, only_section: int | None = None) -> None:
    """
    Ensure section shells exist for this project code.
    Uses the active db.session instead of Model.query to avoid app-binding issues.
    """
    have = set(
        db.session.scalars(
            select(PQPSection.section_number)
            .where(PQPSection.project_code == code)
        ).all()
    )
    needed = [only_section] if only_section else list(range(1, 10))
    to_make = [n for n in needed if n not in have]
    if not to_make:
        return

    for n in to_make:
        db.session.add(PQPSection(
            project_code=code,
            section_number=n,
            title=DEFAULT_SECTION_TITLES.get(n, f"Section {n}"),
            rows_json="[]",            # store as JSON string; template reads fine
            completed=False,
        ))
    db.session.flush()  # caller will commit





# ------------------------------------------------------------------------------
# Dashboard & legacy ID-based form (left intact)
# ------------------------------------------------------------------------------
@pqp_bp.route("/", methods=["GET"])
def pqp_dashboard():
    projects = Project.query.order_by(Project.id.desc()).all()
    return render_template("pqp_dashboard.html", projects=projects)

@pqp_bp.route("/projects/create", methods=["POST"])
def create_project():
    name = (request.form.get("project_name") or "").strip()
    if not name:
        flash("Project name is required.", "danger")
        return redirect(url_for("pqp.pqp_dashboard"))

    project = Project(name=name)
    db.session.add(project)
    db.session.commit()

    detail = PQPDetail.query.filter_by(project_id=project.id).first()
    if not detail:
        detail = PQPDetail(project_id=project.id)
        db.session.add(detail)
        db.session.commit()

    flash("Project created.", "success")
    return redirect(url_for("pqp.pqp_form", project_id=project.id))

@pqp_bp.route("/form/<int:project_id>", methods=["GET", "POST"])
def pqp_form(project_id: int):
    project = Project.query.get_or_404(project_id)
    detail = PQPDetail.query.filter_by(project_id=project.id).first()
    if not detail:
        detail = PQPDetail(project_id=project.id)
        db.session.add(detail)
        db.session.commit()

    if request.method == "POST":
        # Section 1
        detail.description = request.form.get("description")
        detail.location = request.form.get("location")
        detail.client_organisation = request.form.get("client_organisation")
        detail.vat_number = request.form.get("vat_number")
        detail.contact_designation = request.form.get("contact_designation")
        detail.primary_contact_name = request.form.get("primary_contact_name")
        detail.invoice_address = request.form.get("invoice_address")
        detail.overview_summary = request.form.get("overview_summary")
        # Section 2
        detail.team_description = request.form.get("team_description")
        detail.team_checklist_complete = as_bool(request.form.get("team_checklist_complete"))
        # Section 3
        detail.appointment_milestones = request.form.get("appointment_milestones")
        detail.appointment_status = request.form.get("appointment_status")
        # Section 4
        detail.planning_notes = request.form.get("planning_notes")
        detail.planning_verified = as_bool(request.form.get("planning_verified"))
        # Section 5
        detail.tender_instructions = request.form.get("tender_instructions")
        detail.tender_status = request.form.get("tender_status")
        # Section 6
        detail.works_plan = request.form.get("works_plan")
        detail.works_checklist_complete = as_bool(request.form.get("works_checklist_complete"))
        # Section 7
        detail.extras_description = request.form.get("extras_description")
        detail.extras_approved = as_bool(request.form.get("extras_approved"))
        # Section 8
        detail.closeout_summary = request.form.get("closeout_summary")
        detail.feedback_rating = request.form.get("feedback_rating")
        # Section 9
        detail.scope_notes = request.form.get("scope_notes")

        # Uploads
        for form_name in [
            "overview_uploaded_file","appointment_uploaded_file","planning_uploaded_file",
            "tender_uploaded_file","works_progress_photo","closeout_uploaded_file","scope_uploaded_file"
        ]:
            f = request.files.get(form_name)
            if f and f.filename:
                filename = secure_filename(f.filename)
                f.save(os.path.join(UPLOAD_FOLDER, filename))
                setattr(detail, form_name, filename)

        detail.last_updated = date.today()
        db.session.commit()
        flash("PQP saved.", "success")
        return redirect(url_for("pqp.pqp_form", project_id=project.id))

    return render_template("pqp_form.html", project=project, pqp_detail=detail)

# ------------------------------------------------------------------------------
# Code-based selector & editor
# ------------------------------------------------------------------------------
@pqp_bp.route("/form", methods=["GET", "POST"])
def pqp_form_select_by_code():
    """
    Project selector:
      - GET: list/filter ProjectRecords with real columns
      - POST: accept a single code from a form and go to the form page
    """
    from datetime import datetime, date

    def _to_date_str(v):
        if v is None:
            return ""
        if isinstance(v, (datetime, date)):
            # ISO-like; change format if you prefer e.g. %d %b %Y
            return v.strftime("%Y-%m-%d")
        # if it came as string already (e.g. from a view), pass through
        try:
            return str(v)
        except Exception:
            return ""

    if request.method == "POST":
        code = (request.form.get("project_code") or "").strip()
        if not code:
            flash("Enter a Project Code.", "danger")
            return redirect(url_for("pqp.pqp_form_select_by_code"))
        return redirect(url_for("pqp.pqp_form_by_code", code=code))

    filter_code = (request.args.get("code") or "").strip()

    PR = _projectrecords(db.engine)

    # Required columns:
    cols = [
        _col(PR, PR_COL_CODE).label("Code"),
        _col(PR, PR_COL_SHORTDESC).label("Short Description"),
        _col(PR, PR_COL_CLIENT).label("Client"),
        _col(PR, PR_COL_PM).label("Project Manager"),
    ]
    # Optional columns (add if present in the view):
    for cname, lbl in [("start_date", "start_date"), ("end_date", "end_date"), (PR_COL_STATUS, "Status")]:
        try:
            cols.append(_col(PR, cname).label(lbl))
        except KeyError:
            pass

    query = select(*cols).order_by(_col(PR, PR_COL_CODE))
    if filter_code:
        query = query.where(_col(PR, PR_COL_CODE).ilike(f"%{filter_code}%"))

    rows = db.session.execute(query.limit(200)).fetchall()

    options = []
    for r in rows:
        m = r._mapping
        code_val = (m.get("Code") or "").strip()
        if not code_val:
            continue
        options.append({
            "code":   code_val,
            "short":  (m.get("Short Description") or "").strip(),
            "client": (m.get("Client") or "").strip(),
            "pm":     (m.get("Project Manager") or "").strip(),
            "status": (m.get("Status") or "").strip(),
            "start":  _to_date_str(m.get("start_date")),
            "end":    _to_date_str(m.get("end_date")),
        })

    return render_template("project_selector.html", project_options=options)







# ------------------------------------------------------------------------------
# Admin helper: find duplicate Codes
# ------------------------------------------------------------------------------
@pqp_bp.route("/admin/duplicates")
def pqp_duplicates():
    PR = _projectrecords(db.engine)
    rows = db.session.execute(
        select(_col(PR, PR_COL_CODE).label("Code"), func.count().label("Count"))
        .group_by(_col(PR, PR_COL_CODE))
        .having(func.count() > 1)
        .order_by(func.count().desc(), _col(PR, PR_COL_CODE))
    ).fetchall()
    dups = [dict(r._mapping) for r in rows]
    return render_template("duplicates.html", duplicates=dups)

# Dev seed
@pqp_bp.route("/seed")
def seed():
    p = Project(name="Seeded Project")
    db.session.add(p)
    db.session.commit()
    d = PQPDetail(project_id=p.id)
    db.session.add(d)
    db.session.commit()
    return redirect(url_for("pqp.pqp_form", project_id=p.id))

# ------------------------------------------------------------------------------
# Per‑section IMPORT (simple preview used by the small "Import" button on each tab)
# ------------------------------------------------------------------------------
@pqp_bp.post("/pqp/<code>/section/<int:section_idx>/import")
def pqp_section_import(code, section_idx):
    file = request.files.get("file")
    if not file or file.filename == "":
        return jsonify({"ok": False, "error": "No file uploaded."}), 400

    filename = file.filename
    resp = {"ok": True, "code": code, "section_idx": section_idx, "filename": filename, "preview": None}

    if filename.lower().endswith(".csv"):
        try:
            text = io.TextIOWrapper(file.stream, encoding="utf-8", errors="replace")
            reader = csv.reader(text)
            rows = []
            for i, row in enumerate(reader):
                rows.append(row)
                if i >= 19:
                    break
            header = rows[0] if rows else []
            body = rows[1:] if len(rows) > 1 else []
            resp["preview"] = {"header": header, "rows": body, "row_count_preview": len(body)}
            return jsonify(resp)
        except Exception as e:
            return jsonify({"ok": False, "error": f"Failed to read CSV: {e}"}), 400

    return jsonify({"ok": True, "note": "Preview currently implemented for CSV only."})

# ------------------------------------------------------------------------------
# BULK IMPORT: preview + commit (for Import/Export tab)
# ------------------------------------------------------------------------------
def _read_csv_file(file_storage):
    text = io.TextIOWrapper(file_storage.stream, encoding="utf-8", errors="replace")
    reader = csv.reader(text)
    rows = list(reader)
    header = rows[0] if rows else []
    body = rows[1:] if len(rows) > 1 else []
    return header, body

@pqp_bp.post("/pqp/import/<code>/<int:section_idx>/preview")
def pqp_import_preview(code, section_idx):
    file = request.files.get("file")
    if not file or file.filename == "":
        return jsonify({"ok": False, "error": "No file uploaded"}), 400

    expected = get_section_columns(section_idx)
    header, body = _read_csv_file(file)

    missing = [c for c in expected if c not in header]
    extra   = [c for c in header if c not in expected]

    preview_rows = []
    for r in body[:100]:
        rec = {}
        for ix, col in enumerate(header):
            if col in expected:
                rec[col] = r[ix] if ix < len(r) else ""
        preview_rows.append(rec)

    return jsonify({
        "ok": True,
        "code": code,
        "section_idx": section_idx,
        "filename": file.filename,
        "expected_columns": expected,
        "header": header,
        "missing_columns": missing,
        "extra_columns": extra,
        "row_count": len(body),
        "preview_rows": preview_rows
    })



# --- AI import: preview & commit (multi-file safe) ---

# ==== BEGIN REPLACEMENT: /import/ai/preview ==================================
@pqp_bp.post("/import/ai/preview")
def pqp_ai_import_preview():
    """
    Accepts one or more .xlsx files, builds a preview payload for each, and
    stores an ImportJob with status='preview'. If the primary parser returns
    empty sections, we fall back to a very forgiving pandas-based table reader
    so rows actually show up in Preview and on the form after Commit.
    """
    from io import BytesIO
    import re, json
    try:
        import pandas as pd
    except Exception:
        pd = None

    files = request.files.getlist("file")
    if not files:
        return jsonify({"ok": False, "error": "No files uploaded"}), 400

    # --- robust project-id detection (keeps suffix like 'P700' if present) ---
    def _detect_project_id(text: str) -> str:
        """
        Looks for '<3digits><2letters>' optionally followed by ' P<digits>'.
        Examples:
          '291RT P700' -> '291RT P700'
          '322IN'      -> '322IN'
        """
        s = (text or "").upper().strip()
        # First: full pattern with suffix
        m = re.search(r"\b(\d{3}[A-Z]{2}\s+P\d+)\b", s)
        if m:
            return m.group(1).strip()
        # Fallback: core code only
        m = re.search(r"\b(\d{3}[A-Z]{2})\b", s)
        return m.group(1).strip() if m else ""

    # --- pandas fallback so preview is never empty ----------------------------
    def _fallback_payload(xlsx_bytes: bytes, fname: str, project_id: str) -> dict:
        payload = {"code": project_id or _detect_project_id(fname), "sections": []}
        if not pd:
            # fill 1..9 empty sections to keep UI stable
            for i in range(1, 10):
                payload["sections"].append({"index": i, "columns": [], "rows": []})
            return payload

        try:
            with BytesIO(xlsx_bytes) as bio:
                # read ALL sheets, header=None so we can infer a header line
                all_sheets = pd.read_excel(bio, sheet_name=None, header=None, engine="openpyxl")
        except Exception:
            for i in range(1, 10):
                payload["sections"].append({"index": i, "columns": [], "rows": []})
            return payload

        def _first_table(df):
            if df is None or df.empty:
                return [], []
            df2 = df.copy()
            df2 = df2.dropna(axis=0, how="all").dropna(axis=1, how="all")
            if df2.empty:
                return [], []
            header_row = None
            for i in range(min(len(df2), 30)):
                if df2.iloc[i].notna().sum() >= 2:
                    header_row = i
                    break
            if header_row is None:
                return [], []
            df2.columns = [str(c).strip() if pd.notna(c) else f"Col{j+1}" for j, c in enumerate(df2.iloc[header_row])]
            df2 = df2.iloc[header_row + 1:].dropna(how="all")
            cols = list(df2.columns)
            rows = []
            for _, r in df2.iterrows():
                row = {}
                any_val = False
                for c in cols:
                    v = r[c]
                    if pd.notna(v):
                        any_val = True
                        row[str(c)] = v
                    else:
                        row[str(c)] = ""
                if any_val:
                    rows.append(row)
            return cols, rows

        put = False
        for sheet_name, df in all_sheets.items():
            cols, rows = _first_table(df)
            if rows:
                payload["sections"].append({
                    "index": 1, "title": str(sheet_name), "columns": cols, "rows": rows
                })
                put = True
                break

        start = 2 if put else 1
        for i in range(start, 10):
            payload["sections"].append({"index": i, "columns": [], "rows": []})
        return payload

    results = []
    for f in files:
        raw_name = f.filename or ""
        override = (request.form.get("code") or "").strip()
        project_id = (override or _detect_project_id(raw_name)).strip()

        xbytes = f.read()

        # Primary parser – do NOT pass project_id here (old signatures accept
        # (bytes) or (bytes, filename)). We try 2-arg then 1-arg.
        payload = None
        try:
            from app.pqp.ingest.ai_import import parse_workbook_to_payload
            try:
                payload = parse_workbook_to_payload(xbytes, raw_name)
            except TypeError:
                payload = parse_workbook_to_payload(xbytes)
        except Exception as e:
            current_app.logger.warning(f"parse_workbook_to_payload failed for {raw_name}: {e}")

        # Need fallback if no sections or all sections lack rows/data
        def _is_empty(sec_list):
            if not isinstance(sec_list, list):
                return True
            for s in sec_list:
                if any(s.get(k) for k in ("rows", "data", "items", "table")):
                    return False
            return True

        if not payload or not isinstance(payload, dict) or _is_empty(payload.get("sections")):
            payload = _fallback_payload(xbytes, raw_name, project_id)

        # Finalize code (normalize by collapsing internal spaces)
        final_code = (override or payload.get("code") or project_id or "").upper().strip()
        final_code = " ".join(final_code.split())  # e.g. "291RT   P700" -> "291RT P700"
        payload["code"] = final_code

        job = ImportJob(
            filename=raw_name,
            project_code=final_code,
            status="preview",
            issues=[],
            payload=payload,
        )
        db.session.add(job)
        db.session.flush()

        # Count sections that actually have some data
        sec_count = 0
        for s in (payload.get("sections") or []):
            if any(s.get(k) for k in ("rows", "data", "items", "table")):
                sec_count += 1

        results.append({
            "job_id": job.id,
            "filename": raw_name,
            "detected_code": final_code,
            "sections": sec_count,
            "status": "previewed"
        })

    db.session.commit()
    return jsonify({"ok": True, "results": results})
# ==== END REPLACEMENT: /import/ai/preview ====================================


# ==== BEGIN REPLACEMENT: /import/ai/commit ===================================
@pqp_bp.post("/import/ai/commit")
def pqp_ai_import_commit():
    """
    Commits a preview job: writes domain data (via your ingest module) and
    mirrors table rows into PQPSection so the PQP Form immediately shows them.
    """
    import json

    job_id = (request.form.get("job_id") or request.args.get("job_id") or "").strip()
    override = (request.form.get("code") or request.args.get("code") or "").strip()
    if not job_id:
        return jsonify({"ok": False, "error": "Missing job_id"}), 400

    job = db.session.get(ImportJob, int(job_id))
    if not job:
        return jsonify({"ok": False, "error": f"Job {job_id} not found"}), 404

    # Resolve project_id (preferred alias for project_code)
    def _norm(x: str) -> str:
        x = (x or "").upper().strip()
        return " ".join(x.split())
    project_id = _norm(override or (job.payload.get("code") if isinstance(job.payload, dict) else job.project_code))
    if not project_id:
        return jsonify({"ok": False, "error": "No project_id provided or detected"}), 400

    # Ensure section shells exist (1..9)
    try:
        _ensure_sections(project_id)
    except Exception as e:
        current_app.logger.warning(f"_ensure_sections failed for {project_id}: {e}")

    # Write via your ingest module (but don't hard-fail if it errors)
    ok = True
    write_issues = []
    try:
        from app.pqp.ingest.ai_import import commit_payload
        res = commit_payload(job.payload, project_id, db.session)
        if isinstance(res, tuple) and len(res) == 2:
            ok, write_issues = res
        elif isinstance(res, bool):
            ok = res
    except Exception as e:
        ok = False
        write_issues.append(f"commit_payload error: {e}")

    # Mirror preview rows into PQPSection.rows_json (what the forms read)
    def _rows_from_section(sec: dict):
        if not isinstance(sec, dict):
            return []
        return (sec.get("rows") or sec.get("data") or sec.get("items") or sec.get("table") or [])

    try:
        sections = (job.payload or {}).get("sections") if isinstance(job.payload, dict) else None
        if isinstance(sections, list):
            for sec in sections:
                try:
                    idx = int(sec.get("index") or sec.get("section") or 0)
                except Exception:
                    idx = 0
                if idx < 1 or idx > 9:
                    continue

                cols = (sec.get("columns") or sec.get("headers") or [])
                rows = _rows_from_section(sec)

                # Ensure the single section record exists, then upsert its rows/cols
                try:
                    _ensure_sections_by_code(project_id, idx)
                except Exception as e:
                    write_issues.append(f"_ensure_sections_by_code {idx} error: {e}")
                try:
                    _upsert_section_rows(project_id, idx, rows, columns=cols)
                except Exception as e:
                    write_issues.append(f"_upsert_section_rows {idx} error: {e}")
                    ok = False
        else:
            write_issues.append("No sections found in payload to mirror.")
    except Exception as e:
        write_issues.append(f"Mirror error: {e}")
        ok = False

    job.status = "committed" if ok else "failed"
    job.project_code = project_id
    job.issues = write_issues
    db.session.add(job)
    db.session.commit()

    return jsonify({
        "ok": ok,
        "job_id": job.id,
        "project_id": project_id,
        "issues": write_issues,
        "status": job.status
    })
# ==== END REPLACEMENT: /import/ai/commit =====================================






def _load_section_rows(sec):
    payload = None
    if hasattr(sec, "rows_json") and sec.rows_json:
        try: payload = json.loads(sec.rows_json)
        except Exception: payload = None
    if payload is None and getattr(sec, "content", None):
        try: payload = json.loads(sec.content)
        except Exception: payload = None
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
        return payload["rows"]
    return []

def _dump_section_rows(rows): return json.dumps(rows, ensure_ascii=False)

@pqp_bp.post("/pqp/import/<code>/<int:section_idx>/commit")
def pqp_import_commit(code, section_idx):
    file = request.files.get("file")
    if not file or file.filename == "":
        return jsonify({"ok": False, "error": "No file uploaded"}), 400

    expected = get_section_columns(section_idx)
    header, body = _read_csv_file(file)

    missing = [c for c in expected if c not in header]
    if missing:
        return jsonify({"ok": False, "error": "Missing columns", "missing": missing}), 400

    db_number = section_idx + 1
    sec = PQPSection.query.filter_by(project_code=code, section_number=db_number).first()
    if not sec:
        sec = PQPSection(project_code=code, section_number=db_number, title=f"Section {db_number}")
        db.session.add(sec)
        db.session.flush()

    rows = _load_section_rows(sec)

    results = {"ok": True, "created": 0, "updated": 0, "skipped": 0, "errors": []}
    for row_ix, r in enumerate(body, start=2):
        try:
            row = {c: (r[header.index(c)] if c in header and header.index(c) < len(r) else "") for c in expected}
            rid = str(row.get("id") or "")
            if not rid:
                row["id"] = str(int(time.time() * 1000))
                rows.append(row)
                results["created"] += 1
            else:
                updated = False
                for rr in rows:
                    if str(rr.get("id")) == rid:
                        rr.update(row)
                        updated = True
                        break
                if updated:
                    results["updated"] += 1
                else:
                    rows.append(row)
                    results["created"] += 1
        except Exception as e:
            results["skipped"] += 1
            results["errors"].append({"row": row_ix, "reason": str(e)})

    if hasattr(sec, "rows_json"):
        sec.rows_json = _dump_section_rows(rows)
    else:
        sec.content = _dump_section_rows(rows)
    db.session.commit()
    return jsonify(results)

# ------------------------------------------------------------------------------
# Per‑section SAVE (used by Add/Edit modals)
# ------------------------------------------------------------------------------
@pqp_bp.route("/pqp/<code>/section/<int:section_idx>/save", methods=["POST"])
def pqp_section_save(code, section_idx):
    db_number = int(section_idx) + 1
    cols = get_section_columns(section_idx)
    form = request.form.to_dict(flat=True)
    data = {c: form.get(c, "") for c in cols if c != "id"}

    sec = PQPSection.query.filter_by(project_code=code, section_number=db_number).first()
    if not sec:
        sec = PQPSection(project_code=code, section_number=db_number, title=f"Section {db_number}")
        db.session.add(sec)
        db.session.flush()

    table = _load_section_rows(sec)

    edit_id = form.get("id")
    if edit_id:
        replaced = False
        for r in table:
            if str(r.get("id") or "") == str(edit_id):
                r.update(data)
                replaced = True
                break
        if not replaced:
            table.append({"id": edit_id, **data})
    else:
        new_id = str(int(time.time() * 1000))
        table.append({"id": new_id, **data})

    if hasattr(sec, "rows_json"):
        sec.rows_json = _dump_section_rows(table)
    else:
        sec.content = _dump_section_rows(table)

    db.session.commit()
    flash("Saved.", "success")
    return redirect(url_for("pqp.pqp_form_by_code", code=code))

# ------------------------------------------------------------------------------
# Export & Reports
# ------------------------------------------------------------------------------
@pqp_bp.get("/pqp/<code>/export/zip-csv")
def pqp_export_zip_csv(code):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for idx, cols in enumerate(SECTION_DEFS):
            sec = PQPSection.query.filter_by(project_code=code, section_number=idx+1).first()
            rows = _load_section_rows(sec) if sec else []
            s = io.StringIO(newline="")
            w = csv.writer(s)
            w.writerow(cols)
            for r in rows:
                if isinstance(r, dict):
                    w.writerow([r.get(c, "") for c in cols])
                elif isinstance(r, (list, tuple)):
                    w.writerow([r[i] if i < len(r) else "" for i in range(len(cols))])
            zf.writestr(f"section_{idx+1}.csv", s.getvalue())
    buf.seek(0)
    return send_file(buf, mimetype="application/zip",
                     as_attachment=True, download_name=f"PQP_{code}_CSV.zip")

@pqp_bp.get("/pqp/<code>/summary")
def pqp_summary_html(code):
    project = {"Code": code}
    sections = []
    for idx, cols in enumerate(SECTION_DEFS):
        sec = PQPSection.query.filter_by(project_code=code, section_number=idx+1).first()
        rows = _load_section_rows(sec) if sec else []
        sections.append({"index": idx+1, "columns": cols, "rows": rows})
    return render_template("pqp_summary.html", project=project, sections=sections)


# ===== Add to app/pqp/pqp_routes.py (once) =====

@pqp_bp.get("/export")
def pqp_export_center():
    """Landing page that lists projects and offers one-click CSV ZIP export and Summary."""
    # If you already have a query for codes elsewhere, reuse it
    
    PR = _projectrecords(db.engine)
    rows = db.session.execute(
        select(_col(PR, PR_COL_CODE).label("Code"), _col(PR, PR_COL_SHORTDESC).label("Short Description"))
        .order_by(_col(PR, PR_COL_CODE)).limit(500)
    ).fetchall()
    items = [{"code": r._mapping["Code"], "short": r._mapping["Short Description"]} for r in rows if r._mapping["Code"]]
    return render_template("export_center.html", items=items)

@pqp_bp.get("/reminders")
def pqp_reminders():
    """Landing page to trigger overdue-email reminders per project."""
    
    PR = _projectrecords(db.engine)
    rows = db.session.execute(
        select(_col(PR, PR_COL_CODE).label("Code"), _col(PR, PR_COL_PM).label("Project Manager"))
        .order_by(_col(PR, PR_COL_CODE)).limit(500)
    ).fetchall()
    items = [{"code": r._mapping["Code"], "pm": r._mapping["Project Manager"]} for r in rows if r._mapping["Code"]]
    return render_template("reminders.html", items=items)




# --- Put these near the bottom of app/pqp/pqp_routes.py ---








# --- NAV PAGES (must match the names used in base.html) ---






@pqp_bp.get("/settings")
def pqp_settings():
    return render_template("settings.html")

@pqp_bp.get("/help")
def pqp_help():
    return render_template("help.html")


# ======== NAV LANDING PAGES (safe, non-conflicting) ========
# Do NOT add /pqp in the paths below – the blueprint already has url_prefix="/pqp"

@pqp_bp.get("/reports", endpoint="pqp_reports")
def _pqp_reports_page():
    """Reports landing page."""
    
    PR = _projectrecords(db.engine)
    rows = db.session.execute(
        select(_col(PR, PR_COL_CODE).label("Code"),
               _col(PR, PR_COL_SHORTDESC).label("Short Description"))
        .order_by(_col(PR, PR_COL_CODE)).limit(300)
    ).fetchall()
    items = [{"code": r._mapping["Code"], "short": r._mapping["Short Description"]}
             for r in rows if r._mapping["Code"]]
    return render_template("reports.html", items=items)






# ======== END NAV LANDING PAGES ========


# ---------- NAV/UTILITY ALIASES (ADD ONCE) ----------

# Hard-link target for the navbar "Project Selector"
@pqp_bp.get("/form/select", endpoint="pqp_form_select")
def _pqp_form_select_alias():
    # If you already have a selector view (likely endpoint: pqp_form_select_by_code),
    # redirect to it. Otherwise, render your selector template here.
    return redirect(url_for("pqp.pqp_form_select_by_code"))

# POST API to trigger email reminders for a project (matches templates' expectation)
@pqp_bp.post("/<code>/email/reminders", endpoint="pqp_email_reminders")
def _pqp_email_reminders_api(code: str):
    """
    Demo implementation: wire your real logic here (find overdue items, send emails).
    Returning JSON keeps the page snappy; pages call this via fetch().
    """
    # TODO: replace with real logic:
    # sent_count = send_overdue_emails_for_project(code)
    sent_count = 0
    return jsonify({"ok": True, "project": code, "sent": sent_count})


# --- CSV template for a given section (used by Import/Export tab) ---
import io, csv
from flask import make_response

@pqp_bp.get("/template/csv/<int:section_idx>", endpoint="pqp_template_csv")
def pqp_template_csv(section_idx: int):
    # Use your SECTION_DEFS if available; fall back to a simple header
    try:
        cols = SECTION_DEFS[section_idx]
        if not isinstance(cols, (list, tuple)) or not cols:
            cols = ["id","Title","Description","Status","Notes"]
    except Exception:
        cols = ["id","Title","Description","Status","Notes"]

    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(cols)
    data = buf.getvalue()
    buf.close()

    resp = make_response(data)
    resp.headers["Content-Type"] = "text/csv; charset=utf-8"
    resp.headers["Content-Disposition"] = (
        f'attachment; filename="pqp_section_{section_idx+1}_template.csv"'
    )
    return resp


# === Project quick view/edit ===
def load_project_options():
    PR = _projectrecords(db.engine)
    query = select(
        _col(PR, PR_COL_CODE).label("Code"),
        _col(PR, PR_COL_SHORTDESC).label("Short Description"),
        _col(PR, PR_COL_CLIENT).label("Client"),
        _col(PR, PR_COL_PM).label("Project Manager"),
        _col(PR, "Appointment\nDate").label("Start"),
        _col(PR, "Close-Out\nDate").label("End"),
    ).order_by(_col(PR, PR_COL_CODE))
    rows = db.session.execute(query.limit(200)).fetchall()
    options = []
    for r in rows:
        m = r._mapping
        code_val = (m.get("Code") or "").strip()
        if not code_val:
            continue
        options.append({
            "code": code_val,
            "short": (m.get("Short Description") or "").strip(),
            "client": (m.get("Client") or "").strip(),
            "pm": (m.get("Project Manager") or "").strip(),
            "start": (m.get("Start") or "").strip(),
            "end": (m.get("End") or "").strip(),
        })
    return options

# --- Full-detail loader (all columns) ---
def load_project_detail(code: str):
    PR = _projectrecords(db.engine)
    row = db.session.execute(
        select(PR).where(_col(PR, PR_COL_CODE) == code)
    ).mappings().first()
    return dict(row) if row else None


@pqp_bp.route("/project/<string:code>", methods=["GET"])
def project_edit(code):
    detail = load_project_detail((code or "").strip())
    
    
    
    
    if not detail:
        return render_template("project_edit.html", project=None, code=code), 404

    # Build dynamic field list: slug -> original column name (even with spaces/newlines)

    used, fields = set(), []
    for col_name, value in detail.items():
        slug = re.sub(r"[^A-Za-z0-9]+", "_", col_name).strip("_").lower() or "col"
        base, i = slug, 2
        while slug in used:
            slug = f"{base}_{i}"; i += 1
        used.add(slug)

        ftype = "date" if _is_date_col(col_name) else ("money" if _is_money_col(col_name) else "text")
        fvalue = _to_input_date(value) if ftype == "date" else ("" if value is None else str(value))

        fields.append({
            "slug": slug,
            "col": col_name,
            "value": fvalue,
            "readonly": (col_name == PR_COL_CODE),
            "type": ftype,
        })



    # Sort: Code first, then alphabetical
    fields.sort(key=lambda f: (f["readonly"] is False, f["col"].lower()))
    return render_template("project_edit.html", code=code, project=detail, fields=fields)

@pqp_bp.route("/project/<string:code>/update", methods=["POST"], endpoint="project_update")
def project_update(code):
    PR = _projectrecords(db.engine)
    form = request.form

    # reconstruct {original_column_name -> posted_value} using the hidden mapping
    payload = {}
    for k, v in form.items():
        if not k.startswith("__orig__"):
            continue
        slug = k[len("__orig__"):]
        orig_col = v or ""
        if not orig_col or orig_col == PR_COL_CODE:  # keep Code read-only
            continue
        if slug in form:
            payload[orig_col] = form[slug]

    if not payload:
        return jsonify({"ok": False, "error": "No fields to update"}), 400

    # Use real column objects so names with spaces/newlines work
    assigns = { _col(PR, col_name): val for col_name, val in payload.items() }

    db.session.execute(
        update(PR)
        .where(_col(PR, PR_COL_CODE) == code)
        .values(assigns)    # pass dict, NOT **kwargs
    )
    db.session.commit()
    return jsonify({"ok": True})






def _is_date_col(col_name: str) -> bool:
    n = col_name.lower().replace("\n", " ")
    return ("date" in n) or (("start" in n or "end" in n) and "update" not in n)

def _to_input_date(v):
    if v is None:
        return ""
    if isinstance(v, (datetime, date)):
        return v.date().isoformat() if isinstance(v, datetime) else v.isoformat()
    s = str(v).strip()
    # strip weekday: "Wed 13 Aug 2025" -> "13 Aug 2025"
    s = re.sub(r'^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*\s+', '', s, flags=re.I)
    fmts = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%d %b %Y", "%d %B %Y"]
    for f in fmts:
        try:
            return datetime.strptime(s, f).date().isoformat()
        except Exception:
            pass
    # try generic
    try:
        return datetime.fromisoformat(s).date().isoformat()
    except Exception:
        m = re.fullmatch(r"(\d{4})[-/ ]?(\d{2})[-/ ]?(\d{2})", s)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        return ""

def _is_money_col(col_name: str) -> bool:
    n = col_name.lower()
    # expand this list if you need
    return any(k in n for k in ("fee","fees","cost","costs","budget","amount","value","price","rand","zar"))



def _digits_only(s: str) -> str:
    return re.sub(r"\D+", "", (s or ""))

@pqp_bp.get("/project/check_code")
def project_code_check():
    code = (request.args.get("code") or "").strip()
    if not code:
        return jsonify({"ok": False, "duplicate": False, "reason": "empty"}), 200

    PR = _projectrecords(db.engine)
    existing = db.session.execute(
        select(_col(PR, PR_COL_CODE))
    ).scalars().all()

    cand = code.upper()
    cand_digits = _digits_only(code)
    dup = False
    match = None
    for c in existing:
        if not c: continue
        cu = str(c).upper()
        if cu == cand:                # exact match ignoring case
            dup, match = True, c; break
        if _digits_only(str(c)) == cand_digits and cand_digits != "":
            dup, match = True, c; break

    return jsonify({"ok": True, "duplicate": dup, "match": match}), 200


@pqp_bp.post("/project/create")
def project_create():
    PR = _projectrecords(db.engine)
    form = request.form

    code = (form.get("code") or "").strip()
    name = (form.get("name") or "").strip()
    client = (form.get("client") or "").strip()
    manager = (form.get("manager") or "").strip()
    start = (form.get("start_date") or "").strip()
    end   = (form.get("end_date") or "").strip()

    if not code:
        return jsonify({"ok": False, "error": "Project Code is required"}), 400
    # duplicate check (same rules as /check_code)
    cc = request.args.get("skipdup") != "1"
    if cc:
        chk = project_code_check().json if hasattr(project_code_check(), "json") else None  # fallback
        # safer: query again here
        existing = db.session.execute(select(_col(PR, PR_COL_CODE))).scalars().all()
        if any((str(c or "").upper() == code.upper()) or (_digits_only(c) == _digits_only(code) and _digits_only(code)!="") for c in existing):
            return jsonify({"ok": False, "error": "Duplicate project code"}), 409

    # convert dates to DB's expected string (we store as text; update if your columns are DATE)
    def norm_date(s): 
        try: return _to_input_date(s)
        except: return s

    assigns = {
        _col(PR, PR_COL_CODE): code,
        _col(PR, PR_COL_SHORTDESC): name,
        _col(PR, PR_COL_CLIENT): client,
        _col(PR, PR_COL_PM): manager,
        _col(PR, "Appointment\nDate"): norm_date(start),
        _col(PR, "Close-Out\nDate"): norm_date(end),
    }

    db.session.execute(insert(PR).values(assigns))
    db.session.commit()
    return jsonify({"ok": True, "code": code})


# app/pqp/pqp_models.py  (append this block)


from app import db

@pqp_bp.get("/import-center", endpoint="import_center")
def import_center():
    return render_template("import_center.html")




# --- Bulk Import PQPs (minimal-risk; reuses your existing parse/commit) ---
from flask import Blueprint, request, jsonify
from werkzeug.datastructures import FileStorage

# If not already imported:
# from .parser import parse_workbook_to_payload
# from .commit import commit_payload

@pqp_bp.post("/pqp/import/bulk")
def pqp_import_bulk():
    files: list[FileStorage] = request.files.getlist('files')
    if not files:
        return jsonify({"ok": False, "error": "No files uploaded"}), 400

    results = []
    for f in files:
        try:
            payload, issues = parse_workbook_to_payload(f)   # uses your existing parser
            if issues:
                results.append({"file": f.filename, "status": "error", "issues": issues})
                continue
            summary = commit_payload(payload)                 # uses your existing commit
            results.append({"file": f.filename, "status": "ok", "summary": summary})
        except Exception as e:
            results.append({"file": f.filename, "status": "error", "error": str(e)})
    return jsonify({"ok": True, "results": results})




# Debugs 

# --- Debug: list recent AI jobs ---
@pqp_bp.get("/debug/jobs")
def pqp_debug_jobs():
    jobs = (db.session.query(ImportJob)
            .order_by(ImportJob.id.desc())
            .limit(25)
            .all())
    out = []
    for j in jobs:
        out.append({
            "id": j.id,
            "filename": j.filename,
            "status": j.status,
            "project_code": (j.project_code or ""),
            "issues": j.issues,
        })
    return jsonify(out)



# --- Debug: DB info (engine URL, sqlite file path, quick counts) ---
@pqp_bp.get("/debug/dbinfo")
def pqp_debug_dbinfo():
    from sqlalchemy import text
    url = str(db.engine.url)
    dbpath = ""
    try:
        if url.startswith("sqlite"):
            res = db.session.execute(text("PRAGMA database_list")).fetchall()
            dbpath = res[0][2] if res else ""
    except Exception:
        pass
    counts = {}
    try:
        counts["pqp_sections"] = db.session.execute(text("SELECT COUNT(*) FROM pqp_section")).scalar()
    except Exception:
        counts["pqp_sections"] = "N/A"
    try:
        counts["import_jobs"] = db.session.execute(text("SELECT COUNT(*) FROM import_job")).scalar()
    except Exception:
        counts["import_jobs"] = "N/A"
    return jsonify({"engine_url": url, "sqlite_file": dbpath, "counts": counts})

@pqp_bp.get("/debug/sections/<code>")
def pqp_debug_sections(code):
    code = (code or "").strip().upper()
    payload = []
    for i in range(1, 10):
        sec = PQPSection.query.filter_by(project_code=code, section_number=i).first()
        rows = []
        if sec:
            raw = getattr(sec, "rows_json", None) or getattr(sec, "content", None)
            if raw:
                try:
                    js = json.loads(raw)
                    if isinstance(js, list):
                        rows = js
                    elif isinstance(js, dict) and "rows" in js:
                        rows = js.get("rows") or []
                except Exception:
                    rows = []
        payload.append({"section": i, "row_count": len(rows), "sample": rows[:3]})
    return jsonify({"code": code, "sections": payload})



# ======================= JSON API (non-UI)  =======================
# This leaves your big UI blueprint intact. We add a tiny API-only blueprint.
from flask import current_app

pqp_api_bp = Blueprint("pqp_api", __name__, url_prefix="/api/pqp")

def _needs_org():
    return current_app.config.get("HAS_ORG_ID", False)

def _get_org():
    # Require org_id only when SaaS columns exist
    if not _needs_org(): 
        return None, None
    org_id = (
        request.headers.get("X-Org-Id")
        or request.args.get("org_id")
        or (request.get_json(silent=True) or {}).get("org_id")
    )
    if not org_id:
        return None, (jsonify(error="org_id is required (SaaS mode)"), 400)
    return org_id, None

@pqp_api_bp.get("/projects")
def api_list_projects():
    org_id, err = _get_org()
    if err: return err
    if _needs_org():
        rows = db.session.execute(
            text("select id, name, org_id, created_at from project where org_id=:o order by id desc"),
            {"o": org_id}
        ).fetchall()
    else:
        rows = db.session.execute(
            text("select id, name, created_at from project order by id desc")
        ).fetchall()
    return jsonify([dict(r._mapping) for r in rows])

@pqp_api_bp.post("/projects")
def api_create_project():
    data = request.get_json(force=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify(error="name is required"), 400

    if _needs_org():
        org_id, err = _get_org()
        if err: return err
        row = db.session.execute(
            text("insert into project (name, org_id) values (:n, :o) returning id, name, org_id, created_at"),
            {"n": name, "o": org_id}
        ).fetchone()
    else:
        row = db.session.execute(
            text("insert into project (name) values (:n) returning id, name, created_at"),
            {"n": name}
        ).fetchone()
    db.session.commit()
    return jsonify(dict(row._mapping)), 201

@pqp_api_bp.get("/projects/<int:pid>/checklist")
def api_get_checklist(pid: int):
    org_id, err = _get_org()
    if err: return err
    if _needs_org():
        rows = db.session.execute(
            text("""select id, project_id, org_id, section, item, status, due_date, completed_at, assigned_to
                    from checklist_item where project_id=:p and org_id=:o order by id"""),
            {"p": pid, "o": org_id}
        ).fetchall()
    else:
        rows = db.session.execute(
            text("""select id, project_id, section, item, status, due_date, completed_at, assigned_to
                    from checklist_item where project_id=:p order by id"""),
            {"p": pid}
        ).fetchall()
    return jsonify([dict(r._mapping) for r in rows])

@pqp_api_bp.post("/projects/<int:pid>/checklist")
def api_add_checklist(pid: int):
    org_id, err = _get_org()
    if err: return err
    payload = request.get_json(force=True)
    if isinstance(payload, dict): payload = [payload]
    if not isinstance(payload, list) or not payload:
        return jsonify(error="Send a JSON object or a list of objects"), 400

    out = []
    for o in payload:
        section = (o.get("section") or "").strip()
        item    = (o.get("item") or "").strip()
        status  = (o.get("status") or "pending").strip().lower()
        due_date = o.get("due_date")
        assigned_to = o.get("assigned_to")
        if not section or not item:
            return jsonify(error="Each checklist item needs 'section' and 'item'"), 400
        if due_date:
            datetime.strptime(due_date, "%Y-%m-%d")  # validate

        if _needs_org():
            row = db.session.execute(
                text("""insert into checklist_item (project_id, org_id, section, item, status, due_date, assigned_to)
                        values (:p,:o,:s,:i,:st,:d,:a)
                        returning id, project_id, org_id, section, item, status, due_date, completed_at, assigned_to"""),
                {"p": pid, "o": org_id, "s": section, "i": item, "st": status, "d": due_date, "a": assigned_to}
            ).fetchone()
        else:
            row = db.session.execute(
                text("""insert into checklist_item (project_id, section, item, status, due_date, assigned_to)
                        values (:p,:s,:i,:st,:d,:a)
                        returning id, project_id, section, item, status, due_date, completed_at, assigned_to"""),
                {"p": pid, "s": section, "i": item, "st": status, "d": due_date, "a": assigned_to}
            ).fetchone()
        out.append(dict(row._mapping))
    db.session.commit()
    return jsonify(out), 201

@pqp_api_bp.patch("/checklist/<int:item_id>")
def api_update_checklist(item_id: int):
    org_id, err = _get_org()
    if err: return err
    data = request.get_json(force=True) or {}
    sets, params = [], {"id": item_id}

    for k in ("section", "item", "status", "assigned_to"):
        if k in data and data[k] is not None:
            sets.append(f"{k} = :{k}"); params[k] = data[k]

    if "due_date" in data:
        if data["due_date"]:
            datetime.strptime(data["due_date"], "%Y-%m-%d")
            sets.append("due_date = :due_date"); params["due_date"] = data["due_date"]
        else: sets.append("due_date = null")

    if "completed_at" in data:
        if data["completed_at"]:
            datetime.fromisoformat(data["completed_at"])
            sets.append("completed_at = :completed_at"); params["completed_at"] = data["completed_at"]
        else: sets.append("completed_at = null")

    if not sets:
        return jsonify(error="No fields to update"), 400

    where = "id = :id" + (" and org_id = :o" if _needs_org() else "")
    if _needs_org(): params["o"] = org_id

    row = db.session.execute(
        text(f"update checklist_item set {', '.join(sets)} where {where} returning *"), params
    ).fetchone()
    if not row:
        return jsonify(error="Checklist item not found"), 404
    db.session.commit()
    return jsonify(dict(row._mapping))

@pqp_api_bp.delete("/checklist/<int:item_id>")
def api_delete_checklist(item_id: int):
    org_id, err = _get_org()
    if err: return err
    where = "id = :id" + (" and org_id = :o" if _needs_org() else "")
    params = {"id": item_id}
    if _needs_org(): params["o"] = org_id
    row = db.session.execute(text(f"delete from checklist_item where {where} returning id"), params).fetchone()
    if not row:
        return jsonify(error="Checklist item not found"), 404
    db.session.commit()
    return jsonify(ok=True)
# ===================== end JSON API (non-UI) ======================




# --- Section 3 loader (for your main UI) ---

def _to_str(v):
    """Coerce any DB value to a user-safe short string."""
    if v is None:
        return ""
    if isinstance(v, (date, datetime)):
        # store/compare as ISO date (ignore time for this UI)
        return v.strftime("%Y-%m-%d")
    return str(v)

def _normalize_rows(cols, rows):
    """
    Ensure each row is a dict with all keys in cols and 'id' if needed.
    Values are stringified for display.
    """
    out = []
    for r in rows:
        if isinstance(r, dict):
            d = {c: _to_str(r.get(c, "")) for c in cols}
        elif isinstance(r, (list, tuple)):
            d = {c: _to_str(r[i] if i < len(r) else "") for i, c in enumerate(cols)}
        else:
            continue
        if "id" in cols and "id" not in d:
            d["id"] = ""
        out.append(d)
    return out

def _load_section3_from_unified(engine, project_code):
    """
    Reads pqp.section3_unified and returns:
      (cols_by_part, data_by_part)
    where part ∈ {'31','32','33'}.
    Columns are the union of keys found in JSONB 'data' for that part,
    ordered with 'id' first if present.
    """
    sql = text("""
        select part, row_id, data
        from pqp.section3_unified
        where project_code = :code
        order by part::int, row_id
    """)
    part_rows = {"31": [], "32": [], "33": []}
    try:
        rows = db.session.execute(sql, {"code": project_code}).mappings().all()
    except Exception:
        # If table/view doesn't exist yet, just return empty structures
        return {k: [] for k in part_rows}, {k: [] for k in part_rows}

    # Collect raw dict rows and the union of keys per part
    keys_by_part = {k: set() for k in part_rows}
    raw_by_part = {k: [] for k in part_rows}

    for m in rows:
        part = str(m["part"])
        if part not in part_rows:
            continue  # ignore unexpected parts
        payload = m.get("data")
        if isinstance(payload, (dict, list)):
            row_obj = payload
        else:
            # JSONB may have come back as string on some drivers
            try:
                row_obj = json.loads(payload) if payload is not None else {}
            except Exception:
                row_obj = {}

        if isinstance(row_obj, dict):
            raw_by_part[part].append(row_obj)
            keys_by_part[part].update(row_obj.keys())
        else:
            # If somehow it's not a dict, skip (or wrap as {'value': ...})
            continue

    # Finalize column orders (id first when present)
    cols_by_part = {}
    data_by_part = {}
    for part, keys in keys_by_part.items():
        cols = list(sorted(keys))
        if "id" in keys:
            cols.remove("id")
            cols = ["id"] + cols
        cols_by_part[part] = cols
        data_by_part[part] = _normalize_rows(cols, raw_by_part[part])

    return cols_by_part, data_by_part


@pqp_bp.route("/form/code/<code>", methods=["GET"])
def pqp_form_by_code(code):
    """
    Project form for a single project_code.
    Sections 1,2,4–9 come from PQPSection JSON.
    Section 3 comes from pqp.section3_unified (parts 31/32/33).
    """
    from datetime import date, datetime

    # ---- Project header (from "ProjectRecords" view) ----
    PR = _projectrecords(db.engine)

    # Required columns, plus optional ones if present
    cols = [
        _col(PR, PR_COL_CODE).label("Code"),
        _col(PR, PR_COL_SHORTDESC).label("Short Description"),
        _col(PR, PR_COL_CLIENT).label("Client"),
        _col(PR, PR_COL_PM).label("Project Manager"),
    ]
    for cname, lbl in [("start_date", "start_date"),
                       ("end_date", "end_date"),
                       (PR_COL_STATUS, "Status")]:
        try:
            cols.append(_col(PR, cname).label(lbl))
        except KeyError:
            pass

    row = db.session.execute(
        select(*cols).where(_col(PR, PR_COL_CODE) == code)
    ).first()
    if not row:
        flash("Project Code not found.", "danger")
        return redirect(url_for("pqp.pqp_form_select_by_code"))

    # stringify DB values for the template
    project = {k: _to_str(v) for k, v in row._mapping.items()}

    # Derive a Status if the view doesn't provide one
    if not project.get("Status"):
        status = "Active"
        try:
            end_dt = row._mapping.get("end_date")
            if isinstance(end_dt, datetime):
                status = "Closed" if end_dt.date() < date.today() else "Active"
            elif isinstance(end_dt, date):
                status = "Closed" if end_dt < date.today() else "Active"
        except Exception:
            pass
        project["Status"] = status

    # ---- Ensure PQPSection scaffold exists ----
    _ensure_sections_by_code(code)

    # ---- Load sections 1,2,4–9 from PQPSection ----
    section_columns = SECTION_DEFS
    section_data = [[] for _ in range(len(section_columns))]

    def _rows_of(sec):
        payload = None
        if hasattr(sec, "rows_json") and sec.rows_json:
            try:
                payload = json.loads(sec.rows_json)
            except Exception:
                pass
        if payload is None and getattr(sec, "content", None):
            try:
                payload = json.loads(sec.content)
            except Exception:
                pass
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
            return payload["rows"]
        return []

    sections = (PQPSection.query
                .filter_by(project_code=code)
                .order_by(PQPSection.section_number)
                .all())

    for sec in sections:
        if (sec.section_number or 1) == 3:
            continue
        idx = max(0, (sec.section_number or 1) - 1)
        cols = section_columns[idx]
        section_data[idx].extend(_normalize_rows(cols, _rows_of(sec)))

    # ---- Load Section 3 (parts 31/32/33) from pqp.section3_unified ----
    # old:
    # section3_cols, section3_data = _load_section3_from_unified(db.engine, code)

    # new:
    section3_cols, section3_data = _load_section3_from_parts(code)


    return render_template(
        "pqp_form.html",
        project=project,
        section_columns=section_columns,  # non-Section-3 panels
        section_data=section_data,        # non-Section-3 panels
        section3_cols=section3_cols,      # {'31':[...],'32':[...],'33':[...]}
        section3_data=section3_data,      # {'31':[{}],'32':[{}],'33':[{}]}
        pqp_detail=None,
    )
