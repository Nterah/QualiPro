# app/pqp/pqp_routes.py
import os
import io
import csv
import json
import time
import zipfile
from datetime import date, datetime

import psycopg2.extras
from flask import jsonify

# DB connection helper
from contextlib import contextmanager
from sqlalchemy import text
from app.extensions import db

@contextmanager
def get_db_connection():
    """
    Yields a SQLAlchemy Connection. We also defensively set the schema,
    in case the global event hook didn't run.
    """
    with db.engine.connect() as conn:
        try:
            conn.execute(text("SET search_path TO pqp, public"))
        except Exception:
            # harmless if it fails, the app-level event usually handles this
            pass
        yield conn



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

def _norm_code(s: str) -> str:
    # normalizes "291RT+P232" → "291RT P232", trims stray spaces
    return (s or "").replace("+", " ").strip()



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



# --- Harden section lookup: map codes -> table(s)
SECTION_META = {
    "section101":   {"tables": ["pqp.section101"], "title": "Section 101"},
    "risk_concept": {"tables": ["pqp.risk_concept"], "title": "Concept & Design Development"},
    "risk_docs":    {"tables": ["pqp.risk_docs"],    "title": "Documentation & Tender"},
    "risk_works":   {"tables": ["pqp.risk_works"],   "title": "Implementation / Works"},
    # Example for multi-table sections later:
    # "section3": {"tables": ["pqp.section3_header", "pqp.section3_items"], "title": "Appointment & Milestones"}
}

def _columns_for_table(engine, table_qualified: str):
    """Return column list using information_schema (ordered)."""
    schema, table = table_qualified.split('.', 1)
    sql = """
    select column_name
    from information_schema.columns
    where table_schema = :s and table_name = :t
    order by ordinal_position
    """
    from sqlalchemy import text
    with engine.connect() as c:
        return [r[0] for r in c.execute(text(sql), {"s": schema, "t": table})]





# --- Section → DB table mapping (aligned with Supabase current schema) ---
# NOTE: Section 3 is composite (31/32/33) and is handled by _load_section3_from_parts.
#       Section 9 (Scope Register) may be composite (91/92); we keep it special-friendly.
SECTION_TABLE = {
    1: "pqp.section1",
    2: "pqp.section2",
    3: None,              # composite, handled separately
    4: "pqp.section4",
    5: "pqp.section5",
    6: "pqp.section6",
    7: "pqp.section7",
    8: "pqp.section8",
    9: "pqp.section9",    # if yours is split 91/92, leave this as None and load via its own helper
    10: None,             # keep spare; your UI shows up to 10 panels
}

# --- UI column labels per panel (kept exactly as your templates expect) ---
SECTION_COLS = {
    1: ["id","Project Description","Location","Client Organisation","Primary Contact Name",
        "VAT Number","Designation","Invoice Address"],
    2: ["id","Role","Req'd","Organisation","Representative Name","Email","Cell",
        "Subconsultant to HN?","Subconsultant Agreement?","CPG Partner?","CPG %","Comments"],
    3: ["id","In Place","Date","Filing Location","Notes",
        "Appointment Date","Expected Duration","Contract/Ref No","Comments"],
    4: ["id","Design Criteria/Requirements","Planning & Design Risks",
        "Scope Register Location","Design Notes"],
    5: ["id","Client Tender Doc Requirements","Form of Contract","Standard Specs",
        "Client Template Date","Documentation Risks","Tender Phase Notes"],
    6: ["id","Construction Description","Contractor Organisation","Contract Number",
        "Award Value (incl VAT)","Award Date","Original Order No","Original Date of Order",
        "Inception Meeting Date","Final Payment Cert Date","Final Value (incl VAT)",
        "Commencement of Works","Date of EA's Instruction","Where Instruction Recorded",
        "Completion Date","Final Approval Date","Client Takeover Date",
        "Commencement Instruction Date","Commencement Instruction Location",
        "Construction Phase Risks","Construction Phase Notes"],
    7: ["id","Additional Services Done","Project-specific Risks",
        "Mitigating Measures","Record of Action Taken","Notes"],
    8: ["id","Date CSQ Submitted","Date CSQ Received","CSQ Rating","Location",
        "Comments on Feedback","Actual Close-Out Date","General Remarks/Lessons Learned"],
    9: ["id","Scope Item","Category","Owner","Status","Due Date","Notes"],
    10: ["id"],  # reserved; template will tolerate empty rows
}

# --- DB→UI column name mapping per section (non-destructive; best effort) ---
# Left side: actual DB column names; right side: your UI label names in SECTION_COLS.
# If a column is missing, it is silently ignored.
DB_TO_UI_COLS = {
    1: {
        "project_code": "id",
        "project_description": "Project Description",
        "location": "Location",
        "client_organisation": "Client Organisation",
        "primary_contact_name": "Primary Contact Name",
        "vat_number": "VAT Number",
        "designation": "Designation",
        "invoice_address": "Invoice Address",
    },
    2: {
        "project_code": "id",
        "role": "Role",
        "required": "Req'd",
        "organisation": "Organisation",
        "representative_name": "Representative Name",
        "email": "Email",
        "cell": "Cell",
        "is_subconsultant": "Subconsultant to HN?",
        "has_subconsultant_agreement": "Subconsultant Agreement?",
        "is_cpg_partner": "CPG Partner?",
        "cpg_percent": "CPG %",
        "comments": "Comments",
    },
    4: {
        "project_code": "id",
        "design_criteria": "Design Criteria/Requirements",
        "planning_design_risks": "Planning & Design Risks",
        "scope_register_location": "Scope Register Location",
        "design_notes": "Design Notes",
    },
    5: {
        "project_code": "id",
        "client_tender_requirements": "Client Tender Doc Requirements",
        "form_of_contract": "Form of Contract",
        "standard_specs": "Standard Specs",
        "client_template_date": "Client Template Date",
        "documentation_risks": "Documentation Risks",
        "tender_phase_notes": "Tender Phase Notes",
    },
    6: {
        "project_code": "id",
        "construction_description": "Construction Description",
        "contractor_organisation": "Contractor Organisation",
        "contract_number": "Contract Number",
        "award_value_incl_vat": "Award Value (incl VAT)",
        "award_date": "Award Date",
        "original_order_no": "Original Order No",
        "original_order_date": "Original Date of Order",
        "inception_meeting_date": "Inception Meeting Date",
        "final_payment_cert_date": "Final Payment Cert Date",
        "final_value_incl_vat": "Final Value (incl VAT)",
        "commencement_of_works": "Commencement of Works",
        "ea_instruction_date": "Date of EA's Instruction",
        "ea_instruction_location": "Where Instruction Recorded",
        "completion_date": "Completion Date",
        "final_approval_date": "Final Approval Date",
        "client_takeover_date": "Client Takeover Date",
        "commencement_instruction_date": "Commencement Instruction Date",
        "commencement_instruction_location": "Commencement Instruction Location",
        "construction_phase_risks": "Construction Phase Risks",
        "construction_phase_notes": "Construction Phase Notes",
    },
    7: {
        "project_code": "id",
        "additional_services_done": "Additional Services Done",
        "project_specific_risks": "Project-specific Risks",
        "mitigating_measures": "Mitigating Measures",
        "record_of_action": "Record of Action Taken",
        "notes": "Notes",
    },
    8: {
        "project_code": "id",
        "date_csq_submitted": "Date CSQ Submitted",
        "date_csq_received": "Date CSQ Received",
        "csq_rating": "CSQ Rating",
        "location": "Location",
        "feedback_comments": "Comments on Feedback",
        "actual_close_out_date": "Actual Close-Out Date",
        "general_remarks": "General Remarks/Lessons Learned",
    },
    9: {
        "project_code": "id",
        "scope_item": "Scope Item",
        "category": "Category",
        "owner": "Owner",
        "status": "Status",
        "due_date": "Due Date",
        "notes": "Notes",
    },
}

# === BEGIN ADD: multi-table section metadata ===============================

# For sections that are composed of multiple physical tables (e.g., 3 = 31/32/33,
# 4 = 41/42, etc). The UI will use these to render sub-panels, and the loader
# will know which underlying tables to read from.
SECTION_PART_TABLES = {
    3:  {
        "31": ("Appointment — Records/Storage",   "pqp.section31"),
        "32": ("Appointment — Review",            "pqp.section32"),
        "33": ("Appointment — Deliverables",      "pqp.section33"),
    },
    4:  {
        "41": ("Planning & Design — Criteria",    "pqp.section41"),
        "42": ("Planning & Design — Approvals",   "pqp.section42"),
    },
    5:  {
        "51": ("Documentation — Requirements",    "pqp.section51"),
        "52": ("Tender — Notes/Risks",            "pqp.section52"),
    },
    6:  {
        "61": ("Works — Contract/Award",          "pqp.section61"),
        "62": ("Works — Dates/Instructions",      "pqp.section62"),
        "63": ("Works — Completion/Final",        "pqp.section63"),
    },
    7:  {
        "71": ("Additional Services — Items",     "pqp.section71"),
        "72": ("Additional Services — Actions",   "pqp.section72"),
    },
    9:  {
        "91": ("Scope Register — Items",          "pqp.section91"),
        "92": ("Scope Register — Tasks",          "pqp.section92"),
    },
    10: {
        "101": ("Risk Register",                  "pqp.section101"),
    },
}

# Optional: if you already know a part’s column order, declare it here so the UI
# can label columns consistently. When not provided, we’ll derive from the table
# itself (information_schema) and still display.
SECTION_PART_COLS = {
    # Section 3 already has a dedicated loader; you can leave it empty here or
    # duplicate it for consistency. Example:
    3: {
        "31": ["id", "item", "in_place", "date_val", "filing_location", "notes"],
        "32": ["id", "appointment_review_date", "appointment_reviewer", "review_comments",
               "appointment_roles", "appointment_date", "expected_duration",
               "original_end_date", "contract_ref_no", "general_comments"],
        "33": ["id", "ecsa_project_stage", "date_completed", "description_of_deliverable",
               "deliverable", "deliverable_accepted", "employer_approved", "comments"],
    },
    # Section 10 (Risk Register) — from your screenshot/notes
    10: {
        "101": [
            "heading_id","id","risk_code","title","description","cause","consequence",
            "category","likelihood","impact","treatment","owner","due_date","status",
            "extra","date_created","date_modified","row_id"
        ],
    },
}

# === END ADD: multi-table section metadata =================================




# --- MULTI-TABLE SECTIONS (add once) ---------------------------------------
from collections import OrderedDict

SUBSECTIONS = {
    3: OrderedDict([
        ("31", {"title": "3.1 Appointment",                  "table": "pqp.section31"}),
        ("32", {"title": "3.2 Milestones",                   "table": "pqp.section32"}),
        ("33", {"title": "3.3 Deliverables / Approvals",     "table": "pqp.section33"}),
    ]),
    4: OrderedDict([
        ("41", {"title": "4.1 Planning & Design",            "table": "pqp.section41"}),
        ("42", {"title": "4.2 Project-specific Risks",       "table": "pqp.section42"}),
    ]),
    5: OrderedDict([
        ("51", {"title": "5.1 Documentation",                "table": "pqp.section51"}),
        ("52", {"title": "5.2 Tender",                       "table": "pqp.section52"}),
    ]),
    6: OrderedDict([
        ("61", {"title": "6.1 Contracts / Orders",           "table": "pqp.section61"}),
        ("62", {"title": "6.2 Payments / Certificates",      "table": "pqp.section62"}),
        ("63", {"title": "6.3 Instructions / Handover",      "table": "pqp.section63"}),
    ]),
    7: OrderedDict([
        ("71", {"title": "7.1 Additional Services",          "table": "pqp.section71"}),
        ("72", {"title": "7.2 Actions & Notes",              "table": "pqp.section72"}),
    ]),
    9: OrderedDict([
        ("91", {"title": "9.1 Scope Items",                  "table": "pqp.section91"}),
        ("92", {"title": "9.2 Scope Notes",                  "table": "pqp.section92"}),
    ]),
    10: OrderedDict([
        ("101", {"title": "10 Risk Register",                "table": "pqp.section101"}),
    ]),
}

# name-matching hints for auto-guessing when a table name differs
SECTION_KEYWORDS = {
    31: ["section31","appoint","appointment"],
    32: ["section32","milestone"],
    33: ["section33","deliver","approval"],
    41: ["section41","planning","design"],
    42: ["section42","risk"],
    51: ["section51","doc","documentation"],
    52: ["section52","tender"],
    61: ["section61","contract","order"],
    62: ["section62","payment","certificate"],
    63: ["section63","instruction","handover"],
    71: ["section71","additional","service"],
    72: ["section72","action","note"],
    91: ["section91","scope","item"],
    92: ["section92","scope","note"],
    101:["section101","risk","register"],
}

# exact UI column set for 101 (Risk Register)
COLS_101 = [
    "id","risk_code","title","description","cause","consequence","category",
    "likelihood","impact","treatment","owner","due_date","status",
    "heading_id","extra","date_created","date_modified","row_id"
]
# ---------------------------------------------------------------------------

# ---- PQP SUBSECTION → TABLE MAP (schema 'pqp') ----
SUB_TABLE_MAP = {
    31: "section31", 32: "section32", 33: "section33",
    41: "section41", 42: "section42",
    51: "section51", 52: "section52",
    61: "section61", 62: "section62", 63: "section63",
    71: "section71", 72: "section72",
    91: "section91", 92: "section92",       # Scope Register
    101: "risk_register"                     # Risk Register
}
# Columns we don't want to *display* by default (still kept in row dicts)
HIDE_DISPLAY_COLS = {"id", "project_code", "project code", "tenant_id", "tenant id"}



from sqlalchemy import text
from flask import abort

def q(conn, sql, **params):
    """Execute a text SQL and return rows as dictionaries."""
    return conn.execute(text(sql), params).mappings().all()

def fetch_section_rows(conn, project_code: str, section_number: int):
    """
    Read rows for a *single-table* section using SECTION_TABLE/SECTION_COLS.
    Composite sections (3, 9) are intentionally excluded and should use their dedicated loaders.
    """
    # Guard: composite/special sections are handled elsewhere
    if section_number in (3, 9):
        abort(400, f"Section {section_number} is composite; use its dedicated loader")

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
                    _upsert_section_rows(project_id, idx, cols, rows)
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



# ======================= JSON API (non-UI)  =======================
# This leaves your big UI blueprint intact. We add a tiny API-only blueprint.
from flask import current_app

pqp_api_bp = Blueprint("pqp_api", __name__, url_prefix="/api/pqp")

@pqp_api_bp.post("/section/<code>/<int:sec_no>/materialize")
def api_materialize_section(code, sec_no):
    """
    Copy rows (read-only hydrated from physical table) into PQPSection.rows_json
    so the panel becomes editable. No schema changes; pure app-level copy.
    """
    if sec_no in (3,):
        return jsonify({"ok": False, "error": "Section 3 is composite"}), 400

    table = SECTION_TABLE.get(sec_no)
    labels = SECTION_COLS.get(sec_no, [])
    if not table or not labels:
        return jsonify({"ok": False, "error": f"Unknown section {sec_no}"}), 404

    with db.engine.connect() as conn:
        raw = _fetch_table_rows(conn, table, code)

    if not raw:
        return jsonify({"ok": True, "copied": 0})

    rows = [_remap_db_row(sec_no, r) for r in raw]
    rows = [r for r in rows if any(v for k, v in r.items() if k != "id")]

    # Persist into PQPSection using your existing helper
    _upsert_section_rows(code, sec_no, labels, rows)

    return jsonify({"ok": True, "copied": len(rows)})




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

# ---------- BEGIN: generic CRUD for subsection tables ----------

from flask import request, redirect, url_for
from sqlalchemy import text

EXCLUDE_INPUTS = {"id", "row_id", "tenant_id", "project_code", "created_at", "updated_at", "date_created", "date_modified"}

def _split_qualified(qname: str):
    if "." in qname:
        s, t = qname.split(".", 1)
        return s, t
    return "public", qname

def _pk_for_table(conn, qualified: str) -> str:
    """Find PK column name; fallback to row_id or id."""
    s, t = _split_qualified(qualified)
    sql = """
    select a.attname
    from pg_index i
    join pg_class c on c.oid = i.indrelid
    join pg_namespace n on n.oid = c.relnamespace
    join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
    where i.indisprimary and n.nspname=:s and c.relname=:t
    """
    pk = db.session.execute(text(sql), {"s": s, "t": t}).scalar()
    return pk or ("row_id" if "row_id" in _introspect_columns_pretty(db.engine, t if s == "public" else qualified) else "id")

def _columns_for(qualified: str):
    s, t = _split_qualified(qualified)
    return [c for c in _introspect_columns_pretty(db.engine, qualified)]

def _filter_payload_for_table(qualified: str, code: str, form_or_json: dict):
    cols = set(_columns_for(qualified))
    data = {}
    # carry tenant/project if present on table
    if "tenant_id" in cols:
        data["tenant_id"] = code
    if "project_code" in cols:
        data["project_code"] = code
    for k, v in form_or_json.items():
        if k in cols and k not in EXCLUDE_INPUTS:
            data[k] = v
    return data

def _table_for_sub_required(sub_no: int) -> str:
    spec = SUBSECTIONS.get(sub_no) or {}
    qname = spec.get("table")
    if not qname:
        # last resort: your guesser
        qname = _guess_table_for_sub(sub_no)
    if not qname:
        abort(404, f"No table mapping for subsection {sub_no}")
    return qname

# UI-post endpoints (redirect back to the form)
@pqp_bp.post("/form/<code>/sub/<int:sub_no>/add")
def pqp_ui_add_row(code, sub_no):
    qname = _table_for_sub_required(sub_no)
    payload = request.form.to_dict()
    data = _filter_payload_for_table(qname, code, payload)
    if not data:
        flash("Nothing to save.", "warning")
        return redirect(url_for("pqp.pqp_form_by_code", code=code))
    cols_sql = ",".join(data.keys())
    vals_sql = ",".join([f":{k}" for k in data.keys()])
    sql = text(f"insert into {qname} ({cols_sql}) values ({vals_sql})")
    db.session.execute(sql, data)
    db.session.commit()
    flash("Row added.", "success")
    return redirect(url_for("pqp.pqp_form_by_code", code=code) + f"#sub-{sub_no}")

@pqp_bp.post("/form/<code>/sub/<int:sub_no>/edit/<rid>")
def pqp_ui_edit_row(code, sub_no, rid):
    qname = _table_for_sub_required(sub_no)
    payload = request.form.to_dict()
    data = _filter_payload_for_table(qname, code, payload)
    pk = _pk_for_table(db.engine, qname)
    if not data:
        flash("Nothing to update.", "warning")
        return redirect(url_for("pqp.pqp_form_by_code", code=code))
    sets = ",".join([f"{k}=:{k}" for k in data.keys()])
    data["_rid"] = rid
    sql = text(f"update {qname} set {sets} where {pk} = :_rid")
    db.session.execute(sql, data)
    db.session.commit()
    flash("Row updated.", "success")
    return redirect(url_for("pqp.pqp_form_by_code", code=code) + f"#sub-{sub_no}")

@pqp_bp.post("/form/<code>/sub/<int:sub_no>/delete/<rid>")
def pqp_ui_delete_row(code, sub_no, rid):
    qname = _table_for_sub_required(sub_no)
    pk = _pk_for_table(db.engine, qname)
    db.session.execute(text(f"delete from {qname} where {pk}=:rid"), {"rid": rid})
    db.session.commit()
    flash("Row deleted.", "success")
    return redirect(url_for("pqp.pqp_form_by_code", code=code) + f"#sub-{sub_no}")

# ---------- END: generic CRUD for subsection tables ----------



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

    - Sections 1,2,4,5,6,7,8,9 use PQPSection JSON; if empty we hydrate once from tables.
    - Composite sections (3.x, 4.x, 5.x, 6.x, 7.x, 9.x, 10/101) are rendered from their
      own physical tables; even with 0 rows we still provide column headers so the
      template always shows the panel and keeps your layout intact.
    """
    code = _norm_code(code)

    # ---------- Project header ----------
    PR = _projectrecords(db.engine)
    cols = [
        _col(PR, PR_COL_CODE).label("Code"),
        _col(PR, PR_COL_SHORTDESC).label("Short Description"),
        _col(PR, PR_COL_CLIENT).label("Client"),
        _col(PR, PR_COL_PM).label("Project Manager"),
    ]
    # add optional columns if they exist
    for cname, lbl in (("start_date", "start_date"), ("end_date", "end_date"), (PR_COL_STATUS, "Status")):
        try:
            cols.append(_col(PR, cname).label(lbl))
        except KeyError:
            pass

    row = db.session.execute(select(*cols).where(_col(PR, PR_COL_CODE) == code)).first()
    if not row:
        flash("Project Code not found.", "danger")
        return redirect(url_for("pqp.pqp_form_select_by_code"))
    project = {k: _to_str(v) for k, v in row._mapping.items()}
    if not project.get("Status"):
        # cheap inferred status
        try:
            end_dt = row._mapping.get("end_date")
            if isinstance(end_dt, datetime):
                project["Status"] = "Closed" if end_dt.date() < date.today() else "Active"
            elif isinstance(end_dt, date):
                project["Status"] = "Closed" if end_dt < date.today() else "Active"
            else:
                project["Status"] = "Active"
        except Exception:
            project["Status"] = "Active"

    # ---------- Ensure scaffolding ----------
    _ensure_sections_by_code(code)

    # ---------- Single-table panels (1,2,4,5,6,7,8,9) from PQPSection ----------
    section_columns = list(SECTION_DEFS)
    if len(section_columns) < 10:
        section_columns += [[] for _ in range(10 - len(section_columns))]
    section_count = len(section_columns)
    section_data = [[] for _ in range(section_count)]

    def _rows_of(sec):
        payload = None
        if getattr(sec, "rows_json", None):
            try:
                payload = json.loads(sec.rows_json)
            except Exception:
                payload = None
        if payload is None and getattr(sec, "content", None):
            try:
                payload = json.loads(sec.content)
            except Exception:
                payload = None
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
            return payload["rows"]
        return []

    for sec in (PQPSection.query
                .filter_by(project_code=code)
                .order_by(PQPSection.section_number)
                .all()):
        snum = (sec.section_number or 1)
        if snum in {3, 31, 32, 33, 41, 42, 51, 52, 61, 62, 63, 71, 72, 91, 92, 101}:
            continue  # sub-panels handled separately
        if 1 <= snum <= section_count:
            cols_for_panel = section_columns[snum - 1] or ["title", "description"]
            section_data[snum - 1].extend(_normalize_rows(cols_for_panel, _rows_of(sec)))

    # If a single-table section has *no* JSON rows, try hydrating once from physical tables
    try:
        hydration_meta = _hydrate_from_tables_if_empty(code, section_columns, section_data)
    except Exception:
        hydration_meta = {}

    # ---------- Composite/multi-table panels (3.x,4.x,5.x,6.x,7.x,9.x,10/101) ----------
    group_cols, group_data, group_meta = {}, {}, {}
    with db.engine.connect() as conn:
        for parent_no, parts in SUBSECTIONS.items():
            for sub_code, spec in parts.items():
                sub_no = int(sub_code)
                table = spec.get("table")

                # Always provide column headers so the panel renders even with 0 rows
                labels = _introspect_columns_pretty(conn, table) if table else []

                # Data: try the declared table; if nothing, try a name-guess
                raw = _fetch_table_rows(conn, table, code) if table else []
                guessed = None
                if not raw:
                    guessed = _guess_table_for_sub(conn, sub_no, spec.get("title", ""))
                    if guessed:
                        labels = _introspect_columns_pretty(conn, guessed) or labels
                        raw = _fetch_table_rows(conn, guessed, code)

                rows = [_remap_db_row_to_labels(labels, r) for r in raw] if raw else []

                # Store (even if rows == [] so Jinja shows the shell)
                group_cols[sub_no] = labels
                group_data[sub_no] = rows
                group_meta[sub_no] = {
                    "hydrated": bool(rows),
                    "table": guessed or table or "(none)",
                    "rowcount": len(rows),
                    "guessed": bool(guessed and rows),
                }
    # Build tab titles 1..9 from your canonical titles dict
    section_titles = [DEFAULT_SECTION_TITLES.get(i, f"Section {i}") for i in range(1, 10)]


    # ---------- Render ----------
    return render_template(
        "pqp_form.html",
        code=code,
        project=project,
        sections=section_titles,        # was SECTIONS (undefined)
        section_columns=section_columns,    # was section_cols
        section_data=section_data,          # was section_rows
        p_cols=group_cols,
        p_rows=group_data,
        p_meta=group_meta,                  # was p_meta (undefined)
        read_only=True,
        read_only=False,           # show Add/Edit/Delete (set True to lock)
        show_debug_tables=False,   # hides “pqp.sectionXX” name badges
    )




def _infer_cols(rows):
    if not rows:
        return []
    keys = []
    seen = set()
    for r in rows:
        if isinstance(r, dict):
            for k in r.keys():
                if k not in seen:
                    keys.append(k); seen.add(k)
    return keys




def _get_project_pk(conn, project_code: str):
    """Return numeric pqp.project.id for a given project_code, or None."""
    from sqlalchemy import text
    try:
        return conn.execute(
            text("select id from pqp.project where project_code = :c"),
            {"c": project_code}
        ).scalar()
    except Exception:
        return None

def _fetch_table_rows(conn, table_qualified: str, project_code: str):
    """
    Read rows for this project from a physical section table.
    Tries, in order:
      1) project_code = :code
      2) id (TEXT) = :code
      3) project_id (INT) = project.id for :code
    Returns a list[dict].
    """
    from sqlalchemy import text
    schema, table = table_qualified.split(".", 1)

    # Introspect columns + types once
    cols = conn.execute(text("""
        select column_name, data_type
        from information_schema.columns
        where table_schema = :s and table_name = :t
    """), {"s": schema, "t": table}).mappings().all()
    col_types = {r["column_name"]: (r["data_type"] or "").lower() for r in cols}
    col_names = set(col_types.keys())

    # 1) project_code TEXT
    if "project_code" in col_names:
        rows = conn.execute(
            text(f"select * from {table_qualified} where project_code = :code order by 1"),
            {"code": project_code}
        ).mappings().all()
        return [dict(r) for r in rows]

    # 2) id TEXT (be careful not to use it when it's a numeric PK)
    if "id" in col_names and "character" in col_types.get("id", "") or col_types.get("id") == "text":
        rows = conn.execute(
            text(f"select * from {table_qualified} where id = :code order by 1"),
            {"code": project_code}
        ).mappings().all()
        return [dict(r) for r in rows]

    # 3) project_id INT (FK to pqp.project.id)
    if "project_id" in col_names:
        pid = _get_project_pk(conn, project_code)
        if pid is not None:
            rows = conn.execute(
                text(f"select * from {table_qualified} where project_id = :pid order by 1"),
                {"pid": pid}
            ).mappings().all()
            return [dict(r) for r in rows]

    # If none matched, last resort: try id numerically == project PK
    if "id" in col_names and "integer" in col_types.get("id", ""):
        pid = _get_project_pk(conn, project_code)
        if pid is not None:
            rows = conn.execute(
                text(f"select * from {table_qualified} where id = :pid order by 1"),
                {"pid": pid}
            ).mappings().all()
            return [dict(r) for r in rows]

    return []


def _remap_db_row(section_no: int, raw: dict) -> dict:
    """
    Map a physical DB row (raw) to the UI labels for a section.
    1) Try the explicit DB_TO_UI_COLS map (exact snake_case keys).
    2) If most labels are still empty, fall back to a tolerant matcher:
       - case-insensitive
       - ignores spaces, punctuation, accents
       - uses synonyms per label (e.g., 'Cell' ~ mobile/phone)
       - 'id' is taken from project_code or id
    """
    labels = SECTION_COLS.get(section_no, [])
    out = {lbl: "" for lbl in labels}

    if not isinstance(raw, dict) or not labels:
        return out

    # --- 1) exact mapping path (kept as-is) ---
    mapping = DB_TO_UI_COLS.get(section_no, {})
    used_db_cols = set()
    for db_key, ui_label in mapping.items():
        if ui_label in out and db_key in raw:
            val = raw.get(db_key)
            out[ui_label] = _to_str(val)
            used_db_cols.add(db_key)

    # always set 'id' when present in the label spec
    if "id" in out and not out["id"]:
        out["id"] = _to_str(raw.get("project_code") or raw.get("id") or "")

    # if we already filled most labels, stop here
    filled = sum(1 for k, v in out.items() if k != "id" and v not in (None, ""))
    if filled >= max(1, (len(labels) - ("id" in labels)) // 2):
        return out

    # --- 2) tolerant matcher (fallback) ---
    import re, unicodedata

    def norm(s: str) -> str:
        s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode()
        return re.sub(r"[^a-z0-9]+", "", s.strip().lower())

    db_cols = [c for c in raw.keys() if c not in used_db_cols]
    norm_db = {c: norm(c) for c in db_cols}

    # synonyms by UI label (expandable)
    SYN = {
        # Section 1
        "Project Description": ["description", "projectdesc", "projdesc"],
        "Location":            ["location", "loc"],
        "Client Organisation": ["clientorganisation", "clientorganization", "clientorg", "client"],
        "Primary Contact Name":["primarycontactname", "contactname", "representativename", "name"],
        "VAT Number":          ["vat", "vatno", "vatnumber"],
        "Designation":         ["designation", "title", "position"],
        "Invoice Address":     ["invoiceaddress", "billingaddress"],
        # Section 2
        "Role":                ["role", "position"],
        "Req'd":               ["reqd", "required", "mandatory", "needed"],
        "Organisation":        ["organisation", "organization", "org", "company", "firm"],
        "Representative Name": ["representativename", "repname", "contactname", "name"],
        "Email":               ["email", "e_mail", "mail"],
        "Cell":                ["cell", "mobile", "phone", "tel", "telephone", "contactnumber"],
        "Subconsultant to HN?":      ["subconsultanttohn", "subconsultant", "tohn", "issubconsultant"],
        "Subconsultant Agreement?":  ["subconsultantagreement", "subagreement", "hasagreement"],
        "CPG Partner?":        ["cpgpartner", "iscpgpartner"],
        "CPG %":               ["cpgpercent", "cpgpct", "cpgpercentage", "cpg"],
        "Comments":            ["comments", "notes", "remarks", "comment"],
        # Section 4
        "Design Criteria/Requirements": ["designcriteria", "requirements", "designrequirements"],
        "Planning & Design Risks":      ["planningdesignrisks", "designrisks", "risks"],
        "Scope Register Location":      ["scoperegisterlocation", "scopelocation", "registerlocation"],
        "Design Notes":                 ["designnotes", "notes"],
        # Section 5
        "Client Tender Doc Requirements": ["clienttenderdocrequirements", "tenderrequirements"],
        "Form of Contract":              ["formofcontract", "contractform"],
        "Standard Specs":                ["standardspecs", "specs", "specifications"],
        "Client Template Date":          ["clienttemplatedate", "templatedate"],
        "Documentation Risks":           ["documentationrisks", "docsrisks", "risks"],
        "Tender Phase Notes":            ["tenderphasenotes", "notes"],
        # Section 6 (subset; many columns – matcher will still align)
        "Construction Description": ["constructiondescription", "description"],
        "Contractor Organisation":  ["contractororganisation", "contractororganization", "contractor", "org"],
        "Contract Number":          ["contractnumber", "contractno"],
        "Award Value (incl VAT)":   ["awardvalueinclvat", "awardvalue", "value", "amount"],
        "Award Date":               ["awarddate"],
        "Original Order No":        ["originalorderno", "origorderno"],
        "Original Date of Order":   ["originaldateoforder", "origorderdate"],
        "Inception Meeting Date":   ["inceptionmeetingdate", "inceptiondate"],
        "Final Payment Cert Date":  ["finalpaymentcertdate", "finalpaymentdate"],
        "Final Value (incl VAT)":   ["finalvalueinclvat", "finalvalue"],
        "Commencement of Works":    ["commencementofworks", "commencement"],
        "Date of EA's Instruction": ["dateofeasinstruction", "eainstructiondate"],
        "Where Instruction Recorded":["whereinstructionrecorded", "instructionlocation", "recordlocation"],
        "Completion Date":          ["completiondate"],
        "Final Approval Date":      ["finalapprovaldate"],
        "Client Takeover Date":     ["clienttakeoverdate"],
        "Commencement Instruction Date": ["commencementinstructiondate"],
        "Commencement Instruction Location": ["commencementinstructionlocation"],
        "Construction Phase Risks": ["constructionphaserisks", "risks"],
        "Construction Phase Notes": ["constructionphasenotes", "notes"],
        # Section 7
        "Additional Services Done": ["additionalservicesdone", "additionalservices", "servicesdone"],
        "Project-specific Risks":   ["projectspecificrisks", "risks"],
        "Mitigating Measures":      ["mitigatingmeasures", "mitigation"],
        "Record of Action Taken":   ["recordofactiontaken", "actiontaken", "actions"],
        # Section 8
        "Date CSQ Submitted":       ["datecsqsubmitted", "csqsubmitteddate"],
        "Date CSQ Received":        ["datecsqreceived", "csqreceiveddate"],
        "CSQ Rating":               ["csqrating", "rating"],
        "Comments on Feedback":     ["commentsonfeedback", "feedbackcomments"],
        "Actual Close-Out Date":    ["actualcloseoutdate", "closeoutdate"],
        "General Remarks/Lessons Learned": ["generalremarkslessonslearned", "generalremarks", "lessonslearned"],
        # Section 9
        "Scope Item":               ["scopeitem", "item"],
        "Category":                 ["category"],
        "Owner":                    ["owner", "responsible"],
        "Status":                   ["status", "state"],
        "Due Date":                 ["duedate", "due"],
        "Notes":                    ["notes", "comments", "remarks"],
    }

    def find_best(ui_label: str):
        targets = SYN.get(ui_label, [])
        n_targets = [norm(t) for t in targets] + [norm(ui_label)]
        # exact/contains preference
        for db_name, ndb in norm_db.items():
            for nt in n_targets:
                if nt and (ndb == nt or nt in ndb or ndb in nt):
                    return db_name
        # fallback: token overlap score
        best, best_score = None, 0
        toks = set(re.findall(r"[a-z0-9]+", n_targets[-1]))
        for db_name, ndb in norm_db.items():
            score = sum(1 for t in toks if t and t in ndb)
            if score > best_score:
                best, best_score = db_name, score
        return best

    for lbl in labels:
        if lbl == "id":
            continue
        if out.get(lbl):
            continue
        cand = find_best(lbl)
        if cand and raw.get(cand) is not None:
            out[lbl] = _to_str(raw.get(cand))

    return out

def _hydrate_from_tables_if_empty(project_code: str, section_columns, section_data):
    """
    If a section has no JSON rows, pull from a configured table; if that gives no rows,
    guess a better table by name and use it. Returns meta per section for the UI.
    """
    meta = {}
    with db.engine.connect() as conn:
        for sec_no in range(1, min(10, len(section_columns)) + 1):
            if sec_no in (3,):
                continue

            configured = SECTION_TABLE.get(sec_no)
            table_to_use = configured
            rows = []

            if configured:
                rows = _fetch_table_rows(conn, configured, project_code)

            # auto-guess if nothing came back
            guessed = None
            if not rows:
                guessed = _guess_table_for_section(conn, sec_no, project_code)
                if guessed and guessed != configured:
                    try:
                        rows = _fetch_table_rows(conn, guessed, project_code)
                        if rows:
                            SECTION_TABLE[sec_no] = guessed  # cache for this process
                            table_to_use = guessed
                    except Exception:
                        pass

            labels = SECTION_COLS.get(sec_no, [])
            if rows and labels:
                hydrated = [_remap_db_row(sec_no, r) for r in rows]
                hydrated = [r for r in hydrated if any(v for k, v in r.items() if k != "id")]
                if hydrated and not section_data[sec_no - 1]:
                    section_data[sec_no - 1].extend(hydrated)
                meta[sec_no] = {
                    "hydrated": True,
                    "table": table_to_use or configured or "(auto)",
                    "rowcount": len(hydrated),
                    "guessed": (table_to_use == guessed and guessed is not None)
                }
            else:
                meta[sec_no] = {
                    "hydrated": False,
                    "table": table_to_use or configured or "(none)",
                    "rowcount": 0,
                    "guessed": False
                }
    return meta



# Heuristics to spot tables by name if SECTION_TABLE is wrong/missing
SECTION_KEYWORDS = {
    1: ["overview", "project_overview", "section1"],
    2: ["team", "project_team", "section2"],
    4: ["planning", "design", "planning_design", "section4"],
    5: ["documentation", "docs", "tender", "section5"],
    6: ["works", "handover", "construction", "section6"],
    7: ["additional", "services", "section7"],
    8: ["close", "closeout", "feedback", "section8"],
    9: ["scope", "register", "scope_register", "section9"],
}


# === BEGIN ADD: multi-table section helpers ================================

def _ensure_id_in_row(d: dict) -> dict:
    """Guarantee an 'id' key exists (string) so editing and the UI are stable."""
    if "id" not in d:
        d["id"] = _to_str(d.get("row_id") or d.get("project_code") or d.get("heading_id") or "")
    return d

def _derive_columns_from_table(conn, table_qualified: str) -> list[str]:
    """Ordered column names using information_schema; 'id' first if present."""
    cols = _columns_for_table(db.engine, table_qualified)
    # keep 'id' first when available
    if "id" in cols:
        cols = ["id"] + [c for c in cols if c != "id"]
    return cols

def _normalize_rows_for_columns(cols: list[str], raw_rows: list[dict]) -> list[dict]:
    """Map each raw DB row into a dict with exactly the keys in cols, stringified."""
    out = []
    for r in raw_rows or []:
        d = {c: _to_str(r.get(c)) for c in cols}
        d = _ensure_id_in_row(d)
        out.append(d)
    return out

def _load_multipart_section(code: str, section_no: int):
    """
    Generic loader for sections that consist of multiple physical tables.
    Returns (cols_by_part: dict, rows_by_part: dict, meta_by_part: dict).
    If SECTION_PART_COLS is missing for a part, we derive columns from the table.
    """
    parts = SECTION_PART_TABLES.get(section_no, {})
    cols_by_part, rows_by_part, meta_by_part = {}, {}, {}

    if not parts:
        return cols_by_part, rows_by_part, meta_by_part

    with db.engine.connect() as conn:
        for part_key, (title, table) in parts.items():
            try:
                raw = _fetch_table_rows(conn, table, code)
            except Exception as e:
                raw, err = [], str(e)
            else:
                err = None

            # Use declared columns when available; otherwise derive
            cols = (SECTION_PART_COLS.get(section_no, {}).get(part_key)
                    or _derive_columns_from_table(conn, table))

            rows = _normalize_rows_for_columns(cols, raw)
            cols_by_part[part_key] = cols
            rows_by_part[part_key] = rows
            meta_by_part[part_key] = {
                "title": title,
                "table": table,
                "rowcount": len(rows),
                "error": err,
            }

    return cols_by_part, rows_by_part, meta_by_part

# === END ADD: multi-table section helpers ==================================

# --- Helpers for multi-table sections (add once) ---------------------------
# --- Build "multipart" (multi-table payload for sections 3,4,5,6,7,9,10) ----
def _fetch_rows_for_project(conn, table_qualified: str, project_code: str):
    """Return (columns, rows) for the given table filtered to this project."""
    from sqlalchemy import text
    try:
        schema, table = table_qualified.split('.', 1)
    except ValueError:
        return [], []
    meta = conn.execute(text("""
        select column_name, data_type
        from information_schema.columns
        where table_schema=:s and table_name=:t
        order by ordinal_position
    """), {"s": schema, "t": table}).mappings().all()
    colnames = [m["column_name"] for m in meta]
    coltypes = {m["column_name"]: (m["data_type"] or "").lower() for m in meta}

    # choose the best filter column
    where, params = None, {}
    if "project_code" in colnames:
        where, params = "project_code=:pc", {"pc": project_code}
    elif "id" in colnames and (("character" in coltypes["id"]) or coltypes["id"] == "text"):
        where, params = "id=:pc", {"pc": project_code}
    elif "project_id" in colnames:
        pid = _get_project_pk(conn, project_code)  # you already have this helper
        if pid is not None:
            where, params = "project_id=:pid", {"pid": pid}
    elif "id" in colnames and "integer" in coltypes["id"]:
        pid = _get_project_pk(conn, project_code)
        if pid is not None:
            where, params = "id=:pid", {"pid": pid}

    q = f"select * from {table_qualified}"
    if where: q += f" where {where}"
    q += " order by 1 desc"
    rows = conn.execute(text(q), params).mappings().all()
    return colnames, rows

def _build_multipart(conn, project_code: str) -> dict:
    """
    Shape:
      { <section_no>: {
          'parts': SUBSECTIONS[section_no],   # mapping of sub keys -> {title, table?}
          'cols':  {sub_key: [labels...]},
          'rows':  {sub_key: [row-dicts]},
          'rowcount': int
        }, ... }
    """
    result = {}
    for sec_no, parts in SUBSECTIONS.items():
        sec_payload = {"parts": parts, "cols": {}, "rows": {}, "rowcount": 0}
        for sub_key, spec in parts.items():
            tbl = spec.get("table") or _guess_table_for_sub(conn, int(sub_key), spec.get("title", ""))
            if not tbl:
                continue
            labels = _introspect_columns_pretty(conn, tbl) or []
            _cols, raw_rows = _fetch_rows_for_project(conn, tbl, project_code)
            mapped = [_remap_db_row_to_labels(labels or _cols, dict(r)) for r in raw_rows]
            sec_payload["cols"][sub_key] = labels or _cols
            sec_payload["rows"][sub_key] = mapped
            sec_payload["rowcount"] += len(mapped)
        result[sec_no] = sec_payload
    return result
# ---------------------------------------------------------------------------
def _fetch_all_dicts(conn, sql, params=()):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

def _introspect_columns(conn, schema, table):
    sql = """
    select column_name
    from information_schema.columns
    where table_schema = %s and table_name = %s
    order by ordinal_position
    """
    rows = _fetch_all_dicts(conn, sql, (schema, table))
    return [r["column_name"] for r in rows]

def hydrate_known_sections(conn, project_code):
    """
    Returns:
      (group_cols, group_data)
      group_cols: {sub_no: [col1, col2, ...]}
      group_data: {sub_no: [row_dict, ...]}
    The row dicts include *all* columns; group_cols hides noisy keys for display.
    """
    g_cols, g_rows = {}, {}

    code = (project_code or "").strip()

    for sub_no, tbl in SUB_TABLE_MAP.items():
        # Only proceed if table exists
        cols = _introspect_columns(conn, "pqp", tbl)
        if not cols:
            continue

        # Generic filter: many of your tables use either tenant_id or project_code
        sql = f"""
            select *
            from pqp.{tbl}
            where (tenant_id = %s or project_code = %s)
            order by 1
        """
        rows = _fetch_all_dicts(conn, sql, (code, code))

        # Choose display columns: keep order from introspection
        display_cols = [c for c in cols if c.lower() not in HIDE_DISPLAY_COLS]
        g_cols[sub_no] = display_cols
        g_rows[sub_no] = rows

    return g_cols, g_rows



def _introspect_columns_pretty(conn, table_qualified: str):
    """Return list of column labels taken from information_schema, with 'id' first if present."""
    from sqlalchemy import text
    try:
        schema, table = table_qualified.split('.',1)
    except ValueError:
        return []
    cols = [r[0] for r in conn.execute(text("""
        select column_name
        from information_schema.columns
        where table_schema=:s and table_name=:t
        order by ordinal_position
    """), {"s": schema, "t": table}).fetchall()]

    def pretty(c):
        if c == "id":
            return "id"
        return " ".join(w.capitalize() for w in c.replace("_"," ").split())

    if "id" in cols:
        return ["id"] + [pretty(c) for c in cols if c!="id"]
    return [pretty(c) for c in cols]

def _list_pqp_tables(conn):
    from sqlalchemy import text
    return [f"{r['table_schema']}.{r['table_name']}" for r in conn.execute(text("""
        select table_schema, table_name
        from information_schema.tables
        where table_schema='pqp' and table_type='BASE TABLE'
    """)).mappings()]

def _count_rows_for_table(conn, table_qualified: str, project_code: str):
    """Return count of rows in table that belong to this project."""
    from sqlalchemy import text
    try:
        schema, table = table_qualified.split('.',1)
    except ValueError:
        return 0
    cols = {r['column_name']: (r['data_type'] or '').lower() for r in conn.execute(text("""
        select column_name, data_type
        from information_schema.columns
        where table_schema=:s and table_name=:t
    """), {"s": schema, "t": table}).mappings()}
    names = set(cols)

    # project_code column
    if "project_code" in names:
        try:
            return int(conn.execute(text(f"select count(*) from {table_qualified} where project_code=:c"),
                                    {"c": project_code}).scalar() or 0)
        except Exception:
            pass
    # id as text
    if "id" in names and ("character" in cols.get("id","") or cols.get("id")=="text"):
        try:
            return int(conn.execute(text(f"select count(*) from {table_qualified} where id=:c"),
                                    {"c": project_code}).scalar() or 0)
        except Exception:
            pass
    # project_id integer FK
    if "project_id" in names:
        try:
            pid = _get_project_pk(conn, project_code)  # present elsewhere in your file
        except Exception:
            pid = None
        if pid is not None:
            try:
                return int(conn.execute(text(f"select count(*) from {table_qualified} where project_id=:p"),
                                        {"p": pid}).scalar() or 0)
            except Exception:
                pass
    # id as int (FK)
    if "id" in names and "integer" in cols.get("id",""):
        try:
            pid = _get_project_pk(conn, project_code)
        except Exception:
            pid = None
        if pid is not None:
            try:
                return int(conn.execute(text(f"select count(*) from {table_qualified} where id=:p"),
                                        {"p": pid}).scalar() or 0)
            except Exception:
                pass
    return 0

def _guess_table_for_sub(conn, sub_no: int, title: str):
    """Pick the most name-relevant table for a sub-section (e.g. 31, 41, 101)."""
    keywords = set(SECTION_KEYWORDS.get(sub_no, []))
    keywords |= {str(sub_no)}
    keywords |= {w.lower() for w in (title or "").split()}
    best_tbl, best_score = None, -1
    for tbl in _list_pqp_tables(conn):
        name = tbl.split('.',1)[1].lower()
        score = sum(1 for k in keywords if k and k in name)
        if score > best_score:
            best_tbl, best_score = tbl, score
    return best_tbl

def _remap_db_row_to_labels(labels: list, raw: dict) -> dict:
    """Map a raw DB row into a dict keyed by the provided labels (tolerant matching)."""
    out = {lbl: "" for lbl in labels}
    # id
    if "id" in out:
        out["id"] = _to_str(raw.get("project_code") or raw.get("id") or raw.get("project_id") or "")
    # tolerant name match
    import re, unicodedata
    def norm(s):
        s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii","ignore").decode()
        return re.sub(r"[^a-z0-9]+","",s.strip().lower())
    norm_db = {k: norm(k) for k in raw.keys()}
    for lbl in labels:
        if lbl == "id":
            continue
        nl = norm(lbl)
        match = None
        for k, nk in norm_db.items():
            if nk == nl or nl in nk or nk in nl:
                match = k
                break
        if match and raw.get(match) is not None:
            out[lbl] = _to_str(raw.get(match))
    return out
# ---------------------------------------------------------------------------





# --- Debug: counts of sections and backing tables ---

@pqp_bp.route("/debug/guess/<code>")
def debug_guess(code):
    from sqlalchemy import text
    out = {"code": code, "project_pk": None, "sections": []}
    with db.engine.connect() as conn:
        out["project_pk"] = _get_project_pk(conn, code)
        for sec_no in range(1, 10):
            configured = SECTION_TABLE.get(sec_no)
            guessed = _guess_table_for_section(conn, sec_no, code)
            data = {"section": sec_no, "configured": configured, "guessed": guessed}
            if configured:
                data["configured_count"] = _count_rows_for_table(conn, configured, code)
            if guessed:
                data["guessed_count"] = _count_rows_for_table(conn, guessed, code)
            out["sections"].append(data)
    return jsonify(out)



@pqp_bp.route("/debug/hydrate/<code>")
def debug_hydrate(code):
    from sqlalchemy import text
    out = {"code": code, "tables": {}}
    with db.engine.connect() as conn:
        # figure out project PK
        pid = _get_project_pk(conn, code)
        out["project_pk"] = pid

        for sec_no, tbl in SECTION_TABLE.items():
            if not tbl:
                continue
            schema, table = tbl.split(".", 1)
            cols = conn.execute(text("""
                select column_name, data_type
                from information_schema.columns
                where table_schema = :s and table_name = :t
            """), {"s": schema, "t": table}).mappings().all()
            col_types = {r["column_name"]: (r["data_type"] or "").lower() for r in cols}
            col_names = set(col_types.keys())

            used = None
            count = None

            try:
                if "project_code" in col_names:
                    used = "project_code"
                    count = conn.execute(text(f"select count(*) from {tbl} where project_code=:c"), {"c": code}).scalar()
                elif "id" in col_names and ("character" in col_types.get("id", "") or col_types.get("id") == "text"):
                    used = "id(text)"
                    count = conn.execute(text(f"select count(*) from {tbl} where id=:c"), {"c": code}).scalar()
                elif "project_id" in col_names and pid is not None:
                    used = f"project_id={pid}"
                    count = conn.execute(text(f"select count(*) from {tbl} where project_id=:p"), {"p": pid}).scalar()
                elif "id" in col_names and "integer" in col_types.get("id", "") and pid is not None:
                    used = f"id={pid}"
                    count = conn.execute(text(f"select count(*) from {tbl} where id=:p"), {"p": pid}).scalar()
                else:
                    used = "no match"
                    count = None
            except Exception as e:
                used = f"error: {e}"
                count = None

            out["tables"][sec_no] = {"table": tbl, "filter": used, "count": int(count) if count is not None else None}
    return jsonify(out)

# Debug: inspect hydrated subsections for a project code
# ---------- DEBUG ENDPOINTS ----------
# At the very top of this file, make sure you have:
#   from flask import jsonify
# and you're already using:
#   pqp_bp = Blueprint("pqp", __name__, url_prefix="/pqp")

@pqp_bp.route("/debug/hydrate/<code>")
def pqp_debug_hydrate(code):
    """
    Rich debug: shows each hydrated sub-table with col names and row counts.
    URL: /pqp/debug/hydrate/<code>
    """
    with get_db_connection() as conn:
        group_cols, group_data = hydrate_known_sections(conn, code)
        out = []
        for sub_no in sorted(group_cols):
            out.append({
                "sub": sub_no,
                "table": SUB_TABLE_MAP.get(sub_no),
                "cols": group_cols[sub_no],
                "row_count": len(group_data.get(sub_no, [])),
            })
        return jsonify({"code": code, "subs": out})


@pqp_bp.route("/debug/sections/<code>")
def pqp_debug_sections(code):
    """
    Backwards-compatible debug view (keeps your older URL working) but
    powered by the same hydrator so it reflects real data.
    URL: /pqp/debug/sections/<code>
    """
    with get_db_connection() as conn:
        group_cols, group_data = hydrate_known_sections(conn, code)
        sections = []
        for sub_no in sorted(group_cols):
            sections.append({
                "section": sub_no,                      # keeping key name "section"
                "row_count": len(group_data.get(sub_no, [])),
                "sample": group_data.get(sub_no, [])[:1]  # sample first row
            })
        return jsonify({"code": code, "sections": sections})
# -------------------------------------



def _list_pqp_tables(conn):
    from sqlalchemy import text
    rs = conn.execute(text("""
        select table_schema, table_name
        from information_schema.tables
        where table_schema='pqp' and table_type='BASE TABLE'
    """)).mappings().all()
    return [f"{r['table_schema']}.{r['table_name']}" for r in rs]

def _count_rows_for_table(conn, table_qualified: str, project_code: str):
    """
    Return number of rows for this project in a given table, trying project_code, id(text), project_id, id(int).
    """
    from sqlalchemy import text
    try:
        schema, table = table_qualified.split(".", 1)
    except Exception:
        return 0
    # introspect columns
    cols = conn.execute(text("""
        select column_name, data_type
        from information_schema.columns
        where table_schema=:s and table_name=:t
    """), {"s": schema, "t": table}).mappings().all()
    if not cols:
        return 0
    types = {r["column_name"]: (r["data_type"] or "").lower() for r in cols}
    names = set(types.keys())

    # project_code
    if "project_code" in names:
        try:
            return int(conn.execute(text(f"select count(*) from {table_qualified} where project_code=:c"), {"c": project_code}).scalar() or 0)
        except Exception:
            pass
    # id as text
    if "id" in names and ("character" in types.get("id","") or types.get("id") == "text"):
        try:
            return int(conn.execute(text(f"select count(*) from {table_qualified} where id=:c"), {"c": project_code}).scalar() or 0)
        except Exception:
            pass
    # project_id
    if "project_id" in names:
        pid = _get_project_pk(conn, project_code)
        if pid is not None:
            try:
                return int(conn.execute(text(f"select count(*) from {table_qualified} where project_id=:p"), {"p": pid}).scalar() or 0)
            except Exception:
                pass
    # id as int (project FK)
    if "id" in names and "integer" in types.get("id",""):
        pid = _get_project_pk(conn, project_code)
        if pid is not None:
            try:
                return int(conn.execute(text(f"select count(*) from {table_qualified} where id=:p"), {"p": pid}).scalar() or 0)
            except Exception:
                pass
    return 0

def _guess_table_for_section(conn, sec_no: int, project_code: str):
    """
    If SECTION_TABLE[sec_no] is wrong or empty for this project, guess a better table by:
      - scanning all pqp.* tables
      - preferring names containing section keywords
      - requiring row count > 0 for this project
    Returns table name or None.
    """
    cand_kw = [k.lower() for k in SECTION_KEYWORDS.get(sec_no, [])]
    if not cand_kw:
        return None

    best_tbl, best_score, best_count = None, -1, 0
    for tbl in _list_pqp_tables(conn):
        tname = tbl.split(".",1)[1].lower()
        # must look relevant by name
        score = sum(1 for k in cand_kw if k in tname)
        if score <= 0:
            continue
        cnt = _count_rows_for_table(conn, tbl, project_code)
        if cnt > 0 and (score > best_score or (score == best_score and cnt > best_count)):
            best_tbl, best_score, best_count = tbl, score, cnt

    return best_tbl



