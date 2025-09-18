from app import db
from datetime import date
from werkzeug.utils import secure_filename



# ==========================
# 🔵 PROJECT MASTER TABLE
# ==========================
class Project(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_code = db.Column(db.String(50), nullable=False)
    name = db.Column(db.String(120), nullable=False)
    client = db.Column(db.String(120))
    manager = db.Column(db.String(120))
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)

    # One-to-one relationship
    pqp_detail = db.relationship("PQPDetail", back_populates="project", uselist=False)

    # Other relationships
    scope = db.relationship("Scope", backref="project", uselist=False)
    risks = db.relationship("RiskLog", backref="project")
    issues = db.relationship("CorrectiveAction", backref="project")
    kpis = db.relationship("KPI", backref="project")
    files = db.relationship("ProjectFile", backref="project")


# ==========================
# 🔹 SCOPE TABLE
# ==========================
class Scope(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    background = db.Column(db.Text)
    outputs = db.Column(db.Text)
    deliverables = db.Column(db.Text)
    exclusions = db.Column(db.Text)


# ==========================
# 🔹 PQP (Simple Table)
# ==========================
class PQP(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    quality_controls = db.Column(db.Text)
    responsibilities = db.Column(db.Text)
    documentation = db.Column(db.Text)
    last_reviewed = db.Column(db.Date)


# ==========================
# ✅ PQP DETAIL (MAIN FORM)
# ==========================
class PQPDetail(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey('project.id'), unique=True)

    # Section 1: Project Overview
    description = db.Column(db.Text)
    location = db.Column(db.String(255))
    client_organisation = db.Column(db.String(255))
    vat_number = db.Column(db.String(50))
    primary_contact_name = db.Column(db.String(120))
    contact_designation = db.Column(db.String(120))
    invoice_address = db.Column(db.Text)
    overview_summary = db.Column(db.Text)
    overview_uploaded_file = db.Column(db.String(255))

    # Section 2: Project Team
    team_description = db.Column(db.Text)
    team_checklist_complete = db.Column(db.Boolean, default=False)

    # Section 3: Appointment & Milestones
    appointment_milestones = db.Column(db.Text)

    # Section 4: Planning & Design
    planning_notes = db.Column(db.Text)

    # Section 5: Documentation & Procurement
    documentation_notes = db.Column(db.Text)

    # Section 6: Works & Handover
    contract_description = db.Column(db.Text)
    construction_notes = db.Column(db.Text)

    # Section 7: Additional Services
    additional_services = db.Column(db.Text)

    # Section 8: Close-Out & Feedback
    closeout_notes = db.Column(db.Text)
    csq_rating = db.Column(db.String(10))

    # Section 9: Scope Register
    scope_notes = db.Column(db.Text)

    # Relationship
    project = db.relationship("Project", back_populates="pqp_detail")


# ==========================
# ✅ TEAM MEMBER – Shared Pool
# ==========================
class TeamMember(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120))
    email = db.Column(db.String(120))
    cell = db.Column(db.String(50))
    designation = db.Column(db.String(120))

    roles = db.relationship("ProjectTeamAssignment", backref="person", cascade="all, delete")


# ==========================
# ✅ ASSIGN TEAM MEMBER TO PROJECT
# ==========================
class ProjectTeamAssignment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey("project.id"))
    team_member_id = db.Column(db.Integer, db.ForeignKey("team_member.id"))
    role = db.Column(db.String(120))
    organisation = db.Column(db.String(120))
    is_required = db.Column(db.Boolean)
    is_subconsultant = db.Column(db.Boolean)
    has_agreement = db.Column(db.Boolean)
    is_cpg = db.Column(db.Boolean)
    cpg_percent = db.Column(db.String(10))


# ==========================
# 📁 PQP UPLOADED FILES (linked by section)
# ==========================
class PQPFileUpload(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey("project.id"))
    section = db.Column(db.String(50))  # e.g. "Planning", "Tender"
    file_label = db.Column(db.String(100))
    filepath = db.Column(db.String(255))
    uploaded_on = db.Column(db.Date, default=date.today)


# ==========================
# ✅ SECTION CHECKLIST
# ==========================
class PQPStageChecklist(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey("project.id"))
    section = db.Column(db.String(50))
    item = db.Column(db.String(255))
    completed = db.Column(db.Boolean, default=False)


# ==========================
# 📉 RISK LOG
# ==========================
class RiskLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    description = db.Column(db.Text)
    likelihood = db.Column(db.String(20))  # Low, Medium, High
    impact = db.Column(db.String(20))
    mitigation = db.Column(db.Text)
    status = db.Column(db.String(20))  # Open, Resolved


# ==========================
# ❗ ISSUES / CORRECTIVE ACTIONS
# ==========================
class CorrectiveAction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    issue = db.Column(db.Text)
    root_cause = db.Column(db.Text)
    action_taken = db.Column(db.Text)
    status = db.Column(db.String(20))
    closed_date = db.Column(db.Date)


# ==========================
# 📊 KPIs
# ==========================
class KPI(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    metric_name = db.Column(db.String(100))
    target_value = db.Column(db.String(50))
    actual_value = db.Column(db.String(50))
    measured_on = db.Column(db.Date)


# ==========================
# 📂 Project File Repository
# ==========================
class ProjectFile(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey('project.id'))
    filename = db.Column(db.String(255))
    filepath = db.Column(db.String(255))
    uploaded_on = db.Column(db.Date, default=date.today)


# ==========================
# Default Upload Path
# ==========================
UPLOAD_FOLDER = 'app/pqp/static/pqp/uploads'


# app/pqp/pqp_models.py  — append at end

from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import JSONB
from app.extensions import db  # your existing db instance

class PQPSection(db.Model):
    __tablename__  = "pqp_sections"
    __table_args__ = {"schema": "pqp"}

    id             = db.Column(db.BigInteger, primary_key=True)
    project_code   = db.Column(db.Text, nullable=False, index=True)
    section_number = db.Column(db.Integer, nullable=False)
    title          = db.Column(db.Text)
    content        = db.Column(JSONB)   # free-form JSON
    rows_json      = db.Column(JSONB)   # grid rows
    completed      = db.Column(db.Boolean, nullable=False, server_default=text("false"))
    last_edited_on = db.Column(db.DateTime(timezone=True), server_default=func.now())
    created_at     = db.Column(db.DateTime(timezone=True), server_default=func.now())
