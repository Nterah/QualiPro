# app/models.py
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

# from . import db
from .extensions import db


class Project(db.Model):
    __tablename__ = "project"   # lives in schema "pqp" via search_path
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    org_id = db.Column(UUID(as_uuid=True), nullable=False)
    created_at = db.Column(db.DateTime(timezone=True), server_default=func.now(), nullable=False)

    items = db.relationship(
        "ChecklistItem",
        backref="project",
        cascade="all, delete-orphan",
        lazy=True,
    )

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "org_id": str(self.org_id),
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

class ChecklistItem(db.Model):
    __tablename__ = "checklist_item"
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.Integer, db.ForeignKey("project.id", ondelete="CASCADE"), nullable=False)
    section = db.Column(db.Integer, nullable=False)
    label = db.Column(db.String(255), nullable=False)
    is_done = db.Column(db.Boolean, nullable=False, server_default=db.text("false"))
    created_at = db.Column(db.DateTime(timezone=True), server_default=func.now(), nullable=False)

    def to_dict(self):
        return {
            "id": self.id,
            "project_id": self.project_id,
            "section": self.section,
            "label": self.label,
            "is_done": self.is_done,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
