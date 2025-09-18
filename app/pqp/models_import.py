# app/pqp/models_import.py
from datetime import datetime
from sqlalchemy import Column, Integer, String, JSON, DateTime

# <-- use the same db you already use everywhere else -->
from app import db


class ImportJob(db.Model):
    __tablename__ = "import_jobs"

    id = Column(Integer, primary_key=True)
    filename = Column(String(260))
    project_code = Column(String(50))
    status = Column(String(30), default="preview")   # preview|committed|failed
    issues = Column(JSON)                            # list[str]
    payload = Column(JSON)                           # normalized data by section
    created_at = Column(DateTime, default=datetime.utcnow)
    committed_at = Column(DateTime)
