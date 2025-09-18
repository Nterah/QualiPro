# app/pqp/sections.py

from __future__ import annotations

# Human-readable titles for each section number (1..9)
DEFAULT_SECTION_TITLES = {
    1: "Project Overview",
    2: "Project Team",
    3: "Appointment & Milestones",
    4: "Planning & Design",
    5: "Documentation & Tender",
    6: "Works & Handover",
    7: "Additional Services",
    8: "Close-Out & Feedback",
    9: "Scope Register",
}

# Column definitions per section. Keep these identical to what your app expects.
SECTION_DEFS = [
    # 1 — Project Overview
    ["id","Project Description","Location","Client Organisation","Primary Contact Name","VAT Number","Designation","Invoice Address"],

    # 2 — Project Team
    ["id","Role","Req'd","Organisation","Representative Name","Email","Cell","Subconsultant to HN?","Subconsultant Agreement?","CPG Partner?","CPG %","Comments"],

    # 3 — Appointment & Milestones
    ["id","In Place","Date","Filing Location","Notes","HN Roles","ECSA Project Stage","Date Completed","Description of Deliverable","Deliverable?","Deliverable Accepted?","Employer Approved?","Appointment Date","Expected Duration","Contract/Ref No","Comments"],

    # 4 — Planning & Design
    ["id","Design Criteria/Requirements","Planning & Design Risks","Project-specific Risks","Mitigating Measures","Record of Action Taken","Approval Type","Date Approved","Status/Reference No.","Deliverable?","Deliverable Accepted?","Approved?","Scope Register Location","Design Notes"],

    # 5 — Documentation & Tender
    ["id","Client Tender Doc Requirements","Form of Contract","Standard Specs","Client Template Date","Documentation Risks","Project-specific Risks","Mitigating Measures","Record of Action Taken","Tender Phase Notes"],

    # 6 — Works & Handover
    ["id","Construction Description","Contractor Organisation","Contract Number","Award Value (incl. VAT)","Award Date","Original Order No.","Original Date Order","Inception Meeting Date","Final Payment Cert Date","Final Value (incl VAT)","Milestones","Commencement of Works","Date of EA's Instruction","Where Instruction is recorded","Employer's Agent","Employer's Agent Representative","Construction Manager (Site Agent)","Record of Appointment Links","Responsibilities Links","Construction Phase Risks","Project-specific Risks","Mitigating Measures","Record of Action Taken","Construction Phase Notes"],

    # 7 — Additional Services
    ["id","Additional Services Done","Project-specific Risks","Mitigating Measures","Record of Action Taken","Notes"],

    # 8 — Close-Out & Feedback
    ["id","Date CSQ Submitted","Date CSQ Received","CSQ Rating","Location","Comments on Feedback","Actual Close-Out Date","General Remarks/Lessons Learned"],

    # 9 — Scope Register
    ["id","Scope Item","Category","Owner","Status","Due Date","Notes"],
]

def get_section_columns(idx: int) -> list[str]:
    """Return the expected columns for a zero-based section index."""
    return SECTION_DEFS[idx]
