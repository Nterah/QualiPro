# seed.py

from app import create_app, db
from app.pqp.pqp_models import (
    Project, Scope, PQP, PQPDetail, RiskLog, CorrectiveAction, KPI, TeamMember,
    ProjectTeamAssignment, PQPFileUpload, PQPStageChecklist
)
from datetime import date

app = create_app()

with app.app_context():
    db.drop_all()
    db.create_all()

    # Create reusable team members
    john = TeamMember(name="John Engineer", email="john@ispan.co.za", cell="0711111111", designation="Civil Engineer")
    lisa = TeamMember(name="Lisa Planner", email="lisa@ispan.co.za", cell="0722222222", designation="Urban Planner")

    db.session.add_all([john, lisa])
    db.session.commit()

    for i in range(1, 4):
        project = Project(
            project_code=f"P700-00{i}",
            name=f"Demo Project {i}",
            client="Eastern Cape DoPW",
            manager="Ntembeko Zifuku",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 12, 31)
        )
        db.session.add(project)
        db.session.flush()  # Get ID

        scope = Scope(
            project_id=project.id,
            background="Infrastructure upgrade in rural area.",
            outputs="Improved road access, new drainage.",
            deliverables="Detailed design drawings, BoQ, site supervision.",
            exclusions="Utility relocations handled separately."
        )

        pqp = PQP(
            project_id=project.id,
            quality_controls="Checklist reviews, internal peer reviews.",
            responsibilities="Engineer: John, Planner: Lisa.",
            documentation="PQP v1.0 approved by PM.",
            last_reviewed=date.today()
        )

        detail = PQPDetail(
            project_id=project.id,
            overview_summary="This project addresses rural mobility.",
            team_description="Team includes engineers, planners and community liaison.",
            appointment_milestones="Stage 1-6 approvals achieved.",
            appointment_status="Finalised",
            planning_notes="Aligned with SDF and EIA.",
            planning_verified=True,
            tender_instructions="CIDB Grade 6CE required.",
            tender_status="Approved",
            works_plan="Works started May 2025.",
            works_checklist_complete=True,
            extras_description="Additional footbridges proposed.",
            extras_approved=True,
            closeout_summary="All deliverables submitted.",
            feedback_rating="Excellent",
            scope_notes="Special conditions apply on terrain."
        )

        # Dummy team assignments
        assign1 = ProjectTeamAssignment(project_id=project.id, team_member_id=john.id, role="Engineer", organisation="ISPAN", is_required=True, is_subconsultant=False, has_agreement=True, is_cpg=True, cpg_percent="30%")
        assign2 = ProjectTeamAssignment(project_id=project.id, team_member_id=lisa.id, role="Planner", organisation="ISPAN", is_required=True, is_subconsultant=False, has_agreement=True, is_cpg=False)

        # Sample risks, issues, kpis
        risk = RiskLog(project_id=project.id, description="Stormwater capacity overload", likelihood="High", impact="Medium", mitigation="Upsize culverts", status="Open")
        issue = CorrectiveAction(project_id=project.id, issue="Design error on invert levels", root_cause="Survey mismatch", action_taken="Redesign & reissue", status="Closed", closed_date=date.today())
        kpi = KPI(project_id=project.id, metric_name="Design Completion", target_value="100%", actual_value="100%", measured_on=date.today())

        # Sample uploads and checklist items
        uploads = [
            PQPFileUpload(project_id=project.id, section="Planning", file_label="SDF Alignment", filepath="/static/uploads/sample.pdf"),
            PQPFileUpload(project_id=project.id, section="Tender", file_label="BoQ", filepath="/static/uploads/sample_boq.pdf"),
        ]

        checklist = [
            PQPStageChecklist(project_id=project.id, section="Planning", item="EIA Completed", completed=True),
            PQPStageChecklist(project_id=project.id, section="Tender", item="Spec Pack Ready", completed=False),
        ]

        db.session.add_all([scope, pqp, detail, assign1, assign2, risk, issue, kpi] + uploads + checklist)

    db.session.commit()
    print("✅ Dummy projects seeded successfully.")
