from app.extensions import db  # adjust import if your db lives elsewhere
from sqlalchemy.dialects.postgresql import JSONB

class Project(db.Model):
    __tablename__ = 'project'
    __table_args__ = {'schema': 'pqp'}
    id = db.Column(db.Integer, primary_key=True)        # existing numeric PK if you have it
    project_code = db.Column(db.String, unique=True)    # used by other tables via TEXT 'id'
    name = db.Column(db.String)
    client = db.Column(db.String)
    manager = db.Column(db.String)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    date_created = db.Column(db.DateTime(timezone=True))
    date_modified = db.Column(db.DateTime(timezone=True))

class Section101(db.Model):
    __tablename__ = 'section101'
    __table_args__ = {'schema': 'pqp'}
    row_id = db.Column(db.BigInteger, primary_key=True)
    heading_id = db.Column(db.BigInteger)
    id = db.Column(db.String, index=True, nullable=False)   # project_code
    risk_code = db.Column(db.String)
    title = db.Column(db.String, nullable=False)
    description = db.Column(db.Text)
    cause = db.Column(db.Text)
    consequence = db.Column(db.Text)
    category = db.Column(db.String)
    likelihood = db.Column(db.String)
    impact = db.Column(db.String)
    treatment = db.Column(db.Text)
    owner = db.Column(db.String)
    due_date = db.Column(db.Date)
    status = db.Column(db.String, default='open')
    extra = db.Column(JSONB, default=dict, nullable=False)
    date_created = db.Column(db.DateTime(timezone=True))
    date_modified = db.Column(db.DateTime(timezone=True))

class RiskConcept(db.Model):
    __tablename__ = 'risk_concept'
    __table_args__ = {'schema': 'pqp'}
    row_id = db.Column(db.BigInteger, primary_key=True)
    heading_id = db.Column(db.BigInteger)
    id = db.Column(db.String, index=True, nullable=False)
    category = db.Column(db.String)
    risk = db.Column(db.Text)
    mitigating_measure = db.Column(db.Text)
    kickoff_inception_mins = db.Column(db.Text)
    design_team_mins = db.Column(db.Text)
    other_evidence = db.Column(db.Text)
    conforming_activity = db.Column(db.Boolean)
    nc_not_conforming = db.Column(db.Boolean)
    ofi_can_improve = db.Column(db.Boolean)
    na_mark = db.Column(db.Boolean)
    remarks = db.Column(db.Text)
    status = db.Column(db.String, default='open')
    extra = db.Column(JSONB, default=dict, nullable=False)
    date_created = db.Column(db.DateTime(timezone=True))
    date_modified = db.Column(db.DateTime(timezone=True))

class RiskDocs(db.Model):
    __tablename__ = 'risk_docs'
    __table_args__ = {'schema': 'pqp'}
    row_id = db.Column(db.BigInteger, primary_key=True)
    heading_id = db.Column(db.BigInteger)
    id = db.Column(db.String, index=True, nullable=False)
    category = db.Column(db.String)
    risk = db.Column(db.Text)
    mitigating_measure = db.Column(db.Text)
    email_from_client = db.Column(db.Text)
    internal_review = db.Column(db.Text)
    other_evidence = db.Column(db.Text)
    conforming_activity = db.Column(db.Boolean)
    nc_not_conforming = db.Column(db.Boolean)
    ofi_can_improve = db.Column(db.Boolean)
    na_mark = db.Column(db.Boolean)
    remarks = db.Column(db.Text)
    status = db.Column(db.String, default='open')
    extra = db.Column(JSONB, default=dict, nullable=False)
    date_created = db.Column(db.DateTime(timezone=True))
    date_modified = db.Column(db.DateTime(timezone=True))

class RiskWorks(db.Model):
    __tablename__ = 'risk_works'
    __table_args__ = {'schema': 'pqp'}
    row_id = db.Column(db.BigInteger, primary_key=True)
    heading_id = db.Column(db.BigInteger)
    id = db.Column(db.String, index=True, nullable=False)
    category = db.Column(db.String)
    risk = db.Column(db.Text)
    mitigating_measure = db.Column(db.Text)
    handover_mins = db.Column(db.Text)
    progress_mins = db.Column(db.Text)
    other_evidence = db.Column(db.Text)
    conforming_activity = db.Column(db.Boolean)
    nc_not_conforming = db.Column(db.Boolean)
    ofi_can_improve = db.Column(db.Boolean)
    na_mark = db.Column(db.Boolean)
    remarks = db.Column(db.Text)
    status = db.Column(db.String, default='open')
    extra = db.Column(JSONB, default=dict, nullable=False)
    date_created = db.Column(db.DateTime(timezone=True))
    date_modified = db.Column(db.DateTime(timezone=True))
