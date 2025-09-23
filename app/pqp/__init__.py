# app/pqp/__init__.py
from .pqp_routes import pqp_bp, pqp_api_bp  # existing
try:
    from .risk_api import bp as risk_api_bp  # if you added risk_api.py
except Exception:  # optional
    risk_api_bp = None

__all__ = ["pqp_bp", "pqp_api_bp", "risk_api_bp"]
# Ensure risk_api_bp is included only if it was successfully imported
