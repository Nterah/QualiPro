# run.py
from pathlib import Path

# Load .env for local dev (ignore if package/file isn't present — Render uses its own env)
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=True)
except Exception:
    pass

from flask import redirect
from app import create_app

app = create_app()

@app.get("/")
def root():
    # Redirect the site root to the PQP dashboard
    return redirect("/pqp", code=302)  # use 308 for a permanent redirect once you're happy

if __name__ == "__main__":
    # no reloader to avoid double imports
    app.run(debug=True, use_reloader=False)
