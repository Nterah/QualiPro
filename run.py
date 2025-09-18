# run.py
from pathlib import Path
from dotenv import load_dotenv

# Load the .env that sits NEXT TO this file (no guessing)
load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=True)

from app import create_app
app = create_app()

if __name__ == "__main__":
    # IMPORTANT: no reloader (avoids duplicate imports / duplicate tables / dup blueprints)
    app.run(debug=True, use_reloader=False)
