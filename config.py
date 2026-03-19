"""
config.py
---------
Centralized configuration loader.
Reads all settings from the .env file and exposes them as
typed constants used throughout the project.
"""
 
import os
from dotenv import load_dotenv
 
# Load .env file into environment variables.
# If .env doesn't exist, os.getenv() will still work — it just
# returns the default values we specify below.
load_dotenv()
 
 
# # ── Gemini ──────────────────────────────────────────────────
# GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
# GEMINI_MODEL: str = os.getenv("MODEL_NAME", "gemini-2.5-flash")  # Fast and cost-effective for SQL gen
 
GROQ_API_KEY: str = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL: str = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")


# ── BigQuery ─────────────────────────────────────────────────
BIGQUERY_PROJECT_ID: str = os.getenv("BIGQUERY_PROJECT_ID", "")
BIGQUERY_DATASET: str    = os.getenv("BIGQUERY_DATASET", "")
 
# Optional: explicit path to service account JSON.
# If you've already set GOOGLE_APPLICATION_CREDENTIALS in your
# shell, you don't need this — the BigQuery client picks it up
# automatically.
GOOGLE_APPLICATION_CREDENTIALS: str = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS", ""
)
 
 
# ── Validation ───────────────────────────────────────────────
def validate_config() -> None:
    """
    Call this at startup to catch missing config early —
    before any agent tries to make an API call and fails with
    a cryptic error message.
    """
    missing = []
 
    if not GROQ_API_KEY:
        missing.append("GROQ_API_KEY")
    if not BIGQUERY_PROJECT_ID:
        missing.append("BIGQUERY_PROJECT_ID")
    if not BIGQUERY_DATASET:
        missing.append("BIGQUERY_DATASET")
 
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            f"Copy .env.example → .env and fill in the values."
        )
 
    print("✅ Config loaded successfully.")
    print(f"   Project : {BIGQUERY_PROJECT_ID}")
    print(f"   Dataset : {BIGQUERY_DATASET}")
    print(f"   Model   : {GROQ_MODEL}")
 
 
# ── Quick self-test ──────────────────────────────────────────
if __name__ == "__main__":
    validate_config()