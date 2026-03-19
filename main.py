import google.generativeai as genai

import os
from dotenv import load_dotenv

load_dotenv()

# genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

# print("Available models:\n")

# for m in genai.list_models():
#     print(m.name, "→", m.supported_generation_methods)

"""
main.py
-------
FastAPI backend wired to the existing LangGraph workflow.

Matches workflow.py structure exactly:
  State fields: user_input, generated_sql, validation_status,
                query_results, formatted_output
  Graph: sql_graph.invoke(initial_state)

Run with:
  uvicorn main:app --reload --port 8000
"""

import sys
import os
from typing import Any

import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

sys.path.insert(0, os.path.dirname(__file__))

# ── Import your compiled graph ─────────────────────────────────────────────
from app.graph.workflow import sql_graph


# ── App setup ──────────────────────────────────────────────────────────────
app = FastAPI(title="NL-to-SQL Agent API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Request / Response models ──────────────────────────────────────────────
class ChatRequest(BaseModel):
    message: str


class ChatResponse(BaseModel):
    query:   str
    sql:     str
    summary: str
    columns: list[str]
    rows:    list[dict]
    error:   str | None = None


# ── Helpers ────────────────────────────────────────────────────────────────

def make_serializable(value: Any) -> Any:
    """Recursively convert non-JSON-serializable types to plain Python."""
    if value is None:
        return None
    if isinstance(value, pd.DataFrame):
        return value.to_dict(orient="records")
    if isinstance(value, list):
        return [make_serializable(v) for v in value]
    if isinstance(value, dict):
        return {k: make_serializable(v) for k, v in value.items()}
    try:
        import numpy as np
        if isinstance(value, np.integer):  return int(value)
        if isinstance(value, np.floating): return float(value)
    except ImportError:
        pass
    return value


def extract_columns(rows: list[dict]) -> list[str]:
    if not rows:
        return []
    return list(rows[0].keys())


# ── POST /chat ─────────────────────────────────────────────────────────────
@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    if not request.message.strip():
        return ChatResponse(
            query="", sql="", summary="", columns=[], rows=[],
            error="Please enter a question."
        )

    try:
        # ── Build initial state — matches your workflow.py exactly ───────
        initial_state = {
            "user_input":        request.message,
            "generated_sql":     None,
            "validation_status": None,
            "query_results":     None,
            "formatted_output":  None,
        }

        # ── Run the graph ────────────────────────────────────────────────
        final_state = sql_graph.invoke(initial_state)

        # ── Extract from final state ─────────────────────────────────────
        sql           = final_state.get("generated_sql")    or ""
        validation_ok = final_state.get("validation_status") or False
        raw_results   = final_state.get("query_results")
        summary       = final_state.get("formatted_output") or ""

        # ── Serialize results ────────────────────────────────────────────
        rows    = make_serializable(raw_results) or []
        columns = extract_columns(rows)

        # ── Surface validation failure as error ──────────────────────────
        error = None
        if not validation_ok and not rows:
            error = "SQL validation failed. Query was not executed."

        return ChatResponse(
            query   = request.message,
            sql     = sql,
            summary = summary,
            columns = columns,
            rows    = rows,
            error   = error,
        )

    except Exception as e:
        return ChatResponse(
            query="", sql="", summary="", columns=[], rows=[],
            error=f"Pipeline error: {str(e)}"
        )


# ── GET /health ────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}