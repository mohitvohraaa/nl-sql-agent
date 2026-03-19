"""
agents/sql_generator.py
-----------------------
Step 4: Natural Language → SQL Generator Agent.

This agent receives:
  - user_query   : a plain English question (e.g. "top 5 products by revenue")
  - table_schema : a formatted string describing the GA4 table columns

It returns:
  - generated_sql : a valid BigQuery SELECT statement

How it works:
  1. Formats the schema + question into a carefully engineered prompt.
  2. Sends the prompt to Gemini 1.5 Flash.
  3. Strips any markdown/explanation from the response.
  4. Returns only the clean SQL string.
"""

import re
import sys
import os

import google.generativeai as genai

# sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
# from config import GEMINI_API_KEY, GEMINI_MODEL


# # ── Configure Gemini once at import time ─────────────────────────────────────
# genai.configure(api_key=GEMINI_API_KEY)


from groq import Groq
from config import GROQ_API_KEY, GROQ_MODEL

_client = Groq(api_key=GROQ_API_KEY)

# ── Schema formatter ─────────────────────────────────────────────────────────

def format_schema_for_prompt(schema: dict) -> str:
    """
    Convert the raw schema dict (from get_dataset_schema) into a
    clean text block that Gemini can read easily.

    Example output:
        Table: `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
        Columns:
          - event_date (STRING)
          - event_name (STRING)
          - ecommerce (RECORD)
              • transaction_id (STRING)
              • purchase_revenue (FLOAT)
          ...

    Args:
        schema: Dict returned by get_dataset_schema().

    Returns:
        A formatted multi-line string describing the table schema.
    """
    lines = []

    for table_ref, fields in schema.items():
        lines.append(f"Table: `{table_ref}`")
        lines.append("Columns:")

        for field in fields:
            lines.append(f"  - {field['name']} ({field['type']})")
            # Show nested sub-fields for RECORD types (GA4 uses these a lot)
            for sub in field.get("fields", []):
                lines.append(f"      • {sub['name']} ({sub['type']})")

    return "\n".join(lines)


# ── Prompt template ──────────────────────────────────────────────────────────

def build_prompt(user_query: str, schema_text: str) -> str:
    """
    Build the full prompt sent to Gemini.

    Prompt engineering decisions:
      - We give Gemini the exact table reference so it doesn't hallucinate
        table names.
      - We explicitly tell it to use the events_* wildcard (covers all 92
        daily shards in the GA4 dataset).
      - We ask for SQL ONLY — no explanation, no markdown fences — so the
        output is directly usable without parsing.
      - We include GA4-specific hints (event_params unnesting, ecommerce
        fields) because GA4 schema is unusual and Gemini needs the hint.

    Args:
        user_query:  The natural language question from the user.
        schema_text: Formatted schema string from format_schema_for_prompt().

    Returns:
        The complete prompt string to send to Gemini.
    """
    return f"""You are an expert Google BigQuery SQL writer specialising in GA4 event data.

    SCHEMA:
    {schema_text}

    IMPORTANT RULES:
    1. Always use the FULL table reference with backticks:
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    2. Always use the wildcard `events_*` to cover all date shards.
    3. For product/item data, use the `items` array with UNNEST():
    UNNEST(items) AS item
    4. For custom event parameters, use UNNEST(event_params):
    UNNEST(event_params) AS ep WHERE ep.key = 'your_key'
    5. For revenue, use `ecommerce.purchase_revenue` (FLOAT field).
    6. Only write SELECT statements — no INSERT, UPDATE, DELETE, DROP.
    7. Always include a LIMIT clause (default LIMIT 100 unless asked otherwise).
    8. Return ONLY the SQL query — no explanation, no markdown, no code fences.

    USER QUESTION:
    {user_query}

    SQL QUERY:"""


# ── SQL cleaner ──────────────────────────────────────────────────────────────

def clean_sql(raw_response: str) -> str:
    """
    Strip any markdown formatting or explanation that Gemini might add
    despite our instructions, returning only the bare SQL string.

    Handles cases like:
      ```sql
      SELECT ...
      ```
      or:
      "Here is the SQL: SELECT ..."

    Args:
        raw_response: The raw text returned by Gemini.

    Returns:
        Clean SQL string with no markdown or preamble.
    """
    # Remove ```sql ... ``` or ``` ... ``` fences
    cleaned = re.sub(r"```(?:sql)?\s*", "", raw_response)
    cleaned = re.sub(r"```", "", cleaned)

    # Remove any lines before the SELECT keyword
    # (handles "Here is the query:\nSELECT ...")
    lines = cleaned.strip().split("\n")
    sql_start = 0
    for i, line in enumerate(lines):
        if line.strip().upper().startswith("SELECT"):
            sql_start = i
            break

    return "\n".join(lines[sql_start:]).strip()


# ── Main agent function ───────────────────────────────────────────────────────

# def generate_schema_from_bigquery(client) -> dict:
#     """
#     Fetch the GA4 schema from BigQuery and return it as a dict.

#     This is a helper function that the SQL Generator Agent can call to get
#     the schema it needs to generate SQL.

#     Args:
#         client: An authenticated BigQuery client.

#     Returns:
#         A dict in the same format as get_dataset_schema() returns.
#     """
#     from utils.bigquery_client import get_dataset_schema

#     return get_dataset_schema(client)


def generate_sql(user_query: str, schema: dict) -> dict:
    """
    Main entry point for the SQL Generator Agent.

    Takes a natural language question and table schema, calls Gemini,
    and returns a structured result dict.

    Args:
        user_query: Plain English question from the user.
        schema:     Dict from get_dataset_schema() in bigquery_client.py.

    Returns:
        On success:
            {
                "status":        "success",
                "generated_sql": str,   # the SQL query
                "user_query":    str    # echoed back for traceability
            }
        On failure:
            {
                "status":     "error",
                "message":    str,
                "user_query": str
            }
    """
    
    if not user_query.strip():
        return {
            "status":     "error",
            "message":    "Empty user query provided.",
            "user_query": user_query,
        }

    if not schema:
        return {
            "status":     "error",
            "message":    "No schema provided. Run get_dataset_schema() first.",
            "user_query": user_query,
        }

    try:
        # 1. Format schema into readable text
        schema_text = format_schema_for_prompt(schema)

        # 2. Build the full prompt
        prompt = build_prompt(user_query, schema_text)

        print(f"\n🤖 Sending query to Gemini: '{user_query}'")

        # 3. Call Gemini
        # model = genai.GenerativeModel(
        #     model_name=GEMINI_MODEL,
        #     generation_config={
        #         # Low temperature = more deterministic SQL output
        #         # We want consistent SQL, not creative variations
        #         "temperature":     0.1,
        #         "max_output_tokens": 1024,
        #     }
        # )

        response = _client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=1024,
        )
        raw_sql = response.choices[0].message.content
        # response = model.generate_content(prompt)
        # raw_sql = response.text

        # 4. Clean the response
        generated_sql = clean_sql(raw_sql)

        print(f"✅ SQL generated successfully")
        print(f"\nGenerated SQL:\n{generated_sql}\n")

        return {
            "status":        "success",
            "generated_sql": generated_sql,
            "user_query":    user_query,
        }

    except Exception as e:
        print(f"❌ Gemini error: {e}")
        return {
            "status":     "error",
            "message":    f"Gemini API error: {str(e)}",
            "user_query": user_query,
        }


# ── Quick self-test ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    from utils.bigquery_client import get_bigquery_client, get_dataset_schema

    print("=" * 55)
    print("Step 4: SQL Generator Agent Test")
    print("=" * 55)

    # 1. Get schema from BigQuery
    print("\n📋 Fetching GA4 schema...")
    client = get_bigquery_client()
    schema = get_dataset_schema(client)

    # 2. Test with several natural language questions
    test_queries = [
        "What are the top 5 products by revenue?",
        "How many unique users visited the store each day?",
        "What is the total revenue by month?",
    ]

    for query in test_queries:
        print(f"\n{'='*55}")
        result = generate_sql(query, schema)

        if result["status"] == "success":
            print(f"Question : {result['user_query']}")
            print(f"SQL      :\n{result['generated_sql']}")
        else:
            print(f"Error    : {result['message']}")