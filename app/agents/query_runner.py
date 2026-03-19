"""
agents/query_runner_agent.py
----------------------------
Step 6: Query Runner Agent.

This agent receives:
  - validated_sql : the SQL string that passed the Validator Agent
  - client        : an authenticated BigQuery client

It returns:
  - query_results : a dict containing the DataFrame + metadata
  - error_message : if the query fails on BigQuery

Why wrap run_query() in an agent?
  run_query() in utils/query_runner.py is a low-level utility — it just
  executes SQL. This agent adds the business logic layer on top:
    - Checks that validated_sql actually exists before querying
    - Formats the final output for the user
    - Fits cleanly into the LangGraph node pattern (Step 8)
"""

import sys
import os
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from utils.query_runner import run_query, format_results
from utils.bigquery_client import get_bigquery_client


def run_query_agent(validated_sql: str, client) -> dict:
    """
    Execute validated SQL on BigQuery and return structured results.

    Args:
        validated_sql : SQL string that passed the Validator Agent.
        client        : Authenticated BigQuery client.

    Returns:
        On success:
            {
                "status":          "success",
                "query_results":   pd.DataFrame,
                "row_count":       int,
                "formatted_output": str,        # ready to show to user
                "bytes_processed": int,
                "elapsed_seconds": float
            }
        On failure:
            {
                "status":        "error",
                "message":       str
            }
    """

    # ── Guard: ensure we have SQL to run ─────────────────────────────────
    if not validated_sql or not validated_sql.strip():
        return {
            "status":  "error",
            "message": "No validated SQL provided to Query Runner Agent. "
                       "Check that the Validator Agent ran successfully."
        }

    print(f"\n🚀 Query Runner Agent starting...")

    # ── Execute the query via our Step 3 utility ─────────────────────────
    result = run_query(validated_sql, client)

    # ── Handle execution failure ──────────────────────────────────────────
    if result["status"] == "error":
        return {
            "status":  "error",
            "message": result["message"]
        }

    # ── Format results for display ────────────────────────────────────────
    formatted = format_results(result)

    print(f"✅ Query Runner Agent complete")
    print(f"\n{formatted}")

    return {
        "status":           "success",
        "query_results":    result["data"].to_dict(orient="records"),
        "row_count":        result["row_count"],
        "formatted_output": formatted,               # human-readable string
        "bytes_processed":  result["bytes_processed"],
        "elapsed_seconds":  result["elapsed_seconds"],
    }


# ── Quick self-test ───────────────────────────────────────────────────────────
if __name__ == "__main__":

    print("=" * 55)
    print("Step 6: Query Runner Agent Test")
    print("=" * 55)

    # Get BigQuery client
    client = get_bigquery_client()

    # ── Test 1: Run the SQL Gemini generated in Step 4 ───────────────────
    print("\n--- Test 1: Top 5 products by revenue ---")
    result1 = run_query_agent("""
        SELECT
            item.item_name,
            SUM(item.item_revenue) AS total_revenue
        FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
        CROSS JOIN UNNEST(items) AS item
        WHERE event_name = 'purchase'
        GROUP BY item.item_name
        ORDER BY total_revenue DESC
        LIMIT 5
    """, client)

    print(f"\nStatus      : {result1['status']}")
    print(f"Rows        : {result1.get('row_count', 'N/A')}")

    # ── Test 2: Unique users per day ──────────────────────────────────────
    print("\n--- Test 2: Unique users per day ---")
    result2 = run_query_agent("""
        SELECT
            event_date,
            COUNT(DISTINCT user_pseudo_id) AS unique_users
        FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
        GROUP BY event_date
        ORDER BY event_date
        LIMIT 10
    """, client)

    print(f"\nStatus      : {result2['status']}")
    print(f"Rows        : {result2.get('row_count', 'N/A')}")

    # ── Test 3: Error handling — pass empty SQL ───────────────────────────
    print("\n--- Test 3: Empty SQL guard ---")
    result3 = run_query_agent("", client)
    print(f"Status  : {result3['status']}")
    print(f"Message : {result3['message']}")