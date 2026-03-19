"""
utils/query_runner.py
---------------------
Step 3: BigQuery query execution helper.

Responsibilities:
  - Accept a SQL string and a BigQuery client.
  - Execute the query safely with a row limit guard.
  - Return a consistent result dict: always either success or error —
    never raises exceptions to the caller.
  - Pretty-print results in the terminal for easy testing.

Why a separate helper (not just calling BigQuery directly)?
  Every agent that runs SQL goes through this one function. That means
  error handling, row limits, and logging are all in one place.
"""

import time
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError


# Hard cap on rows returned — prevents accidental full-table scans
# during development. The agent can always request fewer rows via LIMIT.
MAX_ROWS = 1000


def run_query(sql: str, client: bigquery.Client,
              max_rows: int = MAX_ROWS) -> dict:
    """
    Execute a SQL query on BigQuery and return a structured result.

    This function NEVER raises exceptions — all errors are caught and
    returned as a dict with status="error". This makes it safe to call
    from inside a LangGraph node without crashing the graph.

    Args:
        sql:      A valid BigQuery SQL string (SELECT only).
        client:   An authenticated BigQuery client from get_bigquery_client().
        max_rows: Maximum number of rows to return (default 1000).

    Returns:
        On success:
            {
                "status":    "success",
                "data":      pd.DataFrame,   # query results
                "row_count": int,            # number of rows returned
                "bytes_processed": int,      # bytes billed
                "elapsed_seconds": float     # wall-clock query time
            }
        On failure:
            {
                "status":  "error",
                "message": str               # human-readable error description
            }
    """

    # ── Basic sanity check before hitting the API ──────────────────────────
    sql = sql.strip()
    if not sql:
        return {"status": "error", "message": "Empty SQL string provided."}

    # ── Configure the query job ────────────────────────────────────────────
    job_config = bigquery.QueryJobConfig(
        # Dry-run first would be ideal, but adds latency.
        # Instead we rely on the SQL Validator Agent (Step 5) to block
        # dangerous queries before they reach here.

        # Use INTERACTIVE priority (not BATCH) so results come back fast.
        priority=bigquery.QueryPriority.INTERACTIVE,

        # Automatically use cached results when the same query was run
        # recently — saves cost during development/testing.
        use_query_cache=True,
    )

    start_time = time.time()

    try:
        # ── Submit the query job ───────────────────────────────────────────
        print(f"\n⏳ Running query on BigQuery...")
        print(f"   SQL preview: {sql[:120]}{'...' if len(sql) > 120 else ''}")

        query_job = client.query(sql, job_config=job_config)

        # Block until the job completes (or fails).
        # result() raises google.api_core.exceptions.GoogleAPIError on failure.
        results = query_job.result(max_results=max_rows)

        elapsed = round(time.time() - start_time, 2)

        # ── Convert to pandas DataFrame ────────────────────────────────────
        # to_dataframe() downloads all result pages into memory.
        df = results.to_dataframe()

        # Bytes processed is available after the job completes.
        bytes_processed = query_job.total_bytes_processed or 0
        mb_processed = round(bytes_processed / (1024 * 1024), 2)

        print(f"✅ Query complete in {elapsed}s")
        print(f"   Rows returned : {len(df)}")
        print(f"   Data scanned  : {mb_processed} MB")

        return {
            "status":            "success",
            "data":              df,
            "row_count":         len(df),
            "bytes_processed":   bytes_processed,
            "elapsed_seconds":   elapsed,
        }

    except GoogleAPIError as e:
        # BigQuery-specific errors: bad SQL syntax, missing table, etc.
        elapsed = round(time.time() - start_time, 2)
        error_msg = str(e)

        print(f"❌ BigQuery error after {elapsed}s: {error_msg}")
        return {
            "status":  "error",
            "message": f"BigQuery error: {error_msg}",
        }

    except Exception as e:
        # Catch-all for unexpected errors (network, auth expiry, etc.)
        elapsed = round(time.time() - start_time, 2)
        print(f"❌ Unexpected error after {elapsed}s: {e}")
        return {
            "status":  "error",
            "message": f"Unexpected error: {str(e)}",
        }


def format_results(result: dict, max_display_rows: int = 10) -> str:
    """
    Format a run_query() result dict into a readable string for printing
    or passing back to the user as the final agent output.

    Args:
        result:           The dict returned by run_query().
        max_display_rows: How many rows to show in the preview (default 10).

    Returns:
        A formatted string ready to print or return to the user.
    """
    if result["status"] == "error":
        return f"Query failed: {result['message']}"

    df: pd.DataFrame = result["data"]

    if df.empty:
        return "Query ran successfully but returned no rows."

    # Use pandas to_string for a clean table — tabulate also works but
    # adds a dependency; pandas is already required.
    preview = df.head(max_display_rows).to_string(index=False)

    lines = [
        f"Rows returned : {result['row_count']}",
        f"Time taken    : {result['elapsed_seconds']}s",
        f"Data scanned  : {round(result['bytes_processed']/(1024*1024), 2)} MB",
        "",
        preview,
    ]

    if result["row_count"] > max_display_rows:
        lines.append(f"\n… showing first {max_display_rows} of {result['row_count']} rows")

    return "\n".join(lines)


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

    from utils.bigquery_client import get_bigquery_client

    print("=" * 55)
    print("Step 3: Query Runner Test")
    print("=" * 55)

    client = get_bigquery_client()

    # ── Test 1: Simple count query ─────────────────────────────────────────
    print("\n--- Test 1: Count total events ---")
    result = run_query("""
        SELECT
            event_name,
            COUNT(*) AS event_count
        FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
        GROUP BY event_name
        ORDER BY event_count DESC
        LIMIT 10
    """, client)
    print(format_results(result))

    # ── Test 2: Revenue by date ────────────────────────────────────────────
    print("\n--- Test 2: Daily revenue ---")
    result2 = run_query("""
        SELECT
            event_date,
            ROUND(SUM(ecommerce.purchase_revenue), 2) AS daily_revenue
        FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
        WHERE event_name = 'purchase'
        GROUP BY event_date
        ORDER BY event_date
        LIMIT 10
    """, client)
    print(format_results(result2))

    # ── Test 3: Error handling — intentionally bad SQL ─────────────────────
    print("\n--- Test 3: Error handling (bad SQL) ---")
    result3 = run_query("SELECT * FROM non_existent_table_xyz", client)
    print(format_results(result3))