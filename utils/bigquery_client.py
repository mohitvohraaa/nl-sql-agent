"""
utils/bigquery_client.py
------------------------
Step 2: BigQuery connection setup.

Responsibilities:
  - Authenticate with Google Cloud using Application Default Credentials
    (or a service account key if you set GOOGLE_APPLICATION_CREDENTIALS).
  - Return a single, reusable BigQuery client object.
  - Verify the connection by listing datasets visible to the project.

Why a single module?
  All three agents need BigQuery access. Centralising the client here means
  auth logic lives in one place — change credentials once, everything updates.
"""

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

# Pull project config from our central config file
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import BIGQUERY_PROJECT_ID, GOOGLE_APPLICATION_CREDENTIALS


def get_bigquery_client() -> bigquery.Client:
    """
    Create and return an authenticated BigQuery client.

    Authentication priority (google-cloud library handles this automatically):
      1. GOOGLE_APPLICATION_CREDENTIALS env var → service account JSON file
      2. gcloud CLI default login         → `gcloud auth application-default login`
      3. GCE/Cloud Run metadata server    → automatic on GCP VMs

    Returns:
        bigquery.Client: Authenticated client bound to your GCP project.

    Raises:
        EnvironmentError: If BIGQUERY_PROJECT_ID is not set.
        google.auth.exceptions.DefaultCredentialsError: If no credentials found.
    """
    if not BIGQUERY_PROJECT_ID:
        raise EnvironmentError(
            "BIGQUERY_PROJECT_ID is not set. "
            "Add it to your .env file and try again."
        )

    # If a service account JSON path was explicitly provided, point the
    # environment variable at it so the google-auth library picks it up.
    if GOOGLE_APPLICATION_CREDENTIALS and os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS
        print(f"🔑 Using service account: {GOOGLE_APPLICATION_CREDENTIALS}")
    else:
        # Relies on `gcloud auth application-default login` being run already.
        print("🔑 Using Application Default Credentials (gcloud ADC).")

    # Construct the client — this is where auth actually happens.
    # The library reads credentials lazily; real auth occurs on first API call.
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    return client


def verify_connection(client: bigquery.Client) -> bool:
    """
    Send a lightweight API call to confirm the credentials work.

    We list datasets in the PUBLIC project rather than your own project,
    because you might have an empty personal project but still need to
    query public data.

    Args:
        client: An authenticated BigQuery client.

    Returns:
        bool: True if the connection works, False otherwise.
    """
    PUBLIC_PROJECT = "bigquery-public-data"
    PUBLIC_DATASET = "ga4_obfuscated_sample_ecommerce"

    try:
        # list_tables is a cheap call — it doesn't read any data.
        tables = list(client.list_tables(f"{PUBLIC_PROJECT}.{PUBLIC_DATASET}"))
        print(f"\n✅ BigQuery connection verified!")
        print(f"   Project (yours): {client.project}")
        print(f"   Public dataset : {PUBLIC_PROJECT}.{PUBLIC_DATASET}")
        print(f"   Tables found   : {len(tables)}")

        # Show the first few table names so we know what we're working with.
        for table in tables[:5]:
            print(f"     • {table.table_id}")
        if len(tables) > 5:
            print(f"     … and {len(tables) - 5} more")

        return True

    except GoogleAPIError as e:
        print(f"\n❌ BigQuery connection failed: {e}")
        print("\nTroubleshooting tips:")
        print("  1. Run: gcloud auth application-default login")
        print("  2. Check that BIGQUERY_PROJECT_ID is correct in .env")
        print("  3. Ensure BigQuery API is enabled in your GCP project")
        print("     → https://console.cloud.google.com/apis/library/bigquery.googleapis.com")
        return False


def get_dataset_schema(client: bigquery.Client,
                       project: str = "bigquery-public-data",
                       dataset: str = "ga4_obfuscated_sample_ecommerce",
                       table_prefix: str = "events_") -> dict:
    """
    Fetch schema details for tables that match a prefix.

    The GA4 dataset uses date-sharded tables (events_20201101, events_20201102 …).
    We grab the schema from ONE representative table — they all share the same
    schema — and return it as a dict the SQL Generator agent can consume.

    Args:
        client:       Authenticated BigQuery client.
        project:      GCP project containing the dataset.
        dataset:      Dataset name.
        table_prefix: Only include tables whose name starts with this prefix.

    Returns:
        dict: {table_name: [{"name": col_name, "type": col_type, "mode": mode}, ...]}
    """
    tables = list(client.list_tables(f"{project}.{dataset}"))
    matching = [t for t in tables if t.table_id.startswith(table_prefix)]

    if not matching:
        print(f"⚠️  No tables found with prefix '{table_prefix}' in {dataset}.")
        return {}

    # Use the first matching table as the representative schema source.
    sample_table = client.get_table(
        f"{project}.{dataset}.{matching[0].table_id}"
    )

    schema = {}
    schema[f"{project}.{dataset}.events_*"] = [
        {
            "name":  field.name,
            "type":  field.field_type,
            "mode":  field.mode,
            # Include sub-fields for RECORD types (GA4 uses these heavily)
            "fields": [
                {"name": sub.name, "type": sub.field_type}
                for sub in (field.fields or [])
            ]
        }
        for field in sample_table.schema
    ]

    return schema


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("Step 2: BigQuery Connection Test")
    print("=" * 55)

    # 1. Create client
    client = get_bigquery_client()

    # 2. Verify it works
    ok = verify_connection(client)

    if ok:
        # 3. Fetch a summary of the GA4 schema
        print("\n📋 Fetching GA4 table schema …")
        schema = get_dataset_schema(client)

        for table_ref, fields in schema.items():
            print(f"\nTable: {table_ref}")
            print(f"Total columns: {len(fields)}")
            # Show top-level columns only (GA4 has many nested RECORD fields)
            for field in fields[:10]:
                sub = f"  ({len(field['fields'])} sub-fields)" if field["fields"] else ""
                print(f"  {field['name']} [{field['type']}]{sub}")
            if len(fields) > 10:
                print(f"  … and {len(fields) - 10} more columns")