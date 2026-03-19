from typing import TypedDict


class sqlstate(TypedDict):
    user_input: str
    generated_sql: str
    validation_status: bool
    query_results: list[dict] | None  # New field to hold query results
    formatted_output: str | None  # New field to hold formatted output
