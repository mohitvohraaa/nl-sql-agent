
from groq import Groq

from app.graph.state import sqlstate
from app.agents.sql_generator import generate_sql
from app.agents.sql_validator import validate_sql_with_llm
from config import GROQ_API_KEY, GROQ_MODEL
from utils.bigquery_client import get_bigquery_client,get_dataset_schema

from app.agents.query_runner import run_query_agent
def sql_generation_node(state: sqlstate)->sqlstate:
    """
    This node takes the user input and generates SQL using Gemini. It returns a dictionary with the following structure:
    {
        "status": "success" or "error",
        "generated_sql": str (only if status is success),
        "message": str (only if status is error)
    }
    """
    # Here we would call the SQL Generator Agent (from sql_generator.py) to get the generated SQL.
    # For demonstration, let's assume we have a function called generate_sql that does this.
    from app.agents.sql_generator import generate_sql
    client = get_bigquery_client()
    schema = get_dataset_schema(client)
    result = generate_sql(state['user_input'], schema)

    if result["status"] == "success":
        return {
            "user_input": state['user_input'],
            "generated_sql": result["generated_sql"],
            "validation_status": None  # To be filled in the next node
        }
    else:
        return {
            **state,  # Preserve existing state fields
            "generated_sql": None,
            "validation_status": False # Mark as failed due to generation error
        }
    
def sql_validation_node(state: sqlstate)->sqlstate:
    """
    This node takes the generated SQL and validates it using Gemini. It updates the state with the validation status.
    """
    # Here we would call the SQL Validator Agent (from sql_validator.py) to validate the generated SQL.
    # For demonstration, let's assume we have a function called validate_sql_with_llm that does this.
    from app.agents.sql_validator import validate_sql_with_llm
    print("checking if the generated sql is updated or not in validation node", state['generated_sql'])
    if state['generated_sql'] is None:
        return {
            # "user_input": state['user_input'],
            # "generated_sql": None,
            **state,  # Preserve existing state fields
            "validation_status": False  # Cannot validate if generation failed
        }
    
    validation_result = validate_sql_with_llm(state['generated_sql'])
    print()
    print("validation result in validation node", validation_result)
    print()
    return {
        # "user_input": state['user_input'],
        # "generated_sql": state['generated_sql'],
        **state,  # Preserve existing state fields
        "validation_status": validation_result["status"] == "success"
    }

def query_runner_node(state: sqlstate)->sqlstate:
    """
    This node would take the validated SQL and run it against BigQuery, returning the results.
    For simplicity, this is just a placeholder and does not include actual query execution logic.
    """
    print('Checking validation status in query runner node', state['validation_status'])

    if state['validation_status'] is False:
        return {
            **state,  # Preserve existing state fields
            "query_results": None,
            "formatted_output": None
        }
    
    # Placeholder for actual query execution
    client = get_bigquery_client()
    print("generated sql in query runner node", state['generated_sql'])
    query_results = run_query_agent(state['generated_sql'], client)  # This function would execute the SQL and return results
    
    return {
        "user_input": state['user_input'],
        "generated_sql": state['generated_sql'],
        "validation_status": True,
        "query_results": query_results['query_results'],
        "formatted_output": None
    }

def output_node(state: sqlstate)->sqlstate:
    """
    This node would format the query results for output to the user. This is a placeholder.
    """
    
    if state['validation_status'] is False or state['query_results'] is None:
        return {
            "user_input": state['user_input'],
            "generated_sql": state['generated_sql'],
            "validation_status": state['validation_status'],
            "query_results": None,
            "formatted_output": "Unable to generate results due to validation failure."
        }
    _client = Groq(api_key=GROQ_API_KEY)
    user_query = state['user_input']
    validated_sql = state['generated_sql']
    results_json = state['query_results']
    shown_rows = min(5, len(results_json))
    total_rows = len(results_json)
    prompt = f"""You are a helpful data analyst summarizing BigQuery query results.
 
                    USER QUESTION:
                    {user_query}
                    
                    SQL QUERY THAT WAS RUN:
                    {validated_sql}
                    
                    QUERY RESULTS ({shown_rows} of {total_rows} total rows shown):
                    {results_json}
                    
                    YOUR TASK:
                    Write a clear, concise summary that directly answers the user's question
                    using the data above. Follow these rules:
                    
                    1. Lead with the direct answer to the question.
                    2. Highlight the most important numbers and patterns.
                    3. Keep it conversational — no technical jargon.
                    4. If there are rankings or top items, mention the top 3-5 specifically.
                    5. Keep the summary to 3-5 sentences maximum.
                    6. Do NOT mention SQL, BigQuery, or technical details.
                    7. Do NOT say "based on the data" or "the results show" — just answer directly.
                    
                    SUMMARY:"""
                    
    response = _client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=1024,
        )
    # Placeholder for actual formatting logic
    formatted_output = f"Query executed successfully. Here are the results: {response.choices[0].message.content}"
    state['formatted_output'] = formatted_output
    return state