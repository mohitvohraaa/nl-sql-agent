# import os
from dotenv import load_dotenv
from typing import Dict
import json
import re
# load_dotenv()

# genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

# model = genai.GenerativeModel("gemini-2.5-flash") 
from groq import Groq
from config import GROQ_API_KEY, GROQ_MODEL

_client = Groq(api_key=GROQ_API_KEY)
def validate_sql_with_llm(sql: str) -> dict:
    prompt = f"""
    Validate this SQL query:

    {sql}

    Return JSON:
    {{
    "status": "success" or "error",
    "reason": "explanation"
    }}

    Rules:
    - Only SELECT queries allowed
    - Must reference bigquery-public-data.ga4_obfuscated_sample_ecommerce
    - No destructive keywords
    -DESTRUCTIVE_KEYWORDS = [
        "DROP", "DELETE", "INSERT", "UPDATE",
        "TRUNCATE", "ALTER", "CREATE", "REPLACE",
        "MERGE", "CALL", "EXECUTE", "GRANT", "REVOKE",
    ]
    
    -The only BigQuery project our agent is allowed to query
    ALLOWED_PROJECT = "bigquery-public-data"
    
    -The only dataset our agent is allowed to query
    ALLOWED_DATASET = "ga4_obfuscated_sample_ecommerce"
 
    """

    try:
        response = _client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=256,
        )
        # return response.choices[0].message.content
        raw = response.choices[0].message.content
        cleaned = re.sub(r"```(?:json)?\s*", "", raw)
        cleaned = re.sub(r"```", "", cleaned).strip()
        start = cleaned.index("{")
        end   = cleaned.rindex("}") + 1
        cleaned = cleaned[start:end]
        return json.loads(cleaned)
    # except Exception as e:
    #     return {
    #         "status": "error",
    #         "reason": f"API error: {str(e)}"
    #     }

    # ── This is the fix — parse the string into a dict ──
    # Strip markdown fences if Gemini adds them despite instructions
    

    # try:
    #     return json.loads(cleaned)
    except json.JSONDecodeError:
        # If Gemini still returns something unparseable, return safe error
        return {
            "status": "error",
            "reason": f"Validator could not parse Gemini response: {response.choices[0].message.content}"
        }

#TEST CASE
if __name__ == "__main__":
 
    print("=" * 55)
    print("Step 5: SQL Validator Agent Test")
    print("=" * 55)
 
    test_cases = [
        {
            "name":"test1",
            "sql":"""
                SELECT
                item.item_id,
                item.item_name,
                SUM(item.item_revenue_in_usd) AS total_revenue_usd
                FROM
                `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` AS e,
                UNNEST(e.items) AS item
                WHERE
                e.event_name = 'purchase'
                AND PARSE_DATE('%Y%m%d', e.event_date) BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
                                                            AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
                AND item.item_revenue_in_usd IS NOT NULL
                GROUP BY
                item.item_id,
                item.item_name
                ORDER BY
                total_revenue_usd DESC
                LIMIT 5;
            """,
            "expect":"success"
        },
        {
            "name": "Valid SELECT query",
            "sql": """
                SELECT item.item_name, SUM(item.item_revenue) AS total_revenue
                FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
                CROSS JOIN UNNEST(items) AS item
                WHERE event_name = 'purchase'
                GROUP BY item.item_name
                ORDER BY total_revenue DESC
                LIMIT 5
            """,
            "expect": "success"
        },
        {
            "name": "Valid CTE (WITH clause)",
            "sql": """
                WITH revenue AS (
                    SELECT event_date, SUM(ecommerce.purchase_revenue) AS rev
                    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
                    WHERE event_name = 'purchase'
                    GROUP BY event_date
                )
                SELECT * FROM revenue
                ORDER BY rev DESC
                LIMIT 10
            """,
            "expect": "success"
        },
        {
            "name": "Blocked: DELETE statement",
            "sql": "DELETE FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20201101`",
            "expect": "error"
        },
        {
            "name": "Blocked: DROP TABLE",
            "sql": "DROP TABLE `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20201101`",
            "expect": "error"
        },
        {
            "name": "Blocked: wrong dataset",
            "sql": "SELECT * FROM `bigquery-public-data.other_dataset.some_table` LIMIT 10",
            "expect": "error"
        },
        {
            "name": "Warning: missing LIMIT",
            "sql": """
                SELECT event_name, COUNT(*) AS cnt
                FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
                GROUP BY event_name
            """,
            "expect": "success"  # passes but with warning
        },
        {
            "name": "Blocked: empty query",
            "sql": "   ",
            "expect": "error"
        },
    ]
 
    passed = 0
    failed = 0
 
    for i, test in enumerate(test_cases, 1):
        print(f"\n--- Test {i}: {test['name']} ---")
        result = validate_sql_with_llm(test["sql"])
        actual = result["status"]
        print(actual)
        expected = test["expect"]
 
        if actual == expected:
            print(f"✅ PASS — got '{actual}' as expected")
            passed += 1
        else:
            print(f"❌ FAIL — expected '{expected}', got '{actual}'")
            
            print(f"   Reason: {result['message']}")
            failed += 1
 
    print(f"\n{'='*55}")
    print(f"Results: {passed} passed, {failed} failed out of {len(test_cases)} tests")