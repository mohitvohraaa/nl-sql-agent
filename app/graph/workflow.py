from langgraph.graph import StateGraph, START, END
from app.graph.state import sqlstate
from app.graph.nodes import sql_generation_node, sql_validation_node, query_runner_node,output_node

workflow = StateGraph(sqlstate)

#NODES

workflow.add_node("SQL Generation", sql_generation_node)
workflow.add_node("SQL Validation", sql_validation_node)
workflow.add_node("Query Runner", query_runner_node)
workflow.add_node("Output", output_node)

#EDGES
workflow.add_edge(START, "SQL Generation")
workflow.add_edge("SQL Generation", "SQL Validation")
workflow.add_edge("SQL Validation", "Query Runner")
workflow.add_edge("Query Runner", "Output")
workflow.add_edge("Output", END)

# COMPILE

sql_graph = workflow.compile()





