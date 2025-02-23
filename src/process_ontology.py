import os
import json
import pandas as pd

def process_go_json(go_json):
    """
    Processes the Gene Ontology JSON data exported by ingestion.py and transforms it into
    a DataFrame suitable for classification.py.

    The function extracts nodes of type "CLASS" and, for each node:
      - 'gene' is taken from the 'lbl' field (or 'id' if 'lbl' is missing)
      - 'pathways' is the count of basic property values in the node's meta
      - 'interactions' is the count of synonyms in the node's meta

    Parameters:
        go_json (dict): The Gene Ontology data as a dictionary.
    
    Returns:
        pandas.DataFrame: DataFrame with columns: 'gene', 'pathways', 'interactions'
    """
    rows = []
    
    # Ensure that there is at least one graph in the JSON
    graphs = go_json.get("graphs", [])
    if not graphs:
        raise ValueError("No graphs found in the provided GO JSON data.")
    
    # Process the first graph
    graph = graphs[0]
    nodes = graph.get("nodes", [])
    
    for node in nodes:
        if node.get("type") == "CLASS":
            # Use label if available; otherwise, use the id.
            gene_label = node.get("lbl", node.get("id"))
            
            meta = node.get("meta", {})
            basic_props = meta.get("basicPropertyValues", [])
            pathways_count = len(basic_props)
            
            synonyms = meta.get("synonyms", [])
            interactions_count = len(synonyms)
            
            rows.append({
                "gene": gene_label,
                "pathways": pathways_count,
                "interactions": interactions_count
            })
    
    if not rows:
        raise ValueError("No 'CLASS' type nodes found in the GO JSON data.")
    
    df = pd.DataFrame(rows)
    return df

# Example usage for testing:
if __name__ == "__main__":
    # Build the file path relative to this file: ../data/go/go-basic.json
    base_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(base_dir, "..", "data", "go", "go-basic.json")
    
    try:
        with open(json_path, "r") as f:
            go_json = json.load(f)
        df_processed = process_go_json(go_json)
        print("Processed GO DataFrame:")
        print(df_processed.head())
    except Exception as e:
        print(f"Error processing GO JSON: {e}")
