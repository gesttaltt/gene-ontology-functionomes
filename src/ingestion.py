import os
import json
import pandas as pd
import requests
from process_ontology import process_go_json

def fetch_go_data():
    """
    Fetches the Gene Ontology JSON data via the GO API endpoint.
    
    Returns:
        dict: The Gene Ontology data as a JSON object.
    Raises:
        Exception: If the API call fails.
    """
    url = "http://purl.obolibrary.org/obo/go/go-basic.json"
    print(f"Fetching GO data from API: {url}")
    response = requests.get(url)
    if response.status_code == 200:
        # Decode JSON directly from the response
        data_json = response.json()
        print("GO data fetched successfully.")
        return data_json
    else:
        raise Exception(f"Failed to fetch data from {url}. Status code: {response.status_code}")

def export_processed_data(df, export_filename="processed_go.csv"):
    """
    Exports the processed DataFrame to the output folder as a CSV file.
    
    Parameters:
        df (pandas.DataFrame): The processed DataFrame.
        export_filename (str): The name of the CSV file.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "..", "output")
    
    # Create output folder if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    export_path = os.path.join(output_dir, export_filename)
    df.to_csv(export_path, index=False)
    print(f"Processed data exported to: {export_path}")

def run_ingestion_pipeline():
    """
    Runs the ingestion pipeline:
      1. Fetches GO JSON data via API.
      2. Processes the data using process_go_json.
      3. Exports the processed data to a CSV file.
    
    Returns:
        pandas.DataFrame: The processed DataFrame.
    """
    # Fetch GO data from the API endpoint
    go_json = fetch_go_data()
    
    # Process the raw JSON into a DataFrame using process_ontology.py
    processed_df = process_go_json(go_json)
    
    # Export the processed DataFrame to the output folder
    export_processed_data(processed_df)
    
    return processed_df

# Example usage:
if __name__ == "__main__":
    try:
        processed_data = run_ingestion_pipeline()
        print("Ingestion and export completed successfully. Processed data:")
        print(processed_data.head())
    except Exception as e:
        print(f"Error during ingestion: {e}")
