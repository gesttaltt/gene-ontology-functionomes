import os
import json
import pandas as pd
import requests
from process_ontology import process_go_json

def load_raw_data():
    """
    Loads raw gene ontology data from the file located at ./data/go/go-basic.json.
    If the file is not found, it attempts to download it from the GO website.
    
    Returns:
        dict: The Gene Ontology data as a JSON object.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "..", "data", "go", "go-basic.json")
    
    if not os.path.exists(file_path):
        print(f"Database file not found at: {file_path}. Attempting to download from the GO website...")
        url = "http://purl.obolibrary.org/obo/go/go-basic.json"
        response = requests.get(url)
        if response.status_code == 200:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(response.text)
            print(f"Downloaded file and saved to {file_path}.")
        else:
            raise FileNotFoundError(
                f"Database file not found at {file_path} and failed to download from {url}. Status code: {response.status_code}"
            )
    
    print(f"Loading raw data from: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        data_json = json.load(f)
    
    return data_json

def export_processed_data(df, export_filename="processed_go.csv"):
    """
    Exports the processed DataFrame to the output folder as a CSV file.
    
    Parameters:
        df (pandas.DataFrame): The processed DataFrame.
        export_filename (str): The filename for the exported CSV file.
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
    Runs the complete ingestion pipeline:
      1. Loads raw data.
      2. Processes the data using process_go_json.
      3. Exports the processed data to a CSV file.
    
    Returns:
        pandas.DataFrame: The processed DataFrame.
    """
    # Load raw JSON data (download if necessary)
    raw_data = load_raw_data()
    
    # Process the raw JSON into a DataFrame using process_ontology.py
    processed_df = process_go_json(raw_data)
    
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
