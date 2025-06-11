import kagglehub
import os
import shutil
import pandas as pd
import json
from pandas import DataFrame, json_normalize
from prefect import task, flow
from prefect.variables import Variable
from sqlalchemy import create_engine

@task
def download_kaggle_dataset(dataset_id: str):
    path = kagglehub.dataset_download(dataset_id)
    print("Path to dataset files:", path)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    datasets_dir = os.path.join(script_dir, "datasets")
    os.makedirs(datasets_dir, exist_ok=True)

    copied_file_path = None
    copied_file_name = None
    for file_name in os.listdir(path):
        full_file_path = os.path.join(path, file_name)
        if os.path.isfile(full_file_path) and file_name.endswith(".csv"):
            copied_file_path = os.path.join(datasets_dir, file_name)
            shutil.copy(full_file_path, copied_file_path)
            copied_file_name = file_name
            print(f"Copied {file_name} to {script_dir}")
            break

    if copied_file_path and copied_file_name:
        return copied_file_path, copied_file_name
    else:
        raise FileNotFoundError("No CSV file found in the downloaded dataset.")

@task
def process_json_files(directory_path: str):
    json_files = [
        os.path.join(directory_path, f)
        for f in os.listdir(directory_path)
        if os.path.isfile(os.path.join(directory_path, f)) and f.endswith(".json")
    ]
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in the directory: {directory_path}")
    return json_files

@task
def json_to_dataframe(json_file_path: str) -> DataFrame:
    """
    Reads a JSON file and converts it into a DataFrame. Automatically flattens nested JSON structures if necessary.

    Args:
        json_file_path (str): Path to the JSON file.

    Returns:
        DataFrame: Flattened DataFrame ready for database insertion.
    """
    try:
        print(f"Loading JSON data from: {json_file_path}")

        # Load JSON content as a Python object
        with open(json_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        # Handle list of records
        if isinstance(data, list):
            print("Detected list of records.")
            df = json_normalize(data)

        # Handle single JSON object
        elif isinstance(data, dict):
            print("Detected single JSON object.")
            df = json_normalize(data)

        else:
            raise ValueError("Unsupported JSON structure. Only objects and arrays are supported.")

        # Ensure DataFrame is valid and not empty
        if df.empty:
            raise ValueError(f"DataFrame is empty after processing JSON file: {json_file_path}")

        print(f"Loaded DataFrame with {len(df)} rows and {len(df.columns)} columns.")
        return df

    except Exception as e:
        print(f"Error reading JSON file {json_file_path}: {e}")
        return None

@task
def dataframe_to_database(df: pd.DataFrame, table_name: str):
    if df is None:
        print(f"Skipping database load for table '{table_name}' due to empty or invalid DataFrame.")
        return
    
    connection_string = Variable.get("mssql_connection_string")
    engine = create_engine(connection_string)
    print(f"Pushing data to table '{table_name}' in SQL Server.")
    df.to_sql(table_name, engine, index=False, if_exists='replace')
    print(f"DataFrame pushed to Table: {table_name} in SQL Server")


@flow
def process_datasets(datasets, json_directory):
    results = []

    # Process Kaggle Datasets
    for dataset_id, table_name in datasets:
        try:
            print(f"\nProcessing Kaggle dataset: {dataset_id} -> Table: {table_name}")
            final_path, file_name = download_kaggle_dataset(dataset_id)
            df = pd.read_csv(final_path)
            dataframe_to_database(df, table_name)
            results.append((dataset_id, table_name, "Success"))
        except Exception as e:
            print(f"Error processing Kaggle dataset {dataset_id}: {e}")
            results.append((dataset_id, table_name, f"Failed: {e}"))

    # Process JSON Datasets
    try:
        json_files = process_json_files(json_directory)
        for json_file in json_files:
            try:
                table_name = os.path.splitext(os.path.basename(json_file))[0]
                print(f"\nProcessing JSON file: {json_file} -> Table: {table_name}")
                df = json_to_dataframe(json_file)
                dataframe_to_database(df, table_name)
                results.append((json_file, table_name, "Success"))
            except Exception as e:
                print(f"Error processing JSON file {json_file}: {e}")
                results.append((json_file, table_name, f"Failed: {e}"))
    except Exception as e:
        print(f"Error processing JSON directory {json_directory}: {e}")
        results.append((json_directory, "N/A", f"Failed: {e}"))

    return results

if __name__ == "__main__":
    datasets_to_process = Variable.get("kaggle_datasets_to_process")
    json_directory = Variable.get("spotify_json_directory")
    process_datasets(datasets_to_process, json_directory)

