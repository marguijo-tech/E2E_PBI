import kagglehub
import os
import shutil
import json
import pandas as pd
from prefect import task, flow
from prefect.variables import Variable
from sqlalchemy import create_engine

def download_kaggle_dataset(dataset_id: str):
    # Download dataset
    path = kagglehub.dataset_download(dataset_id)
    print("Path to dataset files:", path)

    # Get the directory where this script lives
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Create datasets directory inside the base path if it doesn't exist
    datasets_dir = os.path.join(script_dir, "datasets")
    os.makedirs(datasets_dir, exist_ok=True)

    # Find and copy CSV files to the script's directory
    copied_file_path = None
    copied_file_name = None
    for file_name in os.listdir(path):
        full_file_path = os.path.join(path, file_name)
        if os.path.isfile(full_file_path) and file_name.endswith(".csv"):
            copied_file_path = os.path.join(datasets_dir, file_name)
            shutil.copy(full_file_path, copied_file_path)
            copied_file_name = file_name
            print(f"Copied {file_name} to {script_dir}")
            break  # Exit after finding the first CSV file

    if copied_file_path and copied_file_name:
        return copied_file_path, copied_file_name
    else:
        raise FileNotFoundError("No CSV file found in the downloaded dataset.")
    
def dataset_to_dataframe(csv_file_path: str):
    """Load the specified CSV file into a Pandas DataFrame"""
    try:
        # Load the dataset into a DataFrame
        print(f"Loading dataset from: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        return df

    except Exception as e:
        print(f"Error: {e}")
        return None 
    
def dataframe_to_database(df:pd.DataFrame, table_name:str):
    """Push the DataFrame into a SQL Server Table, overwriting if necessary."""
    # Fetch database connection string from Prefect variables
    connection_string = Variable.get("mssql_connection_string")

    # Connect to SQL Server and push to Dataframe
    engine = create_engine(connection_string)
    print(f"Pushing data to table '{table_name}' in SQL Server.")
    
    # Write the DataFrame to SQL Server, overwriting the table if it exists
    df.to_sql(table_name, engine, index=False, if_exists='replace')
    print(f"DataFrame pushed to Table: {table_name} in SQL Server")

def process_kagglehub_datasets(datasets):
    """
    datasets: list of tuples: (kagglehub_dataset_id, sql_table_name)
    """
    for dataset_id, table_name in datasets:
        try:
            print(f"\nProcessing dataset: {dataset_id} -> Table: {table_name}")
            final_path, file_name = download_kaggle_dataset(dataset_id)
            df = dataset_to_dataframe(final_path)
            dataframe_to_database(df, table_name)
        except Exception as e:
            print(f"Error processing {dataset_id}: {e}")

if __name__ == "__main__":

    datasets_to_process = Variable.get("datasets_to_process")

    process_kagglehub_datasets(datasets_to_process)

    print("All datasets processed successfully.")