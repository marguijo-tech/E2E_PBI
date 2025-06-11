import kagglehub
import os
import shutil
import pandas as pd
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
def dataset_to_dataframe(csv_file_path: str):
    try:
        print(f"Loading dataset from: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

@task
def dataframe_to_database(df: pd.DataFrame, table_name: str):
    connection_string = Variable.get("mssql_connection_string")
    engine = create_engine(connection_string)
    print(f"Pushing data to table '{table_name}' in SQL Server.")
    df.to_sql(table_name, engine, index=False, if_exists='replace')
    print(f"DataFrame pushed to Table: {table_name} in SQL Server")

@flow
def process_kagglehub_datasets(datasets):
    results = []
    for dataset_id, table_name in datasets:
        try:
            print(f"\nProcessing dataset: {dataset_id} -> Table: {table_name}")
            final_path, file_name = download_kaggle_dataset(dataset_id)
            df = dataset_to_dataframe(final_path)
            dataframe_to_database(df, table_name)
            results.append((dataset_id, table_name, "Success"))
        except Exception as e:
            print(f"Error processing {dataset_id}: {e}")
            results.append((dataset_id, table_name, f"Failed: {e}"))
    return results

if __name__ == "__main__":
    datasets_to_process = Variable.get("datasets_to_process")
    process_kagglehub_datasets(datasets_to_process)

