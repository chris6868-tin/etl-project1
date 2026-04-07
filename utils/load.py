import polars as pl
import os
from google.cloud import bigquery
def save_silver_data(lf: pl.LazyFrame, output_path:str):
    lf.collect().write_parquet(output_path, compression="snappy")

def load_to_bigquery(parquet_path, project_id, dataset_id, table_id, credentials_path):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    client = bigquery.Client(project=project_id)

    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print("Loading to cloud...")

    with open(parquet_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config, location="US")
    
    job.result()

    print("Loaded to cloud")