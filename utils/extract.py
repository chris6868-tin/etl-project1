import os
from pathlib import Path
import polars as pl
import kagglehub


def get_data(dataset_name):
    

    path = kagglehub.dataset_download(dataset_name)
    print("Path to dataset files:", path)
    

def csv_to_barquet(csv_path: str, output_dir: str):
    input_file = Path(csv_path)
    out_dir_path = Path(output_dir)
    
    parquet_path = out_dir_path / input_file.with_suffix(".parquet").name
    pl.scan_csv(csv_path).sink_parquet(
        parquet_path,
        compression="snappy",
        row_group_size=100_000
    )

    print(f"parquet saved: {parquet_path}")

    return str(parquet_path)