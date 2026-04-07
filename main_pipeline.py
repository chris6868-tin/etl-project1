import utils.extract as ex
import utils.transform as trans
import utils.load as load
import polars as pl

dataset_name = "peopledatalabssf/free-7-million-company-dataset"

def run_etl():
    # Extract
    print("Converting CSV to Barquet...")
    bronze_path = ex.csv_to_barquet("data/companies_sorted.csv", "data" )
    print("Barquet file is ready.")

    #Transform
    print("Transforming...")
    silver_lf = trans.transform_data(bronze_path)
    print("Transform completed.")

    #Save Silver
    silver_path = "data/silver_companies_cleaned.parquet"
    load.save_silver_data(silver_lf, silver_path)
    print('Saved silver file')

    #Load to cloud
    load.load_to_bigquery(
        parquet_path=silver_path,
        project_id="turnkey-life-430013-n3",
        dataset_id="7milions_companies",
        table_id="fact_companies",
        credentials_path="config/google_keys.json"
    )

if __name__ == "__main__":
    # print("Download csv file...")
    # ex.get_data(dataset_name)
    # print("CSV file is ready.")

    pl.enable_string_cache() 
    run_etl()
