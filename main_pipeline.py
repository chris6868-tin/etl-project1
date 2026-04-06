import utils.extract as ex
import utils.transform as trans

dataset_name = "peopledatalabssf/free-7-million-company-dataset"

def run_etl():
    # Extract
    # ex.get_data(dataset_name)
    parquet_dir = ex.csv_to_barquet("data/companies_sorted.csv", "data" )


    #Transform
    trans.transform_data(parquet_dir)
    

if __name__ == "__main__":
    run_etl()