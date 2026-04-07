import polars as pl
from datetime import datetime
def transform_data(parquet_dir):

    current_year = datetime.now().year
    lf = pl.scan_parquet(parquet_dir)

    actual_columns = [col for col in lf.columns if col.strip() != ""]
    lf = lf.select(actual_columns)

    lf = lf.rename({col: col.strip().lower().replace(" ", "_") for col in lf.columns})

    cols_string = ["name", "domain", "linkedin_url", "locality"]
    cols_categorical = ["country", "industry", "size_range"]
    cols_int = ["year_founded","current_employee_estimate", "total_employee_estimate"]

    lf = lf.with_columns([
        pl.col(cols_int).fill_null(0).cast(pl.Int32),
        pl.col(cols_string).fill_null("unknown").str.strip_chars().str.to_lowercase(),
        pl.col(cols_categorical).fill_null("unknown").str.strip_chars().str.to_lowercase().cast(pl.Categorical)
    ])

    lf = lf.with_columns([
        pl.when((pl.col("year_founded") > current_year) | (pl.col("year_founded") < 1800))
        .then(0)
        .otherwise(pl.col("year_founded"))
        .alias("year_founded")
    ])


    return lf
