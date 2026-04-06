import polars as pl
from datetime import datetime
def transform_data(parquet_dir):
    lf = pl.scan_parquet(parquet_dir)

    cols_string = ["name", "domain", "country", "industry", "linkedin_url", "locality"]
    cols_int = ["year_founded","current_employee_estimate", "total_employee_estimate"]

    lf = lf.with_columns([
        pl.col(cols_int).fill_null(0).cast(pl.Int32),
        pl.col(cols_string).fill_null("unknown").str.strip_chars().str.to_lowercase()
    ])

    lf = lf.with_columns([
        pl.when((pl.col("year_founded") > datetime.now().year) | pl.col("year_founded") < 1800)
        .then(0)
        .otherwise(pl.col("year_founded"))
        .alias("year_founded")
    ])

    return lf