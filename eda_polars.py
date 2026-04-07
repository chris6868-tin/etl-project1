import polars as pl

lf = pl.scan_parquet("data/companies_sorted.parquet")

'''
analysis = lf.select([
    pl.len().alias("total_rows"),
    pl.all().null_count().suffix("_null_count")

]).collect()

print(analysis)
'''
for col, dtype in lf.schema.items():
    print(f"{col}: {dtype}")