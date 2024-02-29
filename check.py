import duckdb

db = duckdb.connect(':memory:')

duckdb.execute("INSTALL httpfs;")
duckdb.execute("LOAD httpfs;")
print("yes")

duckdb.execute("INSTALL aws;")
duckdb.execute("LOAD aws;")


print("yes")

duckdb.execute("CREATE SECRET (TYPE S3,PROVIDER CREDENTIAL_CHAIN,REGION 'ap-southeast-1',ENDPOINT 's3.amazonaws.com',URL_STYLE 'vhost');")
print("yes")

a = duckdb.query("SELECT count(*) FROM read_parquet('s3://cebu-cdp-data-dev/ui/test/passenger.parquet')")
print(a)