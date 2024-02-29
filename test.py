from fastapi import FastAPI
import duckdb
app = FastAPI()

db = duckdb.connect(database=':memory:')

duckdb.execute("INSTALL httpfs;")
duckdb.execute("LOAD httpfs;")
print("yes")

duckdb.execute("INSTALL aws;")
duckdb.execute("LOAD aws;")


print("yes")

duckdb.execute("CREATE SECRET (TYPE S3,PROVIDER CREDENTIAL_CHAIN,REGION 'ap-southeast-1',ENDPOINT 's3.amazonaws.com',URL_STYLE 'vhost');")
print("yes")



@app.get('/api/milestones')
async def milestones() -> dict:
    
    milestone_dict = duckdb.query("SELECT * FROM read_parquet('s3://cebu-cdp-data-dev/ui/index_milestone/*.parquet')").fetch_df().to_dict()
    milestone_dict['passengers'] = milestone_dict.pop('customers')

    return milestone_dict    
     