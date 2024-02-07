from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import Response, JSONResponse
import duckdb
import json
from reframe import reframe_booker_dict, reframe_passenger_dict

app = FastAPI()

def get_db():
    # Synchronous connection to DuckDB
    connection = duckdb.connect(database=':memory:')
    try:
        yield connection
    finally:
        connection.close()

@app.get('/api/milestones')
async def milestones(db: any = Depends(get_db)):  # Use 'any' type temporarily
    try:
        db.execute("DROP TABLE IF EXISTS cdp_milestones")
        db.execute("CREATE TEMPORARY TABLE cdp_milestones AS SELECT * FROM read_parquet('../profiles/index_milestone/*.parquet')")
        milestone_df = db.execute("SELECT * FROM cdp_milestones").df()
        milestone_dict = milestone_df.to_dict()
        milestone_dict = {key: value[0] for key, value in milestone_dict.items()}
        milestone_dict["passengers"] = milestone_dict["customers"]
        del milestone_dict["customers"]
        json_data = json.dumps(milestone_dict, sort_keys=False)
        return Response(content=json_data, media_type='application/json')  # Use media_type instead of content_type
    except Exception as e:
        print(f"Error in milestones route: {e}")
        return JSONResponse({"error": "An internal error occurred"}), 500

@app.get('/api/health')
async def health_check():
    is_healthy = True
    return JSONResponse(content={"status": "OK"} if is_healthy else {"status": "Service Unavailable"}, status_code=200 if is_healthy else 500)
