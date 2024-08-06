# <!-- Copyright (c) 2024. MindGraph Technologies. All rights reserved. -->
# <!-- Proprietary and confidential. Copying and distribution is strictly prohibited. -->

__author__ = "Pradish Pranam"
__copyright__ = "MindGraph"
__version__ = "0.1.0"
__maintainer__ = ["Pradish Pranam", "Mohamed Ashif H"]
__email__ = ["pradishpranam.s@mind-graph.com", "ashif.hm@mind-graph.com"]
__status__ = "Development"
__date__ = "04/Apr/2024"

from fastapi import FastAPI, Depends, Header, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Annotated, Union
import duckdb
from app.ComputePassenger import reframePassengerforathena, reframePassengerforduckdb
from app.ComputeBooker import reframeBookerforduckdb, reframeBookerforathena
from app.Authenticate import authenticate_access_token
import subprocess
import os
from app.LICENSE import check_license
import boto3
from dotenv import load_dotenv
from cachetools import TTLCache
from functools import wraps

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# maximum of 1024 items and a 5-minutes (300 seconds) time-to-live
cache = TTLCache(maxsize=1024, ttl=300)

load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")
ATHENA_SCHEMA_NAME = os.getenv("ATHENA_SCHEMA_NAME")
S3_STAGING_DIR = os.getenv("S3_STAGING_DIR")
USER_ENV = os.getenv("USER_ENV")

if USER_ENV == 'athena':
    athena = boto3.client(
        "athena",
        aws_access_key_id = AWS_ACCESS_KEY,
        aws_secret_access_key = AWS_SECRET_KEY,
        region_name = AWS_REGION
    )
elif USER_ENV == 'local' or USER_ENV == 's3':
    db = duckdb.connect(database=':memory:')

check_license()

async def custom_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"response": exc.detail}
    )

app.exception_handler(HTTPException)(custom_exception_handler)

async def get_access_token(Access_token: Annotated[Union[str, None], Header()] = None):
    return authenticate_access_token(Access_token)

async def fetch_data_from_athena(query: str):
    # Start the query execution
    query_response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_SCHEMA_NAME},
        ResultConfiguration={
                "OutputLocation": S3_STAGING_DIR,
                "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        }
    )

    query_execution_id = query_response['QueryExecutionId']

    # Wait for the query to finish
    while True:
        query_status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        query_state = query_status['QueryExecution']['Status']['State']
        if query_state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

    if query_state == 'SUCCEEDED':
        # Fetch the results
        paginator = athena.get_paginator('get_query_results')
        result_pages = paginator.paginate(QueryExecutionId=query_execution_id)

        values = []
        for results in result_pages:
            column_names = [col['Name'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            for row in results['ResultSet']['Rows'][1:]:  # Skipping the header row
                value = [item.get('VarCharValue', None) for item in row['Data']]
                values.append(value)

        return column_names, values
    else:
        raise HTTPException(status_code=500, detail="Failed to fetch the column names and values")

def cache_result(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        cache_key = f"{func.__name__}:{args}:{kwargs}"
        if cache_key in cache:
            print(f"Cache hit for {cache_key}")
            return cache[cache_key]
        result = await func(*args, **kwargs)
        cache[cache_key] = result
        print(f"Cache miss for {cache_key}")
        return result
    return wrapper

@app.get('/api/health')
async def health_check():
    is_healthy = True
    return ("OK",200) if is_healthy else ("Service Unavailable", 500)

@app.get('/api/milestones')
@cache_result
async def milestones(bool_access_token: Annotated[bool, Depends(get_access_token)]) -> dict:
    if bool_access_token:
        try:
            if USER_ENV == 's3' or USER_ENV == 'local':
                if USER_ENV == 's3':
                    file_path = f's3://{BUCKET_NAME}/data/index_milestone.parquet'
                    print(f"Fetching data from s3: {file_path}")
                    
                elif USER_ENV == 'local':
                    file_path = './data/index_milestone.parquet'
                    print(f"Fetching data from local: {file_path}")
                
                milestone_dict = {key: value[0] for key, value in duckdb.execute(f"SELECT * FROM read_parquet('{file_path}')").fetch_df().to_dict().items()}
                milestone_dict['passengers'] = milestone_dict.pop('customers')  

                return milestone_dict 
            
            elif USER_ENV == 'athena':
                query = f"SELECT * FROM {ATHENA_SCHEMA_NAME}.index_milestone;"
                print(f"Fetching data from Athena: {query}")
                # print(query)

                column_names, values = await fetch_data_from_athena(query)

                value = [int(val) for val in values[0]]

                milestone_dict = dict(zip(column_names, value))
                milestone_dict['passengers'] = milestone_dict.pop('customers') 
            
                return milestone_dict  
              
        except Exception as e:
            raise HTTPException(status_code=404, detail="Query execution failed or was cancelled")
    else:
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.get('/api/profile')
@cache_result
async def profile(profile_type : str,
                  id : str,
                  bool_access_token: Annotated[bool, Depends(get_access_token)]) -> dict:
    if bool_access_token:
        if profile_type == 'passenger':
            passenger_hash = id
            if USER_ENV == 'local' or USER_ENV == 's3': 
                if USER_ENV == 'local':
                    file_path = './data/modified_passenger.parquet'
                elif USER_ENV == 's3':
                    file_path = f's3://{BUCKET_NAME}/data/passenger.parquet'

                passenger_dict = {key:value[0] for key,value in db.execute(f"SELECT * FROM read_parquet('{file_path}') WHERE passenger_hash ='{passenger_hash}'").fetch_df().to_dict().items()}

                passenger_dict = await reframePassengerforduckdb(passenger_dict)
        
                return passenger_dict
            
            if USER_ENV == 'athena':
                try:
                    query = f"SELECT * FROM {ATHENA_SCHEMA_NAME}.passenger_details WHERE passenger_hash = '{passenger_hash}';"

                    column_names, values = await fetch_data_from_athena(query)
                    print("fetch_data_from_athena completed")

                    # Creating a dictionary from column names and values
                    passenger_dict = dict(zip(column_names, values[0]))
                    print("passenger_dict completed")

                    try:
                        passenger_dict = await reframePassengerforathena(passenger_dict)
                    except Exception as e:
                        raise HTTPException(status_code=500, detail="Error processing reframePassengerforathena")
                    
                    return passenger_dict
                except Exception as e:
                    raise HTTPException(status_code=404, detail="Query execution failed or was cancelled")
        
        elif profile_type == 'booker':
            personid = id
            if USER_ENV == 'local' or USER_ENV == 's3':
                if USER_ENV == 'local':
                    file_path = './data/modified_booker.parquet'    
                elif USER_ENV == 's3':
                    file_path = f's3://{BUCKET_NAME}/data/booker.parquet'

                booker_dict = {key:value[0] for key,value in db.execute(f" SELECT * FROM read_parquet('{file_path}') WHERE personid ='{personid}'").fetch_df().to_dict().items()}

                booker_dict = await reframeBookerforduckdb(booker_dict)
                return booker_dict
            
            if USER_ENV == 'athena':
                try:
                    query = f"SELECT * FROM {ATHENA_SCHEMA_NAME}.booker_details WHERE personid = {personid};"
                    print(query)
                
                    column_names, values = await fetch_data_from_athena(query)

                    # Creating a dictionary from column names and values
                    booker_dict = dict(zip(column_names, values[0]))
                
                    try:
                        booker_dict = await reframeBookerforathena(booker_dict)
                    except Exception as e:
                        raise HTTPException(status_code=500, detail="Error processing reframeBookerforathena") 

                    return booker_dict
                except Exception as e:
                    raise HTTPException(status_code=404, detail="Query execution failed or was cancelled")   
        else:
            raise HTTPException(status_code=400, detail="Invalid profile type specified! please specify passenger or booker")
    else:
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.get('/api/profile/search')
@cache_result
async def profile_search(
    profile_type: str,
    bool_access_token: Annotated[bool, Depends(get_access_token)],
    firstname: str = None,
    lastname: str = None,
    phone: str = None,
    email: str = None,
    id: str = None,
    dateofbirth: str = None
) -> dict:
    if not bool_access_token:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        query_string = ""

        if profile_type == "passenger":
            if USER_ENV == 'local' or USER_ENV == 's3':
                if USER_ENV == "local":
                    query_string = f"SELECT passenger_hash,firstname,lastname,phone,emailaddress,dateofbirth FROM read_parquet('./data/modified_passenger.parquet')  "
                elif USER_ENV == "s3":
                    file_path = f"s3://{BUCKET_NAME}/data/passenger.parquet"
                    query_string = f"SELECT passenger_hash,firstname,lastname,phone,emailaddress,dateofbirth FROM read_parquet('{file_path}')  "

            if USER_ENV == "athena":
                query_string = f"SELECT passenger_hash, firstname, lastname, phone, emailaddress, dateofbirth FROM {ATHENA_SCHEMA_NAME}.passenger_details"

        elif profile_type == "booker":
            if USER_ENV == 'local' or USER_ENV == 's3':
                if USER_ENV == "local":
                    query_string = f"SELECT personid, bookerfirstname, bookerlastname, bookermobile, bookeremailaddress FROM read_parquet('./data/modified_booker.parquet')  "
            elif USER_ENV == "s3":
                file_path = f"s3://{BUCKET_NAME}/data/booker.parquet"
                query_string = f"SELECT passenger_hash, firstname, lastname, phone, emailaddress, dateofbirth FROM read_parquet('{file_path}')  "

            if USER_ENV == "athena":
                query_string = f"SELECT personid, bookerfirstname, bookerlastname, bookermobile, bookeremailaddress FROM {ATHENA_SCHEMA_NAME}.booker_details"
        else:
            raise HTTPException(status_code=400, detail="Invalid profile type specified! please specify passenger or booker")

        conditions = []
        if id != None:
            conditions.append(f"passenger_hash = '{id}'" if profile_type == "passenger" else f"personid = {id}")
        if firstname != None:
            conditions.append(f"upper(firstname) LIKE upper('%{firstname}%')" if profile_type == "passenger" else f"upper(bookerfirstname) LIKE upper('%{firstname}%');")
        if lastname != None:
            conditions.append(f"upper(lastname) LIKE upper('%{lastname}%')" if profile_type == "passenger" else f"upper(bookerlastname) LIKE upper('%{lastname}%');")
        if email != None:
            conditions.append(f"emailaddress LIKE '%{email}%'" if profile_type == "passenger" else f"bookeremailaddress LIKE '%{email}%';")
        if phone != None:
            conditions.append(f"phone LIKE '%{phone}%'" if profile_type == "passenger" else f"bookermobile LIKE '%{phone}%';")
        if dateofbirth != None:
            conditions.append(f"dateofbirth = '{dateofbirth}'" if profile_type == "passenger" else "")

        if conditions:
            query_string += " WHERE " + " AND ".join(conditions)
        else:
            query_string += " LIMIT 51;"

        print()
        print(query_string)
        print()

        if USER_ENV == 'athena':
            column_names, values = await fetch_data_from_athena(query_string)

            results = {}
            
            # Iterate through column names
            for col_index, col_name in enumerate(column_names):
                results[col_name] = {}
                
                # Iterate through values and assign them to corresponding column and index
                for row_index, value in enumerate(values):
                    results[col_name][str(row_index)] = int(value[col_index]) if col_name == 'personid' else value[col_index]

            return results
        
        else:
            data_dict = db.execute(query_string).fetch_df().to_dict().items()
            return dict(data_dict)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get('/api/copy_files')
async def copy_files():
    try:
        s3_url = f"s3://{os.getenv('BUCKET_NAME')}/ui/"

        result = subprocess.run(['/usr/local/bin/aws', 's3', 'sync', '--delete', s3_url, '/home/pa-1linuxadmin/profiles/'], capture_output=True, text=True)

        if result.returncode == 0:
            return {'status': 'Files synced successfully'}
        else:
            raise HTTPException(status_code=500, detail=result.stderr.strip())
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
