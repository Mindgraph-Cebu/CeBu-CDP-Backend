from fastapi import FastAPI, Depends, Header, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Annotated, Union
import duckdb
from app.ComputePassenger import reframePassenger
from app.ComputeBooker import reframeBooker
from app.Authenticate import authenticate_access_token
import subprocess
import json
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db = duckdb.connect(database=':memory:')


async def custom_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"response": exc.detail}
    )

app.exception_handler(HTTPException)(custom_exception_handler)

async def get_access_token(Access_token: Annotated[Union[str, None], Header()] = None):
    return authenticate_access_token(Access_token)

@app.get('/api/health')
async def health_check():
    is_healthy = True
    return ("OK",200) if is_healthy else ("Service Unavailable",500)

@app.get('/api/milestones')
async def milestones(bool_access_token: Annotated[bool, Depends(get_access_token)]) -> dict:
    if bool_access_token:
        try:
            milestone_dict = {key: value[0] for key, value in duckdb.execute(f"SELECT * FROM read_parquet('../profiles/index_milestone/*.parquet')").fetch_df().to_dict().items()}
            milestone_dict['passengers'] = milestone_dict.pop('customers')  
            return milestone_dict    
        except Exception as e:
            return {"error": f"An internal error occurred: {str(e)}"}  
      

@app.get('/api/profile')
async def profile(profile_type : str ,
                  id : str,
                  bool_access_token: Annotated[bool, Depends(get_access_token)]) -> dict:
    if bool_access_token:
        if profile_type == 'passenger':             
            passenger_hash = id
            passenger_dict = {key:value[0] for key,value in db.execute(f"SELECT * FROM read_parquet('../profiles/passenger_details/*.parquet') WHERE passenger_hash ='{passenger_hash}'").fetch_df().to_dict().items()}
            passenger_dict = await reframePassenger(passenger_dict)
            return passenger_dict
    
        elif profile_type == 'booker':
            personid = id
            booker_dict = {key:value[0] for key,value in db.execute(f" SELECT * FROM read_parquet('../profiles/booker_details/*.parquet') WHERE personid ='{personid}'").fetch_df().to_dict().items()}
            booker_dict = await reframeBooker(booker_dict)
            return booker_dict
        
    else:
        return {"error": "Unable to get access token"} 

@app.get('/api/profile/search')
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
    if bool_access_token:
        if profile_type == "passenger":
            
            query = f"SELECT passenger_hash,firstname,lastname,phone,emailaddress,dateofbirth FROM read_parquet('../profiles/passenger_details/*.parquet')  "


        elif profile_type == "booker":
           
            query = f"SELECT personid,bookerfirstname,bookerlastname,bookermobile,bookeremailaddress FROM read_parquet('../profiles/booker_details/*.parquet')  "
            

        # print(query)
        count = 0

        if firstname != None or lastname != None or phone != None or email != None or id != None or dateofbirth!=None:
            query  += "WHERE "
        # print(query)
        if id != None:

            if profile_type == "passenger":
                query += f"passenger_hash = '{id}'"
            elif profile_type == "booker":
                query += f"personid = '{id}'"
            profile_dict = db.execute(query).fetch_df().to_dict()
            return profile_dict
    
        # print(query)
        if firstname != None:
            if profile_type == "passenger":
                query += f"upper(firstname) like upper('%{firstname}%') "
                
            elif profile_type == "booker":
                query += f"upper(bookerfirstname)like upper('%{firstname}%') "
            count += 1
        
        # print(query)
        # print(count)
            
        if lastname != None:
            if count >= 1:
                query += "and "
            if profile_type == "passenger":
                query += f"upper(lastname) like upper('%{lastname}%') "
            elif profile_type == "booker":
                query += f"upper(bookerlastname) like upper('%{lastname}%') "
            count += 1
        # print(query)
        if email  != None:
            if count >= 1:
                query += "and "
            if profile_type == "passenger":
                query += f"emailaddress like '%{email}%' "
            elif profile_type == "booker":
                query += f"bookeremailaddress like '%{email}%' "
            count += 1
        # print(query)
        if phone  != None:
            if count >= 1:
                query += "and "
            if profile_type == "passenger":
                query += f"phone like '%{phone}%' "
            elif profile_type == "booker":
                query += f"bookermobile '%{phone}%' "
            count += 1
        # print(query)
        if dateofbirth  != None:

            if count >= 1:
                query += "and "

            if profile_type == "passenger":
                query += f"dateofbirth == '{dateofbirth}' "
            elif profile_type == "booker":
                query += ""
                 
        query += "limit 51;"
        # print(query)
        
        data_dict = db.execute(query).fetch_df().to_dict().items()

        return dict(data_dict)
    
    else:
        return {"error": "Unable to get access token"}

@app.get('/api/copy_files')
async def copy_files():
    try:
        s3_url = f"s3://{os.getenv('BUCKET_NAME')}/ui/"

        result = subprocess.run(['/usr/local/bin/aws', 's3', 'sync', '--delete', s3_url, '/home/pa-1linuxadmin/profiles/'], capture_output=True, text=True)

        if result.returncode == 0:
            return {'status': 'Files synced successfully'}
        else:
            return {'error': result.stderr.strip()}
        
    except Exception as e:
        return {'error': str(e)}
