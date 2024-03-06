from fastapi import FastAPI, Depends, Header
from typing import Annotated, Union
import duckdb
from app.ComputePassenger import reframePassenger
from app.ComputeBooker import reframeBooker
from app.auth import authenticate_access_token
import subprocess
import json

app = FastAPI()

db = duckdb.connect(database=':memory:')

with open('config.json', 'r') as config_file:
    config_data = json.load(config_file)

user_env = config_data.get('env')
bucket_name = config_data.get('bucket-name')

if user_env == 's3':
    duckdb.execute("INSTALL httpfs;")
    duckdb.execute("LOAD httpfs;")

    duckdb.execute("INSTALL aws;")
    duckdb.execute("LOAD aws;")

    duckdb.execute("CREATE SECRET (TYPE S3,PROVIDER CREDENTIAL_CHAIN,REGION 'ap-southeast-1',ENDPOINT 's3.amazonaws.com',URL_STYLE 'vhost');")

async def get_access_token(access_token: Annotated[Union[str, None], Header()] = None):
    return authenticate_access_token(access_token)

@app.get('/api/health')
async def health_check():
    is_healthy = True
    return ("OK",200) if is_healthy else ("Service Unavailable",500)

@app.get('/api/milestones')
async def milestones(bool_access_token: Annotated[bool, Depends(get_access_token)]) -> dict:
    if bool_access_token:
        try:
            if user_env == 'local':
                file_path = './data/index_milestone.parquet'
            elif user_env == 's3':
                file_path = f's3://{bucket_name}/data/index_milestone.parquet'
            else:
                return {"error": "Invalid environment specified. Please provide local or s3"} 

            milestone_dict = {key: value[0] for key, value in duckdb.execute(f"SELECT * FROM read_parquet('{file_path}')").fetch_df().to_dict().items()}
            milestone_dict['passengers'] = milestone_dict.pop('customers')  
            return milestone_dict    
        
        except Exception as e:
            return {"error": f"An internal error occurred: {str(e)}"}  
    else:
        return {"error": "Unable to get access token"}  


@app.get('/api/profile')
async def profile(profile_type : str ,
                  id : str,
                  bool_access_token: Annotated[bool, Depends(get_access_token)]) -> dict:
    if bool_access_token:
        if profile_type == 'passenger':
            if user_env == 'local':
                file_path = './data/passenger.parquet'
            elif user_env == 's3':
                file_path = f's3://{bucket_name}/data/passenger.parquet'
            else:
                return {"error": "Invalid environment specified. Please provide local or s3"}
             
            passenger_hash = id
            passenger_dict = {key:value[0] for key,value in db.execute(f"SELECT * FROM read_parquet('{file_path}') WHERE passenger_hash ='{passenger_hash}'").fetch_df().to_dict().items()}
            passenger_dict = await reframePassenger(passenger_dict)
            return passenger_dict
    
        elif profile_type == 'booker':
            if user_env == 'local':
                file_path = './data/booker.parquet'
            elif user_env == 's3':
                file_path = f's3://{bucket_name}/data/booker.parquet'
            else:
                return {"error": "Invalid environment specified. Please provide local or s3"}
             
            personid = id
            booker_dict = {key:value[0] for key,value in db.execute(f" SELECT * FROM read_parquet('{file_path}') WHERE personid ='{personid}'").fetch_df().to_dict().items()}
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
            path = "passenger_details"
            if user_env == "local":
                query = f"SELECT passenger_hash,firstname,lastname,phone,emailaddress,dateofbirth FROM read_parquet('./data/passenger.parquet')  "
            elif user_env == "s3":
                file_path = f"s3://{bucket_name}/data/passenger.parquet"
                query = f"SELECT passenger_hash,firstname,lastname,phone,emailaddress,dateofbirth FROM read_parquet('{file_path}')  "
            else:
                return {"error": "Invalid environment specified. Please provide local or s3"}

        elif profile_type == "booker":
            path = "booker_details"
            if user_env == "local":
                query = f"SELECT personid,bookerfirstname,bookerlastname,bookermobile,bookeremailaddress FROM read_parquet('../profiles/{path}/*.parquet')  "
            elif user_env == "s3":
                file_path = f"s3://{bucket_name}/profiles/{path}/*.parquet"
                query = f"SELECT passenger_hash,firstname,lastname,phone,emailaddress,dateofbirth FROM read_parquet('{file_path}')  "
            else:
                return {"error": "Invalid environment specified. Please provide local or s3"}

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
        s3_url = f"s3://{bucket_name}/ui/"

        result = subprocess.run(['/usr/local/bin/aws', 's3', 'sync', '--delete', s3_url, '/home/pa-1linuxadmin/profiles/'], capture_output=True, text=True)

        if result.returncode == 0:
            return {'message': 'Files synced successfully from S3 to local directory!'}
        else:
            return {'error': result.stderr.strip()}
        
    except Exception as e:
        return {'error': str(e)}
