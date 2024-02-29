from fastapi import FastAPI
import duckdb
from app.ComputePassenger import reframePassenger
from app.ComputeBooker import reframeBooker

app = FastAPI()

db = duckdb.connect(database=':memory:')

@app.get('/api/health')
async def health_check():
    is_healthy = True
    return ("OK",200) if is_healthy else ("Service Unavailable",500)

@app.get('/api/milestones')
async def milestones() -> dict:
    try:
        milestone_dict = {key: value[0] for key, value in duckdb.execute("SELECT * FROM read_parquet('./data/index_milestone.parquet')").fetch_df().to_dict().items()}
        milestone_dict['passengers'] = milestone_dict.pop('customers')

        return milestone_dict    
    except:
        return {"error": "An internal error occurred"} 
    

@app.get('/api/profile')
async def profile(profile_type : str ,
                  id : str ) -> dict:
    
    if profile_type == 'passenger':
        passenger_hash = id
        
        passenger_dict = {key:value[0] for key,value in db.execute(f" SELECT * FROM read_parquet('./data/passenger.parquet') WHERE passenger_hash ='{passenger_hash}'").fetch_df().to_dict().items()}
        passenger_dict = await reframePassenger(passenger_dict)
        return passenger_dict
    
    elif profile_type == 'booker':
        personid = id
        booker_dict = {key:value[0] for key,value in db.execute(f" SELECT * FROM read_parquet('./data/booker.parquet') WHERE personid ='{personid}'").fetch_df().to_dict().items()}

        booker_dict = await reframeBooker(booker_dict)
        
        return booker_dict

@app.get('/api/profile/search')
async def profile_search(
    profile_type: str,
    firstname: str = None,
    lastname: str = None,
    phone: str = None,
    email: str = None,
    id: str = None,
    dateofbirth: str = None
) -> dict:

    # print(profile_type,firstname,lastname,phone,email,id,dateofbirth)

    if profile_type == "passenger":
        path = "passenger_details"
    elif profile_type == "booker":
        path = "booker_details"
    
    # print(path)

    if profile_type == "passenger":
            query = f"SELECT passenger_hash,firstname,lastname,phone,emailaddress,dateofbirth FROM read_parquet('./data/passenger.parquet')  "
    elif profile_type == "booker":
        query = f"SELECT personid,bookerfirstname,bookerlastname,bookermobile,bookeremailaddress FROM read_parquet('../profiles/{path}/*.parquet')  "

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
        
        
    if lastname  != None:
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




        



    