from flask import Flask, Response, jsonify, request
from flask_cors import CORS
import pandas as pd
import duckdb
import json

from reframe import reframe_booker_dict , reframe_passenger_dict



# passenger_hash = "fd68e562482eef0d8941437ed7ee0e4e"
app = Flask(__name__)
# profile_type = "passenger"

conn = duckdb.connect(database=':memory:')


CORS(app,origins="*")


# CORS(app, resources={r"/*": {"origins": ["http://localhost.tiangolo.com", "https://localhost.tiangolo.com", "http://localhost", "http://localhost:8080"]}, "supports_credentials": True})

@app.route('/milestones', methods = ['GET'])
def milestones():
    conn.execute("DROP TABLE IF EXISTS cdp_milestones")
    conn.execute("CREATE TEMPORARY TABLE cdp_milestones AS SELECT * FROM read_parquet('../profiles/index_milestone/*.parquet')")
    # page = request.args.get('page', default=1, type=int)
    # page_size = 100  # Adjust the page size as needed

    milestone_df = conn.execute("SELECT * FROM cdp_milestones").df()
    milestone_dict = milestone_df.to_dict()
    # return jsonify({"response":"Get request called"})
    milestone_dict = {key:value[0] for key,value in milestone_dict.items() }
    json_data = json.dumps(milestone_dict, sort_keys=False)
    return Response(json_data, content_type='application/json')
    
    
    # offset = (page - 1) * page_size

    # milestone_df = conn.execute(f"SELECT * FROM cdp_milestones LIMIT {page_size} OFFSET {offset}").df()
    # milestone_dict = milestone_df.to_dict()

    # return jsonify(milestone_dict)
    
    
@app.route('/profile/search',methods = ['GET'])
def profile_search():
    profile_type = request.args.get("profile_type")

    if profile_type == "passenger":
        path = "passenger_details"
    elif profile_type == "booker":
        path = "booker_details"

    firstname = request.args.get("firstname")
    lastname = request.args.get("lastname")
    phone = request.args.get("phone")
    email = request.args.get("email")
    id = request.args.get("id")
    

    conn.execute(f"DROP TABLE IF EXISTS cdp_profile")

    
    if profile_type == "passenger":
        query = f"CREATE TEMPORARY TABLE cdp_profile AS SELECT passenger_hash,firstname,lastname,phone,emailaddress FROM read_parquet('../profiles/{path}/*.parquet')  "
    elif profile_type == "booker":
        query = f"CREATE TEMPORARY TABLE cdp_profile AS SELECT personid,bookerfirstname,bookerlastname,bookermobile,bookeremailaddress FROM read_parquet('../profiles/{path}/*.parquet')  "

    count = 0

    if firstname != "None" or lastname != "None" or phone != "None" or email != "None" or id != "None":
        query  += "WHERE "

    

    if id != "None":
        if profile_type == "passenger":
            query += f"passenger_hash = '{id}'"
        elif profile_type == "booker":
            query += f"personid = '{id}'"
        conn.execute(query)
        df = conn.execute(f"SELECT * FROM cdp_profile").df()
        df_dict = df.to_dict()
        # df_dict = {key:value[0] for key,value in df_dict.items() }
        json_data = json.dumps(df_dict, sort_keys=False)
        return Response(json_data, content_type='application/json')
    

    if firstname != "None":
        if profile_type == "passenger":
            query += f"upper(firstname) like upper('%{firstname}%') "
        elif profile_type == "booker":
            query += f"upper(bookerfirstname)like upper('%{firstname}%') "
        count += 1
        print(query)
        print(count)
    
    
    if lastname  != "None":
        if count >= 1:
            query += "and "
        if profile_type == "passenger":
            query += f"upper(lastname) like upper('%{lastname}%') "
        elif profile_type == "booker":
            query += f"upper(bookerlastname) like upper('%{lastname}%') "

    if email  != "None":
        if count >= 1:
            query += "and "
        if profile_type == "passenger":
            query += f"emailaddress like '%{email}%' "
        elif profile_type == "booker":
            query += f"bookeremailaddress like '%{email}%' "

    if phone  != "None":
        if count >= 1:
            query += "and "
        if profile_type == "passenger":
            query += f"phone like '%{phone}%' "
        elif profile_type == "booker":
            query += f"bookermobile '%{phone}%' "
        
    
    query += "limit 51;"
    # print(query)
    
    conn.execute(query)

    df = conn.execute(f"SELECT * FROM cdp_profile").df()
    # print(df.head(5))
    df_dict = df.to_dict()
    # print(df_dict)
    json_data = json.dumps(df_dict, sort_keys=False)
    # print(json_data)
    return Response(json_data, content_type='application/json')
    
    





@app.route('/profile', methods = ['GET'])
def profile():

    profie_type = request.args.get("profile_type")
    
    
    
    if profie_type == 'passenger':
        passenger_hash = request.args.get("id")
        conn.execute(f"DROP TABLE IF EXISTS cdp_passenger_{passenger_hash}")
        conn.execute(f"CREATE TEMPORARY TABLE cdp_passenger_{passenger_hash} AS SELECT * FROM read_parquet('../profiles/passenger_details/*.parquet') WHERE passenger_hash ='{passenger_hash}'")
        passenger_df = conn.execute(f"SELECT * FROM cdp_passenger_{passenger_hash}").df()
        passenger_dict = passenger_df.to_dict()
        passenger_dict = reframe_passenger_dict(passenger_dict)
        passenger_dict["TotalRevenue"] = float(passenger_dict["TotalRevenue"])
        json_data = json.dumps(passenger_dict, sort_keys=False)
        return Response(json_data, content_type='application/json')
    


    elif profie_type == 'booker':
        personid = request.args.get("id")
        conn.execute(f"DROP TABLE IF EXISTS cdp_booker_{personid}")
        conn.execute(f"CREATE TEMPORARY TABLE cdp_booker_{personid} AS SELECT * FROM read_parquet('../profiles/booker_details/*.parquet') WHERE personid ='{personid}'")
        booker_df = conn.execute(f"SELECT * FROM cdp_booker_{personid}").df()
        booker_dict = booker_df.to_dict()
        booker_dict = reframe_booker_dict(booker_dict)
        booker_dict["TotalRevenue"] = float(booker_dict["TotalRevenue"])
        json_data = json.dumps(booker_dict, sort_keys=False)
        return Response(json_data, content_type='application/json')



if __name__ == "__main__":
    app.run(host='0.0.0.0',port=8080)

