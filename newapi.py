from flask import Flask, Response, g, request, jsonify
from flask_cors import CORS
import pandas as pd
import duckdb
import json


from reframe import reframe_booker_dict, reframe_passenger_dict

app = Flask(__name__)
CORS(app, origins="*")


@app.before_request
def before_request():
    g.db = duckdb.connect(database=':memory:')


@app.teardown_request
def teardown_request(exception=None):
    if hasattr(g, 'db'):
        g.db.close()

    

@app.route('/profile', methods = ['GET'])
def profile():

    profie_type = request.args.get("profile_type")
    
    if profie_type == 'passenger':
        try:
            passenger_hash = request.args.get("id")
            g.db.execute(f"DROP TABLE IF EXISTS cdp_passenger_{passenger_hash}")
            g.db.execute(f"CREATE TEMPORARY TABLE cdp_passenger_{passenger_hash} AS SELECT * FROM read_parquet('../profiles/passenger_details/*.parquet') WHERE passenger_hash ='{passenger_hash}'")
            passenger_df = g.db.execute(f"SELECT * FROM cdp_passenger_{passenger_hash}").df()
            passenger_dict = passenger_df.to_dict()
            passenger_dict = reframe_passenger_dict(passenger_dict)
            passenger_dict["TotalRevenue"] = round(float(passenger_dict["TotalRevenue"]),2)
            for i, k in passenger_dict["Details"].items():
                k["revenue"] = round(float(k["revenue"]),2)
            json_data = json.dumps(passenger_dict, sort_keys=False)
            return Response(json_data, content_type='application/json')
        except Exception as e:
            print(f"Error in passenger-profile route: {e}")
            return jsonify({"error": "An internal error occurred"}), 500
    


    elif profie_type == 'booker':
        try:
            personid = request.args.get("id")
            g.db.execute(f"DROP TABLE IF EXISTS cdp_booker_{personid}")
            g.db.execute(f"CREATE TEMPORARY TABLE cdp_booker_{personid} AS SELECT * FROM read_parquet('../profiles/booker_details/*.parquet') WHERE personid ='{personid}'")
            booker_df = g.db.execute(f"SELECT * FROM cdp_booker_{personid}").df()
            booker_dict = booker_df.to_dict()
            # with open('output.json', 'w') as json_file:
            #     json.dump(booker_dict, json_file, indent=4)
            booker_dict = reframe_booker_dict(booker_dict)
            json_data = json.dumps(booker_dict, sort_keys=False)
            return Response(json_data, content_type='application/json')
        except Exception as e:
            print(f"Error in booker-profile route: {e}")
            return jsonify({"error": "An internal error occurred"}), 500
        
        
@app.route('/milestones', methods=['GET'])
def milestones():
    try:
        g.db.execute("DROP TABLE IF EXISTS cdp_milestones")
        g.db.execute("CREATE TEMPORARY TABLE cdp_milestones AS SELECT * FROM read_parquet('../profiles/index_milestone/*.parquet')")
        milestone_df = g.db.execute("SELECT * FROM cdp_milestones").df()
        milestone_dict = milestone_df.to_dict()
        milestone_dict = {key: value[0] for key, value in milestone_dict.items()}
        milestone_dict["passengers"] = milestone_dict["customers"]
        del milestone_dict["customers"]
        json_data = json.dumps(milestone_dict, sort_keys=False)
        return Response(json_data, content_type='application/json')
    except Exception as e:
        print(f"Error in milestones route: {e}")
        return jsonify({"error": "An internal error occurred"}), 500
    

@app.route('/profile/search',methods = ['GET'])
def profile_search():
    try:

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
        dateofbirth = request.args.get("dateofbirth")
        

        g.db.execute(f"DROP TABLE IF EXISTS cdp_profile")

        
        if profile_type == "passenger":
            query = f"CREATE TEMPORARY TABLE cdp_profile AS SELECT passenger_hash,firstname,lastname,phone,emailaddress,dateofbirth FROM read_parquet('../profiles/{path}/*.parquet')  "
        elif profile_type == "booker":
            query = f"CREATE TEMPORARY TABLE cdp_profile AS SELECT personid,bookerfirstname,bookerlastname,bookermobile,bookeremailaddress FROM read_parquet('../profiles/{path}/*.parquet')  "

        count = 0

        if firstname != "None" or lastname != "None" or phone != "None" or email != "None" or id != "None" or dateofbirth!="None":
            query  += "WHERE "

        

        if id != "None":
            if profile_type == "passenger":
                query += f"passenger_hash = '{id}'"
            elif profile_type == "booker":
                query += f"personid = '{id}'"
            g.db.execute(query)
            df = g.db.execute(f"SELECT * FROM cdp_profile").df()
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
            count += 1

        if email  != "None":
            if count >= 1:
                query += "and "
            if profile_type == "passenger":
                query += f"emailaddress like '%{email}%' "
            elif profile_type == "booker":
                query += f"bookeremailaddress like '%{email}%' "
            count += 1

        if phone  != "None":
            if count >= 1:
                query += "and "
            if profile_type == "passenger":
                query += f"phone like '%{phone}%' "
            elif profile_type == "booker":
                query += f"bookermobile '%{phone}%' "
            count += 1

        if dateofbirth  != "None":

            if count >= 1:
                query += "and "

            if profile_type == "passenger":
                query += f"dateofbirth == '{dateofbirth}' "
            elif profile_type == "booker":
                query += ""
            
        
        query += "limit 51;"
        print(query)
        
        g.db.execute(query)

        df = g.db.execute(f"SELECT * FROM cdp_profile").df()
        # print(df.head(5))
        df_dict = df.to_dict()
        # print(df_dict)
        json_data = json.dumps(df_dict, sort_keys=False)
        # print(json_data)
        return Response(json_data, content_type='application/json')


    except Exception as e:
        print(f"Error in profile's route: {e}")
        return jsonify({"error": "An internal error occurred"}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=8080)