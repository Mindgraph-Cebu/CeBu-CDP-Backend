from app.Functions import fill_space,limit_dict,sort_age,sort_months
import json

async def reframeBookerforathena(booker_dict):
        # print("reframeBooker starts")
        TravelOrigin = json.loads(booker_dict["travelorigin"])
        TravelDestination = json.loads(booker_dict["traveldestination"])
        TravelSeat = json.loads(booker_dict["travelseat"])
        Details = json.loads(booker_dict["details"])
        del booker_dict["travelorigin"],booker_dict["traveldestination"],booker_dict["travelseat"],booker_dict["details"]
        booker_dict["travelorigin"]= TravelOrigin
        booker_dict["traveldestination"]= TravelDestination
        booker_dict["travelseat"]= TravelSeat
        booker_dict["details"]= Details
        # print("loaded as json!")

        mylist = ["personid", "totalpassengers", "uniqueclients", "bookingmonth_separator_", "travelmeals_separator_", "bookingchannel_separator_", "isemployee_separator_", "travelbaggage_separator_", 
          "gender_separator_", "isemployeedependent_separator_", "travelinsurance_separator_", "travelsoloorgroup_separator_",
          "isregistered_separator_", "bookingcurrency_separator_", "agerange_separator_"]
        
        # Check if keys in mylist are substrings of keys in passenger_dict
        for key, values in booker_dict.items():
            for substring in mylist:
                if substring in key:
                    # Convert the values to integers and update the dictionary
                    booker_dict[key] = int(values)

     #    print("int converted!")

        Months = {key[23:]:value for key,value in booker_dict.items() if "bookingmonth" in key and value > 0}
        BookingChannel = {key[25:]:value for key,value in booker_dict.items() if "bookingchannel" in key and value > 0}
        TravelBaggage = {key[24:]:value for key,value in booker_dict.items() if "travelbaggage" in key and value > 0}
        TravelInsurance = {key[26:]:value for key,value in booker_dict.items() if "travelinsurance" in key}
        AgeRange = {key[19:]:value for key,value in booker_dict.items() if "agerange" in key and value > 0}

        booker_dict["months"]=Months
        booker_dict["agerange"]=AgeRange
        booker_dict["bookingchannel"]=BookingChannel
        booker_dict["travelbaggage"]=TravelBaggage
        booker_dict["travelinsurance"]=TravelBaggage
        booker_dict["traveltnsurance"]=TravelInsurance
        
        if "above100" in booker_dict["agerange"]:
             booker_dict["agerange"]["unspecified"] = booker_dict["agerange"]["above100"]
             del booker_dict["agerange"]["above100"]

        booker_dict["totaltevenue"] = round(float(booker_dict["totalrevenue"]), 2)

        for i, k in booker_dict["Details"].items():
             k["revenue"] = round(float(k["revenue"]))

        TravelSeat = booker_dict.get("travelseat", {})
        TravelSeat = dict(sorted(TravelSeat.items(), key=lambda item: item[1], reverse=True))
        # print(TravelSeat)

        TravelSeat = {("Unknown" if "\u0000" in key else key): value for key, value in TravelSeat.items()}
     #    print(type(TravelSeat))
        
        # print(TravelSeat)
        TravelOrigin = booker_dict.get("travelorigin", {})
        TravelOrigin = dict(sorted(TravelOrigin.items(), key=lambda item: item[1], reverse=True))

        TravelDestination = booker_dict.get("traveldestination", {})
        TravelDestination = dict(sorted(TravelDestination.items(), key=lambda item: item[1], reverse=True))
        del booker_dict["travelseat"], booker_dict["travelorigin"], booker_dict["traveldestination"]
        booker_dict["travelseat"] = await  limit_dict(TravelSeat, 1000)
        # i = len(booker_dict["TravelSeat"])
        # print(i)
        booker_dict["travelorigin"] = await limit_dict(TravelOrigin, 1000)
        booker_dict["traveldestination"] =await limit_dict(TravelDestination, 1000)
        booker_dict["months"] =await sort_months(booker_dict["months"])
        booker_dict["agerange"] =await sort_age(booker_dict["agerange"])

        booker_dict["travelbaggage"]=await fill_space(booker_dict["travelbaggage"])
        booker_dict["travelinsurance"]=await fill_space(booker_dict["travelinsurance"])
        
        booker_dict["details"] = dict(enumerate(sorted(booker_dict["details"].values(), key=lambda item: (item["travel_date"] if item["travel_date"] != "Unknown" else "9999-12-31", item["booking_date"]), reverse=True)))


        return booker_dict

async def reframeBookerforduckdb(booker_dict):
        TravelOrigin = json.loads(booker_dict["TravelOrigin"])
        TravelDestination = json.loads(booker_dict["TravelDestination"])
        TravelSeat = json.loads(booker_dict["TravelSeat"])
        Details = json.loads(booker_dict["Details"])
        del booker_dict["TravelOrigin"],booker_dict["TravelDestination"],booker_dict["TravelSeat"],booker_dict["Details"]
        booker_dict["TravelOrigin"]= TravelOrigin
        booker_dict["TravelDestination"]= TravelDestination
        booker_dict["TravelSeat"]= TravelSeat
        booker_dict["Details"]= Details
        Months = {key[23:]:value for key,value in booker_dict.items() if "BookingMonth" in key and value > 0}
        BookingChannel = {key[25:]:value for key,value in booker_dict.items() if "BookingChannel" in key and value > 0}
        TravelBaggage = {key[23:]:value for key,value in booker_dict.items() if "TravelBaggage" in key and value > 0}
        TravelInsurance = {key[26:]:value for key,value in booker_dict.items() if "TravelInsurance" in key and value > 0}
        AgeRange = {key[19:]:value for key,value in booker_dict.items() if "AgeRange" in key and value > 0}
        booker_dict = {key:value for key,value in booker_dict.items() if "BookingMonth" not in key and "TravelBaggage" not in key and "BookingChannel" not in key and "BookingCurrency" not in key and "TravelInsurance" not in key and "AgeRange" not in key}
        booker_dict["Months"]=Months
        booker_dict["AgeRange"]=AgeRange
        # print(booker_dict["AgeRange"])
        booker_dict["BookingChannel"]=BookingChannel
        booker_dict["TravelBaggage"]=TravelBaggage
        booker_dict["TravelInsurance"]=TravelBaggage
        booker_dict["TravelInsurance"]=TravelInsurance

        if "Above100" in booker_dict["AgeRange"]:
            booker_dict["AgeRange"]["Unspecified"] = booker_dict["AgeRange"]["Above100"]
            del booker_dict["AgeRange"]["Above100"]

        booker_dict["TotalRevenue"] = round(float(booker_dict["TotalRevenue"]),2)
        for i, k in booker_dict["Details"].items():
            k["revenue"] = round(float(k["revenue"]))

        TravelSeat = booker_dict.get("TravelSeat", {})
        TravelSeat = dict(sorted(TravelSeat.items(), key=lambda item: item[1], reverse=True))
        # print(TravelSeat)

        TravelSeat = {("Unknown" if "\u0000" in key else key): value for key, value in TravelSeat.items()}
        # print(type(TravelSeat))
        
        # print(TravelSeat)
        TravelOrigin = booker_dict.get("TravelOrigin", {})
        TravelOrigin = dict(sorted(TravelOrigin.items(), key=lambda item: item[1], reverse=True))

        TravelDestination = booker_dict.get("TravelDestination", {})
        TravelDestination = dict(sorted(TravelDestination.items(), key=lambda item: item[1], reverse=True))
        del booker_dict["TravelSeat"], booker_dict["TravelOrigin"], booker_dict["TravelDestination"]
        booker_dict["TravelSeat"] = await  limit_dict(TravelSeat, 1000)
        # i = len(booker_dict["TravelSeat"])
        # print(i)
        booker_dict["TravelOrigin"] = await limit_dict(TravelOrigin, 1000)
        booker_dict["TravelDestination"] =await limit_dict(TravelDestination, 1000)
        booker_dict["Months"] =await sort_months(booker_dict["Months"])
        booker_dict["AgeRange"] =await sort_age(booker_dict["AgeRange"])

        booker_dict["TravelBaggage"]=await fill_space(booker_dict["TravelBaggage"])
        booker_dict["TravelInsurance"]=await fill_space(booker_dict["TravelInsurance"])
        
        booker_dict["Details"] = dict(enumerate(sorted(booker_dict["Details"].values(), key=lambda item: (item["travel_date"] if item["travel_date"] != "Unknown" else "9999-12-31", item["booking_date"]), reverse=True)))


        return booker_dict