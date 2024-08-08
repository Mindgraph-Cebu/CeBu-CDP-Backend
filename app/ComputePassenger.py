from app.Functions import fill_space,limit_dict,sort_age,sort_months
import json

async def reframePassengerforathena(passenger_dict):
        
        print("reframePassenger starts!")
        TravelOrigin = json.loads(passenger_dict["travelorigin"])
        TravelDestination = json.loads(passenger_dict["traveldestination"])
        TravelSeat = json.loads(passenger_dict["travelseat"])

        del passenger_dict["travelorigin"],passenger_dict["traveldestination"],passenger_dict["travelseat"]

        passenger_dict["travelorigin"]= TravelOrigin
        passenger_dict["traveldestination"]= TravelDestination
        passenger_dict["travelseat"]= TravelSeat

        if "details1" and "details2" in passenger_dict:
            Details1 = json.loads(passenger_dict["details1"])
            Details2 = json.loads(passenger_dict["details2"]) if passenger_dict["details2"] is not None else {}
            del passenger_dict["details1"], passenger_dict["details2"]
            passenger_dict["details"] = {**Details1, **Details2}

        elif "details" in passenger_dict:
            Details = json.loads(passenger_dict["details"])
            del passenger_dict["details"]
            passenger_dict["details"]= Details

        print("json converted!")

        mylist = ["totalbookings", "bookingmonth_separator_", "bookingcurrency_separator_", "bookingchannel_separator_", "travelbaggage_separator_", "travelinsurance_separator_", "travelmeals_separator_", "travelsoloorgroup_separator_", "isregistered_separator_"]

        # Check if keys in mylist are substrings of keys in passenger_dict
        for key, values in passenger_dict.items():
             for substring in mylist:
                  if substring in key:
                        # Convert the values to integers and update the dictionary
                        passenger_dict[key] = int(values)
        
        print("int converted!")

        Months = {key[23:]: value for key, value in passenger_dict.items() if "bookingmonth_separator_" in key and value > 0}
        BookingChannel = {key[25:]: value for key, value in passenger_dict.items() if "bookingchannel_separator_" in key and value > 0}
        TravelBaggage = {key[24:]: value for key, value in passenger_dict.items() if "travelbaggage_separator_" in key and value > 0}
        TravelInsurance = {key[26:]: value for key, value in passenger_dict.items() if "travelinsurance_separator_" in key and value > 0}

        passenger_dict = {key: value for key, value in passenger_dict.items() if "bookingmonth_separator_" not in key and "travelbaggage_separator_" not in key and "bookingchannel_separator_" not in key and "bookingcurrency_separator_" not in key and "travelinsurance_separator_" not in key}
        
        passenger_dict["months"]=Months
        passenger_dict["bookingchannel"]=BookingChannel
        passenger_dict["travelbaggage"]=TravelBaggage
        passenger_dict["travelinsurance"]=TravelBaggage
        passenger_dict["travelinsurance"]=TravelInsurance

        TravelSeat = passenger_dict.get("travelseat", {})
        TravelSeat = dict(sorted(TravelSeat.items(), key=lambda item: item[1], reverse=True))
        # print(TravelSeat)

        TravelSeat = {("Unknown" if "\u0000" in key else key): value for key, value in TravelSeat.items()}
        # print(type(TravelSeat))    
        print("TravelSeat")

        TravelOrigin = passenger_dict.get("travelorigin", {})
        TravelOrigin = dict(sorted(TravelOrigin.items(), key=lambda item: item[1], reverse=True))
        print("TravelOrigin")

        TravelDestination = passenger_dict.get("traveldestination", {})
        TravelDestination = dict(sorted(TravelDestination.items(), key=lambda item: item[1], reverse=True))
        del passenger_dict["travelseat"], passenger_dict["travelorigin"], passenger_dict["traveldestination"]

        passenger_dict["travelseat"]=TravelSeat
        passenger_dict["travelorigin"]=TravelOrigin
        passenger_dict["traveldestination"]=TravelDestination

        # print("functions started")

        passenger_dict["months"] = await sort_months(passenger_dict["months"])

        passenger_dict["travelbaggage"]=await fill_space(passenger_dict["travelbaggage"])
        passenger_dict["travelinsurance"]=await fill_space(passenger_dict["travelinsurance"])

        # print("functions end")

        # passenger_dict["Details"] = dict(sorted(passenger_dict["Details"].items(), key=lambda item: (item[1]["travel_date"] if item[1]["travel_date"] != "Unknown" else "9999-12-31", item[0])))
        passenger_dict["details"] = dict(enumerate(sorted(passenger_dict["details"].values(), key=lambda item: (item["travel_date"] if item["travel_date"] != "Unknown" else "9999-12-31", item["booking_date"]), reverse=True)))

        # print("Details updated")

        passenger_dict["totalrevenue"] = round(float(passenger_dict["totalrevenue"]),2)
        # del passenger_dict["totalrevenue"]

        # print("TotalRevenue updated")

        for i, k in passenger_dict["details"].items():
            k["revenue"] = round(float(k["revenue"]),2)
            
        return passenger_dict

async def reframePassengerforduckdb(passenger_dict):
        TravelOrigin = json.loads(passenger_dict["TravelOrigin"])
        TravelDestination = json.loads(passenger_dict["TravelDestination"])
        TravelSeat = json.loads(passenger_dict["TravelSeat"])
        Details = json.loads(passenger_dict["Details"])
        # Details1 = json.loads(passenger_dict["Details1"])
        # Details2 = json.loads(passenger_dict["Details2"])

        del passenger_dict["TravelOrigin"],passenger_dict["TravelDestination"],passenger_dict["TravelSeat"],passenger_dict["Details"]
        # del passenger_dict["Details1"], passenger_dict["Details2"]
        passenger_dict["TravelOrigin"]= TravelOrigin
        passenger_dict["TravelDestination"]= TravelDestination
        passenger_dict["TravelSeat"]= TravelSeat
        passenger_dict["Details"]= Details

        # Merge details1 and details2 into details
        # passenger_dict["Details"]= {**Details1, **Details2}
        
        Months = {key[23:]:value for key,value in passenger_dict.items() if "BookingMonth" in key and value > 0}
        BookingChannel = {key[25:]:value for key,value in passenger_dict.items() if "BookingChannel" in key and value > 0}
        TravelBaggage = {key[23:]:value for key,value in passenger_dict.items() if "TravelBaggage" in key and value > 0}
        TravelInsurance = {key[26:]:value for key,value in passenger_dict.items() if "TravelInsurance" in key and value > 0}
        passenger_dict = {key:value for key,value in passenger_dict.items() if "BookingMonth" not in key and "TravelBaggage" not in key and "BookingChannel" not in key and "BookingCurrency" not in key and "TravelInsurance" not in key}
        passenger_dict["Months"]=Months
        passenger_dict["BookingChannel"]=BookingChannel
        passenger_dict["TravelBaggage"]=TravelBaggage
        passenger_dict["TravelInsurance"]=TravelBaggage
        passenger_dict["TravelInsurance"]=TravelInsurance
        TravelSeat = passenger_dict.get("TravelSeat", {})
        TravelSeat = dict(sorted(TravelSeat.items(), key=lambda item: item[1], reverse=True))
        # print(TravelSeat)

        TravelSeat = {("Unknown" if "\u0000" in key else key): value for key, value in TravelSeat.items()}
        # print(type(TravelSeat))    
        # print(TravelSeat)
        TravelOrigin = passenger_dict.get("TravelOrigin", {})
        TravelOrigin = dict(sorted(TravelOrigin.items(), key=lambda item: item[1], reverse=True))

        TravelDestination = passenger_dict.get("TravelDestination", {})
        TravelDestination = dict(sorted(TravelDestination.items(), key=lambda item: item[1], reverse=True))
        del passenger_dict["TravelSeat"], passenger_dict["TravelOrigin"], passenger_dict["TravelDestination"]

        passenger_dict["TravelSeat"]=TravelSeat
        passenger_dict["TravelOrigin"]=TravelOrigin
        passenger_dict["TravelDestination"]=TravelDestination

        passenger_dict["Months"] = await sort_months(passenger_dict["Months"])

        passenger_dict["TravelBaggage"]=await fill_space(passenger_dict["TravelBaggage"])
        passenger_dict["TravelInsurance"]=await fill_space(passenger_dict["TravelInsurance"])
        # passenger_dict["Details"] = dict(sorted(passenger_dict["Details"].items(), key=lambda item: (item[1]["travel_date"] if item[1]["travel_date"] != "Unknown" else "9999-12-31", item[0])))
        passenger_dict["Details"] = dict(enumerate(sorted(passenger_dict["Details"].values(), key=lambda item: (item["travel_date"] if item["travel_date"] != "Unknown" else "9999-12-31", item["booking_date"]), reverse=True)))

        passenger_dict["TotalRevenue"] = round(float(passenger_dict["TotalRevenue"]),2)
        for i, k in passenger_dict["Details"].items():
            k["revenue"] = round(float(k["revenue"]),2)
        return passenger_dict