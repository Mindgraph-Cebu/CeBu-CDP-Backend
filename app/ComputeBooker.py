from app.Functions import fill_space,limit_dict,sort_age,sort_months
import json

async def reframeBooker(booker_dict):
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
        print(type(TravelSeat))
        
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