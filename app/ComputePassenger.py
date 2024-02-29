from app.Functions import fill_space,limit_dict,sort_age,sort_months
import json

async def reframePassenger(passenger_dict):
        TravelOrigin = json.loads(passenger_dict["TravelOrigin"])
        TravelDestination = json.loads(passenger_dict["TravelDestination"])
        TravelSeat = json.loads(passenger_dict["TravelSeat"])
        Details = json.loads(passenger_dict["Details"])
        del passenger_dict["TravelOrigin"],passenger_dict["TravelDestination"],passenger_dict["TravelSeat"],passenger_dict["Details"]
        passenger_dict["TravelOrigin"]= TravelOrigin
        passenger_dict["TravelDestination"]= TravelDestination
        passenger_dict["TravelSeat"]= TravelSeat
        passenger_dict["Details"]= Details
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
        print(type(TravelSeat))    
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