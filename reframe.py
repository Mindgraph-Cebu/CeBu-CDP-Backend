import json
import calendar
import re

def reframe_booker_dict(booker_dict):
        booker_dict = {key:value[0] for key,value in booker_dict.items() }
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
        booker_dict["BookingChannel"]=BookingChannel
        booker_dict["TravelBaggage"]=TravelBaggage
        booker_dict["TravelInsurance"]=TravelBaggage
        booker_dict["TravelInsurance"]=TravelInsurance
        # tb = {}
        # for key,value in booker_dict["TravelBaggage"].items():
        #     check = key.split("_")
        #     toprint = ""
        #     for element in check :
        #         if "space" not in element:
        #             toprint+=element
                    
        #     tb[toprint] = value
        # del booker_dict["TravelBaggage"]
        # booker_dict["TravelBaggage"] = tb
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
        booker_dict["TravelSeats"] = limit_dict(TravelSeat, 1000)
        i = len(booker_dict["TravelSeats"])
        print(i)
        booker_dict["TravelOrigin"] = limit_dict(TravelOrigin, 1000)
        booker_dict["TravelDestination"] = limit_dict(TravelDestination, 1000)
        booker_dict["Months"] = sort_months(booker_dict["Months"])
        booker_dict["AgeRange"] = sort_age(booker_dict["AgeRange"])

        booker_dict["TravelBaggage"]=fill_space(booker_dict["TravelBaggage"])
        booker_dict["TravelInsurance"]=fill_space(booker_dict["TravelInsurance"])
        booker_dict["Details"] = dict(sorted(booker_dict["Details"].items(), key=lambda item: (item[1]["travel_date"] if item[1]["travel_date"] != "Unknown" else "9999-12-31", item[0])))
        

        return booker_dict


def reframe_passenger_dict(passenger_dict):
        passenger_dict = {key:value[0] for key,value in passenger_dict.items() }
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
        # tb = {}
        # for key,value in passenger_dict["TravelBaggage"].items():
        #     check = key.split("_")
        #     toprint = ""
        #     for element in check :
        #         if "space" not in element:
        #             toprint+=element
                    
        #     tb[toprint] = value
        # del passenger_dict["TravelBaggage"]

        # with open('Insurance.json','w') as file:
        #      json.dump(passenger_dict["TravelInsurance"],file)
        # passenger_dict["TravelBaggage"] = tb

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

        passenger_dict["Months"] = sort_months(passenger_dict["Months"])

        
        passenger_dict["TravelBaggage"]=fill_space(passenger_dict["TravelBaggage"])
        passenger_dict["TravelInsurance"]=fill_space(passenger_dict["TravelInsurance"])
        passenger_dict["Details"] = dict(sorted(passenger_dict["Details"].items(), key=lambda item: (item[1]["travel_date"] if item[1]["travel_date"] != "Unknown" else "9999-12-31", item[0])))
        return passenger_dict


def limit_dict(original_dict, limit):
    limited_dict = {key: original_dict[key] for key in list(original_dict.keys())[:limit]}
    return limited_dict



def sort_months(months_dict):
     month_indices = {month: index for index, month in enumerate(calendar.month_name) if month}
     sorted_data = dict(sorted(months_dict.items(), key=lambda item: month_indices[item[0]]))   
     return sorted_data


def sort_age(age_dict):
     age_group_order = [
    "1_to_10", "11_to_20", "21_to_30", "31_to_40", "41_to_50",
    "51_to_60", "61_to_70", "71_to_80", "81_to_90", "91_to_100", "Unspecified"]

    # Sort the original dictionary based on the custom order
     sorted_age_range_dict = dict(sorted(age_dict.items(), key=lambda item: age_group_order.index(item[0])))

     return sorted_age_range_dict


# def fill_space(space_dict):
#      for key in space_dict:
#         new_key = key.replace("_space1_", " ").replace("_space2_", " ").replace("_space3_", " ").replace("_space4_", " ").replace("_space5_", " ").replace("_space6_", " ").replace("_space7_", " ").replace("_", "")
#         new_key = re.sub(r'\s+', ' ', new_key).strip()
#         space_dict[new_key] = space_dict.pop(key)

#      return space_dict
     

def fill_space(space_dict):
    new_space_dict = {}
    for key, value in space_dict.items():
        new_key = key.replace("_space1_", " ").replace("_space2_", " ").replace("_space3_", " ").replace("_space4_", " ").replace("_space5_", " ").replace("_space6_", " ").replace("_space7_", " ").replace("_", "")
        new_key = re.sub(r'\s+', ' ', new_key).strip()
        new_space_dict[new_key] = value

    return new_space_dict
