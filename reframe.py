import json

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
        TravelBaggage = {key:value for key,value in booker_dict.items() if "TravelBaggage" in key and value > 0}
        TravelInsurance = {key[26:]:value for key,value in booker_dict.items() if "TravelInsurance" in key and value > 0}
        AgeRange = {key[19:]:value for key,value in booker_dict.items() if "AgeRange" in key and value > 0}
        booker_dict = {key:value for key,value in booker_dict.items() if "BookingMonth" not in key and "TravelBaggage" not in key and "BookingChannel" not in key and "BookingCurrency" not in key and "TravelInsurance" not in key and "AgeRange" not in key}
        booker_dict["Months"]=Months
        booker_dict["AgeRange"]=AgeRange
        booker_dict["BookingChannel"]=BookingChannel
        booker_dict["TravelBaggage"]=TravelBaggage
        booker_dict["TravelInsurance"]=TravelBaggage
        booker_dict["TravelInsurance"]=TravelInsurance

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
        TravelBaggage = {key:value for key,value in passenger_dict.items() if "TravelBaggage" in key and value > 0}
        TravelInsurance = {key[26:]:value for key,value in passenger_dict.items() if "TravelInsurance" in key and value > 0}
        passenger_dict = {key:value for key,value in passenger_dict.items() if "BookingMonth" not in key and "TravelBaggage" not in key and "BookingChannel" not in key and "BookingCurrency" not in key and "TravelInsurance" not in key}
        passenger_dict["Months"]=Months
        passenger_dict["BookingChannel"]=BookingChannel
        passenger_dict["TravelBaggage"]=TravelBaggage
        passenger_dict["TravelInsurance"]=TravelBaggage
        passenger_dict["TravelInsurance"]=TravelInsurance
        return passenger_dict

