import re
import calendar



async def limit_dict(original_dict, limit):
    limited_dict = {key: original_dict[key] for key in list(original_dict.keys())[:limit]}
    return limited_dict



async def sort_months(months_dict):
     month_indices = {month: index for index, month in enumerate(calendar.month_name) if month}
     sorted_data = dict(sorted(months_dict.items(), key=lambda item: month_indices[item[0]]))   
     return sorted_data


async def sort_age(age_dict):
     age_group_order = [
    "1_to_10", "11_to_20", "21_to_30", "31_to_40", "41_to_50",
    "51_to_60", "61_to_70", "71_to_80", "81_to_90", "91_to_100", "Unspecified"]

    
     sorted_age_range_dict = dict(sorted(age_dict.items(), key=lambda item: age_group_order.index(item[0])))

     return sorted_age_range_dict


async def fill_space(space_dict):
    new_space_dict = {}
    for key, value in space_dict.items():
        new_key = key.replace("_space1_", " ").replace("_space2_", " ").replace("_space3_", " ").replace("_space4_", " ").replace("_space5_", " ").replace("_space6_", " ").replace("_space7_", " ").replace("_", "")
        new_key = re.sub(r'\s+', ' ', new_key).strip()
        new_space_dict[new_key] = value

    return new_space_dict