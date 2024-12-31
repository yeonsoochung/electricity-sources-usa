import requests
import pandas as pd

api_key = """<My API Key>"""

def get_json(url, params):
    try:
        # Make the request to the EIA API
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors
        json_data = response.json()
        # print(json.dumps(json_data, indent=4))  # Pretty print JSON response
    except requests.exceptions.RequestException as e:
        print("Error fetching data:", e)
    return json_data

def create_csv(url, params, csv_name):
    params = params.copy()
    json_data = get_json(url, params)
    df = pd.DataFrame(json_data["response"]["data"])
    # print(len(json_data["response"]["data"]))
    # counter = 1
    # print("counter:", counter)
    while len(json_data["response"]["data"]) == 5000:
        # print(json_data["response"]["data"][0]["period"] , json_data["response"]["data"][-1]["period"])
        # print(json_data["response"]["data"][0]["plantCode"], json_data["response"]["data"][-1]["plantCode"])
        if json_data["response"]["data"][0]["period"] == json_data["response"]["data"][-1]["period"]:
            params["start"] = json_data["response"]["data"][-1]["period"]
            params["offset"] = 5000
            json_data = get_json(url, params)
            # print(len(json_data["response"]["data"]))
            df_new = pd.DataFrame(json_data["response"]["data"])
            df = pd.concat([df, df_new], axis=0, ignore_index=True)
            params["offset"] = None
            # counter += 1
        else:
            params["start"] = json_data["response"]["data"][-1]["period"]
            json_data = get_json(url, params)
            print(len(json_data["response"]["data"]))
            df_new = pd.DataFrame(json_data["response"]["data"])
            df = pd.concat([df, df_new], axis=0, ignore_index=True)
            # counter += 1
        # print("counter:", counter)
    # print("Counter =", counter) 
    # Remove duplicate rows
    df.drop_duplicates(inplace=True)
    df.to_csv(csv_name, index=False)
    print("Length of data frame:", len(df))
    print(f"{csv_name} created successfully!")
    return


''' 
MIDWEST 
'''
url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["IL", "IN", "IA"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_midwest_1.csv")


url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["KS", "MI", "MN"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_midwest_2.csv")


url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["MO", "NE", "ND"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_midwest_3.csv")


url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["OH", "SD", "WI"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_midwest_4.csv")


url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["IL", "IN", "IA"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_midwest_1.csv")


'''
NORTHEAST
'''
url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["CT", "ME", "MA"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_northeast_1.csv")


url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["NH", "NJ", "NY"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_northeast_2.csv")


url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["PA", "RI", "VT"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_northeast_3.csv")


'''
SOUTH
'''
url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["AL", "AR", "FL"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_south_1.csv")


url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["GA", "KY", "LA"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_south_2.csv")


url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["MS", "NC", "OK"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_south_3.csv")


url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["SC", "TN", "TX"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_south_4.csv")


url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["VA", "DC", "MD"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_south_5.csv")


url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["DE", "WV"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_south_6.csv")


'''
WEST
'''
url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["AZ", "AK", "CO"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_west_1.csv")


url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["ID", "HI", "MT"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_west_2.csv")


url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["NV", "OR", "NM"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_west_3.csv")


url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["UT", "WA", "WY"],
    "start": "2001-01",
    "end": None,
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_west_4.csv")


url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["CA"],
    "start": "2001-01",
    "end": "2015-12",
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_west_5.csv")


url_plants = f"https://api.eia.gov/v2/electricity/facility-fuel/data/?api_key={api_key}"
params_plants = {
    "frequency": "monthly",
    "data[]": [ "consumption-for-eg-btu", "total-consumption-btu", "generation", "gross-generation" ],
    "facets[state][]": ["CA"],
    "start": "2015-12",
    "end": None,
    # "end": "2020-12",
    "sort[0][column]": "period",
    "sort[0][direction]": "asc",
    "offset": "0",
    "length": "5000" }

create_csv(url_plants, params_plants, "elec_power_plants_west_6.csv")




