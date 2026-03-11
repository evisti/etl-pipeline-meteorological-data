import requests
import pandas as pd
from datetime import datetime, timedelta


dmi_url = 'https://opendataapi.dmi.dk/v2/metObs/collections/observation/items'

# specify desired start and end time
start_time = datetime(2022, 1, 1)
end_time = datetime(2022, 1, 15)

# specify station ID - Aarhus (Ødum 06072), Odense (Årslev 06126), Ballerup (Jægersborg 06181)
station_id = ['06072', '06126', '06181']

# specify parameter ID
parameter_id = ['radia_glob', 'wind_speed']


def retrieve_data(parameter_id: str, station_id: str, start_time: datetime, end_time: datetime) -> pd.DataFrame:
    datetime_str = start_time.isoformat() + 'Z/' + end_time.isoformat() + 'Z'
    query_params = {
        'datetime' : datetime_str,
        'limit' : '300000'  # max limit
        }
    if parameter_id: query_params['parameterId'] = parameter_id
    if station_id: query_params['stationId'] = station_id
    
    # submit GET request with url and parameters
    r = requests.get(dmi_url, params=query_params)
    print('Request URL:', r.url)
    #print(r.status_code == requests.codes.ok)
    r.raise_for_status()

    # extract json object
    json = r.json()

    # convert json object to a DataFrame
    df = pd.json_normalize(json['features'])

    if not df.empty:
        df['time'] = pd.to_datetime(df['properties.observed'])

        df['longitude'] = [coordinate[0] for coordinate in df['geometry.coordinates']]
        df['latitude'] = [coordinate[1] for coordinate in df['geometry.coordinates']]

        # drop other columns
        df = df[['time', 'properties.parameterId', 'properties.value', 'properties.stationId', 'latitude', 'longitude']]
        
        # rename columns
        df.columns = [c.replace('properties.', '') for c in df.columns]
        
        # drop duplicate rows
        df = df.drop_duplicates()

    return df



