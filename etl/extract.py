import requests
import pandas as pd
from datetime import datetime


url_dmi_stations = 'https://opendataapi.dmi.dk/v2/metObs/collections/station/items'
url_dmi_observations = 'https://opendataapi.dmi.dk/v2/metObs/collections/observation/items'


def _fetch_data(url: str, params: dict) -> pd.DataFrame:
    '''
    Submit GET request with url and parameters, and convert result to DataFrame
    '''
    timeout = 61 # seconds
    try:
        response = requests.get(url, params=params, timeout=timeout) #TODO: pørv med stream=True og brug iter_content(chunck_size=somenumber)
        print('Fetching URL:', response.url)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print('Error: HTTP error')
        print(e)
    except requests.exceptions.ReadTimeout:
        print('Error: Request time out')
    except requests.exceptions.ConnectionError:
        print('Error: Connection error')
    except requests.exceptions.RequestException:
        print('Error: Exception request')

    # decode json response
    result = response.json()
    print(*result['links'], sep='\n')

    # convert to DataFrame
    df = pd.json_normalize(result['features'])
    if df.empty:
        print('No data')

    return df


def fetch_stations(station_id: str=None) -> pd.DataFrame:
    # define query parameters for the request
    query_params = {}
    if station_id: query_params['stationId'] = station_id

    return _fetch_data(url_dmi_stations, query_params)


def fetch_observations(parameter: str, station_id: str, start_time: datetime, end_time: datetime) -> pd.DataFrame:
    # define query parameters for the request
    datetime_str = start_time.isoformat() + 'Z/' + end_time.isoformat() + 'Z'
    query_params = {
        'datetime' : datetime_str#,
#        'limit' : '1000'  # maximum number of observations
        }
    if parameter: query_params['parameterId'] = parameter
    if station_id: query_params['stationId'] = station_id

    return _fetch_data(url_dmi_observations, query_params)
