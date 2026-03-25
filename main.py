import ast
import pandas as pd

from datetime import datetime, timedelta
from glob import glob
from pathlib import Path
from sqlalchemy import MetaData

from db_connection import SQLRunner, get_engine
from helper_functions import save_to_csv
from etl.extract import get_stations, get_observations, get_spac
from etl.transform import clean_stations, clean_observations, clean_spac
from etl.load import stations_table, observations_table, spac_table, create_tables, load_to_sql


url_dmi = 'https://opendataapi.dmi.dk/v2/metObs/collections'
url_spac = 'https://climate.spac.dk/api/records'


def extract_single():
    # specify desired start and end time
    start_time = datetime(2025, 1, 1)
    end_time = datetime(2026, 1, 1)

    # specify station and parameters for dmi observations
    station_id = '23105'
    parameter = None

    # fetch data
    df_observations = get_observations(url_dmi, parameter, station_id, start_time, end_time)

    print(df_observations[df_observations['properties.value'] != 0])

    save_to_csv(
        df_observations, 
        save_folder='data/2025_Aarhus_raw', 
        prefix=f'1_{station_id}', 
        from_time=start_time, 
        to_time=end_time)


def extract_multiple():
    # Extract all observations from 2025 and save to csv files

    start_time = datetime(2025, 1, 1)
    station_id = '06072'
    parameter = None # all parameters

    i = 1
    while True:
        end_time = start_time + timedelta(days=10)
        df = get_observations(url_dmi, parameter, station_id, start_time, end_time)

        # save as csv
        save_folder = 'data/2025_Aarhus_raw'
        save_to_csv(df, save_folder, prefix=f'{i}_{station_id}', from_time=start_time, to_time=end_time) #TODO: save as json instead of csv
        if end_time.year == 2026:
            break

        start_time = end_time
        i += 1

def transform_multiple():
    save_folder = 'data/2025_Aarhus_raw'
    files = glob(f'{save_folder}/*.csv')

    dfs = []
    for f in files:
        print(f)
        df = pd.read_csv(Path(f), dtype={'properties.stationId': str})
        df['geometry.coordinates'] = df['geometry.coordinates'].apply(lambda x: ast.literal_eval(x))
        clean_observations(df)
        dfs.append(df)
    
    df = pd.concat(dfs, axis='rows')
    df.reset_index(drop=True, inplace=True)
    save_to_csv(df, 'data', 'Aarhus_06072_23105', datetime(2025, 1, 1), datetime(2026, 1, 1))

    return dfs





url_dmi = 'https://opendataapi.dmi.dk/v2/metObs/collections'
url_spac = 'https://climate.spac.dk/api/records'

def etl_example():
    ######### EXTRACT #########

    # specify desired start and end time
    end_time = datetime.now()
    start_time = end_time - timedelta(days=1)

    # specify station and parameters for dmi observations
    station_id = '06072'
    parameter = 'wind_speed'

    # fetch data
    df_stations = get_stations(url_dmi)
    df_observations = get_observations(url_dmi, parameter, station_id, start_time, end_time)
    df_spac = get_spac(url_spac)

    ######### TRANSFORM #########

    clean_stations(df_stations)
    clean_observations(df_observations)
    clean_spac(df_spac)

    ######### LOAD #########

    # connect to database
    config_file = Path('./db_config.ini')
    sql_runner = SQLRunner(get_engine(config_file))

    # create metadata object
    metadata = MetaData()

    # define tables
    stations = stations_table(metadata, name='stations_test')
    observations = observations_table(metadata, name='observations_test')
    spac = spac_table(metadata, name='spac_test')

    # create tables in database
    create_tables(sql_runner, metadata)

    # load data into database
    load_to_sql(sql_runner, table=stations, df=df_stations)
    load_to_sql(sql_runner, table=observations, df=df_observations)
    load_to_sql(sql_runner, table=spac, df=df_spac)


etl_example()