import pandas as pd

from datetime import datetime, timedelta, time
from pathlib import Path
from sqlalchemy import MetaData

from db_connection import SQLRunner, get_engine
from etl.extract import fetch_stations, fetch_observations
from etl.transform import transform_stations, transform_observations
from etl.load import stations_table, observations_table, create_tables, load_to_sql


# EXTRACT

# specify desired start and end time (e.g. the most recent 7:00 AM)
now = datetime.now()
end_time = datetime.combine(now.date(), time(7)) - timedelta(days=now.time() < time(7))
start_time = end_time - timedelta(days=30)

# specify station and parameters for observations
station_id = '06072'
parameter = 'wind_speed'

# fetch data
df_observations = fetch_observations(None, station_id, start_time, end_time)
df_stations = fetch_stations()


# TRANSFORM

transform_stations(df_stations)
transform_observations(df_observations)

 

# LOAD

# connect to database
config_file = Path('./db_config.ini')
sql_runner = SQLRunner(get_engine(config_file))

# create tables
metadata = MetaData()
observations = observations_table(metadata)
stations = stations_table(metadata)
create_tables(sql_runner, metadata)

# load data into database
load_to_sql(sql_runner, table=stations, df=df_stations)
load_to_sql(sql_runner, table=observations, df=df_observations)

