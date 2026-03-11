import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import MetaData, Table, Column, Integer, String, Float, DateTime

from db_connection import SQLRunner, get_engine
from request.request import retrieve_data



# specify desired start and end time
end_time = datetime.today()
start_time = end_time - timedelta(hours=24)

# specify station ID - Aarhus (Ødum 06072), Odense (Årslev 06126), Ballerup (Jægersborg 06181)
station_ids = ['06072', '06126', '06181']

# specify parameter ID
parameter_ids = ['radia_glob', 'wind_speed']

#df1 = retrieve_data(parameter_id='wind_speed', station_id='06072', start_time=start_time, end_time=end_time)
#print(df1.info())




# PostgreSQL connection

config_file = Path('./db_config.ini')
sql_runner = SQLRunner(get_engine(config_file))


# Create table

# create a MetaData object
metadata = MetaData()

# define table
table = Table(
    'raw', metadata,
    Column('id', Integer, primary_key=True),
    Column('time', DateTime, nullable=False),
    Column('parameter', String, nullable=False),
    Column('value', Float, nullable=True),
    Column('station_id', String(5), nullable=False),
    Column('latitude', Float, nullable=False),
    Column('longitude', Float, nullable=False)
)

# create the table in the database
#metadata.create_all(sql_runner.engine)



