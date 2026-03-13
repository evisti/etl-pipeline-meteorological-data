import numpy as np
import pandas as pd
from geopy import distance


def date_formatting(df: pd.DataFrame, columns_with_date: list[str]): 
    # format date columns
    df[columns_with_date] = df[columns_with_date].apply(pd.to_datetime)


def drop_duplicates(df: pd.DataFrame, subset_name: str=None):
    if subset_name: 
        df.drop_duplicates(subset=[subset_name], inplace=True)
    else:
        df.drop_duplicates(inplace=True)


def geo_coordinates(df: pd.DataFrame, column_with_coordinates: str):
    # Split column_with_coordinates into two new columns (latitude and longitude), and delete the column_with_coordinates
    df['longitude'] = [coordinate[0] for coordinate in df[column_with_coordinates]]
    df['latitude'] = [coordinate[1] for coordinate in df[column_with_coordinates]]
    df.drop(columns=column_with_coordinates, inplace=True)


def reset_index(df: pd.DataFrame) -> pd.DataFrame:
    df.reset_index(drop=True, inplace=True)
    return df


def transform_stations(df: pd.DataFrame):

    # change date columns dtype to datetime
    columns_with_date = [
        'properties.operationFrom', 
        'properties.operationTo', 
        'properties.created', 
        'properties.validFrom', 
        'properties.validTo'
        ]
    date_formatting(df, columns_with_date)

    # latitude and longitude
    geo_coordinates(df, 'geometry.coordinates')

    # distance to SPAC
    location_spac = { # (lat, lon)
        'ballerup': (55.7341, 12.3890), 
        'aarhus': (56.1554, 10.2114), 
        'odense': (55.4034, 10.3987)
    }
    df['distanceAarhus'] = df.apply(lambda row: distance.distance((row.latitude, row.longitude), location_spac['aarhus']).km, axis = 1)
    df['distanceOdense'] = df.apply(lambda row: distance.distance((row.latitude, row.longitude), location_spac['odense']).km, axis = 1)
    df['distanceBallerup'] = df.apply(lambda row: distance.distance((row.latitude, row.longitude), location_spac['ballerup']).km, axis = 1)

    # delete columns we don't want
    df.drop(columns=['type', 'id', 'geometry.type', 'properties.updated'], inplace=True)

    # rename columns
    df.rename(lambda s: s.replace('properties.', ''), axis="columns", inplace=True)
    df.rename(columns={'parameterId': 'parameters'}, inplace=True)

    # TODO: Replace empty values?

    print('\nStation data info:')
    df.info()


def transform_observations(df: pd.DataFrame):
    
    # change date columns dtype to datetime
    column_with_date = 'properties.observed'
    date_formatting(df, column_with_date)

    # latitude and longitude
    geo_coordinates(df, 'geometry.coordinates')

    # delete columns we don't want
    df.drop(columns=['type', 'id', 'geometry.type', 'properties.created'], inplace=True)

    # rename columns
    df.rename(lambda s: s.replace('properties.', ''), axis="columns", inplace=True)
    df.rename(columns={'parameterId': 'parameter'}, inplace=True)

    # TODO: Replace empty values

    # delete dupllicate rows
    drop_duplicates(df)

    print('\nObservation data info:')
    df.info()
