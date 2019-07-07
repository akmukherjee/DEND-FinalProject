#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *
from sqlalchemy import create_engine
import io


# -*- coding: utf-8 -*-

def process_movies_file(engine, filepath):
    """  
  
    This function processes the movies file and loads it into the movies table.
    
    Parameters: 
    engine: Engine variable with the currently connected DB
    filepath: The top level directory under which all the file is listed
    
    
    Returns: 
....None
...."""

    # Read in the Movies CSV into a Pandas Dataframe

    df_movies = pd.read_csv(filepath)

    # Drop the Columns which have null values more than 0.1%
    # Dropping the columns not required for analysis

    df_movies = df_movies.drop(['belongs_to_collection','homepage','overview','release_date','tagline','poster_path','overview','runtime','status','production_companies','production_countries','spoken_languages','video','imdb_id','title','genres','adult'],axis=1)
    df_movies = df_movies[df_movies.isnull().sum(axis=1) <= 5]

    # Rename the column to movieid

    df_movies.rename(columns={'id': 'movieId'}, inplace=True)

    # Since the language has a fair number of missing columns, we are replacing it with 'en'

    df_movies.loc[pd.isnull(df_movies['original_language']),
                  ['original_language']] = 'en'

    # Dropping rows where revenue is missing

    df_movies = df_movies[~np.isnan(df_movies.revenue)]

    # converting  individual columns into data appropriate data types and replacing null values with 0

    df_movies.budget = pd.to_numeric(df_movies['budget'],
            errors='coerce').fillna(0)
    df_movies.popularity = pd.to_numeric(df_movies['popularity'],
            errors='coerce').fillna(0)

    # Converting movieId field to a number

    df_movies.movieId = pd.to_numeric(df_movies.movieId)



    # Let us drop the duplicates and keep the first one

    df_movies.drop_duplicates(subset=['movieId'], keep='first',
                              inplace=True)
    df_movies.columns = map(str.lower, df_movies.columns)
    #print (df_movies.head())


    # Writing to the DB

    df_movies.to_sql('movies', engine, if_exists='append', index=False, chunksize=10000)

def process_ratings_file(engine, filepath):
    """  
  
    This function processes the ratings file and loads it into the ratings table.
    
    Parameters: 
    curr: Cursor variable with the currently connected DB
    filepath: The top level directory under which all the file is listed
    
    
    Returns: 
    None
    """

    # Read in the Ratings CSV into a Pandas Dataframe

    df_ratings = pd.read_csv(filepath)

    # Let us drop the duplicates and keep the first one

    df_ratings.drop_duplicates(subset=['userId', 'movieId'], keep='last'
                               , inplace=True)

    # Drop rows with any nulls since we need all the info from the row in the CSV

    df_ratings.dropna()

    # Converting to Timestamp field

    df_ratings['timestamp'] = pd.to_datetime(df_ratings['timestamp'],
            unit='ms')

    # Changing Column Name to map to DB in lowercase

    df_ratings.columns = map(str.lower, df_ratings.columns)

    # Insert the ratings Data into the ratings table

    df_ratings.to_sql('ratings', engine, if_exists='append',
                      index=False, chunksize=10000)


def main():
    """ 
    The function to runs the main function on this module. 
  
    This main function first drops the tables and then creates new tables on the connected DB 
  
    Parameters: 
    None
    
    Returns: 
    None
    """

    engine = \
        create_engine('postgresql://udacity:udacity@127.0.0.1/FinalProject'
                      )

    # Specifying the input directory

    input_dir = '.\\the-movies-dataset'

    # Process the ratings data

    process_ratings_file(engine, filepath=input_dir + '\\ratings.csv')

    # Process the movies data

    process_movies_file(engine, filepath=input_dir
                        + '\\movies_metadata.csv')


if __name__ == '__main__':
    main()
