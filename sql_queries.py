""" 
    This file contains the functions to create  and drop the ratings and movies tables.
    Additionally, the create and drop queries are added to a list to be iteratively executed.
"""
# DROP TABLES
"""
SQL String to drop the individual tables are listed below
"""

#CREATE TABLES

ratings_table_drop = "DROP TABLE IF EXISTS ratings;"
movies_table_drop = "DROP TABLE IF EXISTS movies;"

ratings_table_create = ("""
CREATE TABLE ratings (Id SERIAL,  userId BIGINT, movieId BIGINT, rating REAL , timestamp TIMESTAMP);
""")

movies_table_create = ("""
CREATE TABLE movies (movieId BIGINT PRIMARY KEY,  budget BIGINT, original_language VARCHAR(255), original_title TEXT, popularity NUMERIC, revenue NUMERIC, vote_average NUMERIC, vote_count NUMERIC);
""")
# QUERY LISTS

create_table_queries = [ratings_table_create,movies_table_create]
drop_table_queries = [ratings_table_drop,movies_table_drop]

#INSERT TABLES
ratings_table_insert = ("""
INSERT into ratings (userId,movieId,rating,timestamp) VALUES (%s,%s,%s,%s)
""")
movies_table_insert = ("""
INSERT into movies (budget,movieId,original_language,original_title,popularity,revenue,vote_average,vote_count) VALUES (%s,%s,%s,%s,%s,%s,%s)
""")