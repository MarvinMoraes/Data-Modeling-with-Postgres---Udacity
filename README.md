Project :
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The data is in JSON logs in a directory.

This project implements a database schema and ETL pipeline to help the analysis of the songplay data. I use Postgres as DBMS and Python to run the ETL.

Schema
Fact and dimension tables were defined for a star schema with an analytic focus.

Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables
users - users in the app user_id, first_name, last_name, gender, level

songs - songs in music database song_id, title, artist_id, year, duration

artists - artists in music database artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday

Database creation script
A script for creating and recreating the target database is provided for easy editions. Just run ´python create_tables.py´

ETL Notebook
A Jupiter notebook is provided to document and demonstrate the ETL pipeline, step by step.

ETL Script
An ETL script automatically loops through the logs and songs directories, transforms the data using Python/Pandas, and inserts it on the star-schema with relationships, where appropriate.


How it works:

sql_queries.py: The SQL queries to build the tables, insert rows and a SELECT query to find song_id and artist_id based on the song name, artist name and song length.

create_tables.py: connects to the database and builds a sparkify database. After created the database , execute the SQL queries in sql_queries.py to build tables in the schema.

etl.py: processes the JSON files containing song, artist, user and songplay data into the tables. It also uses pandas library to perform some aspects of the ETL

etl.ipynb: implements the same script as etl.py.

test.ipynb: To test the contents.







