# Project 1: Data Modeling with Postgres

## Introduction

In this project, the data engineer creates a database schema and ETL pipeline for a startup called Sparkify who wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Their current data resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app, but there is not an easy way to query data.

## Data Schema 

1. Fact Table
    - songplays: song play data together with user, artist, and song info (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
2. Dimension Tables
    - users: user info (columns: user_id, first_name, last_name, gender, level)
    - songs: song info (columns: song_id, title, artist_id, year, duration)
    - artists: artist info (columns: artist_id, name, location, latitude, longitude)
    - time: detailed time info about song plays (columns: start_time, hour, day, week, month, year, weekday)

## ETL pipeline

1. Create Tables
    - Write CREATE statements in sql_queries.py to create each table.
    - Write DROP statements in sql_queries.py to drop each table if it exists.
2. Run create_tables.py to create the database and tables.
3. Connect database, load json files in etl.ipynb 
4. Write the etl.py based on the process in etl.ipynb
5. Run test.ipynb to confirm the creation of your tables with the correct column that records were successfully inserted into each table. 

## Note
Each steps was run and tested in a jupyter notebook, and the results are examples.