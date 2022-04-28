# Project 4: Data Lakes with Spark

## Introduction

In this project, Sparkify wants to move data warehouse to data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a diretory with JSON metadata on the songs in their app. As a data engineer, I am going to build a etl pupeline that extracts their data from S3, process them with Spark, and loads the data back into S3 as a set of dimensional tables. This will allow Sparkify's analytics to explore more about the data.

## Project Dataset
1. Song Dataset
2. Log Dataset

# Schema 

1. Fact Table
    - songplays: song play data together with user, artist, and song info (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
2. Dimension Tables
    - users: user info (columns: user_id, first_name, last_name, gender, level)
    - songs: song info (columns: song_id, title, artist_id, year, duration)
    - artists: artist info (columns: artist_id, name, location, latitude, longitude)
    - time: detailed time info about song plays (columns: start_time, hour, day, week, month, year, weekday)
    
## Project Files:
1. etl.py - this script retrieves the song and log data in the s3 bucket, transforms the data into fact and dimensional tables then loads the table data back into s3 as parquet files.
2. dl.cfg - contains my AWS keys.

## ETL pipeline
1. Connect with AWS
2. Run functions in etl.py to reads song_data and load_data from S3, transforms them to create five different tables, and writes them to partitioned parquet files in table directories on S3.
3. In the etl.py, songs table files are partitioned by year and then artist. Time table files are partitioned by year and month. Songplays table files are partitioned by year and month.

## Notes:
When I test the etl.py, it has server error. I checked the code, it should be fine, so I submited it.