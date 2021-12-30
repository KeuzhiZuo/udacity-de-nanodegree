# Project 3: Data Warehouse

## Introduction

Sparkify has grown their user base and song database and want to move their processes and data onto the cloud. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. In this project, I am building an ETL pipeline that extracts Sparkify's data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

## Data Schema
- Fact Table
1. songplays - records in event data associated with song plays i.e. records with page NextSong
2. songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
- Dimension Tables
1. users - users in the app
    - user_id, first_name, last_name, gender, level
2. songs - songs in music database
    - song_id, title, artist_id, year, duration
3. artists - artists in music database
    - artist_id, name, location, lattitude, longitude
4. time - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday

## ETL pipeline
1. setup the aws redshift, config the data warehouse, cluster, arn and S3 bucket 
2. load data from S3 to staging tables 
3. insert data from staging tabels to analytical tables 
4. test and run the whole process, and validate the data
5. close the cluster 

## Note
Each steps was run and tested in a jupyter notebook, and the results are examples.