import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS events_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES
# staging tables 
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
       event_id    BIGINT IDENTITY(0,1) NOT NULL,
       artist      VARCHAR              NULL,
       auth        VARCHAR              NULL,
       firstName   VARCHAR              NULL,
       gender      VARCHAR              NULL,
       itemInSession VARCHAR            NULL,
       lastName    VARCHAR              NULL,
       length      VARCHAR              NULL,
       level       VARCHAR              NULL,
       location    VARCHAR              NULL,
       method      VARCHAR              NULL,
       page        VARCHAR              NULL,
       registration VARCHAR             NULL,
       sessionId   INTEGER              NOT NULL SORTKEY DISTKEY,
       song        VARCHAR              NULL,
       status      INTEGER              NULL,
       ts          BIGINT               NOT NULL,
       userAgent   VARCHAR              NULL,
       userId      INTEGER              NULL
       );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INTEGER         NULL,
        artist_id           VARCHAR         NOT NULL SORTKEY DISTKEY,
        artist_latitude     VARCHAR         NULL,
        artist_longitude    VARCHAR         NULL,
        artist_location     VARCHAR         NULL,
        artist_name         VARCHAR         NULL,
        song_id             VARCHAR         NOT NULL,
        title               VARCHAR         NULL,
        duration            NUMERIC         NULL,
        year                INTEGER         NULL
        );
""")

# analytics tables
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0,1)   NOT NULL PRIMARY KEY,
        start_time  TIMESTAMP               NOT NULL,
        user_id     INTEGER(30)             NOT NULL DISTKEY,
        level       VARCHAR(10)             NOT NULL,
        song_id     VARCHAR(30)             NOT NULL,
        artist_id   VARCHAR(30)             NOT NULL,
        session_id  VARCHAR(30)             NOT NULL,
        location    VARCHAR,
        user_agent  VARCHAR
    )
    SORTKEY(songplay_id);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id     INTEGER                 NOT NULL PRIMARY KEY,
        first_name  VARCHAR(50)             NOT NULL,
        last_name   VARCHAR(80)             NOT NULL,
        gender      VARCHAR(10)             NOT NULL,
        level       VARCHAR(10)             NOT NULL
    ) 
    diststyle all
    SORTKEY(user_id);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id     VARCHAR(50)             NOT NULL PRIMARY KEY,
        title       VARCHAR(255)            NOT NULL,
        artist_id   VARCHAR(50)             NOT NULL,
        year        INTEGER                 NOT NULL,
        duration    DECIMAL(9)              NOT NULL
    )
    diststyle all
    SORTKEY(song_id);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id   VARCHAR(50)             NOT NULL PRIMARY KEY,
        name        VARCHAR(255)            NOT NULL,
        location    VARCHAR(255),            
        latitude    DECIMAL(9),            
        longitude   DECIMAL(9) 
    ) 
    SORTKEY(artist_id);
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time TIMESTAMP               PRIMARY KEY,
        hour       INTEGER,
        day        INTEGER,
        week       INTEGER,
        month      INTEGER,
        year       INTEGER,
        weekday    INTEGER
    )
    SORTKEY(start_time)
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    iam_role {}
    region 'us-west-2'
    """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    iam_role {}
    region 'us-west-2'
    """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))
""").format()

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'  AS start_time,
            se.userId      AS user_id,
            se.level       AS level,
            ss.song_id     AS song_id,
            ss.artist_id   AS artist_id,
            se.sessionId   AS session_id,
            se.location    AS location,
            se.userAgent   AS user_agent
    FROM staging_events AS se
    INNER JOIN staging_songs AS ss
        ON (se.artist = ss.artist_name)
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT userId AS user_id,
            firstName       AS first_name,
            lastName        AS last_name,
            gender,
            level
    FROM staging_events AS se
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
        SELECT  DISTINCT song_id, 
                title,
                artist_id,
                year,
                duration
        FROM staging_songs;

""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
    SELECT start_time, 
        EXTRACT(hour FROM start_time)    AS hour,
        EXTRACT(day FROM start_time)     AS day,
        EXTRACT(week FROM start_time)    AS week,
        EXTRACT(month FROM start_time)   AS month,
        EXTRACT(year FROM start_time)    AS year,
        EXTRACT(week FROM start_time)    AS weekday            
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
