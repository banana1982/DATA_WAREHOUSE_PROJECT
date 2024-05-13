import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

S3_LOG_DATA = config.get("S3","LOG_DATA")
S3_LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
S3_SONG_DATA = config.get("S3","SONG_DATA")

ROLE_NAME = config.get("IAM_ROLE","ROLE_NAME")
ROLE_ARN = config.get("IAM_ROLE","ROLE_ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR(1),
        itemInSession INT,
        lastName VARCHAR,
        length NUMERIC,
        level VARCHAR,
        location TEXT,
        method VARCHAR,
        page VARCHAR,
        registration NUMERIC,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts TIMESTAMP,
        userAgent TEXT,
        userId NUMERIC
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_location VARCHAR(max),
        artist_longitude FLOAT,
        artist_name VARCHAR(max),
        song_id VARCHAR,
        title VARCHAR(max),
        duration NUMERIC,
        year SMALLINT
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INT NOT NULL,
        level VARCHAR NOT NULL,
        song_id VARCHAR NOT NULL,
        artist_id TEXT NOT NULL,
        session_id BIGINT NOT NULL,
        location VARCHAR NOT NULL,
        user_agent VARCHAR NOT NULL
    )
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR(1) NOT NULL,
        level VARCHAR NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR(max) NOT NULL,
        artist_id VARCHAR NOT NULL,
        year SMALLINT NOT NULL,
        duration REAL NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR(max) NOT NULL,
        location VARCHAR(max),
        latitude FLOAT,
        longitude FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time TIMESTAMP PRIMARY KEY,
        hour SMALLINT NOT NULL,
        day SMALLINT NOT NULL,
        week SMALLINT NOT NULL,
        month SMALLINT NOT NULL,
        year SMALLINT NOT NULL,
        weekday SMALLINT NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {} 
    credentials 'aws_iam_role={}'
    JSON {} 
    region 'us-west-2'
    timeformat as 'epochmillisecs'
""").format(S3_LOG_DATA, ROLE_ARN, S3_LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {} 
    credentials 'aws_iam_role={}'
    JSON 'auto' region 'us-west-2'
""").format(S3_SONG_DATA, ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT
            DISTINCT(se.ts) AS start_time,
            se.userId AS user_id,
            se.level AS level,
            ss.song_id AS song_id,
            ss.artist_id AS artist_id,
            se.sessionId AS session_id,
            se.location AS location,
            se.userAgent AS user_agent
        FROM staging_events se 
        JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name)
            WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT
            DISTINCT(se.userId) AS user_id,
            se.firstName AS first_name,
            se.lastName AS last_name,
            se.gender AS gender,
            se.level AS level
        FROM staging_events se 
            WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT
            DISTINCT (ss.song_id) AS song_id,
            ss.title AS title,
            ss.artist_id AS artist_id,
            ss.year AS year,
            ss.duration AS duration
        FROM staging_songs ss;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT
            DISTINCT(ss.artist_id) AS artist_id,
            ss.artist_name AS name,
            ss.artist_location AS location,
            ss.artist_latitude AS latitude,
            ss.artist_longitude AS longitude
        FROM staging_songs ss;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT
            DISTINCT(sp.start_time) AS start_time,
            EXTRACT(hour FROM sp.start_time) AS hour,
            EXTRACT(day FROM sp.start_time) AS day,
            EXTRACT(week FROM sp.start_time) AS week,
            EXTRACT(month FROM sp.start_time) AS month,
            EXTRACT(year FROM sp.start_time) AS year,
            EXTRACT(dayofweek FROM sp.start_time) AS weekday
        FROM songplays sp;
""")

# QUERY COUNT TABLES
count_table_staging_events = "SELECT COUNT(*) FROM staging_events"
count_table_staging_songs = "SELECT COUNT(*) FROM staging_songs"
count_table_songplays = "SELECT COUNT(*) FROM songplays"
count_table_users = "SELECT COUNT(*) FROM users"
count_table_songs = "SELECT COUNT(*) FROM songs"
count_table_artists = "SELECT COUNT(*) FROM artists"
count_table_time = "SELECT COUNT(*) FROM time"

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
count_tables_queries = [count_table_staging_events, count_table_staging_songs, count_table_songplays, count_table_users, count_table_songs, count_table_artists, count_table_time]

