import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
[LOG_DATA, LOG_JSONPATH, SONG_DATA] = config['S3'].values()
ARN = config['IAM_ROLE']['ARN']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# do not include any NOT NULLS in staging tables

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table (
    event_id int IDENTITY(0,1),
    artist text,
    auth text,
    firstName text,
    gender text,
    itemInSession int,
    lastName text,
    length float,
    level text,
    location text,
    method text,
    page text,
    registration varchar(50),
    sessionId int,
    song text,
    status int,
    ts bigint,
    userAgent text,
    userId int,
    PRIMARY KEY (event_id))
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table (
    num_songs int,
    artist_id text,
    artist_latitude float,
    artist_longitude float,
    artist_location text,
    artist_name text,
    song_id text,
    title text,
    duration float,
    year int)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int PRIMARY KEY, 
    start_time date, 
    user_id int, 
    level text, 
    song_id text, 
    artist_id text, 
    session_id int, 
    location text, 
    user_agent text ) 
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY, 
    first_name text, 
    last_name text, 
    gender text, 
    level text) 
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id text PRIMARY KEY, 
    title text, 
    artist_id text, 
    year int, 
    duration float) 
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id text PRIMARY KEY, 
    name text, 
    location text, 
    latitude float, 
    longitude float ) 
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time date, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday text) 
""")

# STAGE TABLES

staging_events_copy = ("""
copy {} from '{}'
credentials 'aws_iam_role={}'
region 'us-west-2'
json '{}';
""").format('staging_events_table', LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy {} from '{}'
credentials 'aws_iam_role={}'
region 'us-west-2'
json 'auto'
""").format('staging_songs_table', SONG_DATA, ARN)

# FILL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
    event.event_id                                          AS songplay_id,
    TIMESTAMP 'epoch' + event.ts/1000 * interval '1 second' AS start_time,
    event.userId                                            AS user_id,
    event.level,
    song.song_id,
    song.artist_id,
    event.sessionId                                         AS session_id,
    event.location,
    event.userAgent                                         AS user_agent
FROM staging_events_table event
JOIN staging_songs_table song ON (event.song = song.title)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    event.userId    AS user_id,
    event.firstName AS first_name,
    event.lastName  AS last_name,
    event.gender,
    event.level
FROM staging_events_table event
WHERE event.page='NextSong'
""") # added where clause to prevent blank user_id events

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song.song_id,
    song.title,
    song.artist_id,
    song.year,
    song.duration
FROM staging_songs_table song
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT DISTINCT
    song.artist_id,
    song.artist_name        AS name,
    song.artist_location    AS location,
    song.artist_latitude    AS latitude,
    song.artist_longitude   AS longitude
FROM staging_songs_table song
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
    start_time,
    EXTRACT(hr from start_time)         AS hour,
    EXTRACT(d from start_time)          AS day,
    EXTRACT(w from start_time)          AS week,
    EXTRACT(mon from start_time)        AS month,
    EXTRACT(yr from start_time)         AS year,
    EXTRACT(weekday from start_time)    AS weekday
FROM (
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time
    FROM staging_events_table
)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
