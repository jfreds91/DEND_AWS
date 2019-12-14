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
# staging songs:
# {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table (
    event_id int IDENTITY(0,1),
    artist text,
    auth text,
    firstName text,
    gender text,
    itemInSession varchar(50),
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
    start_time date NOT NULL, 
    user_id int NOT NULL, 
    level text NOT NULL, 
    song_id text, 
    artist_id text, 
    session_id int NOT NULL, 
    location text NOT NULL, 
    user_agent text NOT NULL) 
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY, 
    first_name text NOT NULL, 
    last_name text NOT NULL, 
    gender text NOT NULL, 
    level text NOT NULL) 
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id text PRIMARY KEY, 
    title text NOT NULL, 
    artist_id text NOT NULL, 
    year int NOT NULL, 
    duration float NOT NULL) 
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id text PRIMARY KEY, 
    name text NOT NULL, 
    location text NOT NULL, 
    latitude float NOT NULL, 
    longitude float NOT NULL) 
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
""").format('staging_songs_table', LOG_DATA, ARN)

# FILL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
