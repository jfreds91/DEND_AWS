import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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
    artist text,
    auth text NOT NULL,
    firstName text NOT NULL,
    itemInSession int NOT NULL,
    lastName text NOT NULL,
    length float,
    level text NOT NULL,
    location text NOT NULL,
    method text NOT NULL,
    page text NOT NULL,
    registration text,
    sessionId int NOT NULL,
    song text,
    status int NOT NULL,
    ts int NOT NULL,
    userAgent text,
    userId int)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table (
    num_songs int NOT NULL,
    artist_id text NOT NULL,
    artist_latitude float,
    artist_longitude float,
    artist_location text,
    artist_name text NOT NULL,
    song_id text NOT NULL,
    title text NOT NULL,
    duration float NOT NULL,
    year int NOT NULL)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(0,1) PRIMARY KEY, 
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
""").format()

staging_songs_copy = ("""
""").format()

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
