import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR, 
itemInSession INTEGER,
lastName VARCHAR,
length FLOAT,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration FLOAT,
sessionId INTEGER,
song VARCHAR,
status INTEGER,
ts BIGINT,
userAgent VARCHAR,
userId INTEGER)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
num_songs INTEGER,
artist_id VARCHAR,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR,
artist_name VARCHAR, 
song_id VARCHAR,
title VARCHAR,
duration FLOAT,
year INTEGER)
""")



user_table_create = (""" CREATE TABLE IF NOT EXISTS users (
user_id INTEGER PRIMARY KEY SORTKEY,
first_name VARCHAR NOT NULL,
last_name VARCHAR NOT NULL,
gender VARCHAR NOT NULL,
level VARCHAR NOT NULL
)
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs(
song_id VARCHAR PRIMARY KEY SORTKEY,
title VARCHAR NOT NULL,
artist_id VARCHAR NOT NULL,
year INTEGER,
duration FLOAT )
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (
artist_id VARCHAR PRIMARY KEY SORTKEY,
artist_name VARCHAR NOT NULL,
location VARCHAR,
latitude NUMERIC,
longitude NUMERIC)
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY DISTKEY,
hour INTEGER NOT NULL, 
day INTEGER NOT NULL,
week INTEGER NOT NULL,
month INTEGER NOT NULL,
year INTEGER NOT NULL,
weekday INTEGER NOT NULL)
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays ( 
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
start_time TIMESTAMP NOT NULL DISTKEY SORTKEY,
user_id INTEGER NOT NULL ,
level VARCHAR NOT NULL,
song_id VARCHAR NOT NULL ,
artist_id VARCHAR NOT NULL ,
session_id INTEGER,
location VARCHAR,
user_agent VARCHAR)
""")


# STAGING TABLES
staging_events_copy = ("""copy staging_events from {}
iam_role {} json {}
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""copy staging_songs from {}
iam_role {} json 'auto'
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time,
e.userId as user_id, 
e.level, 
s.song_id, 
s.artist_id, 
e.sessionId as session_id, 
e.location, 
e.userAgent as user_agent
FROM staging_events e 
JOIN staging_songs s ON s.artist_name = e.artist 
WHERE page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT (userId) as user_id, 
firstName as first_name, 
lastName as last_name, 
gender, 
level 
FROM staging_events WHERE page = 'NextSong'
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT (song_id), 
title, 
artist_id, 
year, 
duration
FROM staging_songs
""")


artist_table_insert = ("""INSERT INTO artists (artist_id, artist_name, location, latitude, longitude)

SELECT DISTINCT (artist_id) , 
artist_name, 
artist_location as location, 
artist_latitude as latitude, 
artist_longitude as longitude
FROM staging_songs
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT  DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
EXTRACT(hour FROM start_time)    AS hour,
EXTRACT(day FROM start_time)     AS day,
EXTRACT(week FROM start_time)    AS week,
EXTRACT(month FROM start_time)   AS month,
EXTRACT(year FROM start_time)    AS year,
EXTRACT(week FROM start_time)    AS weekday
FROM staging_events 
WHERE page = 'NextSong'

""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
