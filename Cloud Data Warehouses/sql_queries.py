import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

ARN = config.get("IAM_ROLE","ARN")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS 
staging_events (
    artist varchar(300),
    auth varchar(50), 
    first_Name varchar(100),
    gender varchar(40),
    item_In_Session int,
    last_Name varchar(100),
    length float,
    level varchar(20),
    location varchar(300),
    method varchar(20),
    page varchar(20),
    registration bigint,
    session_Id bigint,
    song varchar(300),
    status varchar(100),
    ts bigint,
    user_Agent varchar(300),
    user_Id int
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS 
staging_songs (
    artist_id varchar(50), 
    artist_latitude float,
    artist_location varchar(300),
    artist_longitude float,
    artist_name varchar(300),
    duration float,
    num_songs int,
    song_id varchar(50),
    title varchar(300), 
    year int
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS 
songplays (
    songplay_id bigint IDENTITY(0,1) primary key, 
    start_time timestamp not null, 
    user_id int, 
    level text, 
    song_id varchar(50), 
    artist_id varchar(50), 
    session_id int, 
    location varchar(300), 
    user_agent varchar(300)
)
distkey (level)
sortkey (start_time);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS 
users (
    user_id int primary key, 
    first_name text, 
    last_name text, 
    gender text, 
    level text
)
distkey (level)
compound sortkey(last_name, first_name)
;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS 
songs (
    song_id varchar(255) primary key, 
    title text, 
    artist_id varchar(255) not null, 
    year int, 
    duration float
)
distkey (year)
sortkey (title)
;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS 
artists (
    artist_id varchar primary key, 
    name varchar, 
    location text, 
    latitude float, 
    longitude float
)
distkey(location)
sortkey(location)
;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS 
times (
    start_time timestamp primary key, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
)
distkey (year)
sortkey (start_time)
;
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from '{}' 
credentials 'aws_iam_role={}'
json '{}'
region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from '{}' 
credentials 'aws_iam_role={}'
json 'auto'
region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays 
        (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
        se.user_id,
        se.level,
        ss.song_id, 
        ss.artist_id,
        se.session_id,
        se.location,
        se.user_agent
    FROM staging_events se 
    JOIN staging_songs ss
        ON ss.artist_name = se.artist
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users 
        (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
        se.user_id, 
        se.first_name,
        se.last_name,
        se.gender,
        se.level
    FROM staging_events se 
    WHERE se.user_id IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs
        (song_id, title, artist_id, year, duration)
    SELECT DISTINCT 
        ss.song_id, 
        ss.title, 
        ss.artist_id,
        ss.year,
        ss.duration
    FROM staging_songs ss
""")

artist_table_insert = ("""
    INSERT INTO artists 
        (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT 
        ss.artist_id,
        ss.artist_name,
        ss.artist_location,
        ss.artist_latitude,
        ss.artist_longitude
    FROM staging_songs ss
""")

time_table_insert = ("""
    INSERT INTO times (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT 
        TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
        EXTRACT(hour FROM start_time),
        EXTRACT(day FROM start_time),
        EXTRACT(week FROM start_time),
        EXTRACT(month FROM start_time),
        EXTRACT(year FROM start_time),
        EXTRACT(week FROM start_time)
    FROM staging_events se
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
