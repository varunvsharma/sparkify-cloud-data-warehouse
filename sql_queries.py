import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= """CREATE TABLE IF NOT EXISTS staging_events (artist varchar,
                                                                           auth varchar,
                                                                           first_name varchar,
                                                                           gender varchar,
                                                                           item_in_session int,
                                                                           last_name varchar,
                                                                           length float,
                                                                           level varchar,
                                                                           location varchar,
                                                                           method varchar,
                                                                           page varchar,
                                                                           registration float,
                                                                           session_id int,
                                                                           song varchar,
                                                                           status int,
                                                                           ts timestamp,
                                                                           user_agent varchar,
                                                                           user_id int)"""

staging_songs_table_create = """CREATE TABLE IF NOT EXISTS staging_songs (num_songs int,
                                                                          artist_id varchar(18),
                                                                          artist_latitude float,
                                                                          artist_longitude float,
                                                                          artist_location varchar,
                                                                          artist_name varchar,
                                                                          song_id varchar(18),
                                                                          title varchar,
                                                                          duration float,
                                                                          year int)"""

songplay_table_create = """CREATE TABLE IF NOT EXISTS songplay (songplay_id int IDENTITY(0,1),
                                                                start_time timestamp NOT NULL,
                                                                user_id int NOT NULL,
                                                                level varchar,
                                                                song_id varchar(18) NOT NULL DISTKEY,
                                                                artist_id varchar(18) NOT NULL,
                                                                session_id int,
                                                                location varchar,
                                                                user_agent varchar,
                                                                PRIMARY KEY (songplay_id))
                            DISTSTYLE KEY"""

user_table_create = """CREATE TABLE IF NOT EXISTS users (user_id int,
                                                        first_name varchar,
                                                        last_name varchar,
                                                        gender varchar,
                                                        level varchar,
                                                        PRIMARY KEY (user_id))
                       DISTSTYLE AUTO"""

song_table_create = """CREATE TABLE IF NOT EXISTS songs (song_id varchar(18) DISTKEY,
                                                        title varchar,
                                                        artist_id varchar(18) NOT NULL,
                                                        year int,
                                                        duration float,
                                                        PRIMARY KEY (song_id))
                       DISTSTYLE KEY"""

artist_table_create = """CREATE TABLE IF NOT EXISTS artists (artist_id varchar(18),
                                                            name varchar,
                                                            location varchar,
                                                            latitude float,
                                                            longitude float,
                                                            PRIMARY KEY (artist_id))
                         DISTSTYLE AUTO"""

time_table_create = """CREATE TABLE IF NOT EXISTS time (start_time timestamp,
                                                        hour int,
                                                        day int,
                                                        week int,
                                                        month int,
                                                        year int,
                                                        weekday int,
                                                        PRIMARY KEY (start_time))
                        DISTSTYLE AUTO"""

# STAGING TABLES

staging_events_copy = """COPY staging_events
                         FROM {}
                         iam_role {}
                         json {}
                         TIMEFORMAT 'epochmillisecs'
                         """.format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = """COPY staging_songs
                        FROM {}
                        iam_role {}
                        json 'auto'
                        """.format(SONG_DATA, ARN)

# QUERY LISTS

drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create]

copy_table_queries = [staging_events_copy,
                      staging_songs_copy]
