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

drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]

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
