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

# FINAL TABLES

songplay_table_insert = """CREATE TEMP TABLE stage (start_time timestamp,
                                                    user_id int,
                                                    level varchar,
                                                    song_id varchar(18),
                                                    artist_id varchar(18),
                                                    session_id int,
                                                    location varchar,
                                                    user_agent varchar);

                           INSERT INTO stage (start_time,
                                              user_id,
                                              level,
                                              song_id,
                                              artist_id,
                                              session_id,
                                              location,
                                              user_agent)
                           SELECT ts,
                                  user_id,
                                  level,
                                  song_id,
                                  artist_id,
                                  session_id,
                                  location,
                                  user_agent
                           FROM staging_events
                           JOIN staging_songs
                           ON staging_events.song = staging_songs.title;

                           BEGIN TRANSACTION;

                           UPDATE songplay
                           SET user_id = stage.user_id,
                               level = stage.level,
                               song_id = stage.song_id,
                               artist_id = stage.artist_id,
                               session_id = stage.session_id,
                               location = stage.location,
                               user_agent = stage.user_agent
                           FROM stage
                           WHERE songplay.start_time = stage.start_time
                                 AND songplay.user_id = stage.user_id
                                 AND songplay.level = stage.level
                                 AND songplay.song_id = stage.song_id
                                 AND songplay.artist_id = stage.artist_id
                                 AND songplay.session_id = stage.session_id
                                 AND songplay.location = stage.location
                                 AND songplay.user_agent = stage.user_agent;

                           DELETE FROM stage
                           USING songplay
                           WHERE stage.start_time = songplay.start_time
                                 AND stage.user_id = songplay.user_id
                                 AND stage.level = songplay.level
                                 AND stage.song_id = songplay.song_id
                                 AND stage.artist_id = songplay.artist_id
                                 AND stage.session_id = songplay.session_id
                                 AND stage.location = songplay.location
                                 AND stage.user_agent = songplay.user_agent;

                           INSERT INTO songplay (start_time,
                                                 user_id,
                                                 level,
                                                 song_id,
                                                 artist_id,
                                                 session_id,
                                                 location,
                                                 user_agent)
                           SELECT * FROM stage;

                           END TRANSACTION;

                           DROP TABLE stage;"""

user_table_insert = """CREATE TEMP TABLE stage (LIKE users);

                       INSERT INTO stage
                       SELECT DISTINCT user_id
                       FROM staging_events
                       WHERE user_id IS NOT NULL;

                       BEGIN TRANSACTION;

                       DELETE FROM users
                       USING stage
                       WHERE users.user_id = stage.user_id;

                       INSERT INTO users
                       SELECT * FROM stage;

                       END TRANSACTION;

                       CREATE TEMP TABLE stage2 (LIKE users);

                       INSERT INTO stage2
                       SELECT DISTINCT user_id,
                                       first_name,
                                       last_name,
                                       gender,
                                       level
                       FROM staging_events
                       WHERE user_id IS NOT NULL;

                       BEGIN TRANSACTION;

                       UPDATE users
                       SET first_name = stage2.first_name,
                           last_name = stage2.last_name,
                           gender = stage2.gender,
                           level = stage2.level
                       FROM stage2
                       WHERE users.user_id = stage2.user_id;

                       DELETE FROM stage2
                       USING users
                       WHERE stage2.user_id = users.user_id;

                       INSERT INTO users
                       SELECT * FROM stage2;

                       END TRANSACTION;

                       DROP TABLE stage;
                       DROP TABLE stage2;"""

song_table_insert = """CREATE TEMP TABLE stage (LIKE songs);

                       INSERT INTO stage
                       SELECT DISTINCT song_id,
                                       title,
                                       artist_id,
                                       year,
                                       duration
                       FROM staging_songs;

                       BEGIN TRANSACTION;

                       DELETE FROM songs
                       USING stage
                       WHERE songs.song_id = stage.song_id;

                       INSERT INTO songs
                       SELECT * FROM stage;

                       END TRANSACTION;

                       DROP TABLE stage;"""

artist_table_insert = """CREATE TEMP TABLE stage (LIKE artists);

                         INSERT INTO stage
                         SELECT DISTINCT artist_id
                         FROM staging_songs;

                         BEGIN TRANSACTION;

                         DELETE FROM artists
                         USING stage
                         WHERE artists.artist_id = stage.artist_id;

                         INSERT INTO artists
                         SELECT * FROM stage;

                         END TRANSACTION;

                         CREATE TEMP TABLE stage2 (LIKE artists);

                         INSERT INTO stage2
                         SELECT DISTINCT artist_id,
                                         artist_name,
                                         artist_location,
                                         artist_latitude,
                                         artist_longitude
                         FROM staging_songs;

                         BEGIN TRANSACTION;

                         UPDATE artists
                         SET name = stage2.name,
                             location = stage2.location,
                             latitude = stage2.latitude,
                             longitude = stage2.longitude
                         FROM stage2
                         WHERE artists.artist_id = stage2.artist_id;

                         DELETE FROM stage2
                         USING artists
                         WHERE stage2.artist_id = artists.artist_id;

                         INSERT INTO artists
                         SELECT * FROM stage2;

                         END TRANSACTION;

                         DROP TABLE stage;
                         DROP TABLE stage2;"""

time_table_insert = """CREATE TEMP TABLE stage (LIKE time);

                       INSERT INTO stage
                       SELECT DISTINCT ts AS "start_time",
                                       EXTRACT(h FROM ts) AS "hour",
                                       EXTRACT(d FROM ts) AS "day",
                                       EXTRACT(w FROM ts) AS "week",
                                       EXTRACT(mon FROM ts) AS "month",
                                       EXTRACT(yr FROM ts) AS "year",
                                       EXTRACT(dw FROM ts) AS "weekday"
                       FROM staging_events;

                       BEGIN TRANSACTION;

                       DELETE FROM time
                       USING stage
                       WHERE time.start_time = stage.start_time;

                       INSERT INTO time
                       SELECT * FROM stage;

                       END TRANSACTION;

                       DROP TABLE stage;"""

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

insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]
