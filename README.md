# Sparkify Cloud Data Warehouse

### Purpose

Sparkify’s expanded user base and song database prompted them to move their processes and data onto the cloud. To support the analytics team and to enable them to continue finding insights relating to the songs their users are listening to, a data warehouse has been created on aws using Amazon Redshift.

---
### Schema

![Star Schema](/star_schema.png)

The schema for the data warehouse is a star schema. This schema allows the central unit of measurement, songplays, to be stored in a central fact table that also contains references to the context for each songplay event. Seperate dimension tables that haven't been normalised are then created to provide additional information in relation to the references in the fact table. The star schema is easy to understand and its denormalised structure should improve the perfomance of read queries.

---

### Files

##### *dwh.cfg*

This file contains all the configuration settings for the Redshift Database, the role arn and paths to s3 objects that contain the log data, song data and the jsonpath file.

##### *sql_queries.py*

This file contains all the sql queries used by *create_tables.py* to create the various staging and star schema tables and *etl.py* to extract, transform and load data into all the new tables

##### *create_tables.py*

This script should be run before running *etl.py* and *test.ipynb*

The create_tables.py script connects to the Redshift database and uses various queries in *sql_queries.py* to drop existing staging tables and create new staging tables for the raw data stored in the song and log json files. The staging_events table stores raw data from the log files while the staging_songs table stores raw data from the song files.

It also drops and creates the following fact and dimension tables for the data warehouse's star schema.

**FACT TABLE:**

* songplay - The fact table (songplay) contains songplay data based on user logs and keys to other dimension tables

**DIMENSION TABLES:**

* users - This table contains the first name, last name, gender and level associated with each user_id

* songs - This table contains the song_id, title, artist_id, year, duration for each song

* artist - This table contains the artist_id, name, location, latitude, longitude for each artist

* time - This table breaks down each timestamp (start_time) into hour, day, week, month, year and weekday

To run this file type the following command in to your terminal:

```python

    python create_tables.py

```

##### *etl.py*

This script loads the json files in S3 into the staging tables. It then inserts the data in the staging tables into the fact and dimension tables in the star schema and removes duplicate records where applicable.

To run this file type the following command in to your terminal:

```python

    python etl.py

```

##### *test.ipynb*

This notebook runs tests to check if the data was appropriately loaded into all relevant tables and if all duplicate records were removed. It also runs a query that joins data from the songplay, songs, artists and time tables to display the top 10 songs by play count in November 2018.

To run this file, launch the jupyter notebook app from the directory this file is located in and run all cells.

---

### Sample Query

Below is a sample query from the new data warehouse and its associated output

```sql

    SELECT songs.title AS "song",
           artists.name AS "artist",
           COUNT(*) AS play_count
    FROM songplay
    JOIN songs
    ON songplay.song_id = songs.song_id
    JOIN artists
    ON songs.artist_id = artists.artist_id
    JOIN time
    ON songplay.start_time = time.start_time
    WHERE time.year = 2018 and time.month = 11

    GROUP BY 1,2
    ORDER BY play_count DESC

    LIMIT 10

```
Output:

![Query Output](/query_output.png)
