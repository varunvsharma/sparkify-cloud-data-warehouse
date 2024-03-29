import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Copy data from S3 and load into staging tables in Amazon Redshift.

    Keyword arguments:
    cur -- Database cursor object
    conn -- Database connection object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging tables into tables in the star schema.

    Keyword arguments:
    cur -- Database cursor object
    conn -- Database connection object
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Call the load_staging_tables and insert_tables functions.

    Create database connection and cursor objects using the cluster configuration parameters stored
    in the data warehouse configuration file. Use these objects to call the load_staging_tables and
    insert_tables functions.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
