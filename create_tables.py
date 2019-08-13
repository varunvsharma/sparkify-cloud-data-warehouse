import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop all tables in the drop_table_queries list.

    Keyword arguments:
    cur -- Database cursor object
    conn -- Database connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create all tables in the create_table_queries list.

    Keyword arguments:
    cur -- Database cursor object
    conn -- Database connection object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Call the drop_tables and create_tables functions.

    Create database connection and cursor objects using the cluster configuration parameters stored
    in the data warehouse configuration file. Use these objects to call the drop_tables and create_tables
    functions.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
