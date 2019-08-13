import configparser
import psycopg2
from sql_queries import drop_table_queries

def drop_tables(cur, conn):
    """Drop all tables in the drop_table_queries list.

    Keyword arguments:
    cur -- Database cursor object
    conn -- Database connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
