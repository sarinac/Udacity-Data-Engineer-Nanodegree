import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Copy S3 data into staging tables.
    
    Parameters
    ----------
    cur : cursor object
        Cursor for database connection.
    conn : connection object
        Connection to database.
    
    """
    print("Loading staging tables...")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging to fact/dim tables.
    
    Parameters
    ----------
    cur : cursor object
        Cursor for database connection.
    conn : connection object
        Connection to database.
    
    """
    print("Inserting data in tables...")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Connect to database. Load staging tables. Insert into reporting layer."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    print("Data copied from S3.")
    insert_tables(cur, conn)
    print("Data inserted into tables.")
    
    conn.close()


if __name__ == "__main__":
    main()