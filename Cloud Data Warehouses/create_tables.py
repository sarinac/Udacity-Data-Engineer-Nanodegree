import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop tables in database.
    
    Parameters
    ----------
    cur : cursor object
        Cursor for database connection.
    conn : connection object
        Connection to database.
    
    """
    print("Dropping tables...")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create tables in database.
    
    Parameters
    ----------
    cur : cursor object
        Cursor for database connection.
    conn : connection object
        Connection to database.
    
    """
    print("Creating tables...")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Connect to database. Drop then create tables."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    print("Tables are dropped.")
    create_tables(cur, conn)
    print("Tables are created.")
    
    conn.close()
    print("Connection is closed.")


if __name__ == "__main__":
    main()