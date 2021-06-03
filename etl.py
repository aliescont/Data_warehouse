import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load data from S3 to temporal staging tables
    
    cur : cursor to perform database operations
    conn : establish a connection to the database
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging tables to fact and dimension tables
    
    cur : cursor to perform database operations
    conn : establish a connection to the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Extracts AWS configuration parameters from dwh.cfg file and executes queries to load the data to tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #copy data stored in the S3 to staging tables
    load_staging_tables(cur, conn)
    #load selected data from staging tables to fact and dimension tables
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()