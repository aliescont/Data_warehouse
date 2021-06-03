import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop existing tables to duplicates from same record
    
    cur : cursor to perform database operations
    conn : establish a connection to the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Excutes queries to create tables in the dataset
    
    cur : cursor to perform database operations
    conn : establish a connection to the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Read AWS configuration parameters and create staging, fact and dimension tables """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    #drop tables first to clean previous data from other tests
    drop_tables(cur, conn)
    #create staging, fact and dimension tables
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()