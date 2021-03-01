import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


"""
This function reads the drop_tables_queries from sql_queries to DROP the tables if they exist
"""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

"""
This function reads the create_tables_queries from sql_queries to CREATE the tables if they do NOT exist
"""
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

"""
This function reads the AWS keys and credentials from dwh.cfg to create the connection
"""
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()