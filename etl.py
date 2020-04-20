import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


# Function to load data into staging table
def load_staging_tables(cur, conn):
    """
    :param cur: db cursor
    :param conn: db connection
    :return:
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


# Function to insert data into table
def insert_tables(cur, conn):
    """
    :param cur: db cursor
    :param conn: db connection
    :return:
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


# Main Function to execute  load_staging_tables and insert_tables
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values())
    )
    cur = conn.cursor()

    # load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print("insert data into table completed")

    # Close db connection
    conn.close()


if __name__ == "__main__":
    main()
