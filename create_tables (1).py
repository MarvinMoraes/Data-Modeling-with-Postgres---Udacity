import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():

    # connect to default database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student  password=student")
    except psycopg2.Error as e:
        print("Error: Can't connect to the Postgres database")
        print(e)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.Error as e:
        print("Error: Can't drop Database")
        print(e)
        
    try:
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    except psycopg2.Error as e:
        print("Error: Can't create Database")
        print(e)

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e:
        print("Error: Can't make connection to Database")
        print(e)
     
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Can't get curser to  Database")
        print(e)
    
    return cur, conn

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Can't drop table from query: {}".format(query))
            print(e)

def create_tables(cur, conn):
 
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Can't create table from query: {}".format(query))
            print(e)

def main():
 
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    cur.close();
    conn.close()


if __name__ == "__main__":
    main()