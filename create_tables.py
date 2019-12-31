import mysql.connector
from Sql_Queries import create_table_queries,drop_table_queries

# fn to create and drop database
def create_database():
    #connect to default database
    # i.e information_schema
    conn = None
    cur = None
    try:
        conn = mysql.connector.connect(
            host = "localhost",
            user  = "root",
            password = "rootuser",
            database = "Test"
        )
        cur = conn.cursor()

        cur.execute("Drop database if exists SparkifyDB")
        cur.execute("Create database SparkifyDB")

        conn.close()

        # Now we will connect to Sparkify database

        conn = mysql.connector.connect(
            host = "localhost",
            user  = "root",
            password = "rootuser",
            database = "SparkifyDB"
        )
        cur = conn.cursor()
    except Exception as e:
        print(e)
    
    return cur,conn

def drop_tables(cur,conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur,conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    cur,conn = create_database()

    drop_tables(cur,conn)
    create_tables(cur,conn)

    conn.close()

if __name__ == "__main__":
    main()


