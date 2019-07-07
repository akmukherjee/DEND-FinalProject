""" 
    This file contains the functions to create the DB connection along with creating and dropping the tables. 
"""


import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """ 
    The function to drop the tables on the connected DB. 
  
    This function executes the queries listed in the drop_table_queries list and drops the appropriate
    tables. 
  
    Parameters: 
    conn: The connection variable to the DB
    curr: Cursor variable with the currently connected DB
    
    Returns: 
    None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ 
    The function to create the tables on the connected DB. 
  
    This function executes the queries listed in the create_table_queries list and creates the appropriate
    tables. 
  
    Parameters: 
    conn: The connection variable to the DB
    curr: Cursor variable with the currently connected DB
    
    Returns: 
    None
    """    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ 
    The function to runs the main function on this module. 
  
    This main function first drops the tables and then creates new tables on the connected DB 
  
    Parameters: 
    None
    
    Returns: 
    None
    """    
    conn = psycopg2.connect("host=127.0.0.1 dbname=FinalProject user=udacity password=udacity")
    cur = conn.cursor()
    # Drop all the Tables
    drop_tables(cur, conn)
    # Create all the Tables
    create_tables(cur, conn)
    # Close the Connection
    conn.close()


if __name__ == "__main__":
    main()