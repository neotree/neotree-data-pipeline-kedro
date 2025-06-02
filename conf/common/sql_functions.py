import logging
from typing import Dict
from conf.common.format_error import formatError
from .config import config
from sqlalchemy import create_engine,text
import sys
from sqlalchemy.types import TEXT
import pandas as pd
import psycopg2
from psycopg2 import sql

params = config()
#Postgres Connection String
con = 'postgresql+psycopg2://' + \
params["user"] + ':' + params["password"] + '@' + \
params["host"] + ':' + '5432' + '/' + params["database"]
# engine = create_engine(con, executemany_mode='batch')
con_string = f'''postgresql://{params["user"]}:{params["password"]}@{params["host"]}:5432/{params["database"]}'''

engine = create_engine(
    con,
    pool_size=5,        # Maximum number of connections in the pool
    max_overflow=10,    # Maximum number of connections to allow in excess of pool_size
    pool_timeout=30,    # Maximum number of seconds to wait for a connection to become available
    pool_recycle=1800   # Number of seconds a connection can persist before being recycled
)
#Useful functions to inject sql queries
#Inject SQL Procedures
def inject_sql_procedure(sql_script, file_name):
        try:
            engine.connect().execution_options(isolation_level="AUTOCOMMIT").execute(sql_script)
        except Exception as e:
            logging.error(e)
            logging.error('Something went wrong with the SQL file');
            logging.error(text(sql_script))
            sys.exit()
        logging.info('... {0} has successfully run'.format(file_name))

def inject_sql(sql_script, file_name):
    # ref: https://stackoverflow.com/questions/19472922/reading-external-sql-script-in-python/19473206
    sql_commands = sql_script.split(';;')
    for command in sql_commands[:-1]:
        try:
            #logging.info(text(command))
            engine.connect().execute(text(command))
        # last element in list is empty hence need for [:-1] slicing out the last element
        except Exception as e:
            # logging.error('Something went wrong with the SQL file');
            # logging.error(text(command))
            logging.error(e)
            raise e
    #logging.info('... {0} has successfully run'.format(file_name))

def create_table(df: pd.DataFrame, table_name):
    # create tables in derived schema
    try:
       df.to_sql(table_name, con=engine, schema='derived', if_exists='replace',index=False)
    except Exception as e:
        logging.error(e)
        raise e

def create_exploded_table(df: pd.DataFrame, table_name):
    # create tables in derived schema and restrict all columns to Text
    df.to_sql(table_name, con=engine, schema='derived', if_exists='append',index=False,dtype={col_name: TEXT for col_name in df})

def append_data(df: pd.DataFrame,table_name):
    #Add Data To An Existing Table
    df.to_sql(table_name, con=engine, schema='derived', if_exists='append',index=False)

def inject_sql_with_return(sql_script):
    data = []
    try:
        result =engine.connect().execution_options(isolation_level="AUTOCOMMIT").execute(sql_script)
        for row in result:
            data.append(row)
        result.close()
        return data
    except Exception as e:
        logging.error(e)
        raise e
    
def inject_bulk_sql(queries, batch_size=1000):
    conn = engine.raw_connection()  # Get the raw DBAPI connection
    cursor = conn.cursor()  # Create a cursor from the raw connection
    logging.info("--CONNECTION ESTABLISHED.....")
    try:
        # Execute commands in batches
        for i in range(0, len(queries), batch_size):
            batch = queries[i:i + batch_size]
            for command in batch:
                try:
                    cursor.execute(command)
                except Exception as e:
                    logging.error(f"Error executing command: {command}")
                    logging.error(e)
                    conn.rollback()  # Rollback the transaction on error
                    raise e
            conn.commit()  # Commit the batch
            logging.info("########################### DONE BULK PROCESSING ################")
    except Exception as e:
        logging.error("Something went wrong with the SQL file")
        logging.error(e)
        conn.rollback()  # Rollback the transaction on error
        raise e
    finally:
        cursor.close()  # Close the cursor
        conn.close()  #

def get_table_columns(table_name,table_schema):
    query = f''' SELECT column_name,data_type  FROM information_schema.columns WHERE table_schema = '{table_schema}' AND table_name   = '{table_name}';; ''';
    return inject_sql_with_return(query);

def get_table_column_type(table_name,table_schema,column):
    query = f''' SELECT data_type  FROM information_schema.columns WHERE table_schema = '{table_schema}' AND table_name   = '{table_name}' AND column_name='{column}' ;; ''';
    return inject_sql_with_return(query);

def get_table_column_names(table_name,table_schema):
    query = f''' SELECT column_name FROM information_schema.columns WHERE table_schema = '{table_schema}' AND table_name   = '{table_name}';; ''';
    return inject_sql_with_return(query);

def insert_old_adm_query(target_table, source_table, columns):
    # Join the column names with commas
    columns_str = '","'.join(columns)
    
    # Construct the SQL INSERT INTO ... SELECT statement
    insert_select_statement = (
        f'INSERT INTO {target_table} ("{columns_str}") '
        f'SELECT "{columns_str}" FROM {source_table};'
    )
    
    return insert_select_statement
    
def create_new_columns(table_name,schema,columns):
    for column,col_type in columns:
        if not column_exists(schema,table_name,column):
       
            if col_type == "object":
                sql_type = "TEXT"
            elif "float" in col_type:
                sql_type = "DOUBLE PRECISION"
            elif "int" in col_type:
                sql_type = "INTEGER"
            elif "datetime" in col_type or "date" in col_type:
                sql_type = "TIMESTAMP"
            else:
                sql_type = "TEXT" 
            
            alter_query = f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "{column}" {sql_type};;'
            inject_sql(alter_query,f'ADD {column} ON  "{schema}"."{table_name}"')


def column_exists(schema, table_name,column_name):
    query = f''' SELECT EXISTS (
                SELECT column_name FROM information_schema.columns 
                WHERE  table_schema = '{schema}'
                AND    table_name   = '{table_name}'
                AND column_name = '{column_name}'
                );;'''
    query_result = inject_sql_with_return(query)
    if len(query_result) >0:
        result = query_result[0]
        if 'exists' in result.keys():
            return result['exists']
        else:
            return False
        
def run_query_and_return_df(query):
    try:
       
        conn = psycopg2.connect(con_string)
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as ex:
        logging.error(formatError(ex))
    finally:
        conn.close()

from typing import Union
import pandas as pd
import logging
from psycopg2 import sql

def generate_upsert_queries_and_create_table(table_name: str, df: pd.DataFrame):
    conn = engine.raw_connection()
    cur = conn.cursor()

    try:
        if df.empty:
            return

        # Step 1: Check if the table exists
        cur.execute("SELECT to_regclass(%s)", (f'derived.{table_name}',))
        result = cur.fetchone()
        if result[0] is None:
            # Create table with all current columns
            create_cols = ', '.join([f'"{col}" TEXT' for col in df.columns])
            create_query = f'CREATE TABLE "derived.{table_name}" ({create_cols})'
            cur.execute(create_query)
            conn.commit()

        # Step 2: Ensure all columns exist
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = 'derived' AND table_name = %s
        """, (table_name,))
        existing_cols = {row[0] for row in cur.fetchall()}

        for col in df.columns:
            if col not in existing_cols:
                alter_query = f'ALTER TABLE "derived.{table_name}" ADD COLUMN "{col}" TEXT DEFAULT NULL'
                cur.execute(alter_query)
        conn.commit()

        # Step 3: Generate and execute UPSERTs
        for _, row in df.iterrows():
            columns = list(row.index)
            values = [row[col] if pd.notnull(row[col]) else None for col in columns]

            update_cols = [
                col for col in columns
                if col not in ["uid", "form_id", "created_at", "facility", "review_number"]
            ]

            insert_query = sql.SQL("""
                INSERT INTO derived.{table} ({columns})
                VALUES ({values})
                ON CONFLICT (uid, form_id, created_at, facility, review_number)
                DO UPDATE SET {update_set}
            """).format(
                table=sql.Identifier(table_name),
                columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
                values=sql.SQL(', ').join(sql.Placeholder() * len(columns)),
                update_set=sql.SQL(', ').join([
                    sql.SQL(f'"{col}" = EXCLUDED."{col}"') for col in update_cols
                ])
            )

            logging.info(f"[UPSERT]: {insert_query.as_string(cur)}")
            cur.execute(insert_query, values)

        conn.commit()
        cur.close()

    except Exception as ex:
        logging.error(f"Error upserting into '{table_name}': {ex}")
        conn.rollback()
        cur.close()
        raise
