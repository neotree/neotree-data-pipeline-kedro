import logging
from conf.common.format_error import formatError
from .config import config
from sqlalchemy import create_engine,text
import sys
from sqlalchemy.types import TEXT
import pandas as pd

params = config()
#Postgres Connection String
con = 'postgresql+psycopg2://' + \
params["user"] + ':' + params["password"] + '@' + \
params["host"] + ':' + '5432' + '/' + params["database"]
# engine = create_engine(con, executemany_mode='batch')

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
    
def inject_bulk_sql(sql_script, batch_size=1000):
    # Split the SQL script into individual commands
    sql_commands = sql_script.split(';;')
    
    # Remove the last empty element
    sql_commands = sql_commands[:-1]
    conn = engine.connect()
    # Use psycopg2 for bulk execution
    cursor = conn.cursor()
    
    try:
        # Execute commands in batches
        for i in range(0, len(sql_commands), batch_size):
            batch = sql_commands[i:i + batch_size]
            for command in batch:
                try:
                    cursor.execute(command)
                except Exception as e:
                    logging.error(f"Error executing command: {command}")
                    logging.error(e)
                    conn.rollback()
                    raise e
            conn.commit()
            logging.info("###########################DONE BULK PROCESSING################")
    except Exception as e:
        logging.error("Something went wrong with the SQL file")
        logging.error(e)
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

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