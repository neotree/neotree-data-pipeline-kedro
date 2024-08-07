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
    df.to_sql(table_name, con=engine, schema='derived', if_exists='replace',index=False,dtype={col_name: TEXT for col_name in df})

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

def get_table_columns(table_name,table_schema):
    query = f''' SELECT column_name,data_type  FROM information_schema.columns WHERE table_schema = '{table_schema}' AND table_name   = '{table_name}';; ''';
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

# def create_union_views(view_name,table1,table2,columns, where):
#     query = f''' DROP VIEW  if exists derived.{view_name} cascade;
#                  CREATE VIEW derived.{view_name} AS (
#                   SELECT {columns} FROM derived.{table1} {where} union all
#                   SELECT {columns} FROM derived.{table2}
#                  );'''
#     inject_sql(query,"union-views");