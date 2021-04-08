from kedro.io import DataCatalog
import logging
from conf.common.format_error import formatError
from kedro.extras.datasets.pandas import (
    SQLQueryDataSet,SQLTableDataSet
)
from .config import config
from sqlalchemy import event, create_engine,text
import sys


params = config()
#Postgres Connection String
con = 'postgresql+psycopg2://' + \
params["user"] + ':' + params["password"] + '@' + \
params["host"] + ':' + '5432' + '/' + params["database"]
engine = create_engine(con, executemany_mode='batch')

#Inject SQL Procedures
def inject_sql_procedure(sql_script, file_name):
        try:
            engine.connect().execution_options(isolation_level="AUTOCOMMIT").execute(sql_script)
        except Exception as e:
            logging.error('Something went wrong with the SQL file');
            logging.error(formatError(e))
            sys.exit()
        logging.info('... {0} has successfully run'.format(file_name))

def inject_sql(sql_script, file_name):
    # ref: https://stackoverflow.com/questions/19472922/reading-external-sql-script-in-python/19473206
    sql_commands = sql_script.split(';')
    for command in sql_commands[:-1]:
        try:
            engine.connect().execute(text(command))
        # last element in list is empty hence need for [:-1] slicing out the last element
        except Exception as e:
            logging.error('Something went wrong with the SQL file');
            raise e.with_traceback()
            sys.exit()
    logging.info('... {0} has successfully run'.format(file_name))

def create_table(df, table_name):
    # create tables in derived schema
    df.to_sql(table_name, con=engine, schema='derived', if_exists='replace',index=False)

def append_data(df,table_name):
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
        raise e