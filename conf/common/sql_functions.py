import logging
import json
from typing import Dict
from conf.common.format_error import formatError
from datetime import datetime, date
from .config import config
from sqlalchemy import create_engine,text
import sys
from sqlalchemy.types import TEXT
import pandas as pd
import numpy as np
import logging
from psycopg2 import sql,connect
import re

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
        conn = engine.raw_connection()
        cur = conn.cursor()
        try:
            cur.execute(sql_script)
            conn.commit()
        except Exception as e:
            logging.error(e)
            logging.error('Something went wrong with the SQL file');
            logging.error(sql_script)
            sys.exit()
        logging.info('... {0} has successfully run'.format(file_name))

# def inject_sql(sql_script, file_name):
#     sql_commands = sql_script.split(';;')
#     conn = engine.raw_connection()
#     cur = conn.cursor()
#     for command in sql_commands[:-1]:
#         try:
#             logging.info(text(file_name))
#             cur.execute(text(command))
    
#         # last element in list is empty hence need for [:-1] slicing out the last element
#         except Exception as e:
#             logging.error('Something went wrong with the SQL file');
#             logging.error(text(command))
#             logging.error(e)
#             raise e
#     conn.commit()
#     #logging.info('... {0} has successfully run'.format(file_name))
def inject_sql(sql_script, file_name):
    conn = None
    cur = None
    try:
        conn = engine.raw_connection()
        cur = conn.cursor()
        sql_commands = str(sql_script).split(';;')
        for command in sql_commands:
            try:
                if not command.strip():  # skip empty commands
                    continue
                cur.execute(command)
                conn.commit()
            except Exception as e:
                logging.error(f"Error executing command in {file_name}")
                logging.error(f"Error type: {type(e)}")
                logging.error(f"Full error: {str(e)}")
                raise
        
    except Exception as e:
        logging.error(f"Transaction failed completely for {file_name}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

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
    batch_size = 1000
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        
        # Convert to values you can inspect
        values = batch.replace({pd.NA: None, np.nan: None}).to_dict('records')
        print(f"Batch {i} sample:", values[0])
        
        # Insert the batch
        batch.to_sql(table_name, con=engine, schema='derived',
                if_exists='append', index=False)
        inject_sql(batch,f"APPENDING {table_name} {batch_size}")

# def inject_sql_with_return(sql_script):
#     conn = engine.raw_connection()
#     cur = conn.cursor()
#     data = []
#     try:
#         result =cur.execute(sql_script)
#         conn.commit()
#         rows = [result] if isinstance(result, dict) else result
#         for row in rows:
#             data.append(row)
#         result.close()
#         return data
#     except Exception as e:
#         logging.error(e)
#         raise e

def inject_sql_with_return(sql_script):
    conn = None
    cur = None
    try:
       
        conn = engine.raw_connection()
        cur = conn.cursor()
        if not sql_script.strip():  # skip empty commands
            return []
        # Execute the SQL script
        cur.execute(sql_script)
        
        # Fetch all results
        rows = cur.fetchall()
        
        # Option 1: Return as list of tuples (default)
        data = list(rows)
        
        # Option 2: Return as list of dictionaries (uncomment to use)
        # data = [dict(zip(columns, row)) for row in rows]
        conn.commit()
        return data
        
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error executing SQL: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    
def inject_bulk_sql(queries, batch_size=1000):
    conn = engine.raw_connection()  # Get the raw DBAPI connection
    cursor = conn.cursor()  # Create a cursor from the raw connection
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

def get_date_column_names(table_name,table_schema):
    query = f''' SELECT column_name FROM information_schema.columns WHERE table_schema = '{table_schema}' AND table_name   = '{table_name}' and  (data_type like '%time%' or data_type like '%date%') ;; '''
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
        if result:
            return result[0]
        else:
            return False
        
def run_query_and_return_df(query):
    try:
       
        conn = connect(con_string)
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as ex:
        logging.error(formatError(ex))
    finally:
        conn.close()


def generate_upsert_queries_and_create_table(table_name: str, df: pd.DataFrame):
    if df.empty:
        return

    conn = engine.raw_connection()
    cur = conn.cursor()

    try:
        schema = 'derived'

        # Step 1: Check if the table exists
        cur.execute(
            sql.SQL("SELECT to_regclass(%s)"),
            [f"{schema}.{table_name}"]
        )
        result = cur.fetchone()
        if result[0] is None:
            # Create table with all current columns
            create_cols = ', '.join([f'"{col}" TEXT' for col in df.columns])
            create_query = sql.SQL("CREATE TABLE {}.{} ({})").format(
                sql.Identifier(schema),
                sql.Identifier(table_name),
                sql.SQL(create_cols)
            )
            cur.execute(create_query)
            conn.commit()
            # Step 2: Add Unique Constraint on relevant columns
            constraint_name = f"{table_name}_uid_form_created_facility_review_uq"
            constraint_query = sql.SQL(
                    """
                    ALTER TABLE {schema}.{table}
                    ADD CONSTRAINT {constraint}
                    UNIQUE (uid, form_id, created_at, facility, review_number);
                    """
                ).format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table_name),
                    constraint=sql.Identifier(constraint_name)
                )
            cur.execute(constraint_query)

        # Step 3: Ensure all columns exist
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table_name))
        existing_cols = {row[0] for row in cur.fetchall()}

        for col in df.columns:
            if col not in existing_cols:
                alter_query = sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} TEXT DEFAULT NULL").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name),
                    sql.Identifier(col)
                )
                cur.execute(alter_query)
        conn.commit()

        # Step 4: Generate and execute UPSERTs
        for _, row in df.iterrows():
            columns = list(row.index)
            values = [row[col] if pd.notnull(row[col]) else None for col in columns]

            update_cols = [
                col for col in columns
                if col not in ["uid", "form_id", "created_at", "facility", "review_number"]
            ]

            insert_query = sql.SQL("""
                INSERT INTO {}.{} ({})
                VALUES ({})
                ON CONFLICT (uid, form_id, created_at, facility, review_number)
                DO UPDATE SET {}
            """).format(
                sql.Identifier(schema),
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                sql.SQL(', ').join(sql.Placeholder() for _ in columns),
                sql.SQL(', ').join([
                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
                    for col in update_cols
                ])
            )

            cur.execute(insert_query, values)

        conn.commit()
        cur.close()

    except Exception as ex:
        logging.error(f"Error upserting into '{schema}.{table_name}': {ex}")
        conn.rollback()
        cur.close()
        raise

def generateAndRunUpdateQuery(table:str,df:pd.DataFrame):
    try:
        if(table is not None and df is not None and not df.empty):

            updates = []
            column_types = {col: get_table_column_type('joined_admissions_discharges', 'derived', col)[0][0] for col in df.columns}
            
        
            # Generate UPDATE queries for each row
            update_queries = []
            for _, row in df.iterrows():
                updates = []
                for col in df.columns:
                    col_type = column_types[col]
                    value = row[col]
                    updates.append(format_value(col, value, col_type))
                
                # Join the updates into a single SET clause
                set_clause = ', '.join(updates)
                
                # Add the WHERE condition
                where_condition = f"WHERE uid = '{row['uid']}' AND facility = '{row['facility']}' AND \"unique_key\" = '{row['unique_key']}'"
                
                # Construct the full UPDATE query
                update_query = f"UPDATE {table} SET {set_clause} {where_condition};;"
                update_queries.append(update_query)  
            inject_bulk_sql(update_queries)

    except Exception as ex:
        logging.error(
            "!!! An error occured whilest JOINING DATA THAT WAS UNJOINED ")
        logging.error(formatError(ex))

def is_date_prefix(s):
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}.*', s))

def generate_postgres_insert(df, schema,table_name):
    # Escape column names and join them
    columns = ', '.join(f'"{col}"' for col in df.columns)

    # Generate values part
    values_list = []
    for _, row in df.iterrows():
        row_values = []
        for val in row:
            if is_effectively_na(val) or  str(val) in {'NaT', 'None', 'nan',''}:
                row_values.append("NULL")

            elif isinstance(val, (list, dict)):
                json_val = json.dumps(val)
                row_values.append(f"'{escape_special_characters(json_val)}'")

            elif (is_date_prefix(str(val))):
                row_values.append(f"'{clean_datetime_string(val)}'".replace('.',''))
            elif isinstance(val, (pd.Timestamp, pd.Timedelta)) or ():
                row_values.append(f"'{val}'")
            elif isinstance(val, str):
                row_values.append(f''' '{escape_special_characters(str(val))}' ''') 
            else:
                row_values.append(str(val))
        values_list.append(f"({', '.join(row_values)})")

    values = ',\n'.join(values_list)

    # Compose the full INSERT statement
    insert_query = f'INSERT INTO {schema}."{table_name}" ({columns}) VALUES\n{values};;'
    inject_sql(insert_query,f"INSERTING INTO {table_name}")

def clean_datetime_string(s:str):
    try:
        dt = pd.to_datetime(s, errors='coerce')
        if pd.isna(dt):
            return s  # or return None

        # If time component is zero or missing
        if dt.time() == pd.Timestamp.min.time():
            return dt.strftime('%Y-%m-%d')

        return dt.strftime('%Y-%m-%d %H:%M:%S')

    except Exception:
        return s

def is_effectively_na(val):
    try:
        return pd.isna(val).any() 
    except AttributeError:
        return pd.isna(val) 
    
def format_value(col, value, col_type):
    if is_effectively_na(value) or str(value) in {'NaT', 'None', 'nan'}:
        return f"\"{col}\" = NULL"
    
    col_type_lower = col_type.lower()
    
    if 'timestamp' in col_type_lower:
        try:
            if isinstance(value, (datetime, pd.Timestamp)):
                return f"\"{col}\" = '{str(value)[:19].replace('.','').strftime('%Y-%m-%d %H:%M:%S')}'"
            elif isinstance(value, str):
                # First try to parse as datetime
                try:
                    dt = pd.to_datetime(value, errors='raise',format='%Y-%m-%dT%H:%M:%S').tz_localize(None)
                    return f"\"{col}\" = '{dt.replace('.','').strftime('%Y-%m-%d %H:%M:%S')}'"
                except:
        
                    clean_value = value.strip().replace('.','').replace('T', ' ')
                    # Validate it looks like a timestamp
                    if re.match(r'^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}', clean_value):
                        return f"\"{col}\" = '{str(clean_value)[:19]}'"
                    return f"\"{col}\" = NULL"
            else:
                return f"\"{col}\" = NULL"
        except:
            return f"\"{col}\" = NULL"
            
    elif 'date' in col_type_lower:
        try:
            if isinstance(value, (date, pd.Timestamp)):
                return f"\"{col}\" = '{value.replace('.','').strftime('%Y-%m-%d')}'"
            elif isinstance(value, str):
                try:
                    dt = pd.to_datetime(value, errors='raise',format='%Y-%m-%d').tz_localize(None)
                    return f"\"{col}\" = '{dt.replace('.','').strftime('%Y-%m-%d')}'"
                except:
                    clean_value = value.split('T')[0].strip()
                    if re.match(r'^\d{4}-\d{2}-\d{2}$', clean_value):
                        return f"\"{col}\" = '{clean_value}'"
                    return f"\"{col}\" = NULL"
            else:
                return f"\"{col}\" = NULL"
        except:
            return f"\"{col}\" = NULL"
            
    elif col_type == 'text':
        return f"\"{col}\" = '{escape_special_characters(str(value))}'"
    else:
        return f"\"{col}\" = {value}"
    
def generate_create_insert_sql(df,schema, table_name):
    # Infer PostgreSQL types
    try:
        if not table_exists(schema,table_name):
            dtype_map = {
                'int64': 'INTEGER',
                'float64': 'DOUBLE PRECISION',
                'bool': 'BOOLEAN',
                'object': 'TEXT',
                'datetime64[ns]': 'TIMESTAMP',
                'timedelta[ns]': 'INTERVAL'
            }

            create_cols = []
            for col in df.columns:
                dtype = str(df[col].dtype)
                pg_type = dtype_map.get(dtype, 'TEXT')  # Fallback to TEXT
                create_cols.append(f'"{col}" {pg_type}')

            create_stmt = f'CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({",".join(create_cols)});;'
            inject_sql(create_stmt,f"CREATING {table_name}")
            
        generate_postgres_insert(df,schema,table_name)
    except Exception as ex:
       logging.info(f"FAILED TO INSERT {formatError(ex)}")


def escape_special_characters(input_string): 
    return str(input_string).replace("\\","\\\\").replace("'","")

def table_exists(schema, table_name):
    query = f''' SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_schema = '{schema}'
                AND    table_name   = '{table_name}'
                );;'''
    query_result = inject_sql_with_return(query)

    if query_result and len(query_result) > 0:
        result = query_result[0]
        if isinstance(result, (tuple, list, np.ndarray)):
            return bool(result[0])
        return bool(result)
    return False