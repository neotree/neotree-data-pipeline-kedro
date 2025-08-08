import logging
from conf.common.logger import setup_logger
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
from psycopg2.extras import execute_values
from collections import defaultdict
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
QUERY_LOG_PATH="logs/queries.log"
query_logger = setup_logger(QUERY_LOG_PATH,'queries')

def inject_sql_procedure(sql_script, file_name):
        conn = engine.raw_connection()
        cur = conn.cursor()
        try:
            cur.execute(sql_script)
            conn.commit()
            query_logger.info(f"::DEDUPLICATING::")
            query_logger.info(f"DEDUP-{sql_script}")
        except Exception as e:
            logging.error(e)
            logging.error('Something went wrong with the SQL file');
            logging.error(sql_script)
            sys.exit()
        logging.info('... {0} has successfully run'.format(file_name))


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
        logging.error(f"Transaction failed completely for {sql_script}")
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
  
    try:
        if "How is the baby being fed?.label" in df:
            df.rename(columns={'FeedAsse.label': 'How is the baby being fed?.label'}, inplace=True)

        if "How is the baby being fed?.value" in df:
            df.rename(columns={'FeedAsse.value': 'How is the baby being fed?.value'}, inplace=True)
        if "How is the baby being fed" in table_name:
            table_name="exploded_twenty_8_day_follow_u_FeedAsse.label"
            
        df.to_sql(table_name, con=engine, schema='derived', if_exists='append',index=False,dtype={col_name: TEXT for col_name in df})
    except Exception as ex:
        logging.error(f"FAILED DF:\n{df.to_string()}")
        logging.error(f'ERR DF=={ex}')

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
    cursor = conn.cursor()
    # Create a cursor from the raw connection
    try:
        # Execute commands in batches
        for i in range(0, len(queries), batch_size):
            batch = queries[i:i + batch_size]
            for command in batch:
                try:
                    cursor.execute(command)
                    conn.commit()
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
                    formatted = format_value(col, value, col_type)
                    if formatted is not None:
                        updates.append(formatted)
                
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
    if not isinstance(s, str):
        return False
    if re.match(r'^\d{4}-\d{2}-\d{2}.*', s):
        return True
        
    # Check for textual month format (DD Mon, YYYY)
    if re.match(r'^\d{1,2}\s+[A-Za-z]{3,},\s*\d{4}.*', s):
        try:
            # Try parsing to validate it's a real date
            datetime.strptime(s[:12], '%d %b, %Y')  # First 12 chars should be enough
            return True
        except ValueError:
            return False
            
    return False

def generate_postgres_insert(df, schema,table_name):
    # Escape column names and join them
    df = df[[col for col in df.columns if len(col) > 1]]
    columns = ', '.join(f'"{col}"' for col in df.columns)
    # Generate values part
    values_list = []
    for _, row in df.iterrows():
        
        if row['uid'] is None:
            continue
            
        if 'uid' not in row or pd.isna(row['uid']) or str(row['uid']).strip().lower() in {'null', 'nan', 'nat','<na>',''}:
            continue
        if 'unique_key' not in row or pd.isna(row['unique_key']) or str(row['unique_key']).strip().lower() in {'null', 'nan', 'nat','<na>',''}:
            continue
        row_values = []
        for  key, val in row.items():
            if len(str(key))<=1:
                
                continue

            if str(val) in {'NaT', 'None', 'nan','','<NA>'}:
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
        values_list.append(f"({','.join(row_values)})")

    values = ',\n'.join(values_list)

    # Compose the full INSERT statement
    if columns and values:
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
    if len(col)<=1:
        return None
    if str(value) in {'NaT', 'None', 'nan','<NA>'}:
        return f"\"{col}\" = NULL"
    
    col_type_lower = col_type.lower()
    
    if 'timestamp' in col_type_lower:
        try:
           
            if isinstance(value, (datetime, pd.Timestamp)):
                return f"\"{col}\" = '{value.strftime('%Y-%m-%d %H:%M:%S')}'"

            elif isinstance(value, str):
                clean_value = value.strip().rstrip(",")  # Remove trailing commas

                try:
                    dt = pd.to_datetime(clean_value, errors='raise', infer_datetime_format=True)
                    return f"\"{col}\" = '{dt.strftime('%Y-%m-%d %H:%M:%S')}'"
                except ValueError:
                    logging.error(f"I HAVE SINNED::::::{value}")
                    timestamp_pattern = re.compile(
                        r'^('
                        r'\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}'                       
                        r'(?:\.\d+)?'                                                   
                        r'(?:Z|[+-]\d{2}:?\d{2})?'                                       
                        r'|'                                                             
                        r'\d{1,2}[ -](?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*[ ,]*\d{4}' 
                        r'|'                                                             
                        r'\d{1,2}[/-]\d{1,2}[/-]\d{4}'                                  
                        r')$',
                        re.IGNORECASE
                            )

                    if re.fullmatch(timestamp_pattern, clean_value):
                        if '.' in clean_value:
                            clean_value = clean_value.split('.')[0]
                        clean_value = clean_value.replace('T', ' ')
                        logging.error(f"I BEEN REDEEMED::::::{clean_value}")
                        return f"\"{col}\" = '{clean_value}'"
                    logging.error(f"I REDEMPTION FAILED I DON'T MATCH::::::{value}")
                    return f"\"{col}\" = NULL"

            else:
                return f"\"{col}\" = NULL"
                    
        except Exception:
            logging.info(f"I HAVE EXCEPTIONED====={value}")
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
    drop_keywords=['surname','firstname']
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
            if("twenty_8_day_follow_up" in table_name):
                df = reorder_dataframe_columns(df,script=table_name)
            for col in df.columns:
                dtype = str(df[col].dtype)
                pg_type = dtype_map.get(dtype, 'TEXT')  # Fallback to TEXT
                create_cols.append(f'"{col}" {pg_type}')

            create_stmt = f'CREATE TABLE IF NOT EXISTS {schema}."{table_name}" ({",".join(create_cols)});;'
            columns_to_drop = df.columns[
                df.columns.str.lower().str.contains('|'.join([kw.lower() for kw in drop_keywords]))
            ]
            #DROP CONFIDENTIAL COLUMNS
            df = df.drop(columns=columns_to_drop)

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


def reorder_dataframe_columns(df:pd.DataFrame, script:str):
    """
    Reorders DataFrame columns based on a case-insensitive match with desired_order,
    keeping unmatched columns at the end in their original order.
    
    Parameters:
        df (pd.DataFrame): Input DataFrame
        desired_order (list): List of base column names to match (case-insensitive)
        
    Returns:
        pd.DataFrame: DataFrame with reordered columns
    """
    # Get all DataFrame columns
    df_columns = df.columns.tolist()
    desired_order = columns_order(script)
    # Split columns into: (1) Matches desired order, (2) Others
    ordered_cols = []
    other_cols = []
    
    for col in df_columns:
        # Check if column matches any base in desired_order (case-insensitive)
        col_lower = col.lower()
        match_found = any(
            re.match(f"^{base.lower()}(\.|$)", col_lower)
            for base in desired_order
        )
        
        if match_found:
            ordered_cols.append(col)
        else:
            other_cols.append(col)
    
    # Sort matched columns based on desired_order priority
    def get_sort_key(col_name):
        col_lower = col_name.lower()
        for i, base in enumerate(desired_order):
            if re.match(f"^{base.lower()}(\.|$)", col_lower):
                return (i, col_name)  # Sort first by order priority, then by name
        return (len(desired_order), col_name)  # Shouldn't happen for matched cols
    
    ordered_cols_sorted = sorted(ordered_cols, key=get_sort_key)
    
    # Combine columns
    final_columns = ordered_cols_sorted + other_cols
    
    # Return reordered DataFrame
    return df[final_columns]


def generate_label_fix_updates(filtered_records, table_name:str):
    if filtered_records is not None:
        return []

    groups = defaultdict(list)

    for row in filtered_records:
        update_keys = tuple(sorted(k for k in row if k not in ['uid', 'unique_key']))
        groups[update_keys].append(row)

    sql_batches = []

    for update_columns in groups:
        update_cols = list(update_columns)
        value_columns = ['uid', 'unique_key'] + update_cols
        rows = groups[update_columns]

        values = [
            tuple(row.get(col) for col in value_columns)
            for row in rows
        ]

        alias = 'v'
        set_clause = ", ".join([
            f"{col} = {alias}.{col}" for col in update_cols
        ])
        columns_str = ", ".join(value_columns)

        sql = f"""
            UPDATE derived."{table_name}" AS t SET
                {set_clause}
            FROM (
                VALUES %s
            ) AS {alias}({columns_str})
            WHERE t.uid = {alias}.uid AND t.unique_key = {alias}.unique_key
        """
        logging.info(f"###--FERE--{sql}")

        sql_batches.append((sql, values))

    return sql_batches

def run_bulky_query(table:str,filtered_records=None):
 # your connection
    if filtered_records is None: 
        pass
    else:
        conn = engine.raw_connection()
        cur = conn.cursor()

        sql_batches = generate_label_fix_updates(filtered_records, table_name=table)

        for sql, values in sql_batches:
            execute_values(cur, sql, values)
        conn.commit()
    

def columns_order (script: str):
    if "twenty_8_day_follow_up" in script:
        return [
    "Dobtob",
    "Feedasse",
    "CurrCond",
    "FeedAsse",
    "ReasAdmi",
    "ReceWeigKnow",
    "ReceWeig",
    "Anyilln",
    "NameIlln",
    "MediHelp",
    "MediHelpType",
    "MediHelpWher",
    "Hosp",
    "NameHosp",
    "MediGive",
    "MediGiveType",
    "TermPre",
    "PreTermClinAtte",
    "DatePreTermClinAtte",
    "AnySupp",
    "AnySuppType",
    "Day3Chec",
    "Day7Chec",
    "Day7WeigKno",
    "Day7Weig",
    "BabyPass",
    "ReasQuit",
    "OthReasQuit",
    "DatetimeDeath",
    "PlacOfDeat",
    "CauseofDeatKnow",
    "CauseDeath",
    "CauseDeathOther",
    "MediHelp",
    "MediHelpType",
    "MediHelpWher",
    "ReceWeigKnow",
    "ReceWeig",
    "MediGive",
    "MediGIveType",
    "Termpre"
]
