import logging
import sys
import json
import re
from typing import TYPE_CHECKING
from collections import defaultdict
from datetime import datetime, date

import pandas as pd
import numpy as np

from conf.common.logger import setup_logger
from conf.common.format_error import formatError
from .config import config

# Import handling with proper type checking
if TYPE_CHECKING:
    from sqlalchemy import Engine  # type: ignore  # noqa: F401
    from sqlalchemy.engine import Connection  # type: ignore  # noqa: F401
    from psycopg2 import sql as psycopg2_sql  # type: ignore  # noqa: F401
    from psycopg2.extras import execute_values as psycopg2_execute_values  # type: ignore  # noqa: F401

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.types import TEXT
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    create_engine = None  # type: ignore
    text = None  # type: ignore
    TEXT = None  # type: ignore

try:
    from psycopg2 import sql
    from psycopg2.extras import execute_values
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    sql = None  # type: ignore
    execute_values = None  # type: ignore

params = config()
# Postgres Connection String
con = 'postgresql+psycopg2://' + \
params["user"] + ':' + params["password"] + '@' + \
params["host"] + ':' + '5432' + '/' + params["database"]

con_string = f'''postgresql://{params["user"]}:{params["password"]}@{params["host"]}:5432/{params["database"]}'''

# Create SQLAlchemy engine with connection pooling
if SQLALCHEMY_AVAILABLE and create_engine:
    engine = create_engine(
        con,
        pool_size=5,        # Maximum number of connections in the pool
        max_overflow=10,    # Maximum number of connections to allow in excess of pool_size
        pool_timeout=30,    # Maximum number of seconds to wait for a connection to become available
        pool_recycle=1800   # Number of seconds a connection can persist before being recycled
    )
else:
    engine = None  # type: ignore
#Useful functions to inject sql queries
#Inject SQL Procedures
QUERY_LOG_PATH="logs/queries.log"
query_logger = setup_logger(QUERY_LOG_PATH,'queries')

def inject_sql_procedure(sql_script, file_name):
    """Execute a SQL procedure using raw psycopg2 connection."""
    if not engine:
        raise RuntimeError("Database engine not initialized")

    # Get raw psycopg2 connection directly from pool
    raw_conn = engine.connect()
    try:
        cur = raw_conn.cursor()
        try:
            cur.execute(sql_script)
            raw_conn.commit()
            query_logger.info(f"::DEDUPLICATING::")
            query_logger.info(f"DEDUP-{sql_script}")
        except Exception as e:
            raw_conn.rollback()
            logging.error(e)
            logging.error('Something went wrong with the SQL file')
            logging.error(sql_script)
            sys.exit()
        finally:
            cur.close()
    finally:
        raw_conn.close()
    logging.info('... {0} has successfully run'.format(file_name))

def insert_session(sess):
    """Insert a session into the database using parameterized query."""
    if not engine:
        raise RuntimeError("Database engine not initialized")

    # Serialize session dict to JSON
    insertion_data = json.dumps(sess)
    ingested_at = datetime.now()
    uid = sess.get("uid")
    scriptId = sess.get("script", {}).get("id")

    # Get raw psycopg2 connection for parameterized query
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        try:
            # Parameterized query — safe for any data
            insertion_query = """
                INSERT INTO public.sessions (ingested_at, uid, scriptid, data)
                VALUES (%s, %s, %s, %s)
            """
            cur.execute(insertion_query, (ingested_at, uid, scriptId, insertion_data))
            raw_conn.commit()
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            cur.close()
    finally:
        raw_conn.close()

def inject_sql(sql_script, file_name):
    """Execute SQL commands with automatic transaction management."""
    if not engine or not text:
        raise RuntimeError("Database engine not initialized")

    if not sql_script.strip():  # skip if empty
        return

    sql_commands = str(sql_script).split(';;')

    try:
        # Use begin() for automatic transaction management with auto-commit/rollback
        with engine.connect() as conn:
            for command in sql_commands:
                command = command.strip()
                if not command:
                    continue
                try:
                    conn.execute(text(command))
                except Exception as e:
                    logging.error(f"Error executing command in {file_name}")
                    logging.error(f"Error type: {type(e)}")
                    logging.error(f"Full error: {str(e)}")
                    raise

    except Exception as e:
        logging.error(f"Transaction failed completely for {file_name}")
        raise

def generate_timestamp_conversion_query(table_name, columns):
    """
    Generate PostgreSQL query to convert columns to TIMESTAMP type.
    
    Args:
        table_name (str): Table name (can include schema, e.g., 'schema.table')
        columns (list): List of column names to convert
        
    Returns:
        Void
    """
    if not columns:
        return ""
    
    # Split table name into schema and table if needed
    if '.' in table_name:
        schema, table = table_name.split('.', 1)
        full_table_name = f'"{schema}"."{table}"'
    else:
        full_table_name = f'"{table_name}"'
    
    # Generate individual ALTER COLUMN statements
    alter_statements = []
    for column in columns:
        alter_statements.append(
            f'ALTER COLUMN "{column}" TYPE TIMESTAMP USING "{column}"::TIMESTAMP'
        )
    
    # Combine into single query
    query = f'ALTER TABLE {full_table_name}\n'
    query += ',\n'.join(alter_statements) + ';;'
    if query:
        inject_sql(query,'ENFORCE TIMESTAMP COLUMNS')
   

def create_table(df: pd.DataFrame, table_name):
    """Create tables in derived schema."""
    if not engine:
        raise RuntimeError("Database engine not initialized")

    try:
        df.to_sql(table_name, con=engine, schema='derived', if_exists='replace', index=False)
    except Exception as e:
        logging.error(e)
        raise e

def create_exploded_table(df: pd.DataFrame, table_name):
    """Create tables in derived schema and restrict all columns to Text."""
    if not engine or not TEXT:
        raise RuntimeError("Database engine not initialized")

    try:
        if "How is the baby being fed?.label" in df:
            df.rename(columns={'FeedAsse.label': 'How is the baby being fed?.label'}, inplace=True)

        if "How is the baby being fed?.value" in df:
            df.rename(columns={'FeedAsse.value': 'How is the baby being fed?.value'}, inplace=True)
        if "How is the baby being fed" in table_name:
            table_name = "exploded_twenty_8_day_follow_u_FeedAsse.label"
        if "exploded_daily_review_Checklist" in table_name:
            table_name = "exploded_daily_review_Surgcheck"

        df.to_sql(table_name, con=engine, schema='derived', if_exists='append', index=False, dtype={col_name: TEXT for col_name in df})
    except Exception as ex:
        logging.error(f"FAILED DF:\n{df.to_string()}")
        logging.error(f'ERR DF=={ex}')

def append_data(df: pd.DataFrame, table_name):
    """Add data to an existing table in batches."""
    if not engine:
        raise RuntimeError("Database engine not initialized")

    batch_size = 1000
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]

        # Convert to values you can inspect
        values = batch.replace({pd.NA: None, np.nan: None}).to_dict('records')
        print(f"Batch {i} sample:", values[0] if values else "No data")

        # Insert the batch
        batch.to_sql(table_name, con=engine, schema='derived',
                     if_exists='append', index=False)
    logging.info(f"Appended {len(df)} rows to {table_name} in {(len(df) // batch_size) + 1} batches")

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
    """Execute SQL and return results as list of tuples."""
    if not engine or not text:
        raise RuntimeError("Database engine not initialized")

    if not sql_script.strip():  # skip empty commands
        return []

    try:
        # Use connect() for read operations - no transaction needed
        with engine.connect() as conn:
            result = conn.execute(text(sql_script))
            data = list(result.fetchall())  # return list of tuples
            return data

    except Exception as e:
        logging.error(f"Error executing SQL: {e}")
        raise

    

def inject_bulk_sql(queries, batch_size=1000):
    """Execute multiple SQL queries in batches with transaction management."""
    if not engine or not text:
        raise RuntimeError("Database engine not initialized")

    try:
        # Process queries in batches
        for i in range(0, len(queries), batch_size):
            batch = queries[i:i + batch_size]

            # Use begin() for automatic transaction commit/rollback per batch
            with engine.begin() as conn:
                for command in batch:
                    try:
                        conn.execute(text(command))
                    except Exception as e:
                        logging.error(f"Error executing command: {command}")
                        logging.error(e)
                        raise e

        logging.info("########################### DONE BULK PROCESSING ################")

    except Exception as e:
        logging.error("Something went wrong with the SQL batch processing")
        logging.error(e)
        raise

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

def get_confidential_columns(table_name,table_schema):
    query= f'''
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{table_schema}'
                AND table_name = '{table_name}'
                AND (
                    column_name ILIKE '%dobtob%'
                    OR column_name ILIKE '%firstname%'
                    OR column_name ILIKE '%lastname%'
                );;
            '''
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
        
def run_query_and_return_df(query) -> pd.DataFrame:
    """Execute query and return results as pandas DataFrame."""
    if not engine or not text:
        raise RuntimeError("Database engine not initialized")

    try:
        if not isinstance(query, str):
            logging.warning(f"Query is of type {type(query)}, converting to string")
            query = str(query)
        # Use connect() for read operations
        with engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn)
        return df
    except Exception as ex:
        logging.error(f"Error in run_query_and_return_df: {formatError(ex)}")
        logging.error(f"Query that caused error: {query}")
        return pd.DataFrame()


def generate_upsert_queries_and_create_table(table_name: str, df: pd.DataFrame):
    """Generate and execute UPSERT queries with automatic table/column creation."""
    if not engine or not sql or not PSYCOPG2_AVAILABLE:
        raise RuntimeError("Database engine and psycopg2 not initialized")

    if df.empty:
        return

    schema = 'derived'

    # Get raw psycopg2 connection for complex operations with psycopg2.sql
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        try:
            # Step 1: Check if table exists
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

                # Add unique constraint
                constraint_name = f"{table_name}_uid_form_created_facility_review_uq"
                constraint_query = sql.SQL("""
                        ALTER TABLE {schema}.{table}
                        ADD CONSTRAINT {constraint}
                        UNIQUE (uid, form_id, created_at, facility, review_number);
                    """).format(
                        schema=sql.Identifier(schema),
                        table=sql.Identifier(table_name),
                        constraint=sql.Identifier(constraint_name)
                    )
                cur.execute(constraint_query)

            # Step 2: Ensure all columns exist
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """, (schema, table_name))
            existing_cols = {row[0] for row in cur.fetchall()}

            for col in df.columns:
                if col not in existing_cols:
                    alter_query = sql.SQL(
                        "ALTER TABLE {}.{} ADD COLUMN {} TEXT DEFAULT NULL"
                    ).format(
                        sql.Identifier(schema),
                        sql.Identifier(table_name),
                        sql.Identifier(col)
                    )
                    cur.execute(alter_query)

            # Step 3: Generate and execute UPSERTs
            for _, row in df.iterrows():
                columns = list(row.index)
                values = []
                for col in columns:
                    val = row[col]
                    # Use explicit scalar check to avoid Series[bool] type issues
                    if val is None or val is pd.NA or (isinstance(val, float) and np.isnan(val)):
                        values.append(None)
                    else:
                        values.append(val)

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

            raw_conn.commit()
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            cur.close()
    except Exception as ex:
        logging.error(f"Error upserting into '{schema}.{table_name}': {ex}")
        raise
    finally:
        raw_conn.close()


def generateAndRunUpdateQuery(table: str, df: pd.DataFrame):
    """Optimized bulk update using PostgreSQL UPDATE FROM VALUES syntax."""
    try:
        if table is None or df is None or df.empty:
            return

        # OPTIMIZATION 1: Batch fetch all column types in a single query
        column_types = {}
        if not df.columns.empty:
            columns_list = "','".join(df.columns)
            batch_type_query = f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'derived'
                AND table_name = 'joined_admissions_discharges'
                AND column_name IN ('{columns_list}')
            """
            results = inject_sql_with_return(batch_type_query)
            column_types = {row[0]: row[1] for row in results} if results else {}

        # Add 'unknown' for columns not found in table
        for col in df.columns:
            if col not in column_types:
                column_types[col] = 'unknown'

        # OPTIMIZATION 2: Use PostgreSQL UPDATE...FROM VALUES for bulk updates
        # This is much faster than individual UPDATE statements per row

        # Build the values for the UPDATE...FROM VALUES clause
        values_rows = []
        for _, row in df.iterrows():
            row_values = []
            # Always include WHERE clause columns first
            uid_val = escape_special_characters(str(row['uid']))
            facility_val = escape_special_characters(str(row['facility']))
            unique_key_val = escape_special_characters(str(row['unique_key']))

            row_values.append(f"'{uid_val}'")
            row_values.append(f"'{facility_val}'")
            row_values.append(f"'{unique_key_val}'")

            # Add update columns
            for col in df.columns:
                if col in ['uid', 'facility', 'unique_key']:
                    continue

                val = row[col]
                col_type = column_types.get(col, 'unknown')

                # Format value based on type
                if pd.isna(val) or str(val) in {'NaT', 'None', 'nan', '<NA>', ''}:
                    row_values.append("NULL")
                elif 'timestamp' in col_type.lower() or 'date' in col_type.lower():
                    if isinstance(val, (datetime, pd.Timestamp)):
                        row_values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                    else:
                        row_values.append("NULL")
                elif col_type == 'text' or col_type == 'unknown':
                    row_values.append(f"'{escape_special_characters(str(val))}'")
                else:
                    row_values.append(str(val))

            values_rows.append(f"({', '.join(row_values)})")

        if not values_rows:
            return

        # Build SET clause for all columns except WHERE clause columns
        update_cols = [col for col in df.columns if col not in ['uid', 'facility', 'unique_key']]
        set_clauses = [f'"{col}" = v."{col}"' for col in update_cols]

        # Build column list for VALUES clause
        value_columns = ['uid', 'facility', 'unique_key'] + update_cols
        columns_str = ', '.join([f'"{col}"' for col in value_columns])

        # Construct the bulk UPDATE query
        values_str = ',\n'.join(values_rows)
        update_query = f"""
            UPDATE {table} AS t
            SET {', '.join(set_clauses)}
            FROM (VALUES
                {values_str}
            ) AS v({columns_str})
            WHERE t.uid = v.uid
            AND t.facility = v.facility
            AND t."unique_key" = v."unique_key";;
        """

        inject_sql(update_query, f"BULK UPDATE {table}")
        logging.info(f"Successfully bulk updated {len(values_rows)} rows in {table}")

    except Exception as ex:
        logging.error("!!! An error occurred whilst JOINING DATA THAT WAS UNJOINED")
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

def generate_postgres_insert(df, schema, table_name):
    """Optimized bulk insert with vectorized operations where possible."""
    # Ensure we only keep "real" columns (skip weird 1-char column names)
    df = df[[col for col in df.columns if len(col) > 1]].copy()
    if 'transformed' not in df.columns:
        df['transformed'] = False

    # OPTIMIZATION 1: Vectorized filtering of invalid rows
    # Filter out rows with invalid uid or unique_key
    if 'uid' in df.columns:
        df = df[df['uid'].notna()]
        df = df[~df['uid'].astype(str).str.strip().str.lower().isin(['null', 'nan', 'nat', '<na>', ''])]

    if 'unique_key' in df.columns:
        df = df[df['unique_key'].notna()]
        df = df[~df['unique_key'].astype(str).str.strip().str.lower().isin(['null', 'nan', 'nat', '<na>', ''])]

    if df.empty:
        return

    # OPTIMIZATION 2: Process values more efficiently
    # Prepare columns list (filter out single-char column names)
    valid_columns = [col for col in df.columns if len(str(col)) > 1]
    columns_str = ', '.join([f'"{col}"' for col in valid_columns])

    # Build values rows
    values_rows = []
    for idx in df.index:
        row_values = []
        for col in valid_columns:
            val = df.at[idx, col]

            # NULL handling
            if pd.isna(val) or str(val) in {'NaT', 'None', 'nan', '', '<NA>'}:
                row_values.append("NULL")
                continue

            # Handle specific types
            if col == 'unique_key':
                row_values.append(f"'{str(val)}'")
            elif isinstance(val, (list, dict)):
                json_val = json.dumps(val)
                row_values.append(f"'{escape_special_characters(json_val)}'")
            elif isinstance(val, (pd.Timestamp, pd.Timedelta)):
                converted = f"'{clean_datetime_string(str(val))}'"
                row_values.append("NULL" if converted.strip("'") in {'NaT', 'None', 'nan', '', '<NA>'} else converted)
            elif is_date_prefix(str(val)) and col != 'unique_key':
                converted_date_like = f"'{clean_datetime_string(str(val))}'"
                row_values.append("NULL" if converted_date_like.strip("'") in {'NaT', 'None', 'nan', '', '<NA>'} else converted_date_like)
            elif isinstance(val, str):
                row_values.append(f"'{escape_special_characters(val)}'")
            else:
                row_values.append("NULL" if str(val) in {'NaT', 'None', 'nan', '', '<NA>'} else str(val))

        if row_values:
            values_rows.append(f"({', '.join(row_values)})")

    if not values_rows:
        return

    # OPTIMIZATION 3: Use batch inserts for very large datasets
    # Split into batches of 1000 rows to avoid query size limits
    batch_size = 1000
    for i in range(0, len(values_rows), batch_size):
        batch = values_rows[i:i + batch_size]
        values_str = ',\n'.join(batch)
        insert_query = f'INSERT INTO {schema}."{table_name}" ({columns_str}) VALUES\n{values_str};;'
        inject_sql(insert_query, f"INSERTING BATCH {i//batch_size + 1} INTO {table_name}")

    logging.info(f"Successfully inserted {len(values_rows)} rows into {schema}.{table_name}")


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
                    dt = pd.to_datetime(clean_value, errors='raise')
                    return f"\"{col}\" = '{dt.strftime('%Y-%m-%d %H:%M:%S')}'"
                except ValueError:
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
                return f"\"{col}\" = '{value.strftime('%Y-%m-%d')}'"
            elif isinstance(value, str):
                try:
                    dt = pd.to_datetime(value, errors='raise', format='%Y-%m-%d').tz_localize(None)
                    return f"\"{col}\" = '{dt.strftime('%Y-%m-%d')}'"
                except:
                    clean_value = value.split('T')[0].strip()
                    if re.match(r'^\d{4}-\d{2}-\d{2}$', clean_value):
                        return f"\"{col}\" = '{clean_value}'"
                    return f"\"{col}\" = NULL"
            else:
                return f"\"{col}\" = NULL"
        except:
            return f"\"{col}\" = NULL"
            
    elif col_type == 'text' or col_type == 'unknown':
        return f"\"{col}\" = '{escape_special_characters(str(value))}'"
    else:
        return f"\"{col}\" = {value}"
    
def generate_create_insert_sql(df,schema, table_name):
    # Infer PostgreSQL types
    drop_keywords=['surname','firstname','dobtob','column_name','mothcell','dob.value',"dob.label","kinaddress","kincell","kinname"]

    try:
        if table_exists(schema,table_name) is False:
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
            if "neolab" in table_name:
                drop_table = f'DROP TABLE IF EXISTS {schema}."{table_name}";;'
                inject_sql(drop_table,f"DROPPING {table_name}")

            for col in df.columns:
                dtype = str(df[col].dtype)
                pg_type = dtype_map.get(dtype, 'TEXT')  # Fallback to TEXT
                create_cols.append(f'"{col}" {pg_type}')

            create_stmt = f'CREATE TABLE IF NOT EXISTS {schema}."{table_name}" ({",".join(create_cols)});;'
            
            inject_sql(create_stmt,f"CREATING {table_name}")
        #DROP CONFIDENTIAL COLUMNS
        columns_to_drop = df.columns[
                df.columns.str.lower().str.contains('|'.join([kw.lower() for kw in drop_keywords]))
            ]
        df = df.drop(columns=columns_to_drop)
        df['transformed'] = False  

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
                );'''
    query_result = inject_sql_with_return(query)
    
    if query_result and len(query_result) > 0:
       return query_result[0][0]
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
            re.match(rf"^{base.lower()}(\.|$)", col_lower)
            for base in desired_order or []
        )
        
        if match_found:
            ordered_cols.append(col)
        else:
            other_cols.append(col)
    
    # Sort matched columns based on desired_order priority
    def get_sort_key(col_name):
        col_lower = col_name.lower()
        for i, base in enumerate(desired_order or []):
            if re.match(rf"^{base.lower()}(\.|$)", col_lower):
                return (i, col_name)  # Sort first by order priority, then by name
        return (len(desired_order or []), col_name)  # Shouldn't happen for matched cols
    
    ordered_cols_sorted = sorted(ordered_cols, key=get_sort_key)
    
    # Combine columns
    final_columns = ordered_cols_sorted + other_cols
    
    # Return reordered DataFrame
    return df[final_columns]


def generate_label_fix_updates(filtered_records, table_name:str):
    if filtered_records is None:
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

        sql_batches.append((sql, values))

    return sql_batches

def run_bulky_query(table: str, filtered_records=None):
    """Execute bulk update queries using execute_values for performance."""
    if not engine or not execute_values or not PSYCOPG2_AVAILABLE:
        raise RuntimeError("Database engine and psycopg2 not initialized")

    if filtered_records is None:
        return

    # Get raw psycopg2 connection for execute_values
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        try:
            sql_batches = generate_label_fix_updates(filtered_records, table_name=table)

            for sql_query, values in sql_batches:
                execute_values(cur, sql_query, values)

            raw_conn.commit()
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            cur.close()
    except Exception as ex:
        logging.error(f"Error in run_bulky_query for table '{table}': {ex}")
        raise
    finally:
        raw_conn.close()
    

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
