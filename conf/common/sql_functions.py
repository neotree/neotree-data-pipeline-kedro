import logging
import sys
import json
import re
from typing import TYPE_CHECKING, Optional, Dict
from collections import defaultdict
from datetime import datetime, date

import pandas as pd
import numpy as np

from conf.common.logger import setup_logger
from conf.common.format_error import formatError
from .config import config
from data_pipeline.pipelines.data_engineering.utils.field_info import load_json_for_comparison

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine import Connection
    from psycopg2 import sql as psycopg2_sql
    from psycopg2.extras import execute_values as psycopg2_execute_values

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
if TYPE_CHECKING:
    engine: Optional['Engine'] = None
else:
    engine = None

if SQLALCHEMY_AVAILABLE and create_engine:
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
    """Execute a SQL procedure using raw psycopg2 connection."""
    if not engine:
        raise RuntimeError("Database engine not initialized")

    # Get raw psycopg2 connection directly from pool
    raw_conn = engine.raw_connection()  # type: ignore[union-attr]
    try:
        cur = raw_conn.cursor()  # type: ignore[union-attr]
        try:
            cur.execute(sql_script)
            raw_conn.commit()  # type: ignore[union-attr]
            query_logger.info(f"::DEDUPLICATING::")
            query_logger.info(f"DEDUP-{sql_script}")
        except Exception as e:
            raw_conn.rollback()  # type: ignore[union-attr]
            logging.error(e)
            logging.error('Something went wrong with the SQL file')
            logging.error(sql_script)
            sys.exit()
        finally:
            cur.close()
    finally:
        raw_conn.close()  # type: ignore[union-attr]
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
    raw_conn = engine.raw_connection()  # type: ignore[union-attr]
    try:
        cur = raw_conn.cursor()  # type: ignore[union-attr]
        try:
            # Parameterized query — safe for any data
            insertion_query = """
                INSERT INTO public.sessions (ingested_at, uid, scriptid, data)
                VALUES (%s, %s, %s, %s)
            """
            cur.execute(insertion_query, (ingested_at, uid, scriptId, insertion_data))
            raw_conn.commit()  # type: ignore[union-attr]
        except Exception:
            raw_conn.rollback()  # type: ignore[union-attr]
            raise
        finally:
            cur.close()
    finally:
        raw_conn.close()  # type: ignore[union-attr]

def inject_sql(sql_script, file_name):
    """Execute SQL commands with automatic transaction management."""
    if not engine or not text:
        raise RuntimeError("Database engine not initialized")

    if not sql_script.strip():  # skip if empty
        return

    sql_commands = str(sql_script).split(';;')

    # Use raw psycopg2 connection to commit after each command
    raw_conn = engine.raw_connection()  # type: ignore[union-attr]
    try:
        cur = raw_conn.cursor()  # type: ignore[union-attr]
        try:
            for idx, command in enumerate(sql_commands):
                command = command.strip()
                if not command:
                    continue
                try:
                    cur.execute(command)
                    raw_conn.commit()  # type: ignore[union-attr]  # Commit after each successful command
                except Exception as e:
                    logging.error(f"Error executing command in {file_name}")
                    logging.error(f"Command {idx + 1}/{len(sql_commands)}")
                    logging.error(f"Error type: {type(e)}")
                    logging.error(f"Full error: {str(e)}")

                    # Log the problematic SQL (truncated to avoid huge logs)
                    if len(command) > 500:
                        logging.error(f"Failed SQL (first 500 chars): {command[:500]}...")
                        # Try to extract the VALUES line that failed
                        lines = command.split('\n')
                        for i, line in enumerate(lines):
                            if 'VALUES' in line.upper() and i + 1 < len(lines):
                                logging.error(f"First VALUES line: {lines[i+1][:200]}")
                                break
                    else:
                        logging.error(f"Failed SQL: {command}")

                    logging.error(f"Note: Previously executed commands were already committed")
                    raise
        finally:
            cur.close()
    finally:
        raw_conn.close()  # type: ignore[union-attr]

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
        with engine.connect() as conn:  # type: ignore[union-attr]
            result = conn.execute(text(sql_script))
            data = list(result.fetchall())  #  type: ignore[union-attr]
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
            with engine.begin() as conn:  # type: ignore[union-attr]
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
    
def is_date_column_by_name(column_name: str, table_name: str = None) -> bool:
    """
    Conservatively detect if a column should be TIMESTAMP based on very specific patterns.

    Only matches columns that are clearly dates - does NOT match generic patterns.

    Args:
        column_name: Name of the column
        table_name: Optional table name to check field metadata

    Returns:
        True if column should be TIMESTAMP type
    """
    # Check column name patterns (case-insensitive)
    col_lower = column_name.lower()

    # CONSERVATIVE: Only match very specific date-related patterns
    # Use startswith/endswith or very specific contains to avoid false positives
    specific_patterns = [
        col_lower.startswith('date'),           # date, datetime, dateofbirth, dateofdeath
        col_lower.endswith('date'),             # admissiondate, dischargedate
        col_lower.endswith('datetime'),         # createddatetime
        'datetime' in col_lower and 'date' in col_lower,  # datetimeadmission
        col_lower.startswith('dob'),            # dob, dobtob
        col_lower.startswith('tob'),            # tob, time of birth (specific)
        col_lower == 'created_at',              # Exact match
        col_lower == 'updated_at',              # Exact match
        col_lower == 'deleted_at',              # Exact match
    ]

    # Return True only if at least one specific pattern matches
    if any(specific_patterns):
        return True

    # Check field metadata if available and table_name provided
    if table_name:
        try:
            schema = load_json_for_comparison(table_name)
            if schema:
                # Build field lookup
                field_info = {}
                if isinstance(schema, list):
                    field_info = {f['key']: f for f in schema}
                elif isinstance(schema, dict):
                    first_value = next(iter(schema.values()), None)
                    if isinstance(first_value, list):
                        field_info = {f['key']: f for f in first_value}
                    else:
                        field_info = schema

                # Check both exact match and without .value/.label suffix
                base_key = column_name
                if column_name.endswith('.value') or column_name.endswith('.label'):
                    base_key = column_name.rsplit('.', 1)[0]

                if base_key in field_info:
                    field = field_info[base_key]
                    data_type = field.get('dataType', field.get('type', ''))
                    if data_type in ['datetime', 'timestamp', 'date']:
                        return True
        except Exception:
            pass  # Metadata not available, continue with pattern matching

    return False


def get_expected_sql_type(col_type: str, column_name: str = None, table_name: str = None):
    """
    Map pandas dtype to PostgreSQL type with intelligent date detection.

    Args:
        col_type: Pandas dtype as string
        column_name: Optional column name for intelligent type detection
        table_name: Optional table name for metadata lookup

    Returns:
        PostgreSQL type string
    """
    # First check if pandas already detected it as datetime
    if "datetime" in col_type or "date" in col_type:
        return "TIMESTAMP"

    # Check if column name suggests it's a date (even if pandas says 'object')
    if column_name and is_date_column_by_name(column_name, table_name):
        return "TIMESTAMP"

    # Standard type mapping
    if col_type == "object":
        return "TEXT"
    elif "float" in col_type:
        return "DOUBLE PRECISION"
    elif "int" in col_type:
        return "INTEGER"
    else:
        return "TEXT"


def normalize_pg_type(pg_type):
    """Normalize PostgreSQL type names for comparison."""
    pg_type = pg_type.lower().strip()
    # Map variations to canonical types
    type_map = {
        'character varying': 'text',
        'varchar': 'text',
        'double precision': 'double precision',
        'timestamp without time zone': 'timestamp',
        'timestamp with time zone': 'timestamp',
        'integer': 'integer',
        'bigint': 'integer',
        'smallint': 'integer',
        'boolean': 'boolean',
        'bool': 'boolean'
    }
    return type_map.get(pg_type, pg_type)


def verify_and_fix_column_type(table_name, schema, column, expected_type):
    """Verify column type matches expected type, and fix if needed."""
    try:
        # Get current column type from database
        current_type_result = get_table_column_type(table_name, schema, column)

        if not current_type_result or len(current_type_result) == 0:
            return  # Column doesn't exist, will be created

        current_pg_type = current_type_result[0][0]
        normalized_current = normalize_pg_type(current_pg_type)
        normalized_expected = normalize_pg_type(expected_type)

        # Check if types match
        if normalized_current != normalized_expected:
            logging.warning(f"Column type mismatch for {schema}.{table_name}.{column}: "
                          f"DB has {current_pg_type}, expected {expected_type}")
            logging.info(f"Altering column {column} from {current_pg_type} to {expected_type}")

            # Use USING clause to handle type conversion
            if expected_type.upper() == "TEXT":
                # Convert to TEXT safely
                alter_query = f'''
                    ALTER TABLE "{schema}"."{table_name}"
                    ALTER COLUMN "{column}" TYPE TEXT USING "{column}"::TEXT;;
                '''
            elif "INTEGER" in expected_type.upper():
                alter_query = f'''
                    ALTER TABLE "{schema}"."{table_name}"
                    ALTER COLUMN "{column}" TYPE {expected_type} USING
                    CASE
                        WHEN "{column}"::TEXT ~ '^\d+$' THEN "{column}"::TEXT::{expected_type}
                        ELSE NULL
                    END;;
                '''
            elif "DOUBLE PRECISION" in expected_type.upper():
                alter_query = f'''
                    ALTER TABLE "{schema}"."{table_name}"
                    ALTER COLUMN "{column}" TYPE {expected_type} USING
                    CASE
                        WHEN "{column}"::TEXT ~ '^\d+\.?\d*$' THEN "{column}"::TEXT::{expected_type}
                        ELSE NULL
                    END;;
                '''
            else:
                # Generic conversion
                alter_query = f'''
                    ALTER TABLE "{schema}"."{table_name}"
                    ALTER COLUMN "{column}" TYPE {expected_type} USING "{column}"::{expected_type};;
                '''

            inject_sql(alter_query, f'ALTER {column} TYPE TO {expected_type} ON "{schema}"."{table_name}"')
            logging.info(f"Successfully altered {column} to {expected_type}")

    except Exception as e:
        logging.warning(f"Could not verify/fix column type for {column}: {e}")


def create_new_columns(table_name, schema, columns):
    """Create new columns with intelligent type detection for dates."""
    for column, col_type in columns:
        # Pass column name and table name for intelligent date detection
        expected_sql_type = get_expected_sql_type(col_type, column_name=column, table_name=table_name)

        if not column_exists(schema, table_name, column):
            # Column doesn't exist - create it
            alter_query = f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN IF NOT EXISTS "{column}" {expected_sql_type};;'
            inject_sql(alter_query, f'ADD {column} ON  "{schema}"."{table_name}"')
            logging.info(f"Created column {column} as {expected_sql_type} in {schema}.{table_name}")
        else:
            # Column exists - verify and fix type if needed
            verify_and_fix_column_type(table_name, schema, column, expected_sql_type)


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
        with engine.connect() as conn:  # type: ignore[union-attr]
            df = pd.read_sql_query(text(query), conn)
        return df
    except Exception as ex:
        logging.error(f"Error in run_query_and_return_df: {formatError(ex)}")
        logging.error(f"Query that caused error: {query}")
        return pd.DataFrame()


def generate_upsert_queries_and_create_table(table_name: str, df: pd.DataFrame):
    """Generate and execute UPSERT queries with automatic table/column creation and intelligent type detection."""
    if not engine or not sql or not PSYCOPG2_AVAILABLE:
        raise RuntimeError("Database engine and psycopg2 not initialized")

    if df.empty:
        return

    schema = 'derived'

    # Helper function to get column type
    def get_column_type(col_name: str) -> str:
        """Get PostgreSQL type for a column based on dtype and name."""
        dtype = str(df[col_name].dtype)
        return get_expected_sql_type(dtype, column_name=col_name, table_name=table_name)

    # Get raw psycopg2 connection for complex operations with psycopg2.sql
    raw_conn = engine.raw_connection()  # type: ignore[union-attr]
    try:
        cur = raw_conn.cursor()  # type: ignore[union-attr]
        try:
            # Step 1: Check if table exists
            cur.execute(
                sql.SQL("SELECT to_regclass(%s)"),
                [f"{schema}.{table_name}"]
            )
            result = cur.fetchone()
            if result[0] is None:
                # Create table with intelligent type detection for each column
                create_col_defs = []
                for col in df.columns:
                    col_type = get_column_type(col)
                    create_col_defs.append(f'"{col}" {col_type}')

                create_cols = ', '.join(create_col_defs)
                create_query = sql.SQL("CREATE TABLE {}.{} ({})").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name),
                    sql.SQL(create_cols)
                )
                cur.execute(create_query)
                logging.info(f"Created table {schema}.{table_name} with intelligent type detection")

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

            # Step 2: Ensure all columns exist with intelligent type detection
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """, (schema, table_name))
            existing_cols = {row[0] for row in cur.fetchall()}

            for col in df.columns:
                if col not in existing_cols:
                    col_type = get_column_type(col)
                    alter_query = sql.SQL(
                        "ALTER TABLE {}.{} ADD COLUMN {} {} DEFAULT NULL"
                    ).format(
                        sql.Identifier(schema),
                        sql.Identifier(table_name),
                        sql.Identifier(col),
                        sql.SQL(col_type)
                    )
                    cur.execute(alter_query)
                    logging.info(f"Added column {col} as {col_type} to {schema}.{table_name}")

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

            raw_conn.commit()  # type: ignore[union-attr]
        except Exception:
            raw_conn.rollback()  # type: ignore[union-attr]
            raise
        finally:
            cur.close()
    except Exception as ex:
        logging.error(f"Error upserting into '{schema}.{table_name}': {ex}")
        raise
    finally:
        raw_conn.close()  # type: ignore[union-attr]


def generateAndRunUpdateQuery(table: str, df: pd.DataFrame):
    """Optimized bulk update using PostgreSQL UPDATE FROM VALUES syntax, 
    with handling for boolean and numeric types."""
    try:
        if table is None or df is None or df.empty:
            return

        # OPTIMIZATION 1: Batch fetch all column types in a single query
        column_types = {}
        if not df.columns.empty:
            # Extract table name from parameter (handles both "schema.table" and "table" formats)
            if '.' in table:
                schema, table_name = table.rsplit('.', 1)
                # Remove quotes if present
                table_name = table_name.strip('"')
            else:
                schema = 'derived'
                table_name = table.strip('"')

            columns_list = "','".join(df.columns)
            batch_type_query = f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table_name}'
                AND column_name IN ('{columns_list}')
            """
            results = inject_sql_with_return(batch_type_query)
            column_types = {row[0]: row[1] for row in results} if results else {}

        # Add 'unknown' for columns not found in table
        for col in df.columns:
            if col not in column_types:
                column_types[col] = 'unknown'

        # Boolean mapping
        bool_map = {
            'y': True, 'yes': True, 'true': True, '1': True, True: True,
            'n': False, 'no': False, 'false': False, '0': False, False: False
        }

        # OPTIMIZATION 2: Use PostgreSQL UPDATE...FROM VALUES for bulk updates
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
                col_type = column_types.get(col, 'unknown').lower()

                # NULL handling (universal)
                if pd.isna(val) or str(val).strip().lower() in {'nat', 'none', 'nan', '<na>', ''}:
                    row_values.append("NULL")
                    continue

                #  Boolean handling
                if 'bool' in col_type:
                    val_str = str(val).strip().lower()
                    mapped = bool_map.get(val_str, None)
                    if mapped is None:
                        # Try direct type check fallback
                        mapped = bool_map.get(val, None)
                    if mapped is None:
                        row_values.append("NULL")
                    else:
                        row_values.append('TRUE' if mapped else 'FALSE')
                    continue

                # Numeric handling
                elif any(t in col_type for t in ['int', 'numeric', 'double', 'real', 'float', 'decimal']):
                    try:
                        num_val = float(val)
                        if np.isnan(num_val):
                            row_values.append("NULL")
                        else:
                            row_values.append(str(num_val))
                    except Exception:
                        row_values.append("NULL")
                    continue

                # timestamp/date handling
                elif 'timestamp' in col_type or 'date' in col_type:
                    if isinstance(val, (datetime, pd.Timestamp)):
                        row_values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                    elif isinstance(val, str):
                        try:
                            parsed_date = pd.to_datetime(val, errors='coerce')
                            if pd.notna(parsed_date):
                                row_values.append(f"'{parsed_date.strftime('%Y-%m-%d %H:%M:%S')}'")
                            else:
                                row_values.append("NULL")
                        except Exception:
                            row_values.append("NULL")
                    else:
                        row_values.append("NULL")
                    continue

                # Existing text/unknown handling
                elif col_type == 'text' or col_type == 'unknown':
                    row_values.append(f"'{escape_special_characters(str(val))}'")
                    continue

                # Default fallback
                else:
                    row_values.append(f"'{escape_special_characters(str(val))}'")

            values_rows.append(f"({', '.join(row_values)})")

        if not values_rows:
            return

        # Build SET clause for all columns except WHERE clause columns
        update_cols = [col for col in df.columns if col not in ['uid', 'facility', 'unique_key']]

        set_clauses = []
        for col in update_cols:
            col_type = column_types.get(col, 'unknown').lower()
            if 'timestamp' in col_type or 'date' in col_type:
                set_clauses.append(f'"{col}" = v."{col}"::TIMESTAMP')
            elif 'bool' in col_type:
                set_clauses.append(f'"{col}" = v."{col}"::BOOLEAN')
            elif any(t in col_type for t in ['int', 'numeric', 'double', 'real', 'float', 'decimal']):
                set_clauses.append(f'"{col}" = v."{col}"::NUMERIC')
            else:
                set_clauses.append(f'"{col}" = v."{col}"')

        value_columns = ['uid', 'facility', 'unique_key'] + update_cols
        columns_str = ', '.join([f'"{col}"' for col in value_columns])

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

def transform_dataframe_with_field_info(df, table_name):
    """
    Transform dataframe using field info for data type conversion and label correction.

    Args:
        df: DataFrame to transform
        table_name: Script name to load field info from

    Returns:
        Transformed DataFrame
    """

    # Load field info using table_name as script
    schema = load_json_for_comparison(table_name)
    if not schema:
        logging.info(f"No field info found for {table_name}, skipping transformation")
        return df

    # Create field lookup
    field_info = {f['key']: f for f in schema}

    # Create a copy for transformation
    transformed_df = df.copy()

    # Columns to exclude from transformation
    exclude_cols = ['transformed', 'unique_key', 'uid']

    for value_col in [col for col in df.columns if col.endswith('.value')]:
        base_key = value_col[:-6]
        label_col = f"{base_key}.label"

        # Skip excluded columns
        if base_key in exclude_cols:
            continue

        # Skip if field not in schema
        if base_key not in field_info:
            continue

        # Skip if value column doesn't exist (defensive check)
        if value_col not in transformed_df.columns:
            continue

        try:
            field = field_info[base_key]
            data_type = field.get('dataType', field.get('type', ''))
            options = field.get('options', [])

            # ====================
            # 1. DATA TYPE CONVERSION
            # ====================

            # Convert dates
            if data_type in ['datetime', 'timestamp', 'date']:
                for idx in transformed_df.index:
                    val = transformed_df.at[idx, value_col]
                    if pd.notna(val) and str(val).strip() not in ['', 'NaT', 'None', 'nan']:
                        # Try multiple date conversion methods
                        converted = None
                        try:
                            # Method 1: pandas to_datetime
                            converted = pd.to_datetime(val, errors='coerce')
                        except:
                            pass

                        if pd.isna(converted):
                            try:
                                # Method 2: Try with different date formats
                                converted = pd.to_datetime(str(val).strip(), errors='coerce')
                            except:
                                pass

                        if pd.notna(converted):
                            transformed_df.at[idx, value_col] = converted
                        else:
                            # Set to None if conversion fails
                            transformed_df.at[idx, value_col] = None

            # Convert numbers
            elif data_type in ['number', 'integer', 'float', 'timer']:
                for idx in transformed_df.index:
                    val = transformed_df.at[idx, value_col]
                    if pd.notna(val) and str(val).strip() not in ['', 'NaT', 'None', 'nan']:
                        try:
                            # Try to convert to numeric
                            converted = pd.to_numeric(val, errors='coerce')
                            if pd.notna(converted):
                                transformed_df.at[idx, value_col] = converted
                            else:
                                transformed_df.at[idx, value_col] = None
                        except:
                            transformed_df.at[idx, value_col] = None

            # Handle booleans - maintain values, check against options
            elif data_type in ['boolean', 'yesno']:
                if options:
                    # Get valid values from options
                    valid_values = [str(opt.get('value', '')).strip() for opt in options]
                    for idx in transformed_df.index:
                        val = transformed_df.at[idx, value_col]
                        if pd.notna(val):
                            val_str = str(val).strip()
                            # Keep value if it's in valid options
                            if val_str not in valid_values:
                                # Try case-insensitive match
                                matched = False
                                for valid_val in valid_values:
                                    if val_str.lower() == valid_val.lower():
                                        transformed_df.at[idx, value_col] = valid_val
                                        matched = True
                                        break
                                if not matched:
                                    transformed_df.at[idx, value_col] = None

            # ====================
            # 2. LABEL CORRECTION
            # ====================

            # Only proceed if label column exists AND options are available
            if label_col in transformed_df.columns and options and len(options) > 0:
                try:
                    # Build value to label mapping
                    value_to_label = {str(opt.get('value', '')).strip(): str(opt.get('valueLabel', '')).strip()
                                    for opt in options if opt.get('value') is not None}

                    # Skip if no valid mappings
                    if not value_to_label:
                        continue

                    field_type = field.get('type', '')

                    # Fix inverted values first (where value and label are swapped)
                    inverted_mask = (
                        transformed_df[value_col].isin(value_to_label.values()) &
                        transformed_df[label_col].isin(value_to_label.keys())
                    )
                    if inverted_mask.any():
                        # Swap value and label
                        temp_values = transformed_df.loc[inverted_mask, value_col].copy()
                        transformed_df.loc[inverted_mask, value_col] = transformed_df.loc[inverted_mask, label_col]
                        transformed_df.loc[inverted_mask, label_col] = temp_values

                    # Set label to None where value is None
                    null_mask = transformed_df[value_col].isna()
                    transformed_df.loc[null_mask, label_col] = None

                    # Fix labels based on values
                    non_null_mask = transformed_df[value_col].notna()
                    if non_null_mask.any():
                        if field_type in ('multi_select', 'checklist'):
                            # Handle multi-select fields
                            transformed_df.loc[non_null_mask, label_col] = transformed_df.loc[non_null_mask, value_col].apply(
                                lambda x: ','.join([
                                    value_to_label.get(v.strip(), v.strip())
                                    for v in str(x).split(',') if v.strip()
                                ]) if pd.notna(x) else None
                            )
                        else:
                            # Single select fields
                            for idx in transformed_df[non_null_mask].index:
                                val_str = str(transformed_df.at[idx, value_col]).strip()
                                if val_str in value_to_label:
                                    transformed_df.at[idx, label_col] = value_to_label[val_str]
                except KeyError as e:
                    # Log and skip if column doesn't exist
                    logging.warning(f"Column not found during label correction for {base_key}: {str(e)}")
                    continue
                except Exception as e:
                    # Log other errors but continue processing
                    logging.warning(f"Error during label correction for {base_key}: {str(e)}")
                    continue

        except KeyError as e:
            # Column doesn't exist - skip this field entirely
            logging.warning(f"Column not found for {base_key}: {str(e)}")
            continue
        except Exception as e:
            # Any other error - log and continue
            logging.warning(f"Error processing field {base_key}: {str(e)}")
            continue

    # Mark as transformed
    transformed_df['transformed'] = True

    return transformed_df


def generate_postgres_insert(df, schema, table_name):
    """Optimized bulk insert with field-info-based data transformation.

    IMPORTANT: Column order is preserved to ensure data integrity.
    The INSERT statement columns and values are built in the same order from df.columns.
    """
    # Make a copy to avoid modifying the original dataframe
    df = df.copy()

    # STEP 1: Transform data using field info
    try:
        df = transform_dataframe_with_field_info(df, table_name)
    except Exception as e:
        logging.warning(f"Field info transformation failed for {table_name}: {str(e)}, proceeding without transformation")
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

    logging.info("::::::::---ADMISSION DATA FRAME IS NOT NULL")

    # OPTIMIZATION 2: Build column list and ensure consistent ordering
    # Store the exact column order that will be used for both column names and values
    valid_columns = list(df.columns)
    columns_str = ', '.join([f'"{col}"' for col in valid_columns])

    logging.info(f"Inserting into {schema}.{table_name} with {len(valid_columns)} columns: {', '.join(valid_columns[:5])}..." if len(valid_columns) > 5 else f"Inserting with columns: {', '.join(valid_columns)}")

    # Verify table structure matches dataframe columns and create missing columns dynamically
    try:
        table_cols = get_table_column_names(table_name, schema)
        table_col_names = [col[0] for col in table_cols] if table_cols else []
        missing_in_table = set(valid_columns) - set(table_col_names)
        missing_in_df = set(table_col_names) - set(valid_columns)

        if missing_in_table:
            logging.warning(f"DataFrame has {len(missing_in_table)} columns not in table: {missing_in_table}")
            logging.info(f"Creating missing columns dynamically for table {schema}.{table_name}")
            # Create missing columns dynamically
            column_pairs = [(col, str(df[col].dtype)) for col in missing_in_table]
            if column_pairs:
                create_new_columns(table_name, schema, column_pairs)
                logging.info(f"Successfully created {len(column_pairs)} missing columns")
        if missing_in_df:
            logging.warning(f"Table has extra columns not in DataFrame: {missing_in_df}")
    except Exception as e:
        logging.warning(f"Could not verify/update table structure: {e}")

    # Build values rows - MUST iterate in the same order as valid_columns
    values_rows = []
    for idx in df.index:
        row_values = []
        # Iterate through columns in the EXACT same order as valid_columns
        for col in valid_columns:
            val = df.at[idx, col]

            # NULL handling (safe for arrays/lists)
            if not isinstance(val, (list, dict)) and (
                pd.isna(val) or str(val) in {'NaT', 'None', 'nan', '', '<NA>'}
            ):
                row_values.append("NULL")
                continue

            # Handle specific types
            if col == 'unique_key':
                row_values.append(f"'{str(val)}'")
            elif isinstance(val, (list, dict)):
                json_val = json.dumps(val)
                row_values.append(f"'{escape_special_characters(json_val)}'")
            elif isinstance(val, (pd.Timestamp, pd.Timedelta)):
                # Handle timezone-aware timestamps by converting to naive
                if isinstance(val, pd.Timestamp) and val.tz is not None:
                    val = val.tz_localize(None)  # Remove timezone info
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
    # Get column types to detect potential type mismatches
    column_types = {}
    try:
        table_cols_with_types = get_table_columns(table_name, schema)
        column_types = {col[0]: col[1] for col in table_cols_with_types} if table_cols_with_types else {}
    except Exception as e:
        logging.warning(f"Could not fetch column types for validation: {e}")

    batch_size = 1000
    for i in range(0, len(values_rows), batch_size):
        batch = values_rows[i:i + batch_size]
        values_str = ',\n'.join(batch)
        insert_query = f'INSERT INTO {schema}."{table_name}" ({columns_str}) VALUES\n{values_str};;'

        try:
            inject_sql(insert_query, f"INSERTING BATCH {i//batch_size + 1} INTO {table_name}")
        except Exception as insert_error:
            # Log detailed information about the failed insert
            error_msg = str(insert_error)

            # Check if it's a boolean type error
            if 'boolean' in error_msg.lower() and 'invalid input syntax' in error_msg.lower():
                logging.error(f"=== BOOLEAN TYPE ERROR DETECTED ===")
                logging.error(f"Table: {schema}.{table_name}")
                logging.error(f"Error: {error_msg}")

                # Extract the invalid value from error message dynamically
                import re
                match = re.search(r'invalid input syntax for type boolean: "([^"]+)"', error_msg)
                invalid_value = match.group(1) if match else None

                if invalid_value:
                    logging.error(f"Invalid value for boolean column: '{invalid_value}'")

                    # Find which column(s) are boolean
                    boolean_columns = [col for col, dtype in column_types.items() if 'bool' in dtype.lower()]
                    logging.error(f"Boolean columns in table: {boolean_columns}")

                    # Check dataframe for columns with this problematic value
                    logging.error(f"Searching DataFrame for value '{invalid_value}' in boolean columns...")
                    for col in boolean_columns:
                        if col in df.columns:
                            # Convert to string for comparison
                            col_str = df[col].astype(str).str.strip()
                            has_invalid = (col_str == invalid_value).any()
                            if has_invalid:
                                count = (col_str == invalid_value).sum()
                                logging.error(f"*** FOUND: Column '{col}' has {count} occurrences of '{invalid_value}' ***")
                                # Show sample values
                                sample_vals = df[col].dropna().astype(str).unique()[:20]
                                logging.error(f"Sample values in '{col}': {list(sample_vals)}")
                                # Show the DataFrame dtype for this column
                                logging.error(f"DataFrame dtype for '{col}': {df[col].dtype}")
                else:
                    # If we couldn't extract the value, still show boolean columns
                    boolean_columns = [col for col, dtype in column_types.items() if 'bool' in dtype.lower()]
                    logging.error(f"Boolean columns in table: {boolean_columns}")
                    # Show all unique values in boolean columns
                    for col in boolean_columns:
                        if col in df.columns:
                            sample_vals = df[col].dropna().astype(str).unique()[:20]
                            logging.error(f"All values in boolean column '{col}': {list(sample_vals)}")

                logging.error(f"=== END BOOLEAN ERROR DETAILS ===")

            # Re-raise the exception after logging
            raise

    logging.info(f"Successfully inserted {len(values_rows)} rows into {schema}.{table_name}")



def clean_datetime_string(s:str):
    try:
        dt = pd.to_datetime(s, errors='coerce')
        if pd.isna(dt):
            return s  # or return None

        # Handle timezone-aware datetime by converting to naive
        if hasattr(dt, 'tz') and dt.tz is not None:
            dt = dt.tz_localize(None)

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
        original_columns = len(df.columns)

        # STEP 1: Filter columns BEFORE any table operations
        # Remove single-character columns
        single_char_cols = [col for col in df.columns if len(col) <= 1]
        if single_char_cols:
            logging.info(f"Removing {len(single_char_cols)} single-character columns: {single_char_cols}")
        df = df[[col for col in df.columns if len(col) > 1]].copy()

        # Remove confidential columns
        columns_to_drop = df.columns[
                df.columns.str.lower().str.contains('|'.join([kw.lower() for kw in drop_keywords]))
            ]
        if len(columns_to_drop) > 0:
            logging.info(f"Removing {len(columns_to_drop)} confidential columns: {list(columns_to_drop)}")
        df = df.drop(columns=columns_to_drop)

        logging.info(f"Column filtering: {original_columns} -> {len(df.columns)} columns for {table_name}")

        # STEP 2: Add 'transformed' column BEFORE table creation
        df['transformed'] = False

        # STEP 3: Ensure 'transformed' column exists in table (for existing tables from old runs)
        if table_exists(schema,table_name):
            # Check if 'transformed' column exists in the existing table
            if not column_exists(schema, table_name, 'transformed'):
                logging.info(f"Adding missing 'transformed' column to existing table {schema}.{table_name}")
                alter_query = f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "transformed" BOOLEAN DEFAULT FALSE;;'
                inject_sql(alter_query, f"ADD transformed TO {table_name}")

        # STEP 4: Create table only if it doesn't exist (using filtered columns INCLUDING 'transformed')
        if not table_exists(schema,table_name):
            create_cols = []
            if("twenty_8_day_follow_up" in table_name):
                df = reorder_dataframe_columns(df,script=table_name)
            if "neolab" in table_name:
                drop_table = f'DROP TABLE IF EXISTS {schema}."{table_name}";;'
                inject_sql(drop_table,f"DROPPING {table_name}")

            # Use intelligent type detection for each column
            for col in df.columns:
                dtype = str(df[col].dtype)
                # Use intelligent type detection with column name and table name
                pg_type = get_expected_sql_type(dtype, column_name=col, table_name=table_name)
                create_cols.append(f'"{col}" {pg_type}')

            create_stmt = f'CREATE TABLE IF NOT EXISTS {schema}."{table_name}" ({",".join(create_cols)});;'

            inject_sql(create_stmt,f"CREATING {table_name}")
            logging.info(f"Created table {schema}.{table_name} with intelligent date type detection")

        # STEP 4: Insert data
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


from collections import defaultdict

def generate_label_fix_updates(filtered_records, table_name: str):
    """Generate batched SQL UPDATE statements with proper quoting for complex column names."""
    if not filtered_records:
        return []

    groups = defaultdict(list)

    for row in filtered_records:
        update_keys = tuple(sorted(k for k in row if k not in ['uid', 'unique_key']))
        groups[update_keys].append(row)

    sql_batches = []

    for update_columns in groups:
        update_cols = list(update_columns)

        # Skip if there are no columns to update (only uid and unique_key present)
        if not update_cols:
            logging.warning(f"Skipping batch with no update columns for table '{table_name}'")
            continue

        value_columns = ['uid', 'unique_key'] + update_cols
        rows = groups[update_columns]
        values = [
            tuple(row.get(col) for col in value_columns)
            for row in rows
        ]

        alias = 'v'

        quoted_update_cols = [f'"{col}"' for col in update_cols]
        quoted_value_columns = [f'"{col}"' for col in value_columns]

        set_clause = ", ".join([
            f'{quoted_update_cols[i]} = {alias}.{quoted_update_cols[i]}'
            for i in range(len(update_cols))
        ])

        columns_str = ", ".join(quoted_value_columns)
        sql = f"""
            UPDATE derived."{table_name}" AS t
            SET {set_clause}
            FROM (
                VALUES %s
            ) AS {alias}({columns_str})
            WHERE t."uid" = {alias}."uid"
              AND t."unique_key" = {alias}."unique_key"
        """

        sql_batches.append((sql.strip(), values))

    return sql_batches


def run_bulky_query(table: str, filtered_records=None):
    """Execute bulk update queries using execute_values for performance."""
    if not engine or not execute_values or not PSYCOPG2_AVAILABLE:
        raise RuntimeError("Database engine and psycopg2 not initialized")

    if filtered_records is None:
        return

    # Convert DataFrame to list of records if needed (defensive check)
    if isinstance(filtered_records, pd.DataFrame):
        if filtered_records.empty:
            return
        filtered_records = filtered_records.to_dict('records')

    # Defensive check: ensure filtered_records is a list
    if not isinstance(filtered_records, list):
        logging.error(f"Error in run_bulky_query: filtered_records should be a list, got {type(filtered_records)}")
        return

    # Handle empty lists
    if not filtered_records:
        return

    # Defensive check: ensure all items in the list are dictionaries
    if not all(isinstance(item, dict) for item in filtered_records):
        logging.error(f"Error in run_bulky_query: filtered_records should contain only dictionaries")
        invalid_items = [type(item) for item in filtered_records if not isinstance(item, dict)]
        logging.error(f"Found invalid item types: {invalid_items}")
        return

    # Get raw psycopg2 connection for execute_values
    raw_conn = engine.raw_connection()  # type: ignore[union-attr]
    try:
        cur = raw_conn.cursor()  # type: ignore[union-attr]
        try:
            sql_batches = generate_label_fix_updates(filtered_records, table_name=table)

            for sql_query, values in sql_batches:
                execute_values(cur, sql_query, values)

            raw_conn.commit()  # type: ignore[union-attr]
        except Exception:
            raw_conn.rollback()  # type: ignore[union-attr]
            raise
        finally:
            cur.close()
    except Exception as ex:
        logging.error(f"Error in run_bulky_query for table '{table}': {ex}")
        raise
    finally:
        raw_conn.close()  # type: ignore[union-attr]
    

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

def store_field_metadata(table_name: str, merged_data: Dict[str, Dict[str, str]]) -> None:
    """
    Store field metadata for a clean_* table to be used during SQL normalization.

    This metadata is used by normalize_clean_tables.sql to determine which fields
    should have _label columns (only dropdown, single_select_option, period).

    Args:
        table_name: Name of the clean_* table (e.g., 'clean_admissions')
        merged_data: Dictionary {field_key: {'key': key, 'dataType': type}}
    """
    if not engine:
        raise RuntimeError("Database engine not initialized")

    if not merged_data or not isinstance(merged_data, dict):
        logging.warning(f"No valid metadata to store for {table_name}")
        return

    # Create metadata table if it doesn't exist
    create_metadata_table_query = """
        CREATE TABLE IF NOT EXISTS derived.field_metadata (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            data_type TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (table_name, column_name)
        );;
    """
    inject_sql(create_metadata_table_query, "CREATE field_metadata table")

    # Delete existing metadata for this table
    delete_query = f"DELETE FROM derived.field_metadata WHERE table_name = '{table_name}';;"
    inject_sql(delete_query, f"DELETE old metadata for {table_name}")

    # Build insert values - deduplicate by lowercase column name
    values_dict = {}  # Use dict to automatically handle duplicates
    seen_columns = set()  # Track columns we've already processed

    for key, meta in merged_data.items():
        if not isinstance(meta, dict):
            continue

        # Handle None values for dataType - use empty string as default
        data_type_value = meta.get('dataType', '') or ''
        data_type = data_type_value.lower() if data_type_value else ''
        if not data_type:
            continue

        # Store lowercase column name
        column_name = key.lower() if key else ''
        if not column_name:
            continue

        # Skip if we've already seen this column name (case-insensitive)
        if column_name in seen_columns:
            logging.debug(f"Skipping duplicate column '{key}' (normalized to '{column_name}') for table {table_name}")
            continue

        seen_columns.add(column_name)

        escaped_table = escape_special_characters(table_name)
        escaped_column = escape_special_characters(column_name)
        escaped_type = escape_special_characters(data_type)

        # Use column name as key to ensure uniqueness
        values_dict[column_name] = f"('{escaped_table}', '{escaped_column}', '{escaped_type}', CURRENT_TIMESTAMP)"

    values_rows = list(values_dict.values())

    if not values_rows:
        logging.warning(f"No valid metadata rows to insert for {table_name}")
        return

    # Insert new metadata in batches
    batch_size = 500
    for i in range(0, len(values_rows), batch_size):
        batch = values_rows[i:i + batch_size]
        values_str = ',\n'.join(batch)
        insert_query = f"""
            INSERT INTO derived.field_metadata (table_name, column_name, data_type, updated_at)
            VALUES {values_str};;
        """
        inject_sql(insert_query, f"INSERT metadata batch {i//batch_size + 1} for {table_name}")

    logging.info(f"Stored metadata for {len(values_rows)} fields in {table_name}")
