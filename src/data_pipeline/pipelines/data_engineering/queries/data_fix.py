import logging
from conf.common.sql_functions import inject_sql_procedure, inject_sql_with_return
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
import re



def deduplicate_table(table:str):  
    if (table_exists('derived',table)):
        deduplicate_derived_tables(table) 
        drop_confidential_columns(table)
        if table=='clean_admissions':
            update_mat_age('admissions','clean_admissions')
        if table=='clean_joined_adm_discharges':
            update_mat_age('joined_admissions_discharges','clean_joined_adm_discharges')
        if table=='clean_maternal_outcomes':
            update_mat_age('maternal_outcomes','clean_maternal_outcomes')
      
             
    

def deduplicate_derived_tables(table: str):
    query = f'''DO $$
        DECLARE
            rows_deleted INTEGER := 1;
            total_deleted INTEGER := 0;
            batch_size INTEGER := 10000;
        BEGIN
            WHILE rows_deleted > 0 LOOP
                WITH ranked_duplicates AS (
                    SELECT ctid,
                        ROW_NUMBER() OVER (
                            PARTITION BY LEFT(unique_key,10), uid
                            ORDER BY ctid
                        ) AS rn
                    FROM derived."{table}"
                    WHERE unique_key IS NOT NULL
                ),
                to_delete AS (
                    SELECT ctid
                    FROM ranked_duplicates
                    WHERE rn > 1
                    LIMIT batch_size
                )
                DELETE FROM derived."{table}" 
                WHERE ctid IN (SELECT ctid FROM to_delete);

                GET DIAGNOSTICS rows_deleted = ROW_COUNT;
                total_deleted := total_deleted + rows_deleted;
                
                RAISE NOTICE 'Deleted % rows in this batch, % total', rows_deleted, total_deleted;
                
                PERFORM pg_sleep(0.1);
            END LOOP;
        END $$;'''
    inject_sql_procedure(query,f"DEDUPLICATE DERIVED {table}")

def drop_confidential_columns(table_name):
   
    query = f'''DO $$
            DECLARE
                cols text;
                sql  text;
            BEGIN
                -- Step 1: find matching columns and build DROP clause dynamically
                SELECT string_agg(format('DROP COLUMN IF EXISTS %I', column_name), ', ')
                INTO cols
                FROM information_schema.columns
                WHERE table_schema = 'derived'
                AND table_name   = '{table_name}'
                AND (
                    column_name ILIKE '%dobtob%'
                    OR column_name ILIKE '%firstname%'
                    OR column_name ILIKE '%lastname%'
                );

                -- Only run if we found matching columns
                IF cols IS NOT NULL THEN
                    sql := format('ALTER TABLE %I.%I %s;', 'derived', '{table_name}', cols);
                    EXECUTE sql;
                END IF;
            END $$;
            '''
    inject_sql_procedure(query,f"DROP CONFIDENTIAL COLS {table_name}")


def update_mat_age(source_table: str, dest_table: str):
    # Determine which column exists in source
    col_check_query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'derived' AND table_name = '{source_table}'
        AND column_name IN ('matageyrs', 'MatAgeYrs.value')
        ORDER BY column_name='matageyrs' DESC  -- prefer matageyrs if present
        LIMIT 1;
    """

    source_col_result = inject_sql_with_return(col_check_query)

    if source_col_result and len(source_col_result) > 0:
        sc = source_col_result[0][0]
        sc_quoted = f'"{sc}"' if '.' in sc else sc

        query = f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'derived'
                    AND table_name = '{dest_table}'
                    AND column_name = 'matageyrs'
                )
                THEN
                    UPDATE derived.{dest_table} d
                    SET matageyrs = s_val.num_val
                    FROM (
                        SELECT
                            uid,
                            unique_key,
                            COALESCE(
                                FLOOR(
                                    CASE
                                        WHEN NULLIF(regexp_replace({sc_quoted}::text, '[^0-9.,]', '', 'g'), '') IS NOT NULL
                                        THEN
                                            CASE
                                                -- If number > 100000, assume it's hours → convert to years
                                                WHEN CAST(replace(regexp_replace({sc_quoted}::text, '[^0-9.,]', '', 'g'), ',', '') AS NUMERIC) > 100000
                                                THEN CAST(replace(regexp_replace({sc_quoted}::text, '[^0-9.,]', '', 'g'), ',', '') AS NUMERIC) / 8766
                                                ELSE CAST(replace(regexp_replace({sc_quoted}::text, '[^0-9.,]', '', 'g'), ',', '') AS NUMERIC)
                                            END
                                    END
                                ),
                                200
                            ) AS num_val
                        FROM derived.{source_table}
                    ) s_val
                    WHERE d.uid = s_val.uid
                    AND d.unique_key = s_val.unique_key
                    AND d.matageyrs IS NULL
                    AND (s_val.num_val <= 85);
                END IF;
            END $$;
            """
        inject_sql_procedure(query, f"FIX MATERNAL AGE {dest_table}")


def datesfix(source_table: str, dest_table: str):
    """
    Fix null date values in destination table by pulling data from source table.

    Efficiently updates all date columns in a single query per table.
    Handles different date formats and three different source scenarios:
    1. public.clean_sessions - extract from JSON structure
    2. derived tables without 'clean' in name - direct mapping
    3. destination with 'clean' in name - lowercase mapping

    Args:
        source_table: Source table name (with schema, e.g., 'public.clean_sessions' or 'derived.admissions')
        dest_table: Destination table name (without schema, in derived schema)
    """
    logging.info(f"Starting date fix from {source_table} to derived.{dest_table}")

    # Parse source schema and table
    if '.' in source_table:
        source_schema, source_name = source_table.split('.', 1)
    else:
        source_schema = 'derived'
        source_name = source_table

    # Check if both tables exist
    if not table_exists('derived', dest_table):
        logging.error(f"Destination table derived.{dest_table} does not exist")
        return

    # Check source table existence
    source_exists_query = f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = '{source_schema}'
            AND table_name = '{source_name}'
        );
    """
    source_exists_result = inject_sql_with_return(source_exists_query)
    if not source_exists_result or not source_exists_result[0][0]:
        logging.error(f"Source table {source_schema}.{source_name} does not exist")
        return

    # Get all date columns from destination table
    # A date column is either: data_type contains 'date'/'time' OR column_name contains 'date'
    date_columns_query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'derived'
        AND table_name = '{dest_table}'
        AND (
            data_type ILIKE '%date%'
            OR data_type ILIKE '%time%'
            OR column_name ILIKE '%date%'
        )
        ORDER BY column_name;
    """

    date_columns_result = inject_sql_with_return(date_columns_query)

    if not date_columns_result or len(date_columns_result) == 0:
        logging.info(f"No date columns found in derived.{dest_table}")
        return

    logging.info(f"Found {len(date_columns_result)} date columns in derived.{dest_table}")

    # Process based on source table type
    if source_schema == 'public' and 'clean_sessions' in source_name:
        _fix_dates_from_clean_sessions(source_table, dest_table, date_columns_result)
    elif source_schema == 'derived' and 'clean' not in source_name:
        _fix_dates_from_derived_direct(source_table, dest_table, date_columns_result)
    elif 'clean' in dest_table.lower():
        _fix_dates_to_clean_table(source_table, dest_table, date_columns_result)
    else:
        logging.warning(f"No matching strategy for source={source_table}, dest={dest_table}")


def datesfix_batch(table_pairs: list):
    """
    Fix dates for multiple tables efficiently.

    Args:
        table_pairs: List of tuples (source_table, dest_table)

    Example:
        datesfix_batch([
            ('public.clean_sessions', 'admissions'),
            ('derived.admissions', 'clean_admissions'),
            ('derived.baseline', 'discharges')
        ])
    """
    total_tables = len(table_pairs)
    logging.info(f"Starting batch date fix for {total_tables} tables")

    for idx, (source_table, dest_table) in enumerate(table_pairs, 1):
        logging.info(f"Processing table {idx}/{total_tables}: {dest_table}")
        try:
            datesfix(source_table, dest_table)
        except Exception as e:
            logging.error(f"Failed to fix dates for {dest_table}: {e}")
            continue

    logging.info(f"Batch date fix completed for {total_tables} tables")


def _fix_dates_from_clean_sessions(source_table: str, dest_table: str, date_columns):
    """
    Fix dates when source is public.clean_sessions
    Extract from: data -> 'entries' -> 'VariableName' -> 'values' -> 'value' ->> 0
    Format: timestamps as 'YYYY-MM-DD HH:MI', dates as 'YYYY-MM-DD'
    Uses batched updates for efficiency.
    """
    logging.info(f"Fixing dates from clean_sessions to derived.{dest_table}")

    is_clean_dest = 'clean' in dest_table.lower()

    # Group columns by variable name for .value/.label pairs
    value_label_pairs = {}
    standalone_cols = []

    for col_info in date_columns:
        dest_col = col_info[0]
        data_type = col_info[1].lower()

        # Determine date format
        if 'timestamp' in data_type or ('time' in data_type and 'date' not in data_type):
            date_format = 'YYYY-MM-DD HH24:MI'
        else:
            date_format = 'YYYY-MM-DD'

        if dest_col.endswith('.value') and not is_clean_dest:
            variable_name = dest_col.rsplit('.', 1)[0]
            if variable_name not in value_label_pairs:
                value_label_pairs[variable_name] = {'format': date_format, 'value_col': dest_col}
        elif dest_col.endswith('.label'):
            continue  # Skip labels, handled with values
        else:
            standalone_cols.append((dest_col, date_format))

    # Build batched update for .value/.label pairs
    if value_label_pairs:
        set_clauses = []
        subquery_selects = []
        where_conditions = []

        for variable_name, info in value_label_pairs.items():
            value_col = info['value_col']
            label_col = f"{variable_name}.label"
            date_format = info['format']

            set_clauses.append(f'"{value_col}" = s."{variable_name}_date"')
            set_clauses.append(f'"{label_col}" = s."{variable_name}_label"')

            subquery_selects.append(
                f"TO_CHAR((s.data -> 'entries' -> '{variable_name}' -> 'values' -> 'value' ->> 0)::timestamp, '{date_format}')::timestamp AS \"{variable_name}_date\""
            )
            subquery_selects.append(
                f"TO_CHAR((s.data -> 'entries' -> '{variable_name}' -> 'values' -> 'value' ->> 0)::timestamp, '{date_format}') AS \"{variable_name}_label\""
            )

            where_conditions.append(
                f"(d.\"{value_col}\" IS NULL AND s.data -> 'entries' -> '{variable_name}' -> 'values' -> 'value' ->> 0 IS NOT NULL AND s.data -> 'entries' -> '{variable_name}' -> 'values' -> 'value' ->> 0 != '')"
            )

        if set_clauses:
            update_query = f"""
                WITH updated AS (
                    UPDATE derived."{dest_table}" d
                    SET {', '.join(set_clauses)}
                    FROM (
                        SELECT
                            s.uid,
                            s.data ->> 'unique_key' as unique_key,
                            {', '.join(subquery_selects)}
                        FROM {source_table} s
                    ) s
                    WHERE d.uid = s.uid
                    AND d.unique_key = s.unique_key
                    AND ({' OR '.join(where_conditions)})
                    RETURNING d.uid, d.unique_key
                )
                SELECT COUNT(*) as updated_count,
                       ARRAY_AGG(DISTINCT uid ORDER BY uid) FILTER (WHERE uid IS NOT NULL) AS sample_uids
                FROM (SELECT uid FROM updated LIMIT 5) sampled;
            """

            try:
                result = inject_sql_with_return(update_query)
                if result and result[0]:
                    count = result[0][0] if result[0][0] else 0
                    sample_uids = result[0][1] if len(result[0]) > 1 and result[0][1] else []

                    columns_fixed = list(value_label_pairs.keys())
                    logging.info(f"✓ Fixed {count} records in {dest_table}")
                    logging.info(f"  Columns: {', '.join(columns_fixed)}")
                    logging.info(f"  Sample UIDs: {sample_uids[:5] if sample_uids else 'None'}")
            except Exception as e:
                logging.error(f"Error fixing .value/.label pairs: {e}")

    # Handle standalone columns (clean tables)
    for dest_col, date_format in standalone_cols:
        variable_name = dest_col

        update_query = f"""
            WITH updated AS (
                UPDATE derived."{dest_table}" d
                SET "{dest_col}" = TO_CHAR(s.date_val::timestamp, '{date_format}')::timestamp
                FROM (
                    SELECT
                        s.uid,
                        s.data ->> 'unique_key' as unique_key,
                        s.data -> 'entries' -> '{variable_name}' -> 'values' -> 'value' ->> 0 as date_val
                    FROM {source_table} s
                    WHERE s.data -> 'entries' -> '{variable_name}' -> 'values' -> 'value' ->> 0 IS NOT NULL
                    AND s.data -> 'entries' -> '{variable_name}' -> 'values' -> 'value' ->> 0 != ''
                ) s
                WHERE d.uid = s.uid
                AND d.unique_key = s.unique_key
                AND d."{dest_col}" IS NULL
                AND s.date_val IS NOT NULL
                AND s.date_val != ''
                RETURNING d.uid
            )
            SELECT COUNT(*) as updated_count,
                   ARRAY_AGG(DISTINCT uid ORDER BY uid) FILTER (WHERE uid IS NOT NULL) AS sample_uids
            FROM (SELECT uid FROM updated LIMIT 5) sampled;
        """

        try:
            result = inject_sql_with_return(update_query)
            if result and result[0]:
                count = result[0][0] if result[0][0] else 0
                sample_uids = result[0][1] if len(result[0]) > 1 and result[0][1] else []

                if count > 0:
                    logging.info(f"✓ Fixed {count} records in {dest_table}")
                    logging.info(f"  Column: {dest_col}")
                    logging.info(f"  Sample UIDs: {sample_uids[:5] if sample_uids else 'None'}")
        except Exception as e:
            logging.error(f"Error fixing {dest_col}: {e}")


def _fix_dates_from_derived_direct(source_table: str, dest_table: str, date_columns):
    """
    Fix dates when source is a derived table without 'clean' in name
    Direct column mapping
    Format: timestamps as 'YYYY-MM-DD HH:MI', dates as 'YYYY-MM-DD'
    Uses batched updates for efficiency.
    """
    logging.info(f"Fixing dates from {source_table} to derived.{dest_table} using direct mapping")

    source_schema, source_name = source_table.split('.', 1) if '.' in source_table else ('derived', source_table)

    # Get columns from source table
    source_cols_query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{source_schema}'
        AND table_name = '{source_name}';
    """

    source_cols_result = inject_sql_with_return(source_cols_query)
    source_cols = {row[0] for row in source_cols_result} if source_cols_result else set()

    # Build column mappings
    column_mappings = []
    for col_info in date_columns:
        dest_col = col_info[0]
        data_type = col_info[1].lower()

        # Determine date format
        if 'timestamp' in data_type or ('time' in data_type and 'date' not in data_type):
            date_format = 'YYYY-MM-DD HH24:MI'
        else:
            date_format = 'YYYY-MM-DD'

        # Find matching source column
        source_col = None
        if dest_col in source_cols:
            source_col = dest_col
        else:
            for sc in source_cols:
                if sc.lower() == dest_col.lower():
                    source_col = sc
                    break

        if source_col:
            column_mappings.append((dest_col, source_col, date_format))

    if not column_mappings:
        logging.warning(f"No matching columns found between {source_table} and {dest_table}")
        return

    # Build single batched update query
    set_clauses = []
    where_conditions = []
    columns_fixed = []

    for dest_col, source_col, date_format in column_mappings:
        dest_col_quoted = f'"{dest_col}"' if '.' in dest_col or ' ' in dest_col else dest_col
        source_col_quoted = f'"{source_col}"' if '.' in source_col or ' ' in source_col else source_col

        set_clauses.append(f'{dest_col_quoted} = TO_CHAR(s.{source_col_quoted}::timestamp, \'{date_format}\')::timestamp')
        where_conditions.append(f'd.{dest_col_quoted} IS NULL')
        columns_fixed.append(dest_col)

    update_query = f"""
        WITH updated AS (
            UPDATE derived."{dest_table}" d
            SET {', '.join(set_clauses)}
            FROM {source_schema}."{source_name}" s
            WHERE d.uid = s.uid
            AND d.unique_key = s.unique_key
            AND ({' OR '.join(where_conditions)})
            AND ({' OR '.join([f's.{("\"" + sc + "\"" if "." in sc or " " in sc else sc)} IS NOT NULL' for _, sc, _ in column_mappings])})
            RETURNING d.uid
        )
        SELECT COUNT(*) as updated_count,
               ARRAY_AGG(DISTINCT uid ORDER BY uid) FILTER (WHERE uid IS NOT NULL) AS sample_uids
        FROM (SELECT uid FROM updated LIMIT 5) sampled;
    """

    try:
        result = inject_sql_with_return(update_query)
        if result and result[0]:
            count = result[0][0] if result[0][0] else 0
            sample_uids = result[0][1] if len(result[0]) > 1 and result[0][1] else []

            if count > 0:
                logging.info(f"✓ Fixed {count} records in {dest_table}")
                logging.info(f"  Columns: {', '.join(columns_fixed)}")
                logging.info(f"  Sample UIDs: {sample_uids[:5] if sample_uids else 'None'}")
            else:
                logging.info(f"No null dates found to fix in {dest_table}")
    except Exception as e:
        logging.error(f"Error fixing dates from {source_table}: {e}")


def _fix_dates_to_clean_table(source_table: str, dest_table: str, date_columns):
    """
    Fix dates when destination table has 'clean' in name
    Try direct mapping first, then lowercase mapping (VariableName.value -> variablename)
    Format: timestamps as 'YYYY-MM-DD HH:MI', dates as 'YYYY-MM-DD'
    Uses batched updates for efficiency.
    """
    logging.info(f"Fixing dates from {source_table} to clean table derived.{dest_table}")

    source_schema, source_name = source_table.split('.', 1) if '.' in source_table else ('derived', source_table)

    # Get columns from source table
    source_cols_query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{source_schema}'
        AND table_name = '{source_name}';
    """

    source_cols_result = inject_sql_with_return(source_cols_query)
    source_cols = {row[0]: row[0] for row in source_cols_result} if source_cols_result else {}

    # Create lowercase mapping for source columns
    source_cols_lower = {}
    for col in source_cols:
        base_col = re.sub(r'\.(value|label)$', '', col, flags=re.IGNORECASE)
        source_cols_lower[base_col.lower()] = col

    # Build column mappings
    column_mappings = []
    for col_info in date_columns:
        dest_col = col_info[0]
        data_type = col_info[1].lower()

        # Determine date format
        if 'timestamp' in data_type or ('time' in data_type and 'date' not in data_type):
            date_format = 'YYYY-MM-DD HH24:MI'
        else:
            date_format = 'YYYY-MM-DD'

        # Find matching source column
        source_col = None
        if dest_col in source_cols:
            source_col = dest_col
        else:
            dest_col_lower = dest_col.lower()
            if dest_col_lower in source_cols_lower:
                source_col = source_cols_lower[dest_col_lower]
            else:
                potential_source = f"{dest_col}.value"
                if potential_source in source_cols:
                    source_col = potential_source

        if source_col:
            column_mappings.append((dest_col, source_col, date_format))

    if not column_mappings:
        logging.warning(f"No matching columns found between {source_table} and {dest_table}")
        return

    # Build single batched update query
    set_clauses = []
    where_conditions = []
    columns_fixed = []

    for dest_col, source_col, date_format in column_mappings:
        dest_col_quoted = f'"{dest_col}"' if '.' in dest_col or ' ' in dest_col else dest_col
        source_col_quoted = f'"{source_col}"' if '.' in source_col or ' ' in source_col else source_col

        set_clauses.append(f'{dest_col_quoted} = TO_CHAR(s.{source_col_quoted}::timestamp, \'{date_format}\')::timestamp')
        where_conditions.append(f'd.{dest_col_quoted} IS NULL')
        columns_fixed.append(f'{dest_col} <- {source_col}')

    update_query = f"""
        WITH updated AS (
            UPDATE derived."{dest_table}" d
            SET {', '.join(set_clauses)}
            FROM {source_schema}."{source_name}" s
            WHERE d.uid = s.uid
            AND d.unique_key = s.unique_key
            AND ({' OR '.join(where_conditions)})
            AND ({' OR '.join([f's.{("\"" + sc + "\"" if "." in sc or " " in sc else sc)} IS NOT NULL' for _, sc, _ in column_mappings])})
            RETURNING d.uid
        )
        SELECT COUNT(*) as updated_count,
               ARRAY_AGG(DISTINCT uid ORDER BY uid) FILTER (WHERE uid IS NOT NULL) AS sample_uids
        FROM (SELECT uid FROM updated LIMIT 5) sampled;
    """

    try:
        result = inject_sql_with_return(update_query)
        if result and result[0]:
            count = result[0][0] if result[0][0] else 0
            sample_uids = result[0][1] if len(result[0]) > 1 and result[0][1] else []

            if count > 0:
                logging.info(f"✓ Fixed {count} records in {dest_table}")
                logging.info(f"  Mappings: {', '.join(columns_fixed)}")
                logging.info(f"  Sample UIDs: {sample_uids[:5] if sample_uids else 'None'}")
            else:
                logging.info(f"No null dates found to fix in {dest_table}")
    except Exception as e:
        logging.error(f"Error fixing dates to clean table: {e}")

