"""
Data Fix Utilities for PostgreSQL Database Maintenance

Available Functions:
--------------------
1. deduplicate_table(table) - Remove duplicate rows
2. drop_confidential_columns(table) - Drop sensitive columns
3. drop_single_letter_columns(table) - Drop all single-letter column names
4. drop_single_letter_columns_all_tables(schema) - Drop single-letter columns from all tables in schema
5. update_mat_age(source, dest) - Fix maternal age values
6. datesfix(source, dest) - Fix null date values
7. date_data_type_fix(table, columns, schema) - Convert columns to TIMESTAMP type
8. count_table_columns(table, schema) - Count active/dropped columns
9. rebuild_table_dry_run(table, schema) - Preview rebuild without executing
10. rebuild_table_to_remove_dropped_columns(table, schema) - Reclaim dropped columns
11. fix_column_limit_error(table, schema, auto_rebuild) - Diagnose/fix 1600 column limit
"""

import logging
from conf.common.sql_functions import inject_sql_procedure, inject_sql_with_return
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.utils.field_info import load_json_for_comparison
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


def drop_single_letter_columns(table_name: str, schema: str = 'derived'):
    """
    Drop all single-letter column names from a specific table.

    Single-letter columns are often artifacts or unintended columns
    that should not exist in production tables.

    Args:
        table_name: Name of the table to clean
        schema: Schema name (default: 'derived')

    Returns:
        Number of columns dropped
    """
    if not table_exists(schema, table_name):
        logging.error(f"Table {schema}.{table_name} does not exist")
        return 0

    # First, get the list of single-letter columns
    check_query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}'
        AND LENGTH(column_name) = 1;
    """

    result = inject_sql_with_return(check_query)

    if not result or len(result) == 0:
        logging.info(f"No single-letter columns found in {schema}.{table_name}")
        return 0

    single_letter_cols = [row[0] for row in result]
    logging.info(f"Found {len(single_letter_cols)} single-letter columns in {schema}.{table_name}: {single_letter_cols}")

    # Build and execute DROP query
    query = f'''DO $$
            DECLARE
                cols text;
                sql  text;
            BEGIN
                -- Find all single-letter column names and build DROP clause
                SELECT string_agg(format('DROP COLUMN IF EXISTS %I', column_name), ', ')
                INTO cols
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table_name}'
                AND LENGTH(column_name) = 1;

                -- Only run if we found matching columns
                IF cols IS NOT NULL THEN
                    sql := format('ALTER TABLE %I.%I %s;', '{schema}', '{table_name}', cols);
                    EXECUTE sql;
                    RAISE NOTICE 'Dropped single-letter columns: %', cols;
                END IF;
            END $$;
            '''

    inject_sql_procedure(query, f"DROP SINGLE-LETTER COLS {schema}.{table_name}")
    logging.info(f"✓ Dropped {len(single_letter_cols)} single-letter columns from {schema}.{table_name}")

    return len(single_letter_cols)


def drop_single_letter_columns_all_tables(schema: str = 'derived'):
    """
    Drop all single-letter column names from ALL tables in a schema.

    This is a cleanup procedure to remove unwanted single-letter columns
    that may have been inadvertently created across multiple tables.

    Args:
        schema: Schema name (default: 'derived')

    Returns:
        Dict with summary of columns dropped per table
    """
    logging.info(f"Starting cleanup of single-letter columns from all tables in schema '{schema}'")

    # Get all tables in the schema
    tables_query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """

    tables_result = inject_sql_with_return(tables_query)

    if not tables_result or len(tables_result) == 0:
        logging.warning(f"No tables found in schema '{schema}'")
        return {}

    tables = [row[0] for row in tables_result]
    logging.info(f"Found {len(tables)} tables in schema '{schema}'")

    # Process each table
    summary = {}
    total_cols_dropped = 0
    tables_affected = 0

    for table_name in tables:
        try:
            cols_dropped = drop_single_letter_columns(table_name, schema)
            if cols_dropped > 0:
                summary[table_name] = cols_dropped
                total_cols_dropped += cols_dropped
                tables_affected += 1
        except Exception as e:
            logging.error(f"Error processing table {schema}.{table_name}: {e}")
            summary[table_name] = f"ERROR: {str(e)}"

    # Report summary
    logging.info("")
    logging.info("="*60)
    logging.info("CLEANUP SUMMARY:")
    logging.info("="*60)
    logging.info(f"Total tables scanned: {len(tables)}")
    logging.info(f"Tables with single-letter columns: {tables_affected}")
    logging.info(f"Total single-letter columns dropped: {total_cols_dropped}")

    if tables_affected > 0:
        logging.info("")
        logging.info("Tables affected:")
        for table, count in summary.items():
            if isinstance(count, int):
                logging.info(f"  - {table}: {count} columns dropped")
            else:
                logging.error(f"  - {table}: {count}")

    logging.info("="*60)

    return summary


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


def _is_actually_date_column(column_name: str, data_type: str, table_name: str) -> bool:
    """
    Intelligently determine if a column is actually a date column.

    Uses multiple heuristics:
    1. Check data_type (timestamp, date, time columns are always dates)
    2. For TEXT columns with "date" in name: sample actual data to verify
    3. Use field metadata if available to validate data type

    Args:
        column_name: Name of the column
        data_type: PostgreSQL data type
        table_name: Table name (for metadata lookup and sampling)

    Returns:
        True if column is a date column, False otherwise
    """
    # Priority 1: Check PostgreSQL data type (most reliable)
    data_type_lower = data_type.lower()
    if any(dt in data_type_lower for dt in ['timestamp', 'date']):
        return True

    if 'time' in data_type_lower and 'date' not in data_type_lower:
        # Could be 'time' type (time without date), which we might want
        return True

    # Priority 2: For TEXT columns that contain "date", sample actual data
    column_lower = column_name.lower()
    if 'text' in data_type_lower or 'character' in data_type_lower:
        if 'date' in column_lower:
            # Sample 3 non-null values from the actual data
            try:
                # Always quote column names to handle mixed-case and special characters
                column_quoted = f'"{column_name}"'
                sample_query = f"""
                    SELECT {column_quoted}
                    FROM derived."{table_name}"
                    WHERE {column_quoted} IS NOT NULL
                    AND {column_quoted} != ''
                    AND {column_quoted} != 'None'
                    LIMIT 3;
                """

                sample_result = inject_sql_with_return(sample_query)

                if sample_result and len(sample_result) > 0:
                    # Check if samples look like dates
                    date_like_count = 0
                    for row in sample_result:
                        value = str(row[0]).strip()
                        if _looks_like_date(value):
                            date_like_count += 1

                    # If at least 2 out of 3 samples look like dates, include it
                    if date_like_count >= min(2, len(sample_result)):
                        logging.debug(f"Including '{column_name}' - {date_like_count}/{len(sample_result)} samples are date-like")
                        return True
                    else:
                        logging.debug(f"Excluding '{column_name}' - only {date_like_count}/{len(sample_result)} samples are date-like")
                        return False
                else:
                    # No data to sample, check metadata or exclude
                    logging.debug(f"No data to sample for '{column_name}'")
            except Exception as e:
                logging.debug(f"Error sampling data for '{column_name}': {e}")
                # Fall through to metadata check

    # Priority 3: Use field metadata if available
    try:
        # Try to load metadata for this table
        metadata = load_json_for_comparison(table_name)
        if metadata:
            # Extract base field key from column name
            base_key = column_name
            if column_name.endswith('.value') or column_name.endswith('.label'):
                base_key = column_name.rsplit('.', 1)[0]

            # Handle both dict and list metadata formats
            field_info = {}
            if isinstance(metadata, dict):
                # Could be {scriptId: [fields]} or {fieldKey: field}
                first_value = next(iter(metadata.values()), None)
                if isinstance(first_value, list):
                    # scriptId format - use first script's fields
                    field_info = {f['key']: f for f in first_value}
                elif isinstance(first_value, dict) and 'key' in first_value:
                    # Already a field dict
                    field_info = metadata
            else:
                # List format
                field_info = {f['key']: f for f in metadata}

            # Check if field exists in metadata
            if base_key in field_info:
                field = field_info[base_key]
                data_type_meta = field.get('dataType', field.get('type', ''))

                # If metadata says it's a date field, trust it
                if data_type_meta in ['datetime', 'timestamp', 'date']:
                    return True

                # If metadata says it's NOT a date field, trust it
                if data_type_meta and data_type_meta not in ['datetime', 'timestamp', 'date']:
                    logging.debug(f"Excluding '{column_name}' - metadata indicates type '{data_type_meta}', not a date")
                    return False
    except Exception as e:
        # Metadata not available or error loading - continue with other checks
        logging.debug(f"Could not load metadata for {table_name}: {e}")
        pass

    # If none of the above, exclude it
    logging.debug(f"Excluding '{column_name}' - insufficient evidence it's a date column")
    return False


def _looks_like_date(value: str) -> bool:
    """
    Check if a string value looks like a date.

    Checks for common date patterns:
    - ISO format: 2023-01-15, 2023-01-15 14:30:00
    - Slash format: 01/15/2023, 15/01/2023
    - Timestamps: 1673784000

    Args:
        value: String value to check

    Returns:
        True if value looks like a date, False otherwise
    """
    if not value or len(value) < 8:  # Minimum date length
        return False

    # Common date patterns
    date_patterns = [
        r'^\d{4}-\d{1,2}-\d{1,2}',           # YYYY-MM-DD or YYYY-M-D
        r'^\d{1,2}/\d{1,2}/\d{4}',           # MM/DD/YYYY or DD/MM/YYYY
        r'^\d{4}/\d{1,2}/\d{1,2}',           # YYYY/MM/DD
        r'^\d{1,2}-\d{1,2}-\d{4}',           # DD-MM-YYYY or MM-DD-YYYY
        r'^\d{10,13}$',                      # Unix timestamp (10-13 digits)
        r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}',   # ISO 8601 with time
        r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}',   # YYYY-MM-DD HH:MM
    ]

    for pattern in date_patterns:
        if re.match(pattern, value):
            return True

    return False


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

    # Check if destination table exists
    if not table_exists('derived', dest_table):
        logging.warning(f"Destination table derived.{dest_table} does not exist - skipping (this is expected in some environments)")
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
        logging.warning(f"Source table {source_schema}.{source_name} does not exist - skipping (this is expected in some environments)")
        return

    # Get all POTENTIAL date columns from destination table
    # Cast a wide net first, then filter intelligently
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
        logging.info(f"No potential date columns found in derived.{dest_table}")
        return

    # Intelligently filter to actual date columns
    actual_date_columns = []
    excluded_columns = []

    for col_info in date_columns_result:
        column_name = col_info[0]
        data_type = col_info[1]

        if _is_actually_date_column(column_name, data_type, dest_table):
            actual_date_columns.append(col_info)
        else:
            excluded_columns.append(column_name)

    # Log what was excluded
    if excluded_columns:
        logging.info(f"Excluded {len(excluded_columns)} non-date columns: {', '.join(excluded_columns[:5])}" +
                    (f"... and {len(excluded_columns) - 5} more" if len(excluded_columns) > 5 else ""))

    if not actual_date_columns:
        logging.info(f"No actual date columns found in derived.{dest_table} (after filtering)")
        return

    logging.info(f"Found {len(actual_date_columns)} actual date columns in derived.{dest_table}")

    # Process based on source table type
    is_dest_clean = 'clean' in dest_table.lower()

    # Check for invalid mapping: public -> clean table
    if source_schema == 'public' and is_dest_clean:
        logging.error(f"Cannot map from public table ({source_table}) to clean table (derived.{dest_table})")
        logging.error("Public tables can only map to non-clean derived tables")
        return

    # Route to appropriate fix function based on source and destination types
    if source_schema == 'public' and 'clean_sessions' in source_name and not is_dest_clean:
        # Public clean_sessions -> Derived (not clean)
        _fix_dates_from_clean_sessions(source_table, dest_table, actual_date_columns)
    elif source_schema == 'derived' and 'clean' not in source_name and not is_dest_clean:
        # Derived (not clean) -> Derived (not clean)
        _fix_dates_from_derived_direct(source_table, dest_table, actual_date_columns)
    elif source_schema == 'derived' and is_dest_clean:
        # Derived -> Derived clean table
        _fix_dates_to_clean_table(source_table, dest_table, actual_date_columns)
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
            logging.warning(f"Could not fix dates for {dest_table}: {e} (this may be expected in some environments)")
            continue

    logging.info(f"Batch date fix completed for {total_tables} tables")


def date_data_type_fix(table_name: str, columns: list, schema: str = 'derived', min_valid_percent: float = 90.0):
    """
    Fix data types for date columns by converting them to TIMESTAMP.

    Intelligently handles multiple date formats to prevent data loss:
    - ISO formats: 2025-07-19, 2025/07/19, 2025.07.19, 20250719
    - With time: 2025-07-19 14:30:00, 2025-07-19T14:30:00
    - Text formats: 19 July 2025, July 19 2025, 19-Jul-2025
    - US/EU formats: 07/19/2025, 19/07/2025, 19.07.2025
    - Short formats: 19/07/25, 07/19/25
    - Unix timestamps: 1721395200

    IMPORTANT: Validates data before conversion
    - Samples data to check for valid dates
    - Skips columns with too many invalid values (< min_valid_percent)
    - Logs warnings instead of crashing

    Checks if table exists, then for each column in the list:
    1. Verifies the column exists
    2. Samples data to validate it contains dates
    3. Intelligently parses various date formats
    4. Alters the column type to TIMESTAMP

    Args:
        table_name: Name of the table
        columns: List of column names to convert to TIMESTAMP
        schema: Schema name (default: 'derived')
        min_valid_percent: Minimum percentage of valid date values required (default: 90.0)

    Returns:
        Number of columns successfully converted
    """
    logging.info(f"Starting date data type fix for {schema}.{table_name}")

    # Check if table exists
    if not table_exists(schema, table_name):
        logging.error(f"Table {schema}.{table_name} does not exist")
        return 0

    logging.info(f"Table {schema}.{table_name} exists")

    converted_count = 0
    skipped_count = 0

    for column_name in columns:
        # Check if column exists
        column_check_query = f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table_name}'
                AND column_name = '{column_name}'
            );
        """

        try:
            result = inject_sql_with_return(column_check_query)

            if result and result[0][0]:
                logging.info(f"Column '{column_name}' exists, validating data...")

                # Quote column name to handle special characters like dots
                column_quoted = f'"{column_name}"' if '.' in column_name or ' ' in column_name else column_name

                # VALIDATION: Sample data to check if it contains valid dates
                sample_query = f"""
                    WITH total_count AS (
                        SELECT COUNT(*) as total
                        FROM {schema}."{table_name}"
                        WHERE {column_quoted} IS NOT NULL
                        AND TRIM({column_quoted}::TEXT) != ''
                        AND LOWER(TRIM({column_quoted}::TEXT)) NOT IN ('nan', 'none', 'nat', '<na>')
                    ),
                    sample_data AS (
                        SELECT {column_quoted}::TEXT as value
                        FROM {schema}."{table_name}"
                        WHERE {column_quoted} IS NOT NULL
                        AND TRIM({column_quoted}::TEXT) != ''
                        AND LOWER(TRIM({column_quoted}::TEXT)) NOT IN ('nan', 'none', 'nat', '<na>')
                        LIMIT 100
                    )
                    SELECT
                        (SELECT total FROM total_count) as total_rows,
                        value
                    FROM sample_data;
                """

                sample_result = inject_sql_with_return(sample_query)

                if not sample_result or len(sample_result) == 0:
                    logging.warning(f"Column '{column_name}' has no non-null data - skipping")
                    skipped_count += 1
                    continue

                # Check if sampled values look like dates
                total_rows = sample_result[0][0] if sample_result[0][0] else 0
                sample_values = [row[1] for row in sample_result if len(row) > 1]

                if not sample_values:
                    logging.warning(f"Column '{column_name}' has no valid sample data - skipping")
                    skipped_count += 1
                    continue

                date_like_count = sum(1 for val in sample_values if _looks_like_date(str(val)))
                percent_valid = (date_like_count / len(sample_values)) * 100 if sample_values else 0

                logging.info(f"Column '{column_name}' validation: {date_like_count}/{len(sample_values)} samples look like dates ({percent_valid:.1f}%)")

                if percent_valid < min_valid_percent:
                    logging.warning(f"⚠ SKIPPING '{column_name}' - only {percent_valid:.1f}% of samples are valid dates (< {min_valid_percent}% threshold)")
                    logging.warning(f"  Sample invalid values: {[val for val in sample_values if not _looks_like_date(str(val))][:5]}")
                    skipped_count += 1
                    continue

                # Log sample values before conversion
                logging.info(f"Sample values from '{column_name}':")
                for i, val in enumerate(sample_values[:10], 1):
                    logging.info(f"  {i}. '{val}'")

                logging.info(f"✓ Column '{column_name}' passed validation, converting to TIMESTAMP")

                # Convert column to TIMESTAMP with intelligent date format parsing
                # IMPORTANT: Set to NULL for unrecognized formats instead of crashing
                alter_query = f"""
                    ALTER TABLE {schema}."{table_name}"
                    ALTER COLUMN {column_quoted} TYPE TIMESTAMP
                    USING (
                        CASE
                            -- Handle NULL, empty strings, 'nan', 'None', 'NaT' FIRST
                            WHEN {column_quoted} IS NULL
                                OR TRIM({column_quoted}::TEXT) = ''
                                OR LOWER(TRIM({column_quoted}::TEXT)) IN ('nan', 'none', 'nat', '<na>')
                                THEN NULL

                            -- Handle trailing dots with T separator (e.g., "2025-06-28T06:00:00.")
                            WHEN {column_quoted}::text ~ '^\\d{{4}}[-/.]\\d{{1,2}}[-/.]\\d{{1,2}}T.*\\.$'
                                THEN TO_TIMESTAMP(RTRIM({column_quoted}::TEXT, '.'), 'YYYY-MM-DD"T"HH24:MI:SS')

                            -- Handle trailing dots with space separator (e.g., "2025-07-03 03:15:00.")
                            WHEN {column_quoted}::text ~ '^\\d{{4}}[-/.]\\d{{1,2}}[-/.]\\d{{1,2}}\\s+.*\\.$'
                                THEN TO_TIMESTAMP(RTRIM({column_quoted}::TEXT, '.'), 'YYYY-MM-DD HH24:MI:SS')

                            -- ISO-like formats: 2025-07-19 or 2025/07/19 or 2025.07.19
                            WHEN {column_quoted}::text ~ '^\\d{{4}}[-/.]\\d{{1,2}}[-/.]\\d{{1,2}}$'
                                THEN TO_TIMESTAMP({column_quoted}::text, 'YYYY-MM-DD')

                            -- ISO with time: 2025-07-19 14:30:00
                            WHEN {column_quoted}::text ~ '^\\d{{4}}[-/.]\\d{{1,2}}[-/.]\\d{{1,2}}\\s+\\d{{1,2}}:\\d{{2}}(:\\d{{2}})?'
                                THEN TO_TIMESTAMP({column_quoted}::text, 'YYYY-MM-DD HH24:MI:SS')

                            -- ISO 8601 with T separator: 2025-07-19T14:30:00
                            WHEN {column_quoted}::text ~ '^\\d{{4}}[-/.]\\d{{1,2}}[-/.]\\d{{1,2}}T\\d{{1,2}}:\\d{{2}}'
                                THEN TO_TIMESTAMP(SUBSTRING({column_quoted}::text FROM '^[^+Z]+'), 'YYYY-MM-DD"T"HH24:MI:SS')

                            -- DD Month YYYY: 19 July 2025
                            WHEN {column_quoted}::text ~ '^\\d{{1,2}}\\s+[A-Za-z]+\\s+\\d{{4}}$'
                                THEN TO_TIMESTAMP({column_quoted}::text, 'DD Month YYYY')

                            -- Month DD, YYYY: July 19, 2025
                            WHEN {column_quoted}::text ~ '^[A-Za-z]+\\s+\\d{{1,2}},?\\s+\\d{{4}}$'
                                THEN TO_TIMESTAMP(REPLACE({column_quoted}::text, ',', ''), 'Month DD YYYY')

                            -- YYYY Month DD: 2025 July 19
                            WHEN {column_quoted}::text ~ '^\\d{{4}}\\s+[A-Za-z]+\\s+\\d{{1,2}}$'
                                THEN TO_TIMESTAMP({column_quoted}::text, 'YYYY Month DD')

                            -- YYYY Month,DD or YYYY Month, DD: 2025 July,19
                            WHEN {column_quoted}::text ~ '^\\d{{4}}\\s+[A-Za-z]+,?\\s?\\d{{1,2}}$'
                                THEN TO_TIMESTAMP(REPLACE({column_quoted}::text, ',', ''), 'YYYY Month DD')

                            -- DD-Month-YYYY or DD Month YYYY: 19-Jul-2025, 19 Jul 2025
                            WHEN {column_quoted}::text ~ '^\\d{{1,2}}[- ]?[A-Za-z]{{3,9}}[- ]?\\d{{4}}$'
                                THEN TO_TIMESTAMP(REPLACE({column_quoted}::text, '-', ' '), 'DD Month YYYY')

                            -- US format MM/DD/YYYY: 07/19/2025
                            WHEN {column_quoted}::text ~ '^(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])/\\d{{4}}$'
                                THEN TO_TIMESTAMP({column_quoted}::text, 'MM/DD/YYYY')

                            -- European DD/MM/YYYY: 19/07/2025
                            WHEN {column_quoted}::text ~ '^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/\\d{{4}}$'
                                THEN TO_TIMESTAMP({column_quoted}::text, 'DD/MM/YYYY')

                            -- European DD.MM.YYYY: 19.07.2025
                            WHEN {column_quoted}::text ~ '^(0?[1-9]|[12][0-9]|3[01])\\.(0?[1-9]|1[0-2])\\.\\d{{4}}$'
                                THEN TO_TIMESTAMP({column_quoted}::text, 'DD.MM.YYYY')

                            -- DD-MM-YYYY: 19-07-2025
                            WHEN {column_quoted}::text ~ '^(0?[1-9]|[12][0-9]|3[01])-(0?[1-9]|1[0-2])-\\d{{4}}$'
                                THEN TO_TIMESTAMP({column_quoted}::text, 'DD-MM-YYYY')

                            -- Short format DD/MM/YY: 19/07/25 (assume 20xx for YY)
                            WHEN {column_quoted}::text ~ '^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/\\d{{2}}$'
                                THEN TO_TIMESTAMP({column_quoted}::text, 'DD/MM/YY')

                            -- Short format MM/DD/YY: 07/19/25 (assume 20xx for YY)
                            WHEN {column_quoted}::text ~ '^(0?[1-9]|1[0-2])/(0?[1-9]|[12][0-9]|3[01])/\\d{{2}}$'
                                THEN TO_TIMESTAMP({column_quoted}::text, 'MM/DD/YY')

                            -- Compact YYYYMMDD: 20250719
                            WHEN {column_quoted}::text ~ '^\\d{{8}}$'
                                THEN TO_TIMESTAMP({column_quoted}::text, 'YYYYMMDD')

                            -- Unix timestamp (10 digits): 1721395200
                            WHEN {column_quoted}::text ~ '^\\d{{10}}$'
                                THEN TO_TIMESTAMP({column_quoted}::text::bigint)

                            -- Unix timestamp milliseconds (13 digits): 1721395200000
                            WHEN {column_quoted}::text ~ '^\\d{{13}}$'
                                THEN TO_TIMESTAMP({column_quoted}::text::bigint / 1000.0)

                            -- Try to convert - will fail with error showing the problematic value
                            ELSE {column_quoted}::timestamp
                        END
                    );
                """

                try:
                    inject_sql_procedure(alter_query, f"CONVERT {table_name}.{column_name} TO TIMESTAMP")
                    logging.info(f"✓ Converted {schema}.{table_name}.{column_name} to TIMESTAMP")
                    converted_count += 1
                except Exception as conversion_error:
                    # Conversion failed - now query and log ALL unique values to understand the format
                    logging.error(f"")
                    logging.error(f"{'='*70}")
                    logging.error(f"❌ CONVERSION FAILED FOR COLUMN: {column_name}")
                    logging.error(f"{'='*70}")
                    logging.error(f"Error: {conversion_error}")
                    logging.error(f"")
                    logging.error(f"Querying column to analyze all unique values...")

                    try:
                        # Get ALL unique values to understand the data format
                        problem_values_query = f"""
                            SELECT DISTINCT {column_quoted}::TEXT as value, COUNT(*) as count
                            FROM {schema}."{table_name}"
                            WHERE {column_quoted} IS NOT NULL
                            AND TRIM({column_quoted}::TEXT) != ''
                            AND LOWER(TRIM({column_quoted}::TEXT)) NOT IN ('nan', 'none', 'nat', '<na>')
                            GROUP BY {column_quoted}::TEXT
                            ORDER BY count DESC
                            LIMIT 50;
                        """

                        problem_result = inject_sql_with_return(problem_values_query)

                        if problem_result:
                            logging.error(f"UNIQUE VALUES IN COLUMN '{column_name}' (by frequency):")
                            logging.error(f"{'='*70}")
                            logging.error(f"{'Rank':<6} {'Value':<40} {'Count':<10}")
                            logging.error(f"{'-'*70}")

                            total_rows = sum(row[1] for row in problem_result if len(row) > 1)

                            for idx, row in enumerate(problem_result, 1):
                                value = row[0] if row[0] else 'NULL'
                                count = row[1] if len(row) > 1 else 0

                                # Show value, truncate if too long
                                display_value = value if len(str(value)) <= 38 else str(value)[:35] + '...'

                                # Show percentage
                                percent = (count / total_rows * 100) if total_rows > 0 else 0

                                logging.error(f"#{idx:<5} '{display_value:<38}' {count:<6} ({percent:.1f}%)")

                            logging.error(f"{'='*70}")
                            logging.error(f"Total unique values shown: {len(problem_result)} (showing up to 50)")
                            logging.error(f"Total non-null rows: {total_rows}")
                            logging.error(f"")
                            logging.error(f"ACTION REQUIRED:")
                            logging.error(f"  1. Analyze these values to understand the date format")
                            logging.error(f"  2. Add a new WHEN clause to handle this format")
                            logging.error(f"  3. Or determine if these are invalid dates that should be NULL")
                            logging.error(f"")
                            logging.error(f"EXAMPLE: If 'T0' means 'Time 0' or baseline measurement:")
                            logging.error(f"  Add: WHEN {column_quoted}::text ~ '^T\\d+$' THEN NULL")
                            logging.error(f"")
                            logging.error(f"{'='*70}")

                    except Exception as query_error:
                        logging.error(f"Could not query problem values: {query_error}")

                    logging.error(f"Skipping column '{column_name}' - requires manual investigation")
                    logging.error(f"")
                    skipped_count += 1
                    continue
            else:
                logging.warning(f"Column '{column_name}' does not exist in {schema}.{table_name}")
        except Exception as e:
            logging.error(f"Error processing column '{column_name}': {e}")
            skipped_count += 1
            # Continue with next column

    # Final summary
    logging.info("")
    logging.info("="*60)
    logging.info(f"DATE TYPE FIX SUMMARY for {schema}.{table_name}")
    logging.info("="*60)
    logging.info(f"Total columns requested: {len(columns)}")
    logging.info(f"✓ Successfully converted: {converted_count}")
    logging.info(f"⚠ Skipped (invalid data): {skipped_count}")
    logging.info(f"✗ Errors: {len(columns) - converted_count - skipped_count}")
    logging.info("="*60)

    return converted_count


def _ensure_label_columns_are_text(dest_table: str, date_columns):
    """
    Ensure that .label columns are of type TEXT before updating them.

    When fixing dates, we update both .value (timestamp) and .label (text) columns.
    If .label column is not text type, we need to convert it first to avoid type errors.

    Args:
        dest_table: Destination table name
        date_columns: List of date column info tuples (column_name, data_type)
    """
    # Find all .value columns that need .label conversion
    value_columns = [col[0] for col in date_columns if col[0].endswith('.value')]

    if not value_columns:
        return  # No .value columns to process

    # Check each corresponding .label column
    for value_col in value_columns:
        variable_name = value_col.rsplit('.', 1)[0]
        label_col = f"{variable_name}.label"

        # Check if .label column exists and its type
        type_check_query = f"""
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = 'derived'
            AND table_name = '{dest_table}'
            AND column_name = '{label_col}';
        """

        try:
            result = inject_sql_with_return(type_check_query)
            if result and result[0]:
                current_type = result[0][0].lower()

                # If not text/character type, convert it
                if 'text' not in current_type and 'character' not in current_type:
                    logging.info(f"Converting {dest_table}.{label_col} from {current_type} to TEXT")

                    convert_query = f"""
                        ALTER TABLE derived."{dest_table}"
                        ALTER COLUMN "{label_col}" TYPE TEXT USING "{label_col}"::TEXT;
                    """

                    inject_sql_procedure(convert_query, f"CONVERT {dest_table}.{label_col} TO TEXT")
                    logging.info(f"✓ Converted {dest_table}.{label_col} to TEXT")
        except Exception as e:
            logging.warning(f"Could not check/convert {label_col}: {e}")
            # Continue - the column might not exist, which is okay


def _fix_dates_from_clean_sessions(source_table: str, dest_table: str, date_columns):
    """
    Fix dates when source is public.clean_sessions
    Extract from: data -> 'entries' -> 'VariableName' -> 'values' -> 'value' ->> 0
    Format: timestamps as 'YYYY-MM-DD HH:MI', dates as 'YYYY-MM-DD'
    Processes each column separately for easier error isolation.
    """
    logging.info(f"Fixing dates from clean_sessions to derived.{dest_table}")

    # Ensure .label columns are TEXT type before we start fixing
    _ensure_label_columns_are_text(dest_table, date_columns)

    # Validate that source table has the 'data' column (required for clean_sessions)
    source_schema, source_name = source_table.split('.', 1) if '.' in source_table else ('public', source_table)

    data_column_check = f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = '{source_schema}'
            AND table_name = '{source_name}'
            AND column_name = 'data'
        );
    """

    data_column_exists_result = inject_sql_with_return(data_column_check)
    if not data_column_exists_result or not data_column_exists_result[0][0]:
        logging.warning(f"Source table {source_table} does not have a 'data' column - cannot extract from clean_sessions format")
        logging.info(f"Skipping date fix for {dest_table} from {source_table} (this is expected in some environments)")
        return

    # Process each date column separately
    for col_info in date_columns:
        dest_col = col_info[0]
        data_type = col_info[1].lower()

        # Determine date format
        if 'timestamp' in data_type or ('time' in data_type and 'date' not in data_type):
            date_format = 'YYYY-MM-DD HH24:MI'
        else:
            date_format = 'YYYY-MM-DD'

        # Extract variable name from destination column
        # For "VariableName.value" or "VariableName.label", extract "VariableName"
        # For "variablename" (clean tables), use as-is
        if dest_col.endswith('.value') or dest_col.endswith('.label'):
            variable_name = dest_col.rsplit('.', 1)[0]
        else:
            variable_name = dest_col

        # Skip .label columns, they will be handled with their .value counterparts
        if dest_col.endswith('.label'):
            continue

        # Build update query for this specific column
        # For .value columns, also update the corresponding .label column
        if dest_col.endswith('.value'):
            label_col = f"{variable_name}.label"

            update_query = f"""
                WITH cleaned AS (
                    SELECT
                        s.uid,
                        s.data ->> 'unique_key' AS unique_key,
                        TRIM(s.data -> 'entries' -> '{variable_name}' -> 'values' ->> 'value') AS raw_val
                    FROM {source_table} s
                    WHERE s.data -> 'entries' -> '{variable_name}' -> 'values' ->> 'value' IS NOT NULL
                    AND s.data -> 'entries' -> '{variable_name}' -> 'values' ->> 'value' NOT IN ('', 'None')
                    AND LOWER(s.data -> 'entries' -> '{variable_name}' -> 'values' ->> 'value') != 'nan'
                ),
                parsed AS (
                    SELECT
                        uid,
                        unique_key,
                        raw_val,
                        CASE
                            WHEN raw_val ~ '^\\d{{4}}[-/]\\d{{1,2}}[-/]\\d{{1,2}}$'
                                THEN TO_TIMESTAMP(raw_val, 'YYYY-MM-DD')
                            WHEN raw_val ~ '^\\d{{1,2}} [A-Za-z]+ \\d{{4}}$'
                                THEN TO_TIMESTAMP(raw_val, 'DD Month YYYY')
                            WHEN raw_val ~ '^\\d{{4}} [A-Za-z]+ \\d{{1,2}}$'
                                THEN TO_TIMESTAMP(raw_val, 'YYYY Month DD')
                            WHEN raw_val ~ '^\\d{{4}} [A-Za-z]+,? ?\\d{{1,2}}$'
                                THEN TO_TIMESTAMP(REPLACE(raw_val, ',', ''), 'YYYY Month DD')
                            WHEN raw_val ~ '^\\d{{1,2}}[- ]?[A-Za-z]{{3,9}}[- ]?\\d{{4}}$'
                                THEN TO_TIMESTAMP(REPLACE(raw_val, '-', ' '), 'DD Month YYYY')
                            ELSE NULL
                        END AS date_val
                    FROM cleaned
                ),
                formatted AS (
                    SELECT
                        uid,
                        unique_key,
                        date_val,
                        TO_CHAR(date_val, '{date_format}') AS label_val
                    FROM parsed
                    WHERE date_val IS NOT NULL
                ),
                updated AS (
                    UPDATE derived."{dest_table}" d
                    SET "{dest_col}" = f.date_val,
                        "{label_col}" = f.label_val
                    FROM formatted f
                    WHERE d.uid = f.uid
                    AND d.unique_key = f.unique_key
                    AND d."{dest_col}" IS NULL
                    RETURNING d.uid
                )
                SELECT COUNT(*) AS updated_count,
                    ARRAY_AGG(DISTINCT uid ORDER BY uid) FILTER (WHERE uid IS NOT NULL) AS sample_uids
                FROM (SELECT uid FROM updated LIMIT 5) sampled;
            """
        else:
                # Standalone column (no .value suffix)
                update_query = f"""
                    WITH cleaned AS (
                        SELECT
                            s.uid,
                            s.data ->> 'unique_key' AS unique_key,
                            TRIM(s.data -> 'entries' -> '{variable_name}' -> 'values' ->> 'value') AS raw_val
                        FROM {source_table} s
                        WHERE s.data -> 'entries' -> '{variable_name}' -> 'values' ->> 'value' IS NOT NULL
                        AND s.data -> 'entries' -> '{variable_name}' -> 'values' ->> 'value' NOT IN ('', 'None')
                        AND LOWER(s.data -> 'entries' -> '{variable_name}' -> 'values' ->> 'value') != 'nan'
                    ),
                    parsed AS (
                        SELECT
                            uid,
                            unique_key,
                            raw_val,
                            CASE
                                WHEN raw_val ~ '^\\d{{4}}[-/]\\d{{1,2}}[-/]\\d{{1,2}}$'
                                    THEN TO_TIMESTAMP(raw_val, 'YYYY-MM-DD')
                                WHEN raw_val ~ '^\\d{{1,2}} [A-Za-z]+ \\d{{4}}$'
                                    THEN TO_TIMESTAMP(raw_val, 'DD Month YYYY')
                                WHEN raw_val ~ '^\\d{{4}} [A-Za-z]+ \\d{{1,2}}$'
                                    THEN TO_TIMESTAMP(raw_val, 'YYYY Month DD')
                                WHEN raw_val ~ '^\\d{{4}} [A-Za-z]+,? ?\\d{{1,2}}$'
                                    THEN TO_TIMESTAMP(REPLACE(raw_val, ',', ''), 'YYYY Month DD')
                                WHEN raw_val ~ '^\\d{{1,2}}[- ]?[A-Za-z]{{3,9}}[- ]?\\d{{4}}$'
                                    THEN TO_TIMESTAMP(REPLACE(raw_val, '-', ' '), 'DD Month YYYY')
                                ELSE NULL
                            END AS date_val
                        FROM cleaned
                    ),
                    updated AS (
                        UPDATE derived."{dest_table}" d
                        SET "{dest_col}" = p.date_val
                        FROM parsed p
                        WHERE d.uid = p.uid
                        AND d.unique_key = p.unique_key
                        AND d."{dest_col}" IS NULL
                        AND p.date_val IS NOT NULL
                        RETURNING d.uid
                    )
                    SELECT COUNT(*) AS updated_count,
                        ARRAY_AGG(DISTINCT uid ORDER BY uid) FILTER (WHERE uid IS NOT NULL) AS sample_uids
                    FROM (SELECT uid FROM updated LIMIT 5) sampled;
                """

        try:
            result = inject_sql_with_return(update_query)
            if result and result[0]:
                count = result[0][0] if result[0][0] else 0
                sample_uids = result[0][1] if len(result[0]) > 1 and result[0][1] else []

                if count > 0:
                    logging.info(f"✓ Fixed {count} records in {dest_table}.{dest_col}")
                    logging.info(f"  Variable: {variable_name}")
                    logging.info(f"  Sample UIDs: {sample_uids[:5] if sample_uids else 'None'}")
                else:
                    logging.debug(f"No null dates found for {dest_table}.{dest_col}")
        except Exception as e:
            logging.warning(f"Could not fix {dest_col} (variable: {variable_name}): {e} - may have invalid date values")
            # Continue with next column even if this one fails


def _fix_dates_from_derived_direct(source_table: str, dest_table: str, date_columns):
    """
    Fix dates when source is a derived table without 'clean' in name
    Direct column mapping - EXACT match only
    Format: timestamps as 'YYYY-MM-DD HH:MI', dates as 'YYYY-MM-DD'
    Processes each column separately for easier error isolation.
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

    # Process each date column separately
    columns_processed = 0
    columns_fixed = 0

    for col_info in date_columns:
        dest_col = col_info[0]
        data_type = col_info[1].lower()

        # Determine date format
        if 'timestamp' in data_type or ('time' in data_type and 'date' not in data_type):
            date_format = 'YYYY-MM-DD HH24:MI'
        else:
            date_format = 'YYYY-MM-DD'

        # For derived to derived (non-clean): ONLY exact match
        # e.g., "DateField.value" in dest -> "DateField.value" in source
        if dest_col not in source_cols:
            logging.debug(f"No exact match found for '{dest_col}' in source table")
            continue

        source_col = dest_col
        logging.debug(f"Exact match: '{dest_col}' -> '{source_col}'")

        # Quote column names if they contain dots or spaces
        dest_col_quoted = f'"{dest_col}"' if '.' in dest_col or ' ' in dest_col else dest_col
        source_col_quoted = f'"{source_col}"' if '.' in source_col or ' ' in source_col else source_col

        # Build individual update query
        update_query = f"""
            WITH cleaned AS (
                SELECT
                    s.uid,
                    s.unique_key,
                    TRIM(s.{source_col_quoted}::text) AS raw_val
                FROM {source_schema}."{source_name}" s
                WHERE s.{source_col_quoted} IS NOT NULL
                AND s.{source_col_quoted}::text NOT IN ('', 'None')
                AND LOWER(s.{source_col_quoted}::text) != 'nan'
            ),
            parsed AS (
                SELECT
                    uid,
                    unique_key,
                    raw_val,
                    CASE
                        WHEN raw_val ~ '^\\d{{4}}[-/]\\d{{1,2}}[-/]\\d{{1,2}}$'
                            THEN TO_TIMESTAMP(raw_val, 'YYYY-MM-DD')
                        WHEN raw_val ~ '^\\d{{1,2}} [A-Za-z]+ \\d{{4}}$'
                            THEN TO_TIMESTAMP(raw_val, 'DD Month YYYY')
                        WHEN raw_val ~ '^\\d{{4}} [A-Za-z]+ \\d{{1,2}}$'
                            THEN TO_TIMESTAMP(raw_val, 'YYYY Month DD')
                        WHEN raw_val ~ '^\\d{{4}} [A-Za-z]+,? ?\\d{{1,2}}$'
                            THEN TO_TIMESTAMP(REPLACE(raw_val, ',', ''), 'YYYY Month DD')
                        WHEN raw_val ~ '^\\d{{1,2}}[- ]?[A-Za-z]{{3,9}}[- ]?\\d{{4}}$'
                            THEN TO_TIMESTAMP(REPLACE(raw_val, '-', ' '), 'DD Month YYYY')
                        ELSE NULL
                    END AS date_val
                FROM cleaned
            ),
            updated AS (
                UPDATE derived."{dest_table}" d
                SET {dest_col_quoted} = TO_CHAR(p.date_val, '{date_format}')::timestamp
                FROM parsed p
                WHERE d.uid = p.uid
                AND d.unique_key = p.unique_key
                AND d.{dest_col_quoted} IS NULL
                AND p.date_val IS NOT NULL
                RETURNING d.uid
            )
            SELECT COUNT(*) AS updated_count,
                ARRAY_AGG(DISTINCT uid ORDER BY uid) FILTER (WHERE uid IS NOT NULL) AS sample_uids
            FROM (SELECT uid FROM updated LIMIT 5) sampled;
        """


        try:
            result = inject_sql_with_return(update_query)
            if result and result[0]:
                count = result[0][0] if result[0][0] else 0
                sample_uids = result[0][1] if len(result[0]) > 1 and result[0][1] else []

                columns_processed += 1
                if count > 0:
                    columns_fixed += 1
                    logging.info(f"✓ Fixed {count} records in {dest_table}.{dest_col}")
                    logging.info(f"  Sample UIDs: {sample_uids[:5] if sample_uids else 'None'}")
                else:
                    logging.debug(f"No null dates found for {dest_table}.{dest_col}")
        except Exception as e:
            logging.warning(f"Could not fix {dest_col}: {e} - may have invalid date values")
            # Continue with next column

    if columns_processed == 0:
        logging.warning(f"No matching columns found between {source_table} and {dest_table}")
    else:
        logging.info(f"Processed {columns_processed} columns, fixed {columns_fixed} columns with data")


def _fix_dates_to_clean_table(source_table: str, dest_table: str, date_columns):
    """
    Fix dates when destination table has 'clean' in name
    Priority 1: VariableName.value -> variablename (case-insensitive)
    Priority 2: VariableName -> variablename (case-insensitive)
    Format: timestamps as 'YYYY-MM-DD HH:MI', dates as 'YYYY-MM-DD'
    Processes each column separately for easier error isolation.
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
    source_cols = [row[0] for row in source_cols_result] if source_cols_result else []

    # Create mappings for source columns:
    # 1. Map columns with .value suffix: "VariableName.value" -> {"variablename": "VariableName.value"}
    # 2. Map columns without .value suffix: "VariableName" -> {"variablename": "VariableName"}
    source_cols_with_value = {}  # Priority 1: columns with .value
    source_cols_without_value = {}  # Priority 2: columns without .value

    for col in source_cols:
        if col.endswith('.value'):
            base_col = col[:-6]  # Remove '.value'
            source_cols_with_value[base_col.lower()] = col
        else:
            source_cols_without_value[col.lower()] = col

    # Process each date column separately
    columns_processed = 0
    columns_fixed = 0

    for col_info in date_columns:
        dest_col = col_info[0]
        data_type = col_info[1].lower()

        # Determine date format
        if 'timestamp' in data_type or ('time' in data_type and 'date' not in data_type):
            date_format = 'YYYY-MM-DD HH24:MI'
        else:
            date_format = 'YYYY-MM-DD'

        # Find matching source column with correct priority
        source_col = None
        dest_col_lower = dest_col.lower()

        # Priority 1: Look for "VariableName.value" in source (case-insensitive)
        if dest_col_lower in source_cols_with_value:
            source_col = source_cols_with_value[dest_col_lower]
            logging.debug(f"Priority 1 match: '{dest_col}' <- '{source_col}' (with .value)")
        # Priority 2: Look for exact match "VariableName" (case-insensitive, no .value)
        elif dest_col_lower in source_cols_without_value:
            source_col = source_cols_without_value[dest_col_lower]
            logging.debug(f"Priority 2 match: '{dest_col}' <- '{source_col}' (exact match)")
        else:
            logging.debug(f"No matching column found for '{dest_col}' in source table")
            continue

        # Quote column names if they contain dots or spaces
        dest_col_quoted = f'"{dest_col}"' if '.' in dest_col or ' ' in dest_col else dest_col
        source_col_quoted = f'"{source_col}"' if '.' in source_col or ' ' in source_col else source_col

        # Build individual update query
        update_query = f"""
                WITH cleaned AS (
                    SELECT
                        s.uid,
                        s.unique_key,
                        TRIM(s.{source_col_quoted}::text) AS raw_val
                    FROM {source_schema}."{source_name}" s
                    WHERE s.{source_col_quoted} IS NOT NULL
                    AND s.{source_col_quoted}::text NOT IN ('', 'None')
                    AND LOWER(s.{source_col_quoted}::text) != 'nan'
                ),
                parsed AS (
                    SELECT
                        uid,
                        unique_key,
                        raw_val,
                        CASE
                            -- ISO-like formats: 2025-07-19 or 2025/07/19
                            WHEN raw_val ~ '^\\d{{4}}[-/]\\d{{1,2}}[-/]\\d{{1,2}}$'
                                THEN TO_TIMESTAMP(raw_val, 'YYYY-MM-DD')
                            -- 19 July 2025
                            WHEN raw_val ~ '^\\d{{1,2}} [A-Za-z]+ \\d{{4}}$'
                                THEN TO_TIMESTAMP(raw_val, 'DD Month YYYY')
                            -- 2025 July 19
                            WHEN raw_val ~ '^\\d{{4}} [A-Za-z]+ \\d{{1,2}}$'
                                THEN TO_TIMESTAMP(raw_val, 'YYYY Month DD')
                            -- 2025 July,19 or 2025 July, 19
                            WHEN raw_val ~ '^\\d{{4}} [A-Za-z]+,? ?\\d{{1,2}}$'
                                THEN TO_TIMESTAMP(REPLACE(raw_val, ',', ''), 'YYYY Month DD')
                            -- 19-Jul-2025 or 19 Jul 2025
                            WHEN raw_val ~ '^\\d{{1,2}}[- ]?[A-Za-z]{{3,9}}[- ]?\\d{{4}}$'
                                THEN TO_TIMESTAMP(REPLACE(raw_val, '-', ' '), 'DD Month YYYY')
                            ELSE NULL
                        END AS date_val
                    FROM cleaned
                ),
                updated AS (
                    UPDATE derived."{dest_table}" d
                    SET {dest_col_quoted} = TO_CHAR(p.date_val, '{date_format}')::timestamp
                    FROM parsed p
                    WHERE d.uid = p.uid
                    AND d.unique_key = p.unique_key
                    AND d.{dest_col_quoted} IS NULL
                    AND p.date_val IS NOT NULL
                    RETURNING d.uid
                )
                SELECT COUNT(*) AS updated_count,
                    ARRAY_AGG(DISTINCT uid ORDER BY uid) FILTER (WHERE uid IS NOT NULL) AS sample_uids
                FROM (SELECT uid FROM updated LIMIT 5) sampled;
            """
        try:
            result = inject_sql_with_return(update_query)
            if result and result[0]:
                count = result[0][0] if result[0][0] else 0
                sample_uids = result[0][1] if len(result[0]) > 1 and result[0][1] else []

                columns_processed += 1
                if count > 0:
                    columns_fixed += 1
                    logging.info(f"✓ Fixed {count} records in {dest_table}.{dest_col}")
                    logging.info(f"  Mapping: {dest_col} <- {source_col}")
                    logging.info(f"  Sample UIDs: {sample_uids[:5] if sample_uids else 'None'}")
                else:
                    logging.debug(f"No null dates found for {dest_table}.{dest_col}")
        except Exception as e:
            logging.warning(f"Could not fix {dest_col} <- {source_col}: {e} - may have invalid date values")
            # Continue with next column

    if columns_processed == 0:
        logging.warning(f"No matching columns found between {source_table} and {dest_table}")
    else:
        logging.info(f"Processed {columns_processed} columns, fixed {columns_fixed} columns with data")


def count_table_columns(table_name: str, schema: str = 'derived') -> dict:
    """
    Count total columns in a table, including dropped (ghost) columns.

    Args:
        table_name: Name of the table
        schema: Schema name (default: 'derived')

    Returns:
        Dict with 'active', 'dropped', and 'total' column counts
    """
    query = f"""
        SELECT
            COUNT(*) FILTER (WHERE NOT attisdropped) AS active_columns,
            COUNT(*) FILTER (WHERE attisdropped) AS dropped_columns,
            COUNT(*) AS total_columns
        FROM pg_attribute
        WHERE attrelid = '{schema}.{table_name}'::regclass
        AND attnum > 0;
    """

    try:
        result = inject_sql_with_return(query)
        if result and result[0]:
            active, dropped, total = result[0]
            logging.info(f"Table {schema}.{table_name}: {active} active, {dropped} dropped, {total} total columns")

            if total >= 1500:
                logging.warning(f"⚠ Table {schema}.{table_name} is approaching the 1600 column limit!")

            return {
                'active': active,
                'dropped': dropped,
                'total': total
            }
    except Exception as e:
        logging.error(f"Error counting columns for {schema}.{table_name}: {e}")

    return {'active': 0, 'dropped': 0, 'total': 0}


def rebuild_table_dry_run(table_name: str, schema: str = 'derived'):
    """
    Dry run: Report what would happen during a rebuild WITHOUT actually rebuilding.

    Shows:
    - Current column counts (active, dropped, total)
    - Row count
    - Estimated space savings
    - What actions would be taken

    Args:
        table_name: Name of the table to analyze
        schema: Schema name (default: 'derived')

    Returns:
        Dict with analysis results
    """
    logging.info(f"=== DRY RUN: Rebuild Analysis for {schema}.{table_name} ===")

    # Check if table exists
    if not table_exists(schema, table_name):
        logging.error(f"Table {schema}.{table_name} does not exist")
        return {'error': 'Table not found', 'can_rebuild': False}

    # Get column counts
    col_info = count_table_columns(table_name, schema)

    # Get row count
    row_count_query = f"SELECT COUNT(*) FROM {schema}.{table_name};"
    row_count_result = inject_sql_with_return(row_count_query)
    row_count = row_count_result[0][0] if row_count_result else 0

    # Calculate potential savings
    percent_used = (col_info['total'] / 1600) * 100
    slots_after_rebuild = col_info['active']
    slots_reclaimed = col_info['dropped']
    slots_available_after = 1600 - col_info['active']

    # Build report
    logging.info("")
    logging.info("="*60)
    logging.info("CURRENT STATE:")
    logging.info("="*60)
    logging.info(f"Table: {schema}.{table_name}")
    logging.info(f"Rows: {row_count:,}")
    logging.info(f"Active columns: {col_info['active']}")
    logging.info(f"Dropped (ghost) columns: {col_info['dropped']}")
    logging.info(f"Total columns: {col_info['total']}/1600 ({percent_used:.1f}%)")

    if col_info['dropped'] == 0:
        logging.info("")
        logging.info("No dropped columns found - rebuild not necessary")
        return {
            'can_rebuild': False,
            'reason': 'No dropped columns',
            'current': col_info,
            'row_count': row_count
        }

    logging.info("")
    logging.info("="*60)
    logging.info("AFTER REBUILD (PROJECTED):")
    logging.info("="*60)
    logging.info(f"Active columns: {slots_after_rebuild}")
    logging.info(f"Dropped columns: 0")
    logging.info(f"Total columns: {slots_after_rebuild}/1600 ({(slots_after_rebuild/1600)*100:.1f}%)")
    logging.info(f"Available slots: {slots_available_after}")

    logging.info("")
    logging.info("="*60)
    logging.info("ACTIONS THAT WOULD BE TAKEN:")
    logging.info("="*60)
    logging.info(f"1. Count original rows: {row_count:,}")
    logging.info(f"2. Create table {schema}.{table_name}_rebuild with {col_info['active']} columns")
    logging.info(f"3. Copy all {row_count:,} rows to rebuild table")
    logging.info(f"4. Verify row count matches ({row_count:,} rows)")
    logging.info(f"5. Rename {table_name} to {table_name}_backup")
    logging.info(f"6. Rename {table_name}_rebuild to {table_name}")
    logging.info(f"7. Drop {table_name}_backup")
    logging.info(f"8. Verify final state")

    logging.info("")
    logging.info("="*60)
    logging.info("BENEFITS:")
    logging.info("="*60)
    logging.info(f"Reclaim {slots_reclaimed} column slots")
    logging.info(f"Free up space from dropped columns")
    logging.info(f"Available capacity: {slots_available_after}/1600 slots")

    if col_info['total'] >= 1500:
        logging.warning("")
        logging.warning(" WARNING: Table is approaching 1600 column limit!")
        logging.warning("   Rebuild is STRONGLY recommended")

    logging.info("")
    logging.info("="*60)
    logging.info("TO EXECUTE ACTUAL REBUILD:")
    logging.info("="*60)
    logging.info(f"rebuild_table_to_remove_dropped_columns('{table_name}', '{schema}')")
    logging.info("")
    logging.info("OR with auto-rebuild:")
    logging.info(f"fix_column_limit_error('{table_name}', '{schema}', auto_rebuild=True)")
    logging.info("="*60)

    return {
        'can_rebuild': True,
        'reason': f'{slots_reclaimed} dropped columns to reclaim',
        'current': col_info,
        'row_count': row_count,
        'after_rebuild': {
            'active': slots_after_rebuild,
            'dropped': 0,
            'total': slots_after_rebuild,
            'available': slots_available_after
        },
        'slots_reclaimed': slots_reclaimed,
        'recommended': col_info['total'] >= 1500
    }


def rebuild_table_to_remove_dropped_columns(table_name: str, schema: str = 'derived'):
    """
    Rebuild a table to physically remove dropped (ghost) columns and reclaim space.

    This is necessary when approaching the 1600 column limit in PostgreSQL.
    Dropped columns still count toward the limit until the table is rebuilt.

    SECURITY FEATURES:
    - Transaction-based with automatic rollback on failure
    - Validates row count before and after
    - Keeps backup table if validation fails
    - Verifies column counts after rebuild

    Args:
        table_name: Name of the table to rebuild
        schema: Schema name (default: 'derived')

    Returns:
        True if rebuild successful, False otherwise
    """
    logging.info(f"Starting rebuild of {schema}.{table_name} to remove dropped columns")

    # Check if table exists
    if not table_exists(schema, table_name):
        logging.error(f"Table {schema}.{table_name} does not exist")
        return False

    # Get column counts BEFORE rebuild
    col_info_before = count_table_columns(table_name, schema)

    if col_info_before['dropped'] == 0:
        logging.info(f"No dropped columns found in {schema}.{table_name} - rebuild not necessary")
        return True

    logging.info(f"Found {col_info_before['dropped']} dropped columns to remove from {schema}.{table_name}")

    # Get row count BEFORE rebuild for validation
    row_count_query = f"SELECT COUNT(*) FROM {schema}.{table_name};"
    row_count_result = inject_sql_with_return(row_count_query)
    original_row_count = row_count_result[0][0] if row_count_result else 0
    logging.info(f"Original table has {original_row_count} rows")

    # Rebuild with comprehensive safety measures
    rebuild_query = f"""
        DO $$
        DECLARE
            original_row_count BIGINT;
            rebuild_row_count BIGINT;
            backup_exists BOOLEAN := FALSE;
        BEGIN
            -- Step 1: Count original rows
            SELECT COUNT(*) INTO original_row_count FROM {schema}.{table_name};
            RAISE NOTICE 'Original table has % rows', original_row_count;

            -- Step 2: Check if backup already exists (safety check)
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = '{schema}'
                AND table_name = '{table_name}_backup'
            ) INTO backup_exists;

            IF backup_exists THEN
                RAISE EXCEPTION 'Backup table {schema}.{table_name}_backup already exists. Please remove it first.';
            END IF;

            -- Step 3: Create rebuild table with only active columns
            RAISE NOTICE 'Creating rebuild table...';
            CREATE TABLE {schema}.{table_name}_rebuild AS
            SELECT * FROM {schema}.{table_name};

            -- Step 4: Verify row count in rebuild table
            SELECT COUNT(*) INTO rebuild_row_count FROM {schema}.{table_name}_rebuild;
            RAISE NOTICE 'Rebuild table has % rows', rebuild_row_count;

            IF rebuild_row_count != original_row_count THEN
                RAISE EXCEPTION 'Row count mismatch! Original: %, Rebuild: %', original_row_count, rebuild_row_count;
            END IF;

            -- Step 5: Rename original to backup (safety measure)
            RAISE NOTICE 'Renaming original to backup...';
            ALTER TABLE {schema}.{table_name}
            RENAME TO {table_name}_backup;

            -- Step 6: Rename rebuild to original name
            RAISE NOTICE 'Activating rebuild table...';
            ALTER TABLE {schema}.{table_name}_rebuild
            RENAME TO {table_name};

            -- Step 7: Drop backup (only after successful rename)
            RAISE NOTICE 'Dropping backup table...';
            DROP TABLE {schema}.{table_name}_backup;

            RAISE NOTICE '✓ Successfully rebuilt table {schema}.{table_name}';

        EXCEPTION
            WHEN OTHERS THEN
                -- Comprehensive rollback
                RAISE WARNING 'Error during rebuild: %', SQLERRM;

                -- Try to restore from backup if it exists
                IF EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = '{schema}'
                    AND table_name = '{table_name}_backup'
                ) THEN
                    -- If new table exists, drop it
                    DROP TABLE IF EXISTS {schema}.{table_name};

                    -- Restore from backup
                    ALTER TABLE {schema}.{table_name}_backup
                    RENAME TO {table_name};

                    RAISE NOTICE 'Restored original table from backup';
                END IF;

                -- Clean up rebuild table if it exists
                DROP TABLE IF EXISTS {schema}.{table_name}_rebuild;

                -- Re-raise the exception
                RAISE EXCEPTION 'Failed to rebuild table: %', SQLERRM;
        END $$;
    """

    try:
        logging.info("Executing rebuild with transaction safety...")
        inject_sql_procedure(rebuild_query, f"REBUILD TABLE {schema}.{table_name}")

        # Verify the rebuild
        logging.info("Verifying rebuild...")

        # Check row count AFTER rebuild
        row_count_after_result = inject_sql_with_return(row_count_query)
        new_row_count = row_count_after_result[0][0] if row_count_after_result else 0

        if new_row_count != original_row_count:
            logging.error(f"❌ Row count mismatch! Before: {original_row_count}, After: {new_row_count}")
            return False

        # Check column counts AFTER rebuild
        col_info_after = count_table_columns(table_name, schema)

        logging.info(f"✓ Rebuild complete and verified")
        logging.info(f"  Rows: {new_row_count} (unchanged)")
        logging.info(f"  Active columns: {col_info_after['active']}")
        logging.info(f"  Dropped columns: {col_info_after['dropped']}")
        logging.info(f"  Reclaimed {col_info_before['dropped']} column slots")

        if col_info_after['dropped'] > 0:
            logging.warning(f"⚠ Still have {col_info_after['dropped']} dropped columns after rebuild")

        return True

    except Exception as e:
        logging.error(f"❌ Error rebuilding table {schema}.{table_name}: {e}")
        logging.error(f"   Original table should be intact")
        return False


def fix_column_limit_error(table_name: str, schema: str = 'derived', auto_rebuild: bool = False):
    """
    Diagnose and optionally fix PostgreSQL's 1600 column limit error.

    When you see: "tables can have at most 1600 columns"
    This function:
    1. Checks current column usage
    2. Identifies dropped columns taking up space
    3. Optionally rebuilds the table to reclaim space

    Args:
        table_name: Name of the table
        schema: Schema name (default: 'derived')
        auto_rebuild: If True, automatically rebuild if dropped columns exist

    Returns:
        True if issue resolved, False otherwise
    """
    logging.info(f"Diagnosing column limit issue for {schema}.{table_name}")

    col_info = count_table_columns(table_name, schema)

    if col_info['total'] == 0:
        logging.error(f"Table {schema}.{table_name} not found or has no columns")
        return False

    # Report status
    percent_used = (col_info['total'] / 1600) * 100
    logging.info(f"Column usage: {col_info['total']}/1600 ({percent_used:.1f}%)")
    logging.info(f"  - Active columns: {col_info['active']}")
    logging.info(f"  - Dropped columns (ghost): {col_info['dropped']}")

    if col_info['total'] < 1600:
        logging.info("✓ Table is below the 1600 column limit")
        if col_info['dropped'] > 0:
            logging.info(f"  However, {col_info['dropped']} dropped columns can be reclaimed")

    # Provide recommendations
    if col_info['dropped'] > 0:
        logging.info("\n" + "="*60)
        logging.info("RECOMMENDATIONS:")
        logging.info("="*60)
        logging.info(f"1. You have {col_info['dropped']} dropped columns counting toward the limit")
        logging.info(f"2. Rebuilding the table will reclaim {col_info['dropped']} column slots")
        logging.info(f"3. After rebuild, you'll have {1600 - col_info['active']} slots available")

        if auto_rebuild:
            logging.info("\nAuto-rebuild enabled - proceeding with table rebuild...")
            return rebuild_table_to_remove_dropped_columns(table_name, schema)
        else:
            logging.info(f"\nTo see detailed analysis WITHOUT rebuilding:")
            logging.info(f"  rebuild_table_dry_run('{table_name}', '{schema}')")
            logging.info(f"\nTo fix manually:")
            logging.info(f"  rebuild_table_to_remove_dropped_columns('{table_name}', '{schema}')")
            logging.info("\nOr enable auto-rebuild:")
            logging.info(f"  fix_column_limit_error('{table_name}', '{schema}', auto_rebuild=True)")
    else:
        logging.warning("No dropped columns found - the table genuinely has too many columns")
        logging.warning("Consider:")
        logging.warning("  1. Normalizing the schema (split into multiple related tables)")
        logging.warning("  2. Moving rarely-used columns to a separate table")
        logging.warning("  3. Using JSONB for semi-structured data")

    return col_info['dropped'] > 0

