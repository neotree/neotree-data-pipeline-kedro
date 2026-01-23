# Import created modules
import pandas as pd
import logging

from conf.common.format_error import formatError
from .extract_key_values import get_key_values, format_repeatables_to_rows
from .explode_mcl_columns import explode_column
from conf.base.catalog import catalog, new_scripts
from conf.common.sql_functions import (
    create_new_columns,
    get_table_column_names,
    generate_upsert_queries_and_create_table,
    generate_create_insert_sql,
    generate_timestamp_conversion_query
)
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.utils.custom_date_formatter import format_date_without_timezone
from data_pipeline.pipelines.data_engineering.utils.data_label_fixes import convert_false_numbers_to_text
from data_pipeline.pipelines.data_engineering.data_validation.validate import validate_dataframe_with_ge
from data_pipeline.pipelines.data_engineering.utils.field_info import update_fields_info, transform_matching_labels
from data_pipeline.pipelines.data_engineering.queries.data_fix import deduplicate_table,date_data_type_fix


def safe_load(dataset_name: str) -> pd.DataFrame:
    """Safely load a dataset from catalog, returning empty DataFrame on failure."""
    try:
        return catalog.load(dataset_name)
    except Exception as e:
        logging.warning(f"Failed to load dataset '{dataset_name}': {formatError(e)}")
        return pd.DataFrame()


def fix_facility_phc_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Fix Facility/PHC column naming inconsistencies."""
    # Move Facility values to PHC when PHC is null
    if "Facility.value" in df.columns and "PHC.value" in df.columns:
        mask = (~df["Facility.value"].isna()) & (df["PHC.value"].isna())
        df.loc[mask, "PHC.value"] = df.loc[mask, "Facility.value"]

    if "Facility.label" in df.columns and "PHC.label" in df.columns:
        mask = (~df["Facility.label"].isna()) & (df["PHC.label"].isna())
        df.loc[mask, "PHC.label"] = df.loc[mask, "Facility.label"]

    # Rename Facility columns to PHC if PHC doesn't exist
    elif "Facility.value" in df.columns and "PHC.value" not in df.columns:
        df = df.rename(columns={"Facility.value": "PHC.value"})

    if "Facility.label" in df.columns and "PHC.label" not in df.columns:
        df = df.rename(columns={"Facility.label": "PHC.label"})

    return df


def fix_feed_assessment_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Fix feed assessment column naming."""
    if "How is the baby being fed?.label" in df.columns:
        df.rename(columns={'FeedAsse.label': 'How is the baby being fed?.label'}, inplace=True)

    if "How is the baby being fed?.value" in df.columns:
        df.rename(columns={'FeedAsse.value': 'How is the baby being fed?.value'}, inplace=True)

    return df


def calculate_time_spent_dynamic(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate time spent for dynamic scripts with multiple date column options."""
    if "started_at" not in df.columns or 'completed_at' not in df.columns:
        df['time_spent'] = None
        return df

    # Try with completed_time first
    if 'completed_time' in df.columns:
        try:
            result = format_date_without_timezone(df, ['started_at', 'completed_time'])
            if result is not None:
                df = result

            df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce').dt.tz_localize(None)
            df['completed_time'] = pd.to_datetime(df['completed_time'], errors='coerce').dt.tz_localize(None)
            df['time_spent'] = (df['completed_time'] - df['started_at']).dt.total_seconds() / 60
            return df
        except Exception as e:
            logging.warning(f"Error calculating time_spent with completed_time: {formatError(e)}")

    # Fallback to completed_at
    try:
        result = format_date_without_timezone(df, ['started_at', 'completed_at'])
        if result is not None:
            df = result

        df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce').dt.tz_localize(None)
        df['completed_at'] = pd.to_datetime(df['completed_at'], errors='coerce').dt.tz_localize(None)
        df['time_spent'] = (df['completed_at'] - df['started_at']).dt.total_seconds() / 60
    except Exception as e:
        logging.warning(f"Error calculating time_spent with completed_at: {formatError(e)}")
        df['time_spent'] = None

    return df


def format_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Format all datetime columns in the dataframe."""
    datetime_types = ["datetime", "datetime64", "datetime64[ns]", "datetimetz"]
    date_columns = df.select_dtypes(include=datetime_types).columns  # type: ignore

    for date_column in date_columns:
        result = format_date_without_timezone(df, [date_column])
        if result is not None:
            df = result

    return df


def add_new_columns_if_needed(df: pd.DataFrame, script_name: str) -> None:
    """
    Add new columns to database table if they don't exist.

    Proactively checks column limit and rebuilds table if approaching PostgreSQL's 1600 limit.
    This prevents column limit errors by reclaiming dropped columns before adding new ones.
    """
    if table_exists('derived', script_name):
        # PROACTIVE COLUMN LIMIT CHECK
        # Import here to avoid circular dependency
        from data_pipeline.pipelines.data_engineering.queries.data_fix import count_table_columns, fix_column_limit_error

        # Check current column usage and rebuild if > 1200 to prevent hitting the 1600 limit
        col_info = count_table_columns(script_name, 'derived')

        if col_info['total'] > 1200:
            logging.warning(f"Table derived.{script_name} has {col_info['total']} columns (> 1200 threshold)")
            logging.warning(f"  Active: {col_info['active']}, Dropped: {col_info['dropped']}")

            if col_info['dropped'] > 0:
                logging.info(f"Proactively rebuilding derived.{script_name} to reclaim {col_info['dropped']} dropped columns")
                rebuild_success = fix_column_limit_error(script_name, 'derived', auto_rebuild=True)

                if rebuild_success:
                    logging.info(f"Successfully reclaimed {col_info['dropped']} column slots in derived.{script_name}")
                else:
                    logging.warning(f"Rebuild of derived.{script_name} did not complete successfully")
            else:
                logging.warning(f"No dropped columns to reclaim. Table genuinely has {col_info['active']} active columns")

        # Now proceed with adding new columns
        cols = pd.DataFrame(get_table_column_names(script_name, 'derived'), columns=["column_name"])
        # Fixed: Compare with actual column names from the table, not the DataFrame column headers
        existing_columns = set(cols["column_name"].values) if not cols.empty else set()
        new_columns = set(df.columns) - existing_columns

        if new_columns:
            logging.info(f"Adding {len(new_columns)} new columns to {script_name}: {new_columns}")
            column_pairs = [(col, str(df[col].dtype)) for col in new_columns]
            if column_pairs:
                create_new_columns(script_name, 'derived', column_pairs)


def finalize_script_dataframe(df: pd.DataFrame, script_name: str) -> pd.DataFrame:
    """Apply final transformations to script dataframe before saving."""
    # Clean column names
    df.columns = df.columns.str.replace(r"[()-]", "_", regex=True)

    # Convert false numbers to text
    df = convert_false_numbers_to_text(df, 'derived', script_name)

    # Remove single character and digit-only columns
    df = df.loc[:, ~df.columns.str.match(r'^\d+$|^[a-zA-Z]$', na=False)].copy()

    # Transform matching labels
    df = transform_matching_labels(df, script=script_name)

    return df


def process_script_repeatables(script_raw: pd.DataFrame, script_name: str) -> None:
    """Process and save repeatable fields for a script."""
    try:
        repeatables = format_repeatables_to_rows(script_raw, script_name)
        for table_name, df in (repeatables or {}).items():
            if not df.empty:
                generate_upsert_queries_and_create_table(table_name, df)
    except Exception as e:
        logging.error(f"!!! An error occurred formatting {script_name} repeatables")
        logging.error(formatError(e))


def process_single_script(script: str) -> None:
    """Process a single dynamic script."""
    catalog_query = f'read_{script}'
    if(script=='combined_maternity_outcomes'):
        logging.info("#############SCRRRRRIIIPT########"+catalog_query)

    # Load raw data
    script_raw = safe_load(catalog_query)
    if script_raw.empty:
        logging.warning(f"No data loaded for script: {script}")
        return

    try:
        # Extract key values
        script_new_entries, script_mcl = get_key_values(script_raw)
    except Exception as e:
        logging.error(f"!!! Error extracting keys for {script}: {formatError(e)}")
        return

    try:
        # Create normalized dataframe
        script_df = pd.json_normalize(script_new_entries)

        if script_df.empty:
            logging.info(f"No entries to process for script: {script}")
            return

        # Apply fixes
        script_df = fix_facility_phc_columns(script_df)
        script_df = fix_feed_assessment_columns(script_df)

        # Update field info
        update_fields_info(script)

        # Set index
        if 'uid' in script_df.columns:
            script_df.set_index(['uid'])

        # Calculate time spent
        script_df = calculate_time_spent_dynamic(script_df)
        script_df['transformed']= False
        # Format datetime columns
        script_df = format_datetime_columns(script_df)

        # Add new columns if needed
        add_new_columns_if_needed(script_df, script)
          # Validate and save
        validate_dataframe_with_ge(script_df, script)
        # Finalize dataframe
        script_df = finalize_script_dataframe(script_df, script)
        generate_create_insert_sql(script_df, 'derived', script)
        deduplicate_table(script)

        # Create MCL tables
        logging.info(f"... Creating MCL count tables for {script}")
        explode_column(script_df, script_mcl, script + '_')
        generate_timestamp_conversion_query(f'derived.{script}', ['completed_at', 'started_at'])

        # Process repeatables
        process_script_repeatables(script_raw, script)

        #Enforce TimeStamp Columns
        date_data_type_fix(script,['completed_at','started_at','DateTimeAdmission.value'
                                   ,'DateTimeDischarge.value','DateTimeDeath.value','DateAdmission.value'])

        logging.info(f"Successfully processed script: {script}")

    except Exception as e:
        logging.error(f"!!! Error processing script {script}: {formatError(e)}")


def tidy_dynamic_tables():
    """Main function to process all dynamic tables."""
    logging.info("... Fetching and processing Dynamic Tables")

    if not new_scripts:
        logging.info("No dynamic scripts to process")
        return

    success_count = 0
    error_count = 0

    for script in new_scripts:
        try:
            process_single_script(script)
            success_count += 1
        except Exception as e:
            error_count += 1
            logging.error(f"!!! Failed to process script {script}: {formatError(e)}")

    logging.info(f"Dynamic tables processing complete: {success_count} successful, {error_count} failed")