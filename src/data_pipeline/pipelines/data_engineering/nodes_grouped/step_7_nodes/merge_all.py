import pandas as pd
import logging
import logging
import sys
import os
from conf.base.catalog import params
from data_pipeline.pipelines.data_engineering.queries.data_fix import (count_table_columns, fix_column_limit_error)
from conf.common.sql_functions import inject_sql,run_query_and_return_df, generateAndRunUpdateQuery, generate_create_insert_sql
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from conf.common.sql_functions import (create_new_columns, get_table_column_names)
from conf.common.scripts import process_dataframe_with_types_raw_data
from conf.common.hospital_config import hospital_conf
from conf.common.scripts import get_script, merge_script_data
from conf.base.catalog import cron_log_file, cron_time, env
from conf.common.format_error import formatError
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_raw_data_not_joined_in_all_table
sys.path.append(os.getcwd())

KEY = ['uid', 'facility']
cron_log = open(cron_log_file, "a+")


def get_metadata_for_script(script_type: str):
    """
    Retrieve and merge metadata for a given script type from all hospitals.

    Args:
        script_type: The script type (e.g., 'admissions', 'discharges')

    Returns:
        Dictionary of merged metadata or empty dict if unavailable
    """
    try:
        hospital_scripts = hospital_conf()
        if not hospital_scripts:
            logging.warning(f"No hospital configuration found for {script_type}")
            return {}

        merged_script_data = None
        for hospital in hospital_scripts:
            ids = hospital_scripts[hospital]
            script_id_entry = ids.get(script_type, '')

            if not script_id_entry:
                continue

            script_ids = str(script_id_entry).split(',')
            for script_id in script_ids:
                script_id = script_id.strip()
                if not script_id:
                    continue

                script_json = get_script(script_id)
                if script_json is not None:
                    merged_script_data = merge_script_data(merged_script_data, script_json)

        if merged_script_data is None:
            logging.warning(f"No metadata found for script type: {script_type}")
            return {}

        return merged_script_data
    except Exception as e:
        logging.error(f"Error retrieving metadata for {script_type}: {str(e)}")
        return {}


def validate_and_process_admissions(adm_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and process admissions dataframe against admissions metadata.
    Applies type coercion and maintains case sensitivity of keys.

    Args:
        adm_df: Raw admissions DataFrame

    Returns:
        Processed admissions DataFrame with type coercion applied
    """
    if adm_df.empty:
        return adm_df

    try:
        # Get metadata for admissions script
        adm_metadata = get_metadata_for_script('admissions')

        if not adm_metadata:
            logging.warning("No metadata found for admissions - processing without type coercion")
            return adm_df

        # Process using metadata while preserving case sensitivity
        processed_adm = process_dataframe_with_types_raw_data(adm_df, adm_metadata)
        logging.info(f"Processed admissions dataframe: {len(adm_df.columns)} → {len(processed_adm.columns)} columns")

        return processed_adm
    except Exception as e:
        logging.error(f"Error processing admissions dataframe: {str(e)}")
        logging.info("Returning unprocessed admissions dataframe")
        return adm_df


def validate_and_process_discharges(dis_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and process discharges dataframe against discharges metadata.
    Applies type coercion and maintains case sensitivity of keys.

    Args:
        dis_df: Raw discharges DataFrame

    Returns:
        Processed discharges DataFrame with type coercion applied
    """
    if dis_df.empty:
        return dis_df

    try:
        # Get metadata for discharges script
        dis_metadata = get_metadata_for_script('discharges')

        if not dis_metadata:
            logging.warning("No metadata found for discharges - processing without type coercion")
            return dis_df

        # Process using metadata while preserving case sensitivity
        processed_dis = process_dataframe_with_types_raw_data(dis_df, dis_metadata)
        logging.info(f"Processed discharges dataframe: {len(dis_df.columns)} → {len(processed_dis.columns)} columns")

        return processed_dis
    except Exception as e:
        logging.error(f"Error processing discharges dataframe: {str(e)}")
        logging.info("Returning unprocessed discharges dataframe")
        return dis_df


def create_all_merged_admissions_discharges(
    new_adm: pd.DataFrame,
    new_dis: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge admissions and discharges using database-level operations.

    IMPORTANT: new_adm and new_dis only contain records NOT already in the database table

    PHASE 1 - PROCESS ADMISSIONS (Database-level):
    1. Validate admissions against metadata and ensure table columns exist
    2. For each admission:
       a. Query database for matching discharge (uid, facility, is_closed=False, has_admission=False)
       b. If found (single or with OFC disambiguation), UPDATE the record with admission fields and set has_admission=True, is_closed=True
       c. If not found, INSERT new admission record with has_admission=True, has_discharge=False, is_closed=False

    PHASE 2 - PROCESS DISCHARGES (Database-level):
    3. Validate discharges against metadata and ensure table columns exist
    4. For each discharge:
       a. Query database for matching admission (uid, facility, is_closed=False, has_discharge=False, has_admission=True)
       b. If found (single or with OFCDis disambiguation), UPDATE the record with discharge fields and set has_discharge=True, is_closed=True
       c. If not found, INSERT new discharge record with has_discharge=True, has_admission=False, is_closed=False

    Args:
        merged_df: Existing merged records DataFrame (not used - kept for backwards compatibility)
        new_adm: New admissions DataFrame to process (only records NOT in database)
        new_dis: New discharges DataFrame to process (only records NOT in database)

    Returns:
        Updated merged DataFrame read from database after all operations
    """
  

    country = params['country']
    country_abrev = 'ZIM' if country.lower() == 'zimbabwe' else ('MWI' if country.lower() == 'malawi' else None)
    table_name = f'ALL_{country_abrev}'
    schema = 'derived'

    # =====================
    # VALIDATE & PROCESS INCOMING DATA WITH METADATA
    # =====================
    new_adm = validate_and_process_admissions(new_adm)
    new_dis = validate_and_process_discharges(new_dis)

    # Cast OFC and OFCDis to numeric (floating point) for consistent matching
    if 'OFC' in new_adm.columns:
        new_adm['OFC'] = pd.to_numeric(new_adm['OFC'], errors='coerce')
    if 'OFCDis' in new_dis.columns:
        new_dis['OFCDis'] = pd.to_numeric(new_dis['OFCDis'], errors='coerce')

    # =====================
    # PHASE 1: PROCESS ADMISSIONS
    # =====================
    logging.info("PHASE 1: Processing admissions...")

    # Ensure all columns from admissions exist in the database table
    if not new_adm.empty:
        add_new_columns_if_needed(new_adm, table_name, schema)

    admissions_inserted = 0
    admissions_updated = 0

    # BATCH QUERY: Get all admissions with their potential matches from database
    if not new_adm.empty:
        uids = tuple(set(new_adm['uid'].unique()))
        facilities = tuple(set(new_adm['facility'].unique()))

        # Query all existing discharges without admission for these uid/facility combos
        batch_query = f'''
            SELECT uid, facility, unique_key_dis, "OFCDis"
            FROM {schema}."{table_name}"
            WHERE uid = ANY(ARRAY{list(uids)})
            AND facility = ANY(ARRAY{list(facilities)})
            AND is_closed = FALSE
            AND has_admission = FALSE
        ;;'''

        existing_discharges = run_query_and_return_df(batch_query)

        # Separate admissions into: matched (to update), unmatched (to insert)
        updates_list = []
        inserts_list = []

        for _, adm in new_adm.iterrows():
            uid_val = adm['uid']
            facility_val = adm['facility']

            # Find matches in existing discharges
            candidates = existing_discharges[
                (existing_discharges['uid'] == uid_val) &
                (existing_discharges['facility'] == facility_val)
            ]

            if not candidates.empty:
                # Found matching discharge(s)
                if len(candidates) == 1:
                    # Single match - prepare for update
                    update_record = adm.to_dict()
                    update_record['has_admission'] = True
                    update_record['is_closed'] = True
                    updates_list.append(update_record)
                    admissions_updated += 1
                else:
                    # Multiple matches - use OFC to disambiguate
                    if 'OFC' in adm.index:
                        ofc_value = adm['OFC']
                        ofc_match = candidates[candidates['OFCDis'] == ofc_value]
                        if not ofc_match.empty:
                            # Found matching OFC - prepare for update
                            update_record = adm.to_dict()
                            update_record['has_admission'] = True
                            update_record['is_closed'] = True
                            updates_list.append(update_record)
                            admissions_updated += 1
                        else:
                            # No OFC match - prepare for insert
                            adm_record = adm.to_dict()
                            adm_record['has_admission'] = True
                            adm_record['has_discharge'] = False
                            adm_record['is_closed'] = False
                            inserts_list.append(adm_record)
                            admissions_inserted += 1
                    else:
                        # No OFC available - prepare for insert
                        adm_record = adm.to_dict()
                        adm_record['has_admission'] = True
                        adm_record['has_discharge'] = False
                        adm_record['is_closed'] = False
                        inserts_list.append(adm_record)
                        admissions_inserted += 1
            else:
                # No matching discharge - prepare for insert
                adm_record = adm.to_dict()
                adm_record['has_admission'] = True
                adm_record['has_discharge'] = False
                adm_record['is_closed'] = False
                inserts_list.append(adm_record)
                admissions_inserted += 1

        # Execute batch update if any
        if updates_list:
            generateAndRunUpdateQuery(f'{schema}."{table_name}"', pd.DataFrame(updates_list))
            logging.debug(f"Batch updated {len(updates_list)} discharge records with admission data")

        # Execute batch insert if any
        if inserts_list:
            generate_create_insert_sql(pd.DataFrame(inserts_list), schema, table_name)
            logging.debug(f"Batch inserted {len(inserts_list)} new admission records")

    logging.info(f"Admissions phase complete: {admissions_updated} updated discharges, {admissions_inserted} new admissions inserted")

    # =====================
    # PHASE 2: PROCESS DISCHARGES
    # =====================
    logging.info("PHASE 2: Processing discharges...")

    # Ensure all columns from discharges exist in the database table
    if not new_dis.empty:
        add_new_columns_if_needed(new_dis, table_name, schema)

    discharges_inserted = 0
    discharges_updated = 0

    # BATCH QUERY: Get all discharges with their potential matches from database
    if not new_dis.empty:
        uids = tuple(set(new_dis['uid'].unique()))
        facilities = tuple(set(new_dis['facility'].unique()))

        # Query all existing admissions without discharge for these uid/facility combos
        batch_query = f'''
            SELECT uid, facility, unique_key, "OFC"
            FROM {schema}."{table_name}"
            WHERE uid = ANY(ARRAY{list(uids)})
            AND facility = ANY(ARRAY{list(facilities)})
            AND is_closed = FALSE
            AND has_discharge = FALSE
            AND has_admission = TRUE
        ;;'''

        existing_admissions = run_query_and_return_df(batch_query)

        # Separate discharges into: matched (to update), unmatched (to insert)
        updates_list = []
        inserts_list = []

        for _, dis in new_dis.iterrows():
            uid_val = dis['uid']
            facility_val = dis['facility']

            # Find matches in existing admissions
            candidates = existing_admissions[
                (existing_admissions['uid'] == uid_val) &
                (existing_admissions['facility'] == facility_val)
            ]

            if not candidates.empty:
                # Found matching admission(s)
                if len(candidates) == 1:
                    # Single match - prepare for update
                    update_record = dis.to_dict()
                    update_record['has_discharge'] = True
                    update_record['is_closed'] = True
                    updates_list.append(update_record)
                    discharges_updated += 1
                else:
                    # Multiple matches - use OFCDis to disambiguate
                    if 'OFCDis' in dis.index:
                        ofcdis_value = dis['OFCDis']
                        ofcdis_match = candidates[candidates['OFC'] == ofcdis_value]
                        if not ofcdis_match.empty:
                            # Found matching OFCDis - prepare for update
                            update_record = dis.to_dict()
                            update_record['has_discharge'] = True
                            update_record['is_closed'] = True
                            updates_list.append(update_record)
                            discharges_updated += 1
                        else:
                            # No OFCDis match - prepare for insert
                            dis_record = dis.to_dict()
                            dis_record['has_admission'] = False
                            dis_record['has_discharge'] = True
                            dis_record['is_closed'] = False
                            inserts_list.append(dis_record)
                            discharges_inserted += 1
                    else:
                        # No OFCDis available - prepare for insert
                        dis_record = dis.to_dict()
                        dis_record['has_admission'] = False
                        dis_record['has_discharge'] = True
                        dis_record['is_closed'] = False
                        inserts_list.append(dis_record)
                        discharges_inserted += 1
            else:
                # No matching admission - prepare for insert
                dis_record = dis.to_dict()
                dis_record['has_admission'] = False
                dis_record['has_discharge'] = True
                dis_record['is_closed'] = False
                inserts_list.append(dis_record)
                discharges_inserted += 1

        # Execute batch update if any
        if updates_list:
            generateAndRunUpdateQuery(f'{schema}."{table_name}"', pd.DataFrame(updates_list))
            logging.debug(f"Batch updated {len(updates_list)} admission records with discharge data")

        # Execute batch insert if any
        if inserts_list:
            generate_create_insert_sql(pd.DataFrame(inserts_list), schema, table_name)
            logging.debug(f"Batch inserted {len(inserts_list)} new discharge records")

    logging.info(f"Discharges phase complete: {discharges_updated} updated admissions, {discharges_inserted} new discharges inserted")

    # Return final merged dataframe from database
    final_query = f'SELECT * FROM {schema}."{table_name}";;'
    result_df = run_query_and_return_df(final_query)
    logging.info(f"Merge complete: Final record count = {len(result_df)}")

    return result_df.reset_index(drop=True) if not result_df.empty else pd.DataFrame()


def add_new_columns_if_needed(df: pd.DataFrame, table_name: str, schema: str = 'derived') -> None:
    """
    Add new columns to database table if they don't exist.

    Proactively checks column limit and rebuilds table if approaching PostgreSQL's 1600 limit.
    This prevents column limit errors by reclaiming dropped columns before adding new ones.
    """
    if table_exists(schema, table_name):
        # PROACTIVE COLUMN LIMIT CHECK
        # Check current column usage and rebuild if > 1200 to prevent hitting the 1600 limit
        col_info = count_table_columns(table_name, schema)

        if col_info['total'] > 1200:
            logging.warning(f"Table {schema}.{table_name} has {col_info['total']} columns (> 1200 threshold)")
            logging.warning(f"  Active: {col_info['active']}, Dropped: {col_info['dropped']}")

            if col_info['dropped'] > 0:
                logging.info(f"Proactively rebuilding {schema}.{table_name} to reclaim {col_info['dropped']} dropped columns")
                rebuild_success = fix_column_limit_error(table_name, schema, auto_rebuild=True)

                if rebuild_success:
                    logging.info(f"Successfully reclaimed {col_info['dropped']} column slots in {schema}.{table_name}")
                else:
                    logging.warning(f"Rebuild of {schema}.{table_name} did not complete successfully")
            else:
                logging.warning(f"No dropped columns to reclaim. Table genuinely has {col_info['active']} active columns")

        # Now proceed with adding new columns
        existing_cols = pd.DataFrame(get_table_column_names(table_name, schema))
        new_columns = set(df.columns) - set(existing_cols.columns)

        if new_columns:
            logging.info(f"Adding {len(new_columns)} new column(s) to {schema}.{table_name}")
            column_pairs = [(col, str(df[col].dtype)) for col in new_columns]
            if column_pairs:
                create_new_columns(table_name, schema, column_pairs)

def seed_all_table (table_name,schema):
    
    # CREATE TABLE IF NOT EXIST WITH MINIMAL FIELDS
    create_table_query = f'''CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" (
    "uid" TEXT,
    "facility" TEXT,
    "unique_key" TEXT,
    "unique_key_dis" TEXT,
    "OFC" NUMERIC(10,2),
    "OFCDis" NUMERIC(10,2),
    "has_admission" BOOLEAN DEFAULT FALSE,
    "has_discharge" BOOLEAN DEFAULT FALSE,
    "is_closed" BOOLEAN DEFAULT FALSE
);;'''
    inject_sql(create_table_query, "CREATE ALL TABLE")
    


def merge_raw_admissions_and_discharges(clean_derived_data_output):
    """Main function to clean and join research data."""
    try:
        # Test if previous node has completed successfully
        if not clean_derived_data_output:
            logging.error("Creating Clean Tables Did Not Complete Well.")
            return None

        logging.info("...........Creating Raw Joined Admission Discharges.............")
        country = params['country']
        country_abrev = 'ZIM' if country.lower() == 'zimbabwe' else ('MWI' if country.lower() == 'malawi' else None)
        table_name = f'ALL_{country_abrev}'
        schema = 'derived'
        ###SEED TABLES IF NOT EXISTS
        seed_all_table(table_name,schema)
        admission_query = read_raw_data_not_joined_in_all_table('admissions',f' NOT EXISTS (SELECT 1 FROM derived."{table_name}" b where a.uid=b.uid and a.facility=b.facility and a.unique_key=b.unique_key)' )
        adm_df= run_query_and_return_df(admission_query)
        discharges_query = read_raw_data_not_joined_in_all_table('discharges',f' NOT EXISTS (SELECT 1 FROM derived."{table_name}" b where a.uid=b.uid and a.facility=b.facility and a.unique_key=b.unique_key_dis)')
        dis_df = run_query_and_return_df(discharges_query)

        if( not adm_df.empty or not dis_df.empty):
            if (not dis_df.empty and 'unique_key' in dis_df):
                dis_df.rename(columns={'unique_key': 'unique_key_dis'}, inplace=True)
                  
            create_all_merged_admissions_discharges(adm_df,dis_df)     

        return dict(status='Success', message="Raw Data Merging Complete")
    except Exception as e:
        logging.error("!!! An error occurred Cleaning Derived Data: ")
        cron_log.write(
            f"StartTime: {cron_time}   Instance: {env}   Status: Failed Stage: Data Cleaning for Research"
        )
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)
