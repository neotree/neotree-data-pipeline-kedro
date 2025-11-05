import logging
import sys
import os

sys.path.append(os.getcwd())

from typing import Optional, Dict, Any
import pandas as pd

from conf.common.scripts import get_script, merge_script_data, process_dataframe_with_types
from conf.common.hospital_config import hospital_conf
from conf.common.format_error import formatError
from conf.common.sql_functions import (
    run_query_and_return_df,
    generate_create_insert_sql,
    get_table_column_names,
    create_new_columns,
    table_exists,
    generateAndRunUpdateQuery
)
from conf.base.catalog import cron_log_file, cron_time, env
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import (
    read_derived_data_query,
    read_clean_admissions_not_joined,
    read_clean_dicharges_not_joined,
    read_all_from_derived_table,
    read_clean_admissions_without_discharges,
    clean_discharges_not_matched
)
from data_pipeline.pipelines.data_engineering.queries.data_fix import deduplicate_table,datesfix_batch,drop_single_letter_columns_all_tables

# This file calls the query to grant privileges to users on the generated tables
cron_log = open(cron_log_file, "a+")


def get_all_script_types(hospital_scripts: Dict) -> set:
    """Extract all unique script types from hospital configurations."""
    all_script_types = set()
    for scripts in hospital_scripts.values():
        all_script_types.update(scripts.keys())
    return all_script_types


def collect_script_data(hospital_scripts: Dict, script: str) -> Optional[Any]:
    """Collect and merge script data from all hospitals for a given script type."""
    merged_script_data = None

    for hospital in hospital_scripts:
        ids = hospital_scripts[hospital]
        script_id_entry = ids.get(script, '')

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

    return merged_script_data


def add_columns_if_needed(df: pd.DataFrame, table_name: str, schema: str = 'derived') -> None:
    """Add new columns to table if they don't exist."""
    if not table_exists(schema, table_name):
        return

    cols = pd.DataFrame(get_table_column_names(table_name, schema), columns=["column_name"])
    new_columns = set(df.columns) - set(cols.columns)

    if new_columns:
        column_pairs = [(col, str(df[col].dtype)) for col in new_columns]
        if column_pairs:
            create_new_columns(table_name, schema, column_pairs)


def process_single_script(script: str, hospital_scripts: Dict) -> None:
    """Process a single script type across all hospitals."""
    if script in ['country', 'name']:
        return

    merged_script_data = collect_script_data(hospital_scripts, script)

    if merged_script_data is None or not bool(merged_script_data):
        return

    query = read_derived_data_query(script, f'clean_{script}')
    new_data_df = run_query_and_return_df(query)

    if new_data_df is None or new_data_df.empty:
        return

    cleaned_df = process_dataframe_with_types(new_data_df, merged_script_data)

    if cleaned_df is None or cleaned_df.empty:
        return

    # Clean column names
    new_data_df.columns = new_data_df.columns.str.replace(r"[()-]", "_", regex=True)

    # Add new columns if needed
    add_columns_if_needed(new_data_df, f'clean_{script}')

    # Save and deduplicate
    generate_create_insert_sql(new_data_df, 'derived', f'clean_{script}')
    deduplicate_table(f'clean_{script}')


def get_admission_discharge_queries() -> tuple:
    """Determine which queries to use for reading admissions and discharges."""
    if table_exists('derived', 'clean_joined_adm_discharges'):
        read_admissions_query = read_clean_admissions_not_joined()
        read_discharges_query = read_clean_dicharges_not_joined()
    else:
        read_admissions_query = read_all_from_derived_table('clean_admissions')
        read_discharges_query = read_all_from_derived_table('clean_discharges')

    return read_admissions_query, read_discharges_query


def process_joined_admissions_discharges() -> None:
    """Process and join admissions and discharges data."""
    read_admissions_query, read_discharges_query = get_admission_discharge_queries()

    clean_adm_df = run_query_and_return_df(read_admissions_query)
    clean_dis_df = run_query_and_return_df(read_discharges_query)
    clean_jn_adm_dis = createCleanJoinedDataSet(clean_adm_df, clean_dis_df)

    if clean_jn_adm_dis is None or clean_jn_adm_dis.empty:
        return

    # Remove single character and digit-only columns
    clean_jn_adm_dis = clean_jn_adm_dis.loc[:, ~clean_jn_adm_dis.columns.str.match(r'^\d+$|^[a-zA-Z]$', na=False)]

    # Add new columns if needed
    add_columns_if_needed(clean_jn_adm_dis, 'clean_joined_adm_discharges')

    # Save data
    generate_create_insert_sql(clean_jn_adm_dis, "derived", "clean_joined_adm_discharges")


def process_unmatched_discharges() -> None:
    """Process admissions without discharges and unmatched discharges."""
    discharge_exists = table_exists('derived', 'clean_discharges')
    joined_exists = table_exists('derived', 'clean_joined_adm_discharges')

    if not (discharge_exists and joined_exists):
        return

    read_admissions_query_2 = read_clean_admissions_without_discharges()
    adm_df_2 = run_query_and_return_df(read_admissions_query_2)

    read_discharges_query_2 = clean_discharges_not_matched()
    dis_df_2 = run_query_and_return_df(read_discharges_query_2)

    if adm_df_2 is None or dis_df_2 is None or adm_df_2.empty:
        return

    jn_adm_dis_2 = createCleanJoinedDataSet(adm_df_2, dis_df_2)
    jn_adm_dis_2.columns = jn_adm_dis_2.columns.astype(str)
    jn_adm_dis_2 = jn_adm_dis_2.loc[:, ~jn_adm_dis_2.columns.str.match(r'^\d+$|^[a-zA-Z]$', na=False)]

    if not jn_adm_dis_2.empty:
        filtered_df = jn_adm_dis_2[jn_adm_dis_2['neotreeoutcome'].notna() & (jn_adm_dis_2['neotreeoutcome'] != '')]
        generateAndRunUpdateQuery('derived.clean_joined_adm_discharges', filtered_df)
        deduplicate_table('clean_joined_adm_discharges')


def clean_data_for_research(create_summary_counts_output):
    """Main function to clean and join research data."""
    try:
        # Test if previous node has completed successfully
        if not create_summary_counts_output:
            logging.error("Granting Privileges Complete Did Not Execute To Completion")
            return None

        logging.info("...........Cleaning Joined Admissions Discharges.............")

        hospital_scripts = hospital_conf()
        if not hospital_scripts:
            return dict(status='Success', message="No hospital scripts to process")

        # Process all script types
        all_script_types = get_all_script_types(hospital_scripts)
        for script in all_script_types:
            process_single_script(script, hospital_scripts)

        # Process admissions and discharges joins
        process_joined_admissions_discharges()

        # Process unmatched discharges
        process_unmatched_discharges()

        #Clean Dirty Dates
        clean_all_dates()
        #Remove All Singe Letter Cols
        drop_single_letter_columns_all_tables()

        return dict(status='Success', message="Data Cleanup Complete")

    except Exception as e:
        logging.error("!!! An error occurred Cleaning Derived Data: ")
        cron_log.write(
            f"StartTime: {cron_time}   Instance: {env}   Status: Failed Stage: Data Cleaning for Research"
        )
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)


def createCleanJoinedDataSet(adm_df: pd.DataFrame, dis_df: pd.DataFrame) -> pd.DataFrame:
    """Create and deduplicate joined admissions-discharges dataset."""
    jn_adm_dis = pd.DataFrame()

    if adm_df.empty or dis_df.empty:
        return jn_adm_dis

    # Merge admissions and discharges
    jn_adm_dis = adm_df.merge(
        dis_df,
        how='left',
        on=['uid', 'facility'],
        suffixes=('', '_discharge')
    )

    if 'unique_key' not in jn_adm_dis.columns:
        return jn_adm_dis

    # Create deduplicater field
    jn_adm_dis['deduplicater'] = jn_adm_dis['unique_key'].map(
        lambda x: str(x)[:10] if len(str(x)) >= 10 else None
    )

    # Primary deduplication on uid, facility, deduplicater
    jn_adm_dis = jn_adm_dis.drop_duplicates(
        subset=["uid", "facility", "deduplicater"],
        keep='first'
    )

    # Further deduplication on ofcdis (helps isolate different admissions mapped to same discharge)
    if "ofcdis" in jn_adm_dis.columns:
        jn_adm_dis = jn_adm_dis.drop_duplicates(
            subset=["uid", "facility", "ofcdis"],
            keep='first'
        )

    # Further deduplication on birthweight_discharge
    if "birthweight_discharge" in jn_adm_dis.columns:
        jn_adm_dis = jn_adm_dis.drop_duplicates(
            subset=["uid", "facility", "birthweight_discharge"],
            keep='first'
        )

    # Add non-existing columns to table
    add_columns_if_needed(jn_adm_dis, 'clean_joined_adm_discharges')

    return jn_adm_dis

def clean_all_dates():
    datesfix_batch([
      ('public.clean_sessions', 'admissions'),
      ('public.clean_sessions', 'discharges'),
      ('public.clean_sessions', 'maternal_outcomes'),
      ('public.clean_sessions', 'vitalsigns'),
      ('public.clean_sessions', 'neolab'),
      ('public.clean_sessions', 'baseline'),
      ('public.clean_sessions', 'infections'),
      ('public.clean_sessions', 'daily_review'),
      ('public.clean_sessions', 'twenty_8_day_follow_up'),
      ('public.clean_sessions', 'maternity_completeness'),
      ('public.clean_sessions','dhis2_maternal_outcomes'),
      ('public.clean_sessions','phc_discharges'),
      ('public.clean_sessions','phc_admissions'),
      ('derived.admissions','joined_admissions_discharges'),
      ('discharges','joined_admissions_discharges'),
      ('derived.admissions', 'clean_admissions'),
      ('derived.discharges', 'clean_discharges'),
      ('derived.joined_admissions_discharges','clean_joined_adm_discharges'),
      ('derived.maternal_outcomes', 'clean_maternal_outcomes'),
      ('derived.vitalsigns', 'clean_vitalsigns'),
      ('derived.neolab', 'clean_neolab'),
      ('derived.baseline', 'clean_baseline'),
      ('derived.infections', 'clean_infections'),
      ('derived.daily_review', 'clean_daily_review'),
      ('derived.twenty_8_day_follow_up', 'clean_twenty_8_day_follow_up'),
      ('derived.maternity_completeness', 'clean_maternity_completeness'),
      ('derived.dhis2_maternal_outcomes','clean_dhis2_maternal_outcomes'),
      ('phc_admissions','clean_phc_admissions'),
      ('phc_discharges','clean_phc_discharges')
  ])

   

    