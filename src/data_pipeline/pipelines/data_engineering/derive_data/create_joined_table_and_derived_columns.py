import logging
from datetime import datetime as dt
from typing import Optional, List, Tuple

import pandas as pd  # type: ignore

from conf.base.catalog import catalog, params
from data_pipeline.pipelines.data_engineering.utils.date_validator import is_date, is_date_formatable
from data_pipeline.pipelines.data_engineering.utils.custom_date_formatter import format_date_without_timezone
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import (
    read_dicharges_not_joined,
    read_admissions_not_joined,
    admissions_without_discharges,
    discharges_not_matched,
    read_all_from_derived_table
)
from conf.common.sql_functions import (
    create_new_columns,
    get_table_column_names,
    generateAndRunUpdateQuery,
    generate_create_insert_sql,
    get_date_column_names,
    run_query_and_return_df
)
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.data_validation.validate import reset_log
from data_pipeline.pipelines.data_engineering.queries.data_fix import deduplicate_table, count_table_columns, fix_column_limit_error


def add_missing_columns(df: pd.DataFrame, table_name: str, schema: str = 'derived') -> None:
    """
    Add any new columns from dataframe to existing table.

    Proactively checks column limit and rebuilds table if approaching PostgreSQL's 1600 limit.
    This prevents column limit errors by reclaiming dropped columns before adding new ones.
    """
    if not table_exists(schema, table_name):
        return

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
                logging.info(f"✓ Successfully reclaimed {col_info['dropped']} column slots in {schema}.{table_name}")
            else:
                logging.warning(f"⚠ Rebuild of {schema}.{table_name} did not complete successfully")
        else:
            logging.warning(f"⚠ No dropped columns to reclaim. Table genuinely has {col_info['active']} active columns")

    # Now proceed with adding new columns
    adm_cols = pd.DataFrame(get_table_column_names(table_name, schema))
    new_columns = set(df.columns) - set(adm_cols.columns)

    if new_columns:
        logging.info(f"Adding {len(new_columns)} new column(s) to {schema}.{table_name}")
        column_pairs = [(col, str(df[col].dtype)) for col in new_columns]
        if column_pairs:
            create_new_columns(table_name, schema, column_pairs)


def get_query_for_table(table_name: str, joined_table_name: str, not_joined_query_fn, all_query_fn) -> str:
    """Determine which query to use based on whether joined table exists."""
    if table_exists('derived', joined_table_name):
        return not_joined_query_fn()
    else:
        return all_query_fn(table_name)


def calculate_date_differences_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Length of Stay and Length of Life using vectorized operations.

    OPTIMIZATION: Replaces iterrows() loop with pandas vectorized operations.
    Performance: ~100-1000x faster for large datasets.
    """
    # Initialize columns
    df['LengthOfStay.label'] = 'Length of Stay'
    df['LengthOfLife.label'] = 'Length of Life'
    df['LengthOfStay.value'] = None
    df['LengthOfLife.value'] = None

    # Ensure date columns are datetime
    date_cols = ['DateTimeAdmission.value', 'DateTimeDischarge.value']
    if 'DateTimeDeath.value' in df.columns:
        date_cols.append('DateTimeDeath.value')

    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Calculate Length of Stay (vectorized)
    if 'DateTimeDischarge.value' in df.columns and 'DateTimeAdmission.value' in df.columns:
        # Create mask for valid dates
        valid_dates = (
            df['DateTimeDischarge.value'].notna() &
            df['DateTimeAdmission.value'].notna()
        )
        # Calculate days difference
        df.loc[valid_dates, 'LengthOfStay.value'] = (
            df.loc[valid_dates, 'DateTimeDischarge.value'] -
            df.loc[valid_dates, 'DateTimeAdmission.value']
        ).dt.days

    # Calculate Length of Life (vectorized)
    if 'DateTimeDeath.value' in df.columns and 'DateTimeAdmission.value' in df.columns:
        valid_death_dates = (
            df['DateTimeDeath.value'].notna() &
            df['DateTimeAdmission.value'].notna()
        )
        df.loc[valid_death_dates, 'LengthOfLife.value'] = (
            df.loc[valid_death_dates, 'DateTimeDeath.value'] -
            df.loc[valid_death_dates, 'DateTimeAdmission.value']
        ).dt.days

    return df


def join_table():
    logging.info("... Starting script to create joined table")

    # Read the raw admissions and discharge data into dataframes
    logging.info("... Fetching admissions and discharges data")
    reset_log('logs/queries.log')

    try:
        # Load Derived Admissions and Discharges
        read_admissions_query = get_query_for_table(
            'admissions',
            'joined_admissions_discharges',
            read_admissions_not_joined,
            read_all_from_derived_table
        )
        read_discharges_query = get_query_for_table(
            'discharges',
            'joined_admissions_discharges',
            read_dicharges_not_joined,
            read_all_from_derived_table
        )

        adm_df = run_query_and_return_df(read_admissions_query)
        logging.info(f"Admissions loaded: {len(adm_df)} rows")

        dis_df = run_query_and_return_df(read_discharges_query)
        logging.info(f"Discharges loaded: {len(dis_df)} rows")

        jn_adm_dis = createJoinedDataSet(adm_df, dis_df)
        logging.info(f"Joined dataset created: {len(jn_adm_dis)} rows")

    except Exception as e:
        logging.error("!!! An error occurred creating joined dataframe")
        raise e

    # Now write the table back to the database
    logging.info("... Writing the output back to the database")
    try:
        # Create Table Using Kedro
        if jn_adm_dis is not None and not jn_adm_dis.empty:
            # Add missing columns
            add_missing_columns(jn_adm_dis, 'joined_admissions_discharges')

            # Format date columns
            date_column_types = pd.DataFrame(get_date_column_names('joined_admissions_discharges', 'derived'))
            if not date_column_types.empty:
                jn_adm_dis = format_date_without_timezone(jn_adm_dis, date_column_types)

            # Clean column names and remove invalid columns
            jn_adm_dis.columns = jn_adm_dis.columns.astype(str)
            jn_adm_dis = jn_adm_dis.loc[:, ~jn_adm_dis.columns.str.match(r'^\d+$|^[a-zA-Z]$', na=False)]

            logging.info(f"Saving joined dataset: {len(jn_adm_dis)} rows")
            generate_create_insert_sql(jn_adm_dis, "derived", "joined_admissions_discharges")

        # MERGE DISCHARGES CURRENTLY ADDED TO THE NEW DATA SET
        discharge_exists = table_exists('derived', 'discharges')
        joined_exists = table_exists('derived', 'joined_admissions_discharges')

        if discharge_exists and joined_exists:
            read_admissions_query_2 = admissions_without_discharges()
            adm_df_2 = run_query_and_return_df(read_admissions_query_2)

            read_discharges_query_2 = discharges_not_matched()
            dis_df_2 = run_query_and_return_df(read_discharges_query_2)

            if adm_df_2 is not None and dis_df_2 is not None and not adm_df_2.empty:
                jn_adm_dis_2 = createJoinedDataSet(adm_df_2, dis_df_2)

                # Clean column names
                jn_adm_dis_2.columns = jn_adm_dis_2.columns.astype(str)
                jn_adm_dis_2 = jn_adm_dis_2.loc[:, ~jn_adm_dis_2.columns.str.match(r'^\d+$|^[a-zA-Z]$', na=False)]

                if not jn_adm_dis_2.empty:
                    # Filter for rows with NeoTreeOutcome
                    filtered_df = jn_adm_dis_2[
                        jn_adm_dis_2['NeoTreeOutcome.value'].notna() &
                        (jn_adm_dis_2['NeoTreeOutcome.value'] != '')
                    ]
                    if isinstance(filtered_df, pd.Series):
                        filtered_df = filtered_df.to_frame().T
                    generateAndRunUpdateQuery('derived.joined_admissions_discharges', filtered_df)
                    deduplicate_table('joined_admissions_discharges')

    except Exception as e:
        logging.error("!!! An error occurred writing join output back to the database")
        raise e

    logging.info("... Join script completed!")

def calculate_match_score(row: pd.Series) -> float:
    """
    Calculate similarity score between admission and discharge records.

    Compares clinical measurements to determine how well a discharge matches an admission:
    - OFC (Occipital-Frontal Circumference)
    - Gestation (weeks)
    - BirthWeight (grams)

    Returns a score where higher values indicate better matches.
    """
    score = 0.0
    comparisons_made = 0

    # Compare OFC: admission OFC.value vs discharge OFCDis.value
    if pd.notna(row.get('OFC.value')) and pd.notna(row.get('OFCDis.value')):
        try:
            ofc_adm = float(row['OFC.value'])
            ofc_dis = float(row['OFCDis.value'])
            # Score based on how close they are (max 10 points if identical)
            # Penalize 1 point per cm difference
            diff = abs(ofc_adm - ofc_dis)
            score += max(0, 10 - diff)
            comparisons_made += 1
        except (ValueError, TypeError):
            pass

    # Compare Gestation: admission Gestation.value vs discharge Gestation.value_discharge
    if pd.notna(row.get('Gestation.value')) and pd.notna(row.get('Gestation.value_discharge')):
        try:
            gest_adm = float(row['Gestation.value'])
            gest_dis = float(row['Gestation.value_discharge'])
            # Score based on how close they are (max 10 points if identical)
            # Penalize 1 point per week difference
            diff = abs(gest_adm - gest_dis)
            score += max(0, 10 - diff)
            comparisons_made += 1
        except (ValueError, TypeError):
            pass

    # Compare BirthWeight: admission BirthWeight.value vs discharge BirthWeight.value_discharge
    # Note: BirthWeight On Discharge is "not to be trusted", so weight it less
    if pd.notna(row.get('BirthWeight.value')) and pd.notna(row.get('BirthWeight.value_discharge')):
        try:
            bw_adm = float(row['BirthWeight.value'])
            bw_dis = float(row['BirthWeight.value_discharge'])
            # Score based on how close they are (max 5 points if identical - weighted less)
            # Penalize 1 point per 500g difference
            diff = abs(bw_adm - bw_dis) / 500
            score += max(0, 5 - diff)
            comparisons_made += 1
        except (ValueError, TypeError):
            pass

    # Normalize score if we made comparisons
    # If no comparisons possible, return -1 to indicate no data
    if comparisons_made == 0:
        return -1.0

    return score


def resolve_duplicate_matches(merged_df: pd.DataFrame, adm_unique_col: str = '_adm_idx') -> pd.DataFrame:
    """
    Resolve duplicate matches by selecting best discharge match for EACH individual admission.

    For each admission with multiple discharge matches, calculates similarity scores
    based on clinical measurements (OFC, Gestation, BirthWeight) and keeps only
    the discharge with the highest score for that specific admission.

    Args:
        merged_df: DataFrame after merge, potentially with duplicate matches per admission
        adm_unique_col: Column name that uniquely identifies each admission row

    Returns:
        DataFrame with duplicates resolved by keeping best discharge match per admission
    """
    logging.info("Resolving duplicate admission-discharge matches using clinical measurement comparison")

    # Count how many discharge matches each admission has
    merged_df['_match_count'] = merged_df.groupby(adm_unique_col)[adm_unique_col].transform('count')

    # Separate admissions with multiple matches from those with single matches
    duplicates = merged_df[merged_df['_match_count'] > 1].copy()
    non_duplicates = merged_df[merged_df['_match_count'] == 1].copy()

    logging.info(f"Found {len(non_duplicates)} admissions with single discharge match")
    logging.info(f"Found {duplicates[adm_unique_col].nunique()} admissions with multiple discharge matches ({len(duplicates)} total rows)")

    if duplicates.empty:
        # No duplicates to resolve
        result = non_duplicates.drop(columns=['_match_count'])
        return result

    # Calculate match scores for all duplicate matches
    duplicates['_match_score'] = duplicates.apply(calculate_match_score, axis=1)

    # For each individual admission, keep the discharge with the highest match score
    def select_best_discharge_for_admission(group):
        """Select the best matching discharge for this specific admission."""
        # If all scores are -1 (no data to compare), keep the first discharge
        if (group['_match_score'] == -1).all():
            logging.debug(f"No clinical data to compare for admission {group.iloc[0][adm_unique_col]} - keeping first discharge")
            return group.iloc[0:1]

        # Otherwise, take the discharge with highest score
        valid_scores = group[group['_match_score'] >= 0]
        if not valid_scores.empty:
            best_idx = valid_scores['_match_score'].idxmax()
            best_match = valid_scores.loc[best_idx]
            logging.debug(f"Best match score {best_match['_match_score']:.2f} for admission {best_match[adm_unique_col]}")
            return valid_scores.loc[[best_idx]]
        else:
            return group.iloc[0:1]

    # Group by each individual admission and select best discharge
    resolved_duplicates = duplicates.groupby(adm_unique_col, group_keys=False).apply(select_best_discharge_for_admission)

    logging.info(f"Resolved {len(duplicates)} duplicate matches down to {len(resolved_duplicates)} best matches")

    # Combine resolved duplicates with non-duplicates
    result = pd.concat([non_duplicates, resolved_duplicates], ignore_index=True)

    # Clean up temporary columns
    result = result.drop(columns=['_match_count', '_match_score'], errors='ignore')

    return result


def createJoinedDataSet(adm_df: pd.DataFrame, dis_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create joined admissions-discharges dataset with intelligent duplicate resolution.

    Uses clinical measurements (OFC, Gestation, BirthWeight) to match each admission
    with its most appropriate discharge record when duplicates exist.
    """
    logging.info("Creating joined dataset")

    if adm_df.empty or dis_df.empty:
        logging.warning("Empty input dataframes - returning empty result")
        return pd.DataFrame()

    # Add unique identifier to each admission row (will be preserved through merge)
    adm_df = adm_df.copy()
    adm_df['_adm_idx'] = range(len(adm_df))

    # Merge admissions and discharges on uid+facility
    # This creates ALL possible admission-discharge combinations for each uid+facility pair
    jn_adm_dis = adm_df.merge(
        dis_df,
        how='left',
        on=['uid', 'facility'],
        suffixes=('', '_discharge')
    )

    logging.info(f"Initial merge created {len(jn_adm_dis)} rows from {len(adm_df)} admissions")

    # Resolve duplicate matches using clinical measurement comparison
    # For each admission, select the discharge with the best matching clinical measurements
    jn_adm_dis = resolve_duplicate_matches(jn_adm_dis, adm_unique_col='_adm_idx')

    # Clean up the temporary admission index column
    jn_adm_dis = jn_adm_dis.drop(columns=['_adm_idx'], errors='ignore')

    # Final deduplication based on unique_key (safety check)
    if 'unique_key' in jn_adm_dis.columns:
        # OPTIMIZATION: Use vectorized string operations instead of lambda map
        jn_adm_dis['DEDUPLICATER'] = jn_adm_dis['unique_key'].astype(str).str[:10]
        # Replace empty strings or short values with None
        jn_adm_dis.loc[jn_adm_dis['unique_key'].astype(str).str.len() < 10, 'DEDUPLICATER'] = None

        # Final deduplication on unique_key
        jn_adm_dis = jn_adm_dis.drop_duplicates(
            subset=["uid", "facility", "DEDUPLICATER"],
            keep='first'
        )

    logging.info(f"After all deduplication: {len(jn_adm_dis)} rows")

    # Add missing columns to database table
    add_missing_columns(jn_adm_dis, 'joined_admissions_discharges')

    # Convert Gestation to numeric
    if 'Gestation.value' in jn_adm_dis.columns:
        jn_adm_dis['Gestation.value'] = pd.to_numeric(
            jn_adm_dis['Gestation.value'],
            errors='coerce'
        )

    # Format dates
    jn_adm_dis = format_date_without_timezone(
        jn_adm_dis,
        ['DateTimeAdmission.value', 'DateTimeDischarge.value']
    )

    # OPTIMIZATION: Use vectorized date calculations instead of iterrows()
    jn_adm_dis = calculate_date_differences_vectorized(jn_adm_dis)

    logging.info(f"Finished creating joined dataset: {len(jn_adm_dis)} rows")
    return jn_adm_dis



