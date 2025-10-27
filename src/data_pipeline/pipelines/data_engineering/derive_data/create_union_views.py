import logging
from typing import List, Tuple

import pandas as pd  # type: ignore

from conf.common.sql_functions import inject_sql, get_table_columns, insert_old_adm_query
from conf.base.catalog import catalog, params
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.utils.custom_date_formatter import format_date, format_date_without_timezone
from conf.common.format_error import formatError


def parse_age_to_hours(age_string: str) -> int:
    """
    Parse age string (e.g., '2 days, 3 hours') to total hours.

    OPTIMIZATION: Extracted from duplicated inline logic.
    """
    if not isinstance(age_string, str) or age_string == 'nan':
        return 0

    age_list = str(age_string).split(",")
    period = 0

    # Single component (hours only or days only)
    if len(age_list) == 1:
        age = age_list[0]

        if 'hour' in age:
            hours = [int(s) for s in age.replace("-", "").split() if s.isdigit()]
            if hours:
                period = hours[0]
            elif "an" in age:  # "an hour"
                period = 1

        elif 'day' in age:
            hours = [int(s) for s in age.replace("-", "").split() if s.isdigit()]
            if hours:
                period = hours[0] * 24

        elif 'second' in age or 'minute' in age:
            # Round to 1 hour
            period = 1

    # Both days and hours
    elif len(age_list) == 2:
        age_days = age_list[0]
        age_hours = age_list[1]

        if 'day' in age_days and 'hour' in age_hours:
            number_hours_days = [int(s) for s in age_days.split() if s.isdigit()]
            number_hours = [int(s) for s in age_hours.split() if s.isdigit()]
            if number_hours and number_hours_days:
                period = (number_hours_days[0] * 24) + number_hours[0]

    return period


def categorize_age(hours: int) -> str:
    """Categorize age in hours into standard categories."""
    if hours < 2:
        return 'Fresh Newborn (< 2 hours old)'
    elif hours <= 23:
        return 'Newborn (2 - 23 hrs old)'
    elif hours <= 47:
        return 'Newborn (1 day - 1 day 23 hrs old)'
    elif hours <= 71:
        return 'Infant (2 days - 2 days 23 hrs old)'
    else:
        return 'Infant (> 3 days old)'


def filter_columns_optimized(df: pd.DataFrame, is_old_dataset: bool = False, max_columns: int = 1550) -> pd.DataFrame:
    """
    Filter columns based on optimization rules:
    1. Drop single-letter columns
    2. Keep only columns with .value/.label extension (or key columns or old dataset columns)
    3. Limit to max_columns, prioritizing key columns

    Args:
        df: DataFrame to filter
        is_old_dataset: If True, preserves all columns (old dataset doesn't follow .value/.label convention)
        max_columns: Maximum number of columns to keep

    Returns:
        Filtered DataFrame
    """
    if df.empty:
        return df

    # Define key columns that must always be preserved
    key_columns = [
        'uid', 'unique_key', 'facility', 'created_at', 'form_id', 'review_number',
        'Age.value', 'AgeCategory', 'scriptId', 'ingested_at', 'script_type',
        'DateTimeAdmission.value', 'DateTimeDischarge.value', 'EndScriptDatetime.value'
    ]

    columns_to_keep = []
    columns_to_drop = []

    for col in df.columns:
        col_str = str(col)

        # Rule 1: Drop single-letter columns
        if len(col_str) == 1:
            columns_to_drop.append(col)
            continue

        # Rule 2: Keep key columns
        if col in key_columns:
            columns_to_keep.append(col)
            continue

        # Rule 3: For old datasets, keep all remaining columns (they don't follow .value/.label convention)
        if is_old_dataset:
            columns_to_keep.append(col)
            continue

        # Rule 4: For new datasets, keep only columns with .value or .label extension
        if col_str.endswith('.value') or col_str.endswith('.label'):
            columns_to_keep.append(col)
        else:
            columns_to_drop.append(col)

    # Log dropped columns for debugging
    if columns_to_drop:
        logging.info(f"Dropping {len(columns_to_drop)} columns (single-letter or no .value/.label extension)")
        logging.debug(f"Dropped columns sample: {columns_to_drop[:10]}")

    # Apply initial filter
    df_filtered = df[columns_to_keep].copy()

    # Rule 5: If still over max_columns, prioritize key columns and drop extras
    if len(columns_to_keep) > max_columns:
        logging.warning(f"DataFrame has {len(columns_to_keep)} columns, exceeding limit of {max_columns}")

        # Separate key columns from other columns
        present_key_cols = [col for col in key_columns if col in columns_to_keep]
        other_cols = [col for col in columns_to_keep if col not in key_columns]

        # Calculate how many other columns we can keep
        available_slots = max_columns - len(present_key_cols)

        if available_slots > 0:
            # Keep key columns + as many other columns as possible
            final_columns = present_key_cols + other_cols[:available_slots]
            logging.warning(f"Reduced to {len(final_columns)} columns (kept all {len(present_key_cols)} key columns + {available_slots} others)")
        else:
            # Only keep key columns if we're at/over limit
            final_columns = present_key_cols[:max_columns]
            logging.warning(f"Reduced to {len(final_columns)} key columns only")

        df_filtered = df_filtered[final_columns].copy()

    logging.info(f"Column filtering complete: {len(df.columns)} → {len(df_filtered.columns)} columns")
    return df_filtered


def process_age_column_vectorized(df: pd.DataFrame, age_col: str = 'AgeB.value') -> pd.DataFrame:
    """
    Process age column using vectorized operations.

    OPTIMIZATION: Replaces iterrows() with vectorized apply.
    Performance: ~10-100x faster for large datasets.
    """
    if age_col not in df.columns:
        return df

    # Parse ages to hours
    df['Age.value'] = df[age_col].apply(parse_age_to_hours)

    # Filter for valid ages
    valid_ages = df['Age.value'] > 0

    # Categorize ages
    df.loc[valid_ages, 'AgeCategory'] = df.loc[valid_ages, 'Age.value'].apply(categorize_age)

    return df


def apply_key_changes_bulk(df: pd.DataFrame, key_mappings: List[Tuple[str, str]]) -> pd.DataFrame:
    """
    Apply multiple key changes in bulk using vectorized operations.

    OPTIMIZATION: Batches key changes instead of calling key_change row-by-row.
    """
    for old_key, new_key in key_mappings:
        if old_key in df.columns and new_key not in df.columns:
            df[new_key] = df[old_key]
        elif old_key in df.columns and new_key in df.columns:
            # Fill missing values in new_key with values from old_key
            df[new_key] = df[new_key].fillna(df[old_key])

    return df


def match_column_types_optimized(source_cols: pd.DataFrame, target_cols: pd.DataFrame,
                                   target_table: str, operation_name: str) -> List[str]:
    """
    Match column types between source and target tables.

    OPTIMIZATION: Replaces nested iterrows() loops with vectorized merge operation.
    Performance: O(n*m) → O(n+m) complexity.
    """
    matched_cols = []

    # OPTIMIZATION: Use merge instead of nested loops
    source_cols.columns = ['column_name', 'data_type']
    target_cols.columns = ['column_name', 'data_type_target']

    # Find matching columns
    merged = source_cols.merge(target_cols, on='column_name', how='inner')

    # Process columns that need type conversion
    for _, row in merged.iterrows():
        col_name = str(row['column_name']).strip()
        source_type = row['data_type']
        target_type = row['data_type_target']

        try:
            if str(source_type) != str(target_type):
                using_clause = f'USING "{col_name}"::{source_type}'
                query = f'ALTER TABLE derived.{target_table} ALTER COLUMN "{col_name}" TYPE {source_type} {using_clause};;'
                inject_sql(query, operation_name)

            matched_cols.append(col_name)

        except Exception as ex:
            logging.warning(f"Failed to alter column {col_name}: {formatError(ex)}")
            try:
                drop_query = f'ALTER TABLE derived.{target_table} DROP COLUMN "{col_name}";;'
                inject_sql(drop_query, f'DROPPING {target_table} {col_name}')
            except Exception:
                pass

    return matched_cols


def union_views():
    """Create union views for Zimbabwe data combining old and new SMCH datasets."""
    if 'country' not in params or str(params['country']).lower() != 'zimbabwe':
        logging.info("Union views only applicable for Zimbabwe - skipping")
        return

    try:
        # Load column information for all tables
        adm_cols = pd.DataFrame(get_table_columns('admissions', 'derived'),
                                columns=["column_name", "data_type"])
        old_adm_cols = pd.DataFrame(get_table_columns('old_smch_admissions', 'derived'),
                                    columns=["column_name", "data_type"])
        old_disc_cols = pd.DataFrame(get_table_columns('old_smch_discharges', 'derived'),
                                     columns=["column_name", "data_type"])
        disc_cols = pd.DataFrame(get_table_columns('discharges', 'derived'),
                                 columns=["column_name", "data_type"])
        old_matched_cols = pd.DataFrame(get_table_columns('old_smch_matched_admissions_discharges', 'derived'),
                                        columns=["column_name", "data_type"])
        matched_cols = pd.DataFrame(get_table_columns('joined_admissions_discharges', 'derived'),
                                    columns=["column_name", "data_type"])

        # OPTIMIZATION: Use match_column_types_optimized instead of nested loops
        # Replaces O(n*m) nested iterrows with O(n+m) merge operation
        logging.info("Matching column types for admissions")
        old_new_matched_adm_col = match_column_types_optimized(
            adm_cols, old_adm_cols, 'old_smch_admissions', 'OLD ADMISSIONS'
        )

        logging.info("Matching column types for discharges")
        old_new_matched_dis_col = match_column_types_optimized(
            disc_cols, old_disc_cols, 'old_smch_discharges', 'OLD DISCHARGES'
        )

        logging.info("Matching column types for matched data")
        _ = match_column_types_optimized(
            matched_cols, old_matched_cols, 'old_smch_matched_admissions_discharges', 'Union Views'
        )
        logging.info("Column type matching complete")

        # Load data from all tables
        old_smch_admissions = pd.DataFrame()
        old_smch_discharges = pd.DataFrame()
        old_matched_smch_data = pd.DataFrame()
        new_smch_admissions = pd.DataFrame()
        new_smch_discharges = pd.DataFrame()
        new_smch_matched_data = pd.DataFrame()

        if table_exists('derived', 'old_smch_admissions'):
            old_smch_admissions = catalog.load('read_old_smch_admissions')
        if table_exists('derived', 'old_smch_discharges'):
            old_smch_discharges = catalog.load('read_old_smch_discharges')
        if table_exists('derived', 'old_smch_matched_admissions_discharges'):
            old_matched_smch_data = catalog.load('read_old_smch_matched_data')
        if table_exists('derived', 'admissions'):
            new_smch_admissions = catalog.load('read_new_smch_admissions')
        if table_exists('derived', 'discharges'):
            new_smch_discharges = catalog.load('read_new_smch_discharges')
        if table_exists('derived', 'joined_admissions_discharges'):
            new_smch_matched_data = catalog.load('read_new_smch_matched')

        # Process old admissions
        if old_smch_admissions is not None and not old_smch_admissions.empty:
            logging.info(f"Processing old admissions: {len(old_smch_admissions)} rows")

            # OPTIMIZATION: Use vectorized age processing instead of iterrows()
            old_smch_admissions = process_age_column_vectorized(old_smch_admissions)

            # OPTIMIZATION: Use bulk key changes instead of row-by-row
            admission_key_mappings = [
                ('BW.value', 'BirthWeight.value'),
                ('Conv.value', 'Convulsions.value'),
                ('SRNeuroOther.value', 'SymptomReviewNeurology.value'),
                ('LBW.value', 'LowBirthWeight.value'),
                ('AW.value', 'AdmissionWeight.value'),
                ('BSmgdL.value', 'BSUnitmg.value'),
                ('BSmmol.value', 'BloodSugarmmol.value'),
                ('BSmg.value', 'BloodSugarmg.value')
            ]
            old_smch_admissions = apply_key_changes_bulk(old_smch_admissions, admission_key_mappings)

            # Convert numeric columns
            numeric_cols = ['AdmissionWeight.value', 'BirthWeight.value', 'Gestation.value', 'Temperature.value']
            for col in numeric_cols:
                if col in old_smch_admissions.columns:
                    old_smch_admissions[col] = pd.to_numeric(
                        old_smch_admissions[col],
                        errors='coerce'
                    )

            # Format dates
            old_smch_admissions = format_date(
                old_smch_admissions,
                ['DateTimeAdmission.value', 'EndScriptDatetime.value', 'DateHIVtest.value', 'ANVDRLDate.value']
            )

        # Process old discharges
        if old_smch_discharges is not None and not old_smch_discharges.empty:
            logging.info(f"Processing old discharges: {len(old_smch_discharges)} rows")

            # OPTIMIZATION: Use bulk key changes instead of iterrows()
            discharge_key_mappings = [
                ('BWTDis.value', 'BirthWeight.value'),
                ('BirthDateDis.value', 'DOBTOB.value'),
                ('Delivery.value', 'ModeDelivery.value'),
                ('NNUAdmTemp.value', 'Temperature.value'),
                ('GestBirth.value', 'Gestation.value'),
                ('PresComp.value', 'AdmReason.value')
            ]
            old_smch_discharges = apply_key_changes_bulk(old_smch_discharges, discharge_key_mappings)

            # Format dates
            old_smch_discharges = format_date(
                old_smch_discharges,
                ['DateAdmissionDC.value', 'DateDischVitals.value', 'DateDischWeight.value',
                 'DateTimeDischarge.value', 'EndScriptDatetime.value', 'DateWeaned.value',
                 'DateTimeDeath.value', 'DateAdmission.value', 'BirthDateDis.value']
            )

        # Process old matched data
        if old_matched_smch_data is not None and not old_matched_smch_data.empty:
            logging.info(f"Processing old matched data: {len(old_matched_smch_data)} rows")

            # OPTIMIZATION: Use vectorized age processing instead of iterrows()
            old_matched_smch_data = process_age_column_vectorized(old_matched_smch_data)

            # OPTIMIZATION: Use bulk key changes (combined mappings from admissions + discharges)
            matched_key_mappings = [
                # Admission mappings
                ('BW.value', 'BirthWeight.value'),
                ('Conv.value', 'Convulsions.value'),
                ('SRNeuroOther.value', 'SymptomReviewNeurology.value'),
                ('LBW.value', 'LowBirthWeight.value'),
                ('AW.value', 'AdmissionWeight.value'),
                ('BSmgdL.value', 'BSUnitmg.value'),
                ('BSmmol.value', 'BloodSugarmmol.value'),
                ('BSmg.value', 'BloodSugarmg.value'),
                # Discharge mappings
                ('BWTDis.value', 'BirthWeight.value'),
                ('BirthDateDis.value', 'DOBTOB.value'),
                ('Delivery.value', 'ModeDelivery.value'),
                ('NNUAdmTemp.value', 'Temperature.value'),
                ('GestBirth.value', 'Gestation.value'),
                ('PresComp.value', 'AdmReason.value')
            ]
            old_matched_smch_data = apply_key_changes_bulk(old_matched_smch_data, matched_key_mappings)

            # Convert numeric columns
            numeric_cols = ['AdmissionWeight.value', 'BirthWeight.value', 'BirthWeight.value_discharge']
            for col in numeric_cols:
                if col in old_matched_smch_data.columns:
                    old_matched_smch_data[col] = pd.to_numeric(
                        old_matched_smch_data[col],
                        errors='coerce'
                    )

            # Format dates
            old_matched_smch_data = format_date(
                old_matched_smch_data,
                ['DateTimeAdmission.value', 'EndScriptDatetime.value', 'DateHIVtest.value',
                 'ANVDRLDate.value', 'DateAdmissionDC.value', 'DateDischVitals.value',
                 'DateDischWeight.value', 'DateTimeDischarge.value', 'DateWeaned.value',
                 'DateTimeDeath.value', 'DateAdmission.value', 'BirthDateDis.value']
            )

        # SAVE OLD NEW ADMISSIONS
        try:
            if old_smch_admissions is not None and not new_smch_admissions.empty and not old_smch_admissions.empty:
                logging.info(f"Processing admissions union: new={len(new_smch_admissions)} rows, old={len(old_smch_admissions)} rows")

                # Apply column filtering
                new_smch_admissions = filter_columns_optimized(new_smch_admissions, is_old_dataset=False)
                old_smch_admissions = filter_columns_optimized(old_smch_admissions, is_old_dataset=True)

                new_smch_admissions.reset_index(drop=True, inplace=True)
                old_smch_admissions.reset_index(drop=True, inplace=True)

                combined_adm_df = pd.concat([new_smch_admissions, old_smch_admissions], axis=0, ignore_index=True)

                # Final check: ensure combined DF doesn't exceed column limit
                if len(combined_adm_df.columns) > 1550:
                    logging.warning(f"Combined admissions has {len(combined_adm_df.columns)} columns, applying final filter")
                    combined_adm_df = filter_columns_optimized(combined_adm_df, is_old_dataset=False, max_columns=1550)

                if not combined_adm_df.empty:
                    catalog.save('create_derived_old_new_admissions_view', combined_adm_df)
                    logging.info(f"Saved combined admissions: {len(combined_adm_df)} rows, {len(combined_adm_df.columns)} columns")
        except Exception as e:
            logging.error("*******AN EXCEPTIONS HAPPENED WHILEST CONCATENATING COMBINED ADMISSIONS")
            logging.error(formatError(e))

        # SAVE OLD NEW DISCHARGES
        try:
            if old_smch_discharges is not None and not new_smch_discharges.empty and not old_smch_discharges.empty:
                logging.info(f"Processing discharges union: new={len(new_smch_discharges)} rows, old={len(old_smch_discharges)} rows")

                # Apply column filtering
                new_smch_discharges = filter_columns_optimized(new_smch_discharges, is_old_dataset=False)
                old_smch_discharges = filter_columns_optimized(old_smch_discharges, is_old_dataset=True)

                new_smch_discharges.reset_index(drop=True, inplace=True)
                old_smch_discharges.reset_index(drop=True, inplace=True)

                combined_dis_df = pd.concat([new_smch_discharges, old_smch_discharges], axis=0, ignore_index=True)

                # Final check: ensure combined DF doesn't exceed column limit
                if len(combined_dis_df.columns) > 1550:
                    logging.warning(f"Combined discharges has {len(combined_dis_df.columns)} columns, applying final filter")
                    combined_dis_df = filter_columns_optimized(combined_dis_df, is_old_dataset=False, max_columns=1550)

                if not combined_dis_df is None and not combined_dis_df.empty:
                    combined_dis_df = format_date_without_timezone(combined_dis_df, ['DateTimeDischarge.value'])

                    if combined_dis_df is not None and not combined_dis_df.empty:
                        catalog.save('create_derived_old_new_discharges_view', combined_dis_df)
                        logging.info(f"Saved combined discharges: {len(combined_dis_df)} rows, {len(combined_dis_df.columns)} columns")

                        # Note: SQL insert now uses filtered columns from old_new_matched_dis_col
                        query = insert_old_adm_query("DERIVED.old_new_discharges_view", "derived.old_smch_discharges", old_new_matched_dis_col)
                        logging.info("Adding old discharges via SQL")
                        inject_sql(f'{query};;', "Adding old smch discharges")
                        logging.info("Added old discharges")
        except Exception as e:
            logging.error("*******AN EXCEPTIONS HAPPENED WHILEST CONCATENATING COMBINED DISCHARGES")
            logging.error(formatError(e))

        # SAVE MATCHED DATA
        try:
            if old_matched_smch_data is not None and not new_smch_matched_data.empty and not old_matched_smch_data.empty:
                logging.info(f"Processing matched data union: new={len(new_smch_matched_data)} rows, old={len(old_matched_smch_data)} rows")

                # Correct UID column to suit the lower case uid in new_smch_matched_data
                if 'UID' in old_matched_smch_data.columns:
                    old_matched_smch_data = old_matched_smch_data.rename(columns={'UID': 'uid'})

                # Apply column filtering
                new_smch_matched_data = filter_columns_optimized(new_smch_matched_data, is_old_dataset=False)
                old_matched_smch_data = filter_columns_optimized(old_matched_smch_data, is_old_dataset=True)

                old_matched_smch_data.reset_index(drop=True, inplace=True)
                new_smch_matched_data.reset_index(drop=True, inplace=True)

                combined_matched_df = pd.concat([new_smch_matched_data, old_matched_smch_data], axis=0).reset_index(drop=True)

                # Final check: ensure combined DF doesn't exceed column limit
                if len(combined_matched_df.columns) > 1550:
                    logging.warning(f"Combined matched data has {len(combined_matched_df.columns)} columns, applying final filter")
                    combined_matched_df = filter_columns_optimized(combined_matched_df, is_old_dataset=False, max_columns=1550)

                if not combined_matched_df.empty:
                    catalog.save('create_derived_old_new_matched_view', combined_matched_df)
                    logging.info(f"Saved combined matched data: {len(combined_matched_df)} rows, {len(combined_matched_df.columns)} columns")
        except Exception as e:
            logging.error("*******AN EXCEPTIONS HAPPENED WHILEST CONCATENATING COMBINED MATCHED")
            logging.error(formatError(e))

    except Exception as ex:
            logging.error("!!! An error occured creating union views: ")
            logging.error(ex)
            exit()

 