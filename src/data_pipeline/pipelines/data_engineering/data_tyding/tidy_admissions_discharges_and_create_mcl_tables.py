import pandas as pd
import logging
from typing import List, Tuple, Any

# Import created modules
from conf.common.format_error import formatError
from .extract_key_values import get_key_values, get_diagnoses_key_values, format_repeatables_to_rows
from .explode_mcl_columns import explode_column
from .create_derived_columns import create_columns
from conf.common.sql_functions import (
    create_new_columns,
    get_date_column_names,
    get_table_column_names,
    generate_create_insert_sql,
    generate_upsert_queries_and_create_table,
    generate_timestamp_conversion_query
)
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from conf.base.catalog import catalog
from data_pipeline.pipelines.data_engineering.utils.assorted_fixes import extract_years
from data_pipeline.pipelines.data_engineering.utils.field_info import update_fields_info, transform_matching_labels
from data_pipeline.pipelines.data_engineering.utils.custom_date_formatter import format_date, format_date_without_timezone
from data_pipeline.pipelines.data_engineering.utils.key_change import key_change
from data_pipeline.pipelines.data_engineering.utils.set_key_to_none import set_key_to_none
from data_pipeline.pipelines.data_engineering.utils.data_label_fixes import format_column_as_numeric, convert_false_numbers_to_text
from .neolab_data_cleanup import neolab_cleanup
from .tidy_dynamic_tables import tidy_dynamic_tables
from data_pipeline.pipelines.data_engineering.queries.data_fix import deduplicate_table
from data_pipeline.pipelines.data_engineering.data_validation.validate import validate_dataframe_with_ge, begin_validation_run, finalize_validation



def safe_load(catalog, dataset_name: str) -> pd.DataFrame:
    """Safely load a dataset from catalog, returning empty DataFrame on failure."""
    try:
        return catalog.load(dataset_name)
    except Exception as e:
        logging.warning(f"Failed to load dataset '{dataset_name}': {formatError(e)}")
        return pd.DataFrame()


def calculate_time_spent(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate time spent between started_at and completed_at columns."""
    if "started_at" in df.columns and 'completed_at' in df.columns:
        try:
            df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce').dt.tz_localize(None)
            df['completed_at'] = pd.to_datetime(df['completed_at'], errors='coerce').dt.tz_localize(None)
            df['time_spent'] = (df['completed_at'] - df['started_at']).dt.total_seconds() / 60
        except Exception as e:
            logging.warning(f"Error calculating time_spent: {formatError(e)}")
            df['time_spent'] = None
    else:
        df['time_spent'] = None
    return df


def parse_age_hours(age_string: str) -> float:
    """Parse age string (e.g., '3 days, 4 hours') and convert to hours."""
    if pd.isna(age_string) or age_string == 'nan':
        return 0

    age_list = str(age_string).split(",")
    hours = 0

    if len(age_list) == 1:
        age = age_list[0]
        if 'hour' in age:
            nums = [int(s) for s in age.replace("-", "").split() if s.isdigit()]
            hours = nums[0] if nums else (1 if "an" in age else 0)
        elif 'day' in age:
            nums = [int(s) for s in age.replace("-", "").split() if s.isdigit()]
            hours = nums[0] * 24 if nums else 0
        elif 'second' in age or 'minute' in age:
            hours = 1  # Round to 1 hour
    elif len(age_list) == 2:
        age_days, age_hours = age_list[0], age_list[1]
        if 'day' in age_days and 'hour' in age_hours:
            days = [int(s) for s in age_days.split() if s.isdigit()]
            hrs = [int(s) for s in age_hours.split() if s.isdigit()]
            if days and hrs:
                hours = days[0] * 24 + hrs[0]

    return hours


def calculate_age_category(age_hours: float) -> str:
    """Calculate age category based on age in hours."""
    if age_hours < 2:
        return 'Fresh Newborn (< 2 hours old)'
    elif age_hours <= 23:
        return 'Newborn (2 - 23 hrs old)'
    elif age_hours <= 47:
        return 'Newborn (1 day - 1 day 23 hrs old)'
    elif age_hours <= 71:
        return 'Infant (2 days - 2 days 23 hrs old)'
    else:
        return 'Infant (> 3 days old)'


def process_age_column_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    """Process and calculate age using vectorized operations where possible."""
    if 'Age.value' not in df.columns:
        return df

    # Handle datetime-format ages
    mask_datetime = df['Age.value'].astype(str).str.contains('T', na=False) & (df['Age.value'].astype(str).str.len() > 10)
    if mask_datetime.any() and "DateTimeAdmission.value" in df.columns:
        admission_dates = pd.to_datetime(df.loc[mask_datetime, 'DateTimeAdmission.value'], format='%Y-%m-%dT%H:%M:%S', utc=True, errors='coerce')
        birth_dates = pd.to_datetime(df.loc[mask_datetime, 'Age.value'], format='%Y-%m-%dT%H:%M:%S', utc=True, errors='coerce')

        # Fix bug where DOB > admission date
        mask_fix = admission_dates < birth_dates
        birth_dates[mask_fix] = birth_dates[mask_fix] - pd.Timedelta(hours=24)

        age_hours = (admission_dates - birth_dates) / pd.Timedelta(hours=1)
        age_hours = age_hours.clip(lower=1).round()
        df.loc[mask_datetime, 'Age.value'] = age_hours

    # Handle numeric ages
    mask_numeric = pd.to_numeric(df['Age.value'], errors='coerce').notna()

    # Handle string ages (e.g., "3 days, 4 hours")
    mask_string = ~mask_numeric & ~mask_datetime & df['Age.value'].notna()
    if mask_string.any():
        df.loc[mask_string, 'Age.value'] = df.loc[mask_string, 'Age.value'].apply(parse_age_hours)

        # Fallback to AgeB.value if Age.value is still not numeric
        if 'AgeB.value' in df.columns:
            mask_still_invalid = ~pd.to_numeric(df['Age.value'], errors='coerce').notna()
            df.loc[mask_still_invalid, 'Age.value'] = df.loc[mask_still_invalid, 'AgeB.value'].apply(parse_age_hours)

    # Convert to numeric and calculate categories
    df['Age.value'] = pd.to_numeric(df['Age.value'], errors='coerce')
    mask_valid = df['Age.value'].notna() & (df['Age.value'] > 0)
    df.loc[mask_valid, 'AgeCategory'] = df.loc[mask_valid, 'Age.value'].apply(calculate_age_category)

    return df


def apply_key_mappings(df: pd.DataFrame, mappings: List[Tuple[str, str, str]]) -> pd.DataFrame:
    """Apply key changes based on mapping tuples (old_key, new_key, new_value_source)."""
    for position in df.index:
        row = df.loc[position]
        for old_key, new_key, _ in mappings:
            if new_key in row and str(row[new_key]) != 'nan' and row[new_key] is not None:
                continue
            key_change(df, row, position, old_key, new_key)
    return df


def add_new_columns_if_needed(df: pd.DataFrame, table_name: str, schema: str = 'derived') -> None:
    """Add new columns to database table if they don't exist."""
    if table_exists(schema, table_name):
        existing_cols = pd.DataFrame(get_table_column_names(table_name, schema))
        new_columns = set(df.columns) - set(existing_cols.columns)

        if new_columns:
            column_pairs = [(col, str(df[col].dtype)) for col in new_columns]
            if column_pairs:
                create_new_columns(table_name, schema, column_pairs)


def finalize_dataframe(df: pd.DataFrame, table_name: str, schema: str = 'derived') -> pd.DataFrame:
    """Apply final transformations to dataframe before saving."""
    df = convert_false_numbers_to_text(df, schema, table_name)
    df = df.loc[:, ~df.columns.str.match(r'^\d+$|^[a-zA-Z]$', na=False)]
    df = transform_matching_labels(df, table_name)
    return df


def process_repeatables(raw_df: pd.DataFrame, table_prefix: str) -> None:
    """Process and save repeatable fields."""
    try:
        repeatables = format_repeatables_to_rows(raw_df, table_prefix)
        for table_name, df in (repeatables or {}).items():
            if not df.empty:
                generate_upsert_queries_and_create_table(table_name, df)
    except Exception as e:
        logging.error(f"!!! An error occurred formatting {table_prefix} repeatables")
        logging.error(formatError(e))


def process_neolab_episodes(neolab_df: pd.DataFrame) -> pd.DataFrame:
    """Process neolab dataframe to calculate episodes and BCType more efficiently."""
    neolab_df['episode'] = 0
    neolab_df['BCType'] = None
    neolab_df['DateBCT.value'] = pd.to_datetime(neolab_df['DateBCT.value'], errors='coerce')

    # Group by uid to process episodes
    for uid in neolab_df['uid'].unique():
        uid_mask = neolab_df['uid'] == uid
        uid_data = neolab_df[uid_mask].sort_values(by=['DateBCT.value']).copy()

        if uid_data.empty:
            continue

        # Calculate episodes based on date changes
        uid_data['date_only'] = uid_data['DateBCT.value'].dt.strftime('%Y-%m-%d')
        uid_data['episode'] = (uid_data['date_only'] != uid_data['date_only'].shift()).cumsum()

        # Update main dataframe with episodes
        neolab_df.loc[uid_mask, 'episode'] = uid_data['episode'].values

        # Process BCType for each episode
        for episode_num in uid_data['episode'].unique():
            episode_mask = uid_mask & (neolab_df['episode'] == episode_num)
            episode_data = neolab_df[episode_mask].sort_values(by=['DateBCR.value']).copy()

            preliminary_index = 1
            for idx in episode_data.index:
                result_value = neolab_df.at[idx, 'BCResult.value']

                # Determine BCType based on result and position
                if result_value not in ['Pos', 'Neg', 'PC']:
                    bc_type = f"PRELIMINARY-{preliminary_index}"
                    preliminary_index += 1
                else:
                    if idx == episode_data.index[-1]:
                        bc_type = "FINAL"
                    else:
                        bc_type = f"PRELIMINARY-{preliminary_index}"
                        preliminary_index += 1

                neolab_df.at[idx, 'BCType'] = bc_type

    return neolab_df


def process_baseline_dates(baseline_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate LengthOfStay and LengthOfLife for baseline dataframe."""
    baseline_df['LengthOfStay.value'] = None
    baseline_df['LengthOfStay.label'] = "Length of Stay"
    baseline_df['LengthOfLife.value'] = None
    baseline_df['LengthOfLife.label'] = "Length of Life"

    # Calculate Length of Stay
    if all(col in baseline_df.columns for col in ['DateTimeDischarge.value', 'DateTimeAdmission.value']):
        discharge_dates = pd.to_datetime(baseline_df['DateTimeDischarge.value'], errors='coerce')
        admission_dates = pd.to_datetime(baseline_df['DateTimeAdmission.value'], errors='coerce')
        valid_mask = discharge_dates.notna() & admission_dates.notna()
        baseline_df.loc[valid_mask, 'LengthOfStay.value'] = (discharge_dates - admission_dates).dt.days

    # Calculate Length of Life
    if all(col in baseline_df.columns for col in ['DateTimeDeath.value', 'DateTimeAdmission.value']):
        death_dates = pd.to_datetime(baseline_df['DateTimeDeath.value'], errors='coerce')
        admission_dates = pd.to_datetime(baseline_df['DateTimeAdmission.value'], errors='coerce')
        valid_mask = death_dates.notna() & admission_dates.notna()
        baseline_df.loc[valid_mask, 'LengthOfLife.value'] = (death_dates - admission_dates).dt.days

    return baseline_df


def process_admissions_dataframe(adm_raw: pd.DataFrame, adm_new_entries: Any, adm_mcl: Any) -> None:
    """Process admissions dataframe with all transformations and save to database."""
    adm_df = pd.DataFrame(adm_new_entries) if isinstance(adm_new_entries, list) else pd.json_normalize(adm_new_entries)

    if adm_df.empty:
        return

    logging.info("Processing admissions dataframe")
    update_fields_info("admissions")
    adm_df.set_index(['uid'])

    # Format dates
    result = format_date_without_timezone(adm_df, ['started_at', 'completed_at', 'EndScriptDatetime.value', 'DateTimeAdmission.value'])
    if result is not None:
        adm_df = result

    result = format_date(adm_df, ['DateHIVtest.value', 'ANVDRLDate.value'])
    if result is not None:
        adm_df = result

    # Calculate age from admission date and DOB if Age is missing
    if "DateTimeAdmission.value" in adm_df.columns and "DOBTOB.value" in adm_df.columns:
        adm_df.loc[
            adm_df["Age.value"].isna() & adm_df["DateTimeAdmission.value"].notna() & adm_df["DOBTOB.value"].notna(),
            "Age.value"
        ] = (
            (pd.to_datetime(adm_df["DateTimeAdmission.value"]) - pd.to_datetime(adm_df["DOBTOB.value"]))
            .dt.total_seconds() / 3600
        )

    # Calculate time spent
    adm_df = calculate_time_spent(adm_df)

    # Set specific keys to None
    adm_df = set_key_to_none(adm_df, [
        'DateHIVtest.value', 'DateHIVtest.label', 'HIVtestResult.value', 'HIVtestResult.label',
        'ANVDRLDate.value', 'ANVDRLDate.label', 'HAART.value', 'HAART.label',
        'LengthHAART.value', 'LengthHAART.label', 'NVPgiven.value', 'NVPgiven.label',
        'ROMlength.label', 'ROMlength.value', 'ROMLength.label', 'ROMLength.value',
        'TempThermia.value', 'AWGroup.value'
    ])

    # Process age column with vectorized operations
    adm_df = process_age_column_vectorized(adm_df)

    # Apply key mappings for backward compatibility
    key_mappings = [
        ('BW.value', 'BirthWeight.value', ''), ('BW .value', 'BirthWeight.value', ''),
        ('Conv.value', 'Convulsions.value', ''), ('SRNeuroOther.value', 'SymptomReviewNeurology.value', ''),
        ('LBW.value', 'LowBirthWeight.value', ''), ('AW.value', 'AdmissionWeight.value', ''),
        ('BSmgdL.value', 'BSUnitmg.value', ''), ('BSmmol.value', 'BloodSugarmmol.value', ''),
        ('BSmg.value', 'BloodSugarmg.value', ''), ('ROMlength.value', 'ROMLength.value', ''),
        ('ROMlength.label', 'ROMLength.label', '')
    ]
    adm_df = apply_key_mappings(adm_df, key_mappings)

    # Format numeric columns
    if 'AdmissionWeight.value' in adm_df:
        adm_df['AdmissionWeight.value'] = pd.to_numeric(adm_df['AdmissionWeight.value'], downcast='integer', errors='coerce')

    # Drop unnecessary columns
    for col in ['BW.value', 'BW .value']:
        if col in adm_df:
            adm_df = adm_df.drop(columns=[col])

    # Create derived columns
    adm_df = create_columns(adm_df)
    adm_df = adm_df[adm_df['uid'] != 'Unknown']

    # Clean column names
    adm_df.columns = adm_df.columns.str.replace(r"[()-]", "_", regex=True)

    # Format numeric fields
    numeric_fields = ['Temperature', 'RR', 'HR', 'Age', 'PAR', 'OFC', 'Apgar1', 'Apgar5', 'Length',
                      'SatsO2', 'SatsAir', 'BalScore', 'VLNumber', 'Gestation', 'MatAgeYrs',
                      'BalScoreWks', 'BirthWeight', 'DurationLab', 'BloodSugarmg', 'AntenatalCare',
                      'BloodSugarmmol', 'AdmissionWeight']
    adm_df = format_column_as_numeric(adm_df, numeric_fields)

    if 'MatAgeYrs' in adm_df:
        adm_df['MatAgeYrs'] = adm_df['MatAgeYrs'].apply(extract_years)

    # Add new columns to database if needed
    add_new_columns_if_needed(adm_df, 'admissions')

    # Final transformations
    adm_df = finalize_dataframe(adm_df, 'admissions')

    # Validate and save
    validate_dataframe_with_ge(adm_df, 'admissions')
    generate_create_insert_sql(adm_df, 'derived', 'admissions')
    deduplicate_table('admissions')

    logging.info("Creating MCL count tables for Admissions")
    explode_column(adm_df, adm_mcl, "")
    generate_timestamp_conversion_query('derived.admissions', ['completed_at', 'started_at'])

    # Process repeatables
    process_repeatables(adm_raw, "admissions")


def process_discharges_dataframe(dis_raw: pd.DataFrame, dis_new_entries: Any, dis_mcl: Any) -> None:
    """Process discharges dataframe with all transformations and save to database."""
    dis_df = pd.json_normalize(dis_new_entries)

    if dis_df.empty:
        return

    logging.info("Processing discharges dataframe")
    update_fields_info("discharges")

    # Format dates
    result = format_date_without_timezone(dis_df, ['started_at', 'completed_at'])
    if result is not None:
        dis_df = result

    result = format_date(dis_df, [
        'DateAdmissionDC.value', 'DateDischVitals.value', 'DateDischWeight.value',
        'DateTimeDischarge.value', 'EndScriptDatetime.value', 'DateWeaned.value',
        'DateTimeDeath.value', 'DateAdmission.value', 'BirthDateDis.value',
        'DateHIVtest.value', 'DateVDRLSameHIV.value', 'started_at', 'completed_at'
    ])
    if result is not None:
        dis_df = result

    # Calculate time spent
    dis_df = calculate_time_spent(dis_df)

    # Apply key mappings for backward compatibility
    key_mappings = [
        ('BWTDis.value', 'BirthWeight.value', ''),
        ('BirthDateDis.value', 'DOBTOB.value', ''),
        ('Delivery.value', 'ModeDelivery.value', ''),
        ('NNUAdmTemp.value', 'Temperature.value', ''),
        ('GestBirth.value', 'Gestation.value', ''),
        ('PresComp.value', 'AdmReason.value', '')
    ]
    dis_df = apply_key_mappings(dis_df, key_mappings)

    # Create derived columns and filter
    dis_df = create_columns(dis_df)
    dis_df = dis_df[dis_df['uid'] != 'Unknown']

    # Add new columns to database if needed
    add_new_columns_if_needed(dis_df, 'discharges')

    # Final transformations
    dis_df = finalize_dataframe(dis_df, 'discharges')

    # Validate and save
    validate_dataframe_with_ge(dis_df, 'discharges')
    generate_create_insert_sql(dis_df, 'derived', 'discharges')
    deduplicate_table('discharges')

    logging.info("Creating MCL count tables for Discharge")
    explode_column(dis_df, dis_mcl, "disc_")
    generate_timestamp_conversion_query('derived.discharges', ['completed_at', 'started_at'])

    # Process repeatables
    process_repeatables(dis_raw, "discharges")


def process_maternal_outcomes_dataframe(mat_outcomes_raw: pd.DataFrame, mat_outcomes_new_entries: Any, mat_outcomes_mcl: Any) -> pd.DataFrame:
    """Process maternal outcomes dataframe with all transformations and save to database."""
    mat_outcomes_df = pd.json_normalize(mat_outcomes_new_entries)

    if mat_outcomes_df.empty:
        return pd.DataFrame()

    logging.info("Processing maternal outcomes dataframe")
    update_fields_info("maternal_outcomes")
    mat_outcomes_df.set_index(['unique_key'])

    # Format dates
    result = format_date_without_timezone(mat_outcomes_df, ['started_at', 'completed_at', 'DateAdmission.value'])
    if result is not None:
        mat_outcomes_df = result

    # Calculate time spent
    mat_outcomes_df = calculate_time_spent(mat_outcomes_df)

    # Set specific keys to None
    result = set_key_to_none(mat_outcomes_df, [
        'Presentation.label', 'BabyNursery.label', 'Reason.label', 'ReasonOther.label',
        'CryBirth.label', 'Apgar1.value', 'Apgar5.value', 'Apgar10.value',
        'PregConditions.label', 'BirthDateDis.value', 'TypeBirth.label', 'TypeBirth.value'
    ])
    if result is not None:
        mat_outcomes_df = result

    # Create derived columns and filter
    mat_outcomes_df = create_columns(mat_outcomes_df)
    mat_outcomes_df = mat_outcomes_df[mat_outcomes_df['uid'] != 'Unknown']

    # Add new columns to database if needed
    add_new_columns_if_needed(mat_outcomes_df, 'maternal_outcomes')

    # Final transformations
    mat_outcomes_df = finalize_dataframe(mat_outcomes_df, 'maternal_outcomes')

    # Validate and save
    validate_dataframe_with_ge(mat_outcomes_df, 'maternal_outcomes')
    generate_create_insert_sql(mat_outcomes_df, 'derived', 'maternal_outcomes')
    deduplicate_table('maternal_outcomes')

    logging.info("Creating MCL count tables for Maternal Outcomes")
    explode_column(mat_outcomes_df, mat_outcomes_mcl, "mat_")

    # Process repeatables
    process_repeatables(mat_outcomes_raw, "maternal_outcomes")

    return mat_outcomes_df


def process_vitalsigns_dataframe(vit_signs_new_entries: Any, vit_signs_mcl: Any) -> None:
    """Process vital signs dataframe with all transformations and save to database."""
    vit_signs_df = pd.json_normalize(vit_signs_new_entries)

    if vit_signs_df.empty:
        return

    logging.info("Processing vital signs dataframe")
    update_fields_info("vitalsigns")
    vit_signs_df.set_index(['uid'])

    # Format dates
    result = format_date_without_timezone(vit_signs_df, ['started_at', 'completed_at'])
    if result is not None:
        vit_signs_df = result

    result = format_date(vit_signs_df, ['D1Date.value', 'TimeTemp1.value', 'TimeTemp2.value', 'EndScriptDatetime.value'])
    if result is not None:
        vit_signs_df = result

    # Calculate time spent
    vit_signs_df = calculate_time_spent(vit_signs_df)

    # Filter
    vit_signs_df = vit_signs_df[vit_signs_df['uid'] != 'Unknown']

    # Add new columns to database if needed
    add_new_columns_if_needed(vit_signs_df, 'vitalsigns')

    # Final transformations
    vit_signs_df = finalize_dataframe(vit_signs_df, 'vitalsigns')

    # Validate and save
    validate_dataframe_with_ge(vit_signs_df, 'vitalsigns')
    generate_create_insert_sql(vit_signs_df, 'derived', 'vitalsigns')
    deduplicate_table('vitalsigns')

    logging.info("Creating MCL count tables for Vital Signs")
    explode_column(vit_signs_df, vit_signs_mcl, "vit_")


def process_neolab_dataframe(neolab_raw: pd.DataFrame, neolab_new_entries: Any) -> None:
    """Process neolab dataframe with all transformations and save to database."""
    neolab_df = pd.json_normalize(neolab_new_entries)

    if neolab_df.empty:
        return

    logging.info("Processing neolab dataframe")
    update_fields_info("neolab")

    # Calculate BCReturnTime
    if ("DateBCR.value" in neolab_df and 'DateBCT.value' in neolab_df and
        neolab_df['DateBCR.value'].notna().any() and neolab_df['DateBCT.value'].notna().any()):
        neolab_df['BCReturnTime'] = (
            pd.to_datetime(neolab_df['DateBCR.value'], format='%Y-%m-%dT%H:%M:%S', utc=True, errors='coerce') -
            pd.to_datetime(neolab_df['DateBCT.value'], format='%Y-%m-%dT%H:%M:%S', utc=True, errors='coerce')
        ).astype('timedelta64[h]')
    else:
        neolab_df['BCReturnTime'] = None

    # Format dates
    result = format_date_without_timezone(neolab_df, ['started_at', 'completed_at'])
    if result is not None:
        neolab_df = result

    # Calculate time spent
    neolab_df = calculate_time_spent(neolab_df)

    # Data cleanup for each row
    for index in neolab_df.index:
        neolab_cleanup(neolab_df, index)

    # Process episodes and BCType
    neolab_df = process_neolab_episodes(neolab_df)

    # Create derived columns
    neolab_df = create_columns(neolab_df)

    # Set index and sort
    if "uid" in neolab_df:
        neolab_df.set_index(['uid'])
        if "episode" in neolab_df:
            neolab_df.sort_values(by=['uid', 'episode'])

    # Filter
    neolab_df = neolab_df[neolab_df['uid'] != 'Unknown']

    # Final transformations
    neolab_df = neolab_df.loc[:, ~neolab_df.columns.str.match(r'^\d+$|^[a-zA-Z]$', na=False)]
    neolab_df = transform_matching_labels(neolab_df, 'neolab')

    # Validate and save
    validate_dataframe_with_ge(neolab_df, 'neolab')
    generate_create_insert_sql(neolab_df, 'derived', 'neolab')
    deduplicate_table('neolab')

    # Process repeatables
    process_repeatables(neolab_raw, "neolab")


def process_baseline_dataframe(baseline_new_entries: Any, baseline_mcl: Any) -> None:
    """Process baseline dataframe with all transformations and save to database."""
    baseline_df = pd.json_normalize(baseline_new_entries)

    if baseline_df.empty:
        return

    logging.info("Processing baseline dataframe")
    update_fields_info("baseline")

    # Format dates
    date_column_types = get_date_column_names('baseline', 'derived')
    result = format_date_without_timezone(baseline_df, date_column_types)
    if result is not None:
        baseline_df = result

    # Calculate time spent
    baseline_df = calculate_time_spent(baseline_df)

    # Format numeric columns
    if 'AdmissionWeight.value' in baseline_df:
        baseline_df['AdmissionWeight.value'] = pd.to_numeric(baseline_df['AdmissionWeight.value'], downcast='integer', errors='coerce')
    if 'BirthWeight.value' in baseline_df:
        baseline_df['BirthWeight.value'] = pd.to_numeric(baseline_df['BirthWeight.value'], downcast='integer', errors='coerce')
    if 'Temperature.value' in baseline_df:
        baseline_df['Temperature.value'] = pd.to_numeric(baseline_df['Temperature.value'], downcast='integer', errors='coerce')

    # Calculate length of stay and life using vectorized function
    baseline_df = process_baseline_dates(baseline_df)

    # Format dates
    result = format_date(baseline_df, ['DateTimeAdmission.value', 'DateTimeDischarge.value', 'DateTimeDeath.value'])
    if result is not None:
        baseline_df = result

    # Set specific keys to None
    result = set_key_to_none(baseline_df, [
        'AWGroup.value', 'BWGroup.value', 'AdmittedFrom.label', 'AdmittedFrom.value',
        'ReferredFrom2.label', 'ReferredFrom2.value', 'ReferredFrom.label', 'ReferredFrom.value',
        'TempThermia.label', 'TempThermia.value', 'TempGroup.label', 'TempGroup.value',
        'GestGroup.label', 'GestGroup.value'
    ])
    if result is not None:
        baseline_df = result

    # Create derived columns
    baseline_df = create_columns(baseline_df)

    if 'Gestation.value' in baseline_df:
        baseline_df['Gestation.value'] = pd.to_numeric(baseline_df['Gestation.value'], errors='coerce')

    # Filter
    baseline_df = baseline_df[baseline_df['uid'] != 'Unknown']

    # Add new columns to database if needed
    add_new_columns_if_needed(baseline_df, 'baseline')

    # Final transformations
    baseline_df = baseline_df.loc[:, ~baseline_df.columns.str.match(r'^\d+$|^[a-zA-Z]$', na=False)]
    baseline_df = transform_matching_labels(baseline_df, 'baseline')

    # Validate and save
    validate_dataframe_with_ge(baseline_df, 'baseline')
    generate_create_insert_sql(baseline_df, 'derived', 'baseline')
    deduplicate_table('baseline')

    logging.info("Creating MCL count tables for Baseline")
    explode_column(baseline_df, baseline_mcl, "bsl_")


def tidy_tables():
    """Main function to process and tidy all medical data tables."""
    begin_validation_run()

    try:
        tidy_dynamic_tables()
    except Exception as e:
        logging.error("!!! An error occurred processing Dynamic Scripts")
        logging.error(formatError(e))

    logging.info("Fetching raw admission and discharge data")

    try:
        # Load all datasets from catalog
        adm_raw = safe_load(catalog, 'read_admissions')
        dis_raw = safe_load(catalog, 'read_discharges')
        mat_outcomes_raw = safe_load(catalog, 'read_maternal_outcomes')
        vit_signs_raw = safe_load(catalog, 'read_vitalsigns')
        neolab_raw = safe_load(catalog, 'read_neolab')
        baseline_raw = safe_load(catalog, 'read_baseline')
        diagnoses_raw = safe_load(catalog, 'read_diagnoses_data')
        mat_completeness_raw = safe_load(catalog, 'read_maternity_completeness')

    except Exception as e:
        logging.error("!!! An error occurred fetching the data")
        logging.error(formatError(e))
        return

    logging.info("Extracting keys from raw data")
    try:
        adm_new_entries, adm_mcl = get_key_values(adm_raw)
        dis_new_entries, dis_mcl = get_key_values(dis_raw)
        mat_outcomes_new_entries, mat_outcomes_mcl = get_key_values(mat_outcomes_raw)
        vit_signs_new_entries, vit_signs_mcl = get_key_values(vit_signs_raw)
        baseline_new_entries, baseline_mcl = get_key_values(baseline_raw)
        diagnoses_new_entries = get_diagnoses_key_values(diagnoses_raw)
        mat_completeness_new_entries, mat_completeness_mcl = get_key_values(mat_completeness_raw)
        neolab_new_entries, neolab_mcl = get_key_values(neolab_raw)

    except Exception as e:
        logging.error("!!! An error occurred extracting keys")
        logging.error(formatError(e))
        return

    # Process all data tables using helper functions
    logging.info("Processing all data tables")

    try:
        # Process admissions
        logging.info("Processing admissions data")
        process_admissions_dataframe(adm_raw, adm_new_entries, adm_mcl)

        # Process discharges
        logging.info("Processing discharges data")
        process_discharges_dataframe(dis_raw, dis_new_entries, dis_mcl)

        # Process maternal outcomes
        logging.info("Processing maternal outcomes data")
        mat_outcomes_df = process_maternal_outcomes_dataframe(mat_outcomes_raw, mat_outcomes_new_entries, mat_outcomes_mcl)

        # Process vital signs
        logging.info("Processing vital signs data")
        process_vitalsigns_dataframe(vit_signs_new_entries, vit_signs_mcl)

        # Process neolab
        logging.info("Processing neolab data")
        process_neolab_dataframe(neolab_raw, neolab_new_entries)

        # Process baseline
        logging.info("Processing baseline data")
        process_baseline_dataframe(baseline_new_entries, baseline_mcl)

        # Process diagnoses
        logging.info("Processing diagnoses data")
        diagnoses_df = pd.json_normalize(diagnoses_new_entries)
        if not diagnoses_df.empty:
            if 'uid' in diagnoses_df:
                diagnoses_df = diagnoses_df[diagnoses_df['uid'] != 'Unknown']
            catalog.save('create_derived_diagnoses', diagnoses_df)

        # Process maternity completeness
        logging.info("Processing maternity completeness data")
        mat_completeness_df = pd.json_normalize(mat_completeness_new_entries)
        if not mat_completeness_df.empty:
            update_fields_info("maternity_completeness")
            result = format_date_without_timezone(mat_completeness_df, ['DateAdmission.value', 'DateTimeAdmission.value'])
            if result is not None:
                mat_completeness_df = result

            # Join Maternal Completeness and Maternal Outcomes (Malawi-specific case)
            if not mat_outcomes_df.empty:
                previous_mat_outcomes_df = mat_outcomes_df[pd.to_datetime(mat_outcomes_df['DateAdmission.value'], errors='coerce') >= '2021-10-01']
                latest_mat_outcomes_df = mat_completeness_df[pd.to_datetime(mat_completeness_df['DateAdmission.value'], errors='coerce') >= '2021-09-30']
                previous_mat_outcomes_df = previous_mat_outcomes_df.reset_index(drop=True)
                latest_mat_outcomes_df = latest_mat_outcomes_df.reset_index(drop=True)
                mat_completeness_df = pd.concat([latest_mat_outcomes_df, previous_mat_outcomes_df], axis=0, ignore_index=True)

            # Add new columns if needed
            if table_exists('derived', 'maternity_completeness'):
                cols = pd.DataFrame(get_table_column_names('maternity_completeness', 'derived'), columns=["column_name"])
                new_columns = set(mat_completeness_df.columns) - set(cols.columns)

                if new_columns:
                    column_pairs = [(col, str(mat_completeness_df[col].dtype)) for col in new_columns]
                    if column_pairs:
                        create_new_columns('maternity_completeness', 'derived', column_pairs)

            # Final transformations and save
            mat_completeness_df = mat_completeness_df.loc[:, ~mat_completeness_df.columns.str.match(r'^\d+$|^[a-zA-Z]$', na=False)]
            mat_completeness_df = transform_matching_labels(mat_completeness_df, 'maternity_completeness')
            generate_create_insert_sql(mat_completeness_df, 'derived', 'maternity_completeness')
            deduplicate_table('maternity_completeness')
            explode_column(mat_completeness_df, mat_completeness_mcl, "matcomp_")

        logging.info("All data tables processed successfully")

    except Exception as ex:
        logging.error("!!! An error occurred processing data tables")
        logging.error(formatError(ex))

    try:
        finalize_validation()
    except Exception as e:
        logging.error("!!! An error occurred finalizing validation")
        logging.error(formatError(e))