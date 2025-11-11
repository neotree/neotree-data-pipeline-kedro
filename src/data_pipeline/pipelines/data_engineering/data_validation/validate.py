import os
import json
import pandas as pd
import numpy as np
import great_expectations as gx
import traceback
from difflib import SequenceMatcher
import smtplib
from email.message import EmailMessage
import logging
import traceback
from .templates import get_html_validation_template
from conf.common.scripts import get_script, merge_script_data
from conf.common.logger import setup_logger
from typing import Dict
from datetime import datetime
from conf.base.catalog import params, hospital_conf
import re
import pdfkit
from data_pipeline.pipelines.data_engineering.utils.field_info import load_json_for_comparison
from difflib import SequenceMatcher

STATUS_FILE = "logs/validation_status.json"


def set_status(status: str):
    with open(STATUS_FILE, "w") as f:
        json.dump({"status": status, "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, f)


def get_status():
    if not os.path.exists(STATUS_FILE):
        return "unknown"
    with open(STATUS_FILE, "r") as f:
        return json.load(f).get("status")


def reset_log(log_file_path="logs/validation.log"):
    with open(log_file_path, "w") as f:
        f.write("")


def begin_validation_run(log_file_path="logs/validation.log"):
    set_status("running")
    reset_log(log_file_path)


def finalize_validation():
    if get_status() == "running":
        set_status("done")
        log_file_path = "logs/validation.log"
        if 'mail_receivers' in params:
            email_recipients = params['MAIL_RECEIVERS'.lower()]
            if email_recipients:
                send_log_via_email(log_file_path, email_receivers=email_recipients)


def get_safe_sample_uids(df: pd.DataFrame, mask: pd.Series, max_samples: int = 2) -> list:
    """
    Get sample UIDs from rows matching the mask, but only return non-NULL UIDs.
    This prevents the contradiction of showing NULL UIDs when reporting NULL values.
    """
    if 'uid' not in df.columns:
        return []

    # Get UIDs from matching rows, but filter out NULL UIDs
    sample_uids = df.loc[mask, 'uid'].dropna().head(max_samples).tolist()
    return sample_uids


def convert_value_to_type(value, data_type, min_val, max_val):
    """
    Convert value to the appropriate type based on data_type and min/max values.

    Returns: (converted_value, conversion_successful)
    """
    if pd.isna(value) or value == '' or str(value).strip() == '':
        return None, True

    try:
        # Determine target type from min/max values or data_type
        if data_type in ['number', 'integer', 'float', 'timer']:
            # Try to convert to numeric
            converted = pd.to_numeric(value, errors='raise')
            return converted, True

        elif data_type in ['datetime', 'timestamp', 'date']:
            # Try to convert to datetime
            converted = pd.to_datetime(value, errors='raise')
            return converted, True

        else:
            # Keep as string
            return str(value), True

    except (ValueError, TypeError):
        return value, False


def check_value_range(value, min_val, max_val, data_type):
    """
    Check if value is within the specified range.

    Returns: (is_valid, error_message)
    """
    if pd.isna(value) or value == '' or str(value).strip() == '':
        return True, None

    # Convert min/max to appropriate type
    try:
        if data_type in ['number', 'integer', 'float', 'timer']:
            if min_val is not None:
                min_val = float(min_val)
            if max_val is not None:
                max_val = float(max_val)
            value_num = float(value)

            if min_val is not None and value_num < min_val:
                return False, f"Value {value_num} is below minimum {min_val}"
            if max_val is not None and value_num > max_val:
                return False, f"Value {value_num} is above maximum {max_val}"

        elif data_type in ['datetime', 'timestamp', 'date']:
            if min_val is not None:
                min_val = pd.to_datetime(min_val, errors='coerce')
            if max_val is not None:
                max_val = pd.to_datetime(max_val, errors='coerce')
            value_dt = pd.to_datetime(value, errors='coerce')

            if pd.notna(min_val) and value_dt < min_val:
                return False, f"Date {value_dt} is before minimum {min_val}"
            if pd.notna(max_val) and value_dt > max_val:
                return False, f"Date {value_dt} is after maximum {max_val}"

        return True, None

    except (ValueError, TypeError) as e:
        return False, f"Cannot validate range: {str(e)}"


def validate_dataframe_with_ge(df: pd.DataFrame, script: str, log_file_path="logs/validation.log"):
    """
    Comprehensive validation using Great Expectations with schema-based rules.
    Optimized to minimize redundant dataframe iterations.

    NEW: Supports scriptId-based validation - splits dataframe by scriptId
    and validates each subset against its corresponding metadata.

    Validates:
    - Required fields (optional=false)
    - Value ranges (minValue, maxValue)
    - Data types
    - Data quality metrics
    """
    context = gx.get_context()
    logger = setup_logger(log_file_path)

    # Load metadata (could be new format {scriptId: [fields]} or legacy [fields])
    metadata = load_json_for_comparison(script)

    if not metadata:
        logger.warning(f"##### SCHEMA FOR SCRIPT {script} NOT FOUND - SKIPPING VALIDATION")
        return

    logger.info(f"\n{'='*60}")
    logger.info(f"VALIDATING: {script.upper()} | Rows: {len(df)} | Cols: {len(df.columns)}")
    logger.info(f"{'='*60}")

    # Check if we have new scriptId-based structure and scriptId column
    if isinstance(metadata, dict) and 'scriptid' in df.columns:
        # NEW FORMAT: Split by scriptId and validate each subset
        logger.info(f"\nUsing scriptId-based validation")

        # Get unique scriptIds from dataframe
        unique_script_ids = df['scriptid'].dropna().unique()
        logger.info(f"Found {len(unique_script_ids)} unique scriptid(s): {unique_script_ids.tolist()}")

        # Validate each scriptId subset
        for script_id in unique_script_ids:
            script_id_str = str(script_id)
            subset_df = df[df['scriptid'] == script_id].copy()
            schema = metadata.get(script_id_str)

            if not schema:
                logger.warning(f"\n⚠ No metadata found for scriptid: {script_id_str} ({len(subset_df)} rows) - SKIPPING")
                continue

            logger.info(f"\n{'─'*60}")
            logger.info(f"Validating scriptid: {script_id_str} | {len(subset_df)} rows")
            logger.info(f"{'─'*60}")

            # Call the validation logic for this subset
            _validate_subset(subset_df, schema, script_id_str, logger, context)

        # Handle rows with NULL scriptId
        null_script_id_df = df[df['scriptid'].isna()]
        if not null_script_id_df.empty:
            logger.warning(f"\n⚠ {len(null_script_id_df)} rows have NULL scriptid - SKIPPING VALIDATION")
            if 'uid' in null_script_id_df.columns:
                sample_uids = null_script_id_df['uid'].dropna().head(3).tolist()
                logger.warning(f"   Sample UIDs: {sample_uids}")

        logger.info(f"\n{'='*60}")
        logger.info(f"COMPLETED: {script.upper()} | All scriptIds validated")
        logger.info(f"{'='*60}\n")
        return

    # LEGACY FORMAT or no scriptId column: Use existing validation
    if isinstance(metadata, dict):
        # Check if this is a flat field dict (legacy converted) or scriptId-based dict
        # Legacy converted format will have 'key' in the dict values
        first_key = next(iter(metadata.keys()))
        first_value = metadata[first_key]

        if isinstance(first_value, dict) and 'key' in first_value:
            # This is a legacy format converted to dict {fieldKey: field}
            logger.info(f"Using legacy validation format (converted from array)")
            schema = metadata
        else:
            # This is scriptId-based format but no scriptId column
            logger.warning(f"No scriptId column in dataframe - using first available schema")
            if len(metadata) == 1:
                schema = list(metadata.values())[0]
                logger.info(f"Using single available schema: {list(metadata.keys())[0]}")
            else:
                logger.warning(f"Multiple schemas available but no scriptId column - using first schema")
                schema = list(metadata.values())[0]
    else:
        logger.error(f"Unexpected metadata type: {type(metadata)}")
        return

    # Call validation logic for entire dataframe (legacy)
    _validate_subset(df, schema, script, logger, context)


def _validate_subset(df: pd.DataFrame, schema, script_or_id: str, logger, context):
    """
    Validate a single dataframe subset against its schema.

    This function contains the core validation logic extracted from validate_dataframe_with_ge.
    It can be called for the entire dataframe (legacy) or for scriptId subsets (new).

    Args:
        df: DataFrame to validate
        schema: Dict of {fieldKey: field} or list of field definitions (legacy)
        script_or_id: Script name or scriptId for logging
        logger: Logger instance
        context: Great Expectations context
    """
    errors = []
    warnings = []

    # Create validator
    validator = context.sources.pandas_default.read_dataframe(df)

    # Create field lookup dictionary
    # Handle both dict (new format) and list (legacy format)
    if isinstance(schema, dict):
        field_info = schema
    else:
        field_info = {f['key']: f for f in schema}

    # 1. VALIDATE UID COLUMN (CRITICAL)
    logger.info("\n[1] UID VALIDATION")

    # Scripts that allow multiple UIDs (e.g., review/follow-up scripts where multiple records per patient are expected)
    SCRIPTS_ALLOWING_MULTIPLE_UIDS = [
        'daily_review',
        'infections',
        'neolab'
    ]

    try:
        if 'uid' in df.columns:
            validator.expect_column_values_to_not_be_null(column="uid")

            # Determine if this script allows multiple UIDs
            script_name_lower = str(script_or_id).lower()
            allows_multiple_uids = any(allowed_script in script_name_lower for allowed_script in SCRIPTS_ALLOWING_MULTIPLE_UIDS)

            # Check for duplicate UIDs (only if script doesn't allow multiple UIDs)
            if not allows_multiple_uids:
                duplicate_uids = df[df.duplicated(subset=['uid'], keep=False)]
                if not duplicate_uids.empty:
                    dup_count = len(duplicate_uids)
                    unique_dup = duplicate_uids['uid'].nunique()
                    logger.error(f"❌ {dup_count} duplicate UID entries ({unique_dup} unique UIDs) | Samples: {duplicate_uids['uid'].unique()[:3].tolist()}")
                    errors.append(f"Duplicate UIDs found: {dup_count} rows")
                else:
                    logger.info("✓ All UIDs unique and non-null")
            else:
                # For scripts allowing multiple UIDs, just report stats
                unique_uids = df['uid'].nunique()
                total_rows = len(df)
                avg_records = total_rows / unique_uids if unique_uids > 0 else 0
                logger.info(f"✓ UIDs validated (multiple entries allowed) | {unique_uids} unique UIDs | {total_rows} total rows | Avg: {avg_records:.2f} records/UID")
        else:
            logger.error("❌ UID column missing from dataset")
            errors.append("UID column missing")
    except Exception as e:
        err_msg = f"Error validating 'uid' column: {str(e)}\n{traceback.format_exc()}"
        logger.error(err_msg)
        errors.append(err_msg)

    # 2. SENSITIVE/CONFIDENTIAL DATA CHECK
    logger.info("\n[2] SENSITIVE/CONFIDENTIAL DATA")

    # Known sensitive keywords (static list)
    drop_keywords = ['surname', 'firstname', 'dobtob', 'column_name', 'mothcell',
                     'dob.value', 'dob.label', 'kinaddress', 'kincell', 'kinname']

    found_sensitive_columns = []
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in drop_keywords:
            found_sensitive_columns.append(col)

    # Check for fields marked as confidential in schema (dynamic)
    confidential_fields_found = []
    # Handle both dict and list formats
    schema_fields = field_info.values() if isinstance(field_info, dict) else schema
    for field in schema_fields:
        field_key = field.get('key')
        is_confidential = field.get('confidential', False)

        if is_confidential:
            # Check if this field exists in the dataset
            value_col = f"{field_key}.value"
            label_col = f"{field_key}.label"

            if value_col in df.columns or label_col in df.columns:
                field_label = field.get('label', field_key)
                confidential_fields_found.append({
                    'key': field_key,
                    'label': field_label,
                    'has_value': value_col in df.columns,
                    'has_label': label_col in df.columns
                })

    # Report both known keywords and schema-based confidential fields together
    total_sensitive = len(found_sensitive_columns) + len(confidential_fields_found)

    if found_sensitive_columns:
        logger.error(f"❌ {len(found_sensitive_columns)} known sensitive column(s): {', '.join(found_sensitive_columns)}")
        warnings.append(f"Found {len(found_sensitive_columns)} sensitive/unwanted columns: {', '.join(found_sensitive_columns)}")

    if confidential_fields_found:
        logger.error(f"❌ {len(confidential_fields_found)} schema-based confidential field(s):")
        for field in confidential_fields_found[:3]:  # Show max 3
            columns = []
            if field['has_value']:
                columns.append(f"{field['key']}.value")
            if field['has_label']:
                columns.append(f"{field['key']}.label")

            # Show sample UIDs with data
            sample_info = ""
            if 'uid' in df.columns and field['has_value']:
                value_col = f"{field['key']}.value"
                non_null_mask = df[value_col].notna()
                if non_null_mask.sum() > 0:
                    sample_uids = get_safe_sample_uids(df, non_null_mask, 2)
                    sample_info = f" | UIDs: {sample_uids}"

            logger.error(f"   {field['key']} ({field['label']}): {', '.join(columns)}{sample_info}")

        if len(confidential_fields_found) > 3:
            logger.error(f"   ... and {len(confidential_fields_found) - 3} more")
        errors.append(f"Found {len(confidential_fields_found)} confidential fields in dataset")

    if total_sensitive == 0:
        logger.info("✓ No sensitive/confidential data detected")

    # ============================================================================
    # OPTIMIZED COMBINED VALIDATION LOOP
    # Combines required fields, value ranges, and data type validation in ONE pass
    # ============================================================================

    # Storage for results to be reported in sections
    required_results = []
    range_results = []
    type_results = []
    label_results = []

    # Single loop through all .value columns
    for value_col in [col for col in df.columns if col.endswith('.value')]:
        base_key = value_col[:-6]
        label_col = f"{base_key}.label"

        if base_key not in field_info:
            continue

        field = field_info[base_key]
        field_type = field.get('type', '')
        data_type = field.get('dataType', '')
        is_optional = field.get('optional', True)
        min_val = field.get('minValue')
        max_val = field.get('maxValue')
        expected_label = field.get('label')
        field_options = field.get('options', [])

        # --- REQUIRED FIELDS VALIDATION ---
        if not is_optional:
            # Check for NULL/empty values
            temp_series = (
                df[value_col]
                .astype(str)
                .replace(['nan', '<NA>', 'None', 'null', 'NAT', 'NaT'], '')
                .str.strip()
                .replace('', np.nan)
            )

            null_count = temp_series.isna().sum()
            if null_count > 0:
                null_pct = (null_count / len(df)) * 100
                null_mask = temp_series.isna()

                # Special handling: if the field being checked is 'uid', we can't show UIDs as samples
                # Instead, show unique_key or row indices
                if base_key.lower() == 'uid':
                    if 'unique_key' in df.columns:
                        sample_identifiers = df.loc[null_mask, 'unique_key'].head(2).tolist()
                    else:
                        # Fallback to row indices
                        sample_identifiers = df[null_mask].head(2).index.tolist()
                else:
                    sample_identifiers = get_safe_sample_uids(df, null_mask, 2)

                required_results.append({
                    'base_key': base_key,
                    'null_count': null_count,
                    'total_count': len(df),
                    'null_pct': null_pct,
                    'sample_identifiers': sample_identifiers,
                    'is_uid_field': base_key.lower() == 'uid'
                })

        # --- VALUE RANGE VALIDATION ---
        # Skip validation if both min and max are None or empty
        has_min = min_val is not None and str(min_val).strip() != ''
        has_max = max_val is not None and str(max_val).strip() != ''

        if has_min or has_max:
            non_null_mask = df[value_col].notna()
            non_null_values = df.loc[non_null_mask, value_col]

            if len(non_null_values) > 0:
                out_of_range_values = []
                for idx, val in non_null_values.items():
                    is_valid, error_msg = check_value_range(val, min_val, max_val, data_type)
                    if not is_valid:
                        uid = df.loc[idx, 'uid'] if 'uid' in df.columns else idx
                        out_of_range_values.append((idx, uid, val, error_msg))

                if out_of_range_values:
                    range_results.append({
                        'base_key': base_key,
                        'violations': out_of_range_values,
                        'total': len(non_null_values),
                        'min_val': min_val,
                        'max_val': max_val
                    })

        # --- DATA TYPE VALIDATION ---
        # Check for all NULL values (WARNING not ERROR)
        temp_base_series = (
            df[value_col]
            .astype(str)
            .replace(['nan', '<NA>', 'None', 'null', 'NAT'], '')
            .str.strip()
            .replace('', np.nan)
        )

        if temp_base_series.isna().all():
            warnings.append(f"Field '{base_key}' has all NULL values")
            continue

        # Validate based on data type
        try:
            if data_type in ['number', 'integer', 'float', 'timer']:
                numeric_regex = r"^\s*$|^-?\d+(\.\d+)?([eE][+-]?\d+)?$"
                result = validator.expect_column_values_to_match_regex(
                    column=value_col,
                    regex=numeric_regex,
                    mostly=1.0
                )

                if not result['success']:
                    invalid_count = result['result'].get('unexpected_count', 0)
                    try:
                        non_empty = df[value_col].astype(str).str.strip().replace('', np.nan).notna()
                        invalid_mask = non_empty & ~df[value_col].astype(str).str.match(numeric_regex, na=False)
                        invalid_samples = df.loc[invalid_mask, [value_col] + (['uid'] if 'uid' in df.columns else [])].head(2)

                        samples_list = []
                        if not invalid_samples.empty:
                            for idx, row in invalid_samples.iterrows():
                                uid_val = f"{row['uid']}={row[value_col]}" if 'uid' in invalid_samples.columns else row[value_col]
                                samples_list.append(uid_val)
                    except (ValueError, TypeError) as mask_error:
                        # Handle array comparison errors
                        samples_list = []
                        logging.warning(f"Could not extract samples for {base_key}: {str(mask_error)}")

                    type_results.append({
                        'base_key': base_key,
                        'invalid_count': invalid_count,
                        'samples': samples_list,
                        'error_type': 'non-numeric'
                    })

            elif data_type in ['datetime', 'timestamp', 'date']:
                # Allow dates with or without seconds: YYYY-MM-DD HH:MM or YYYY-MM-DD HH:MM:SS
                datetime_regex = r"^\s*$|^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:\d{2})?)?$"
                result = validator.expect_column_values_to_match_regex(
                    column=value_col,
                    regex=datetime_regex,
                    mostly=1.0
                )

                if not result['success']:
                    invalid_count = result['result'].get('unexpected_count', 0)
                    try:
                        non_empty = df[value_col].astype(str).str.strip().replace('', np.nan).notna()
                        invalid_mask = non_empty & ~df[value_col].astype(str).str.match(datetime_regex, na=False)
                        invalid_samples = df.loc[invalid_mask, [value_col] + (['uid'] if 'uid' in df.columns else [])].head(2)

                        samples_list = []
                        if not invalid_samples.empty:
                            for idx, row in invalid_samples.iterrows():
                                uid_val = f"{row['uid']}={row[value_col]}" if 'uid' in invalid_samples.columns else row[value_col]
                                samples_list.append(uid_val)
                    except (ValueError, TypeError) as mask_error:
                        # Handle array comparison errors
                        samples_list = []
                        logging.warning(f"Could not extract samples for {base_key}: {str(mask_error)}")

                    type_results.append({
                        'base_key': base_key,
                        'invalid_count': invalid_count,
                        'samples': samples_list,
                        'error_type': 'invalid datetime'
                    })

            elif data_type in ['boolean', 'yesno']:
                pattern = r"(?i)^\s*$|^(true|false|1|0|y|n|yes|no)$"
                result = validator.expect_column_values_to_match_regex(
                    column=value_col,
                    regex=pattern,
                    mostly=1.0
                )

                if not result['success']:
                    invalid_count = result['result'].get('unexpected_count', 0)
                    try:
                        non_empty = df[value_col].astype(str).str.strip().replace('', np.nan).notna()
                        invalid_mask = non_empty & ~df[value_col].astype(str).str.match(pattern, na=False)
                        invalid_samples = df.loc[invalid_mask, [value_col] + (['uid'] if 'uid' in df.columns else [])].head(2)

                        samples_list = []
                        if not invalid_samples.empty:
                            for idx, row in invalid_samples.iterrows():
                                uid_val = f"{row['uid']}={row[value_col]}" if 'uid' in invalid_samples.columns else row[value_col]
                                samples_list.append(uid_val)
                    except (ValueError, TypeError) as mask_error:
                        # Handle array comparison errors
                        samples_list = []
                        logging.warning(f"Could not extract samples for {base_key}: {str(mask_error)}")

                    type_results.append({
                        'base_key': base_key,
                        'invalid_count': invalid_count,
                        'samples': samples_list,
                        'error_type': 'invalid boolean'
                    })

            # --- LABEL VALIDATION ---
            if label_col in df.columns:
                # Only validate labels if options array is not empty AND field type is one of the select types
                if field_options and len(field_options) > 0 and field_type in ('single_select_option', 'dropdown', 'multi_select_option'):
                    # Build a mapping of value -> valueLabel from options
                    value_to_label = {str(opt.get('value', '')).strip(): str(opt.get('valueLabel', '')).strip()
                                     for opt in field_options if opt.get('value') is not None}

                    # Check each row's value-label pair
                    mismatched_rows = []
                    for idx in df.index:
                        row_value = df.loc[idx, value_col]
                        row_label = df.loc[idx, label_col]

                        # Skip if both are null/empty
                        if (pd.isna(row_value) or str(row_value).strip() == '') and \
                           (pd.isna(row_label) or str(row_label).strip() == ''):
                            continue

                        # Skip if value is null/empty
                        if pd.isna(row_value) or str(row_value).strip() == '':
                            continue

                        # Get expected label for this value
                        row_value_str = str(row_value).strip()
                        expected_label_for_value = value_to_label.get(row_value_str)

                        if expected_label_for_value is not None:
                            row_label_str = str(row_label).strip() if pd.notna(row_label) else ''
                            # Case-insensitive comparison
                            if row_label_str.lower() != expected_label_for_value.lower():
                                uid = df.loc[idx, 'uid'] if 'uid' in df.columns else idx
                                mismatched_rows.append({
                                    'uid': uid,
                                    'value': row_value_str,
                                    'actual_label': row_label_str,
                                    'expected_label': expected_label_for_value
                                })

                    if mismatched_rows:
                        label_results.append({
                            'base_key': base_key,
                            'mismatched_rows': mismatched_rows
                        })
                elif expected_label is not None:
                    # For fields without options, validate against expected_label
                    pattern = rf"(?i)^\s*$|^{re.escape(expected_label)}$"
                    result = validator.expect_column_values_to_match_regex(
                        column=label_col,
                        regex=pattern,
                        mostly=1.0
                    )

                    if not result['success']:
                        invalid_count = result['result'].get('unexpected_count', 0)
                        warnings.append(f"Field '{base_key}' label mismatch in {invalid_count} rows")

        except Exception as e:
            err_msg = f"Type validation failed for {base_key}: {str(e)}"
            errors.append(err_msg)
            type_results.append({
                'base_key': base_key,
                'error': err_msg
            })

    # ============================================================================
    # REPORT RESULTS IN STRUCTURED SECTIONS
    # ============================================================================

    # 3. REQUIRED FIELDS RESULTS
    logger.info("\n[3] REQUIRED FIELDS")
    if required_results:
        for result in required_results:
            # Determine the label for sample identifiers
            if result.get('is_uid_field', False):
                identifier_label = "unique_keys" if 'unique_key' in df.columns else "Row indices"
            else:
                identifier_label = "UIDs"

            logger.error(f"❌ '{result['base_key']}': {result['null_count']}/{result['total_count']} ({result['null_pct']:.1f}%) NULL | {identifier_label}: {result['sample_identifiers']}")
            errors.append(f"Required field '{result['base_key']}' has {result['null_count']} NULL values")
        logger.info(f"Summary: {len([r for r in required_results])} fields checked, {len(required_results)} with errors")
    else:
        # Count how many required fields were checked
        required_count = sum(1 for f in field_info.values() if not f.get('optional', True))
        if required_count > 0:
            logger.info(f"✓ All {required_count} required fields populated")

    # 4. VALUE RANGE RESULTS
    logger.info("\n[4] VALUE RANGES")
    if range_results:
        for result in range_results:
            violation_count = len(result['violations'])
            violation_pct = (violation_count / result['total']) * 100
            samples_str = ", ".join([f"UID:{uid}={val}" for _, uid, val, _ in result['violations'][:2]])
            logger.error(f"❌ '{result['base_key']}': {violation_count}/{result['total']} ({violation_pct:.1f}%) out of [{result['min_val']}, {result['max_val']}] | {samples_str}")
            errors.append(f"Field '{result['base_key']}': {violation_count} out-of-range values")
        logger.info(f"Summary: {len(range_results)} fields checked, {len(range_results)} with violations")
    else:
        # Count fields with actual (non-empty) min or max values
        range_count = sum(1 for f in field_info.values()
                         if (f.get('minValue') is not None and str(f.get('minValue')).strip() != '') or
                            (f.get('maxValue') is not None and str(f.get('maxValue')).strip() != ''))
        if range_count > 0:
            logger.info(f"✓ All {range_count} range-validated fields valid")

    # 5. DATA TYPE RESULTS
    logger.info("\n[5] DATA TYPES")
    type_errors_count = len(type_results) + len(label_results)

    for result in type_results:
        if 'error' in result:
            logger.error(f"❌ ERROR: {result['error']}")
        else:
            samples_str = f" | Samples: {result['samples']}" if result['samples'] else ""
            logger.error(f"❌ '{result['base_key']}': {result['invalid_count']} {result['error_type']} values{samples_str}")

    for result in label_results:
        mismatch_count = len(result['mismatched_rows'])
        samples = [f"{m['uid']}:val={m['value']}/lbl={m['actual_label']}" for m in result['mismatched_rows'][:2]]
        logger.error(f"❌ '{result['base_key']}': {mismatch_count} label mismatches | {samples}")
        errors.append(f"Field '{result['base_key']}': {mismatch_count} label mismatches")

    if type_errors_count == 0:
        logger.info(f"✓ All data types valid")
    else:
        logger.info(f"Summary: {type_errors_count} fields with errors")

    # 6. DATA QUALITY CHECKS
    logger.info("\n[6] DATA QUALITY")

    # 6.1 Completeness Check
    total_cells = df.shape[0] * df.shape[1]
    null_cells = df.isnull().sum().sum()
    completeness_pct = ((total_cells - null_cells) / total_cells) * 100
    logger.info(f"   Completeness: {completeness_pct:.2f}% ({total_cells - null_cells}/{total_cells} cells)")

    # Show columns with high NULL rates
    null_rates = (df.isnull().sum() / len(df)) * 100
    high_null_cols = null_rates[null_rates > 50].sort_values(ascending=False)
    if not high_null_cols.empty:
        logger.warning(f"⚠ {len(high_null_cols)} columns >50% NULL:")
        for col, rate in high_null_cols.head(5).items():
            logger.warning(f"   {col}: {rate:.1f}%")
        if len(high_null_cols) > 5:
            logger.warning(f"   ... and {len(high_null_cols) - 5} more")

    # 6.2 Consistency Checks (value-label pairs) - only for required fields
    inconsistencies = 0
    for value_col in [col for col in df.columns if col.endswith('.value')]:
        base_key = value_col[:-6]
        label_col = f"{base_key}.label"

        if label_col in df.columns and base_key in field_info:
            field = field_info[base_key]
            is_optional = field.get('optional', True)

            # Only check consistency for required fields
            if not is_optional:
                inconsistent_mask = df[value_col].isna() & df[label_col].notna()
                if inconsistent_mask.sum() > 0:
                    inconsistencies += 1
                    inconsistent_count = inconsistent_mask.sum()

                    # Special handling for UID field
                    if base_key.lower() == 'uid':
                        if 'unique_key' in df.columns:
                            sample_identifiers = df.loc[inconsistent_mask, 'unique_key'].head(2).tolist()
                            identifier_label = "unique_keys"
                        else:
                            sample_identifiers = df[inconsistent_mask].head(2).index.tolist()
                            identifier_label = "Row indices"
                    else:
                        sample_identifiers = get_safe_sample_uids(df, inconsistent_mask, 2)
                        identifier_label = "UIDs"

                    logger.error(f"❌ '{base_key}': {inconsistent_count} NULL value but non-NULL label | {identifier_label}: {sample_identifiers}")
                    errors.append(f"Required field '{base_key}' has {inconsistent_count} NULL values with non-NULL labels")

    if inconsistencies == 0:
        logger.info("   ✓ No value-label inconsistencies in required fields")
    else:
        logger.error(f"   ❌ {inconsistencies} required fields with inconsistencies")

    # 6.3 Outlier Detection (for numeric fields)
    outlier_fields = 0
    for value_col in [col for col in df.columns if col.endswith('.value')]:
        base_key = value_col[:-6]
        if base_key not in field_info:
            continue

        field = field_info[base_key]
        data_type = field.get('dataType', field.get('type', ''))

        if data_type in ['number', 'integer', 'float', 'timer']:
            try:
                numeric_values = pd.to_numeric(df[value_col], errors='coerce').dropna()
                if len(numeric_values) > 10:
                    Q1 = numeric_values.quantile(0.25)
                    Q3 = numeric_values.quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 3 * IQR
                    upper_bound = Q3 + 3 * IQR
                    outliers = numeric_values[(numeric_values < lower_bound) | (numeric_values > upper_bound)]

                    if len(outliers) > 0:
                        outlier_pct = (len(outliers) / len(numeric_values)) * 100
                        if outlier_pct > 5:
                            logger.warning(f"⚠ '{base_key}': {len(outliers)} ({outlier_pct:.1f}%) outliers | Range: [{lower_bound:.2f}, {upper_bound:.2f}]")
                            outlier_fields += 1
            except Exception:
                pass

    if outlier_fields == 0:
        logger.info("   ✓ No significant outliers")
    else:
        logger.info(f"   {outlier_fields} fields with outliers")

    # 6.4 Referential Integrity
    if 'uid' in df.columns:
        unique_uids = df['uid'].nunique()
        total_rows = len(df)
        avg_records_per_uid = total_rows / unique_uids if unique_uids > 0 else 0
        logger.info(f"   UIDs: {unique_uids} unique | {total_rows} total rows | Avg: {avg_records_per_uid:.2f} records/UID")

    # 7. FINAL SUMMARY
    logger.info(f"\n{'='*60}")
    logger.info(f"SUMMARY: {script_or_id} | Rows: {len(df)} | Cols: {len(df.columns)}")
    logger.info(f"Results: {len(errors)} errors, {len(warnings)} warnings")
    logger.info(f"{'='*60}")

    if errors:
        logger.error(f"❌ VALIDATION FAILED - {len(errors)} ERRORS")
        for i, error in enumerate(errors[:5], 1):  # Show first 5 errors
            logger.error(f"  {i}. {error}")
        if len(errors) > 5:
            logger.error(f"  ... and {len(errors) - 5} more")
    else:
        logger.info("✓ VALIDATION PASSED")

    if warnings:
        logger.warning(f"⚠ {len(warnings)} WARNINGS:")
        for i, warning in enumerate(warnings[:5], 1):  # Show first 5 warnings
            logger.warning(f"  {i}. {warning}")
        if len(warnings) > 5:
            logger.warning(f"  ... and {len(warnings) - 5} more")

    logger.info(f"{'='*60}\n")


def not_90_percent_similar_to_label(x, reference_value):
    """Check if value is less than 90% similar to reference."""
    if x is None:
        return True
    ratio = SequenceMatcher(None, str(x).lower(), str(reference_value).lower()).ratio()
    return ratio < 0.9


def send_log_via_email(log_file_path: str, email_receivers):
    """Send validation log via email with PDF attachment."""
    with open(log_file_path, 'r') as f:
        log_content = f.read()

    if 'ERROR' in log_content or "WARN" in log_content:
        MAIL_HOST = str('smtp.' + params['mail_host'])
        MAIL_USERNAME = params['MAIL_USERNAME'.lower()]
        MAIL_PASSWORD = params['MAIL_PASSWORD'.lower()]
        MAIL_FROM_ADDRESS = params['MAIL_FROM_ADDRESS'.lower()]
        country = params['country']

        pdf_options = {
            'page-size': 'A4',
            'margin-top': '10mm',
            'margin-right': '15mm',
            'margin-bottom': '10mm',
            'margin-left': '15mm',
            'encoding': "UTF-8",
            'no-outline': None,
            'enable-local-file-access': None,
        }

        msg = EmailMessage()
        msg['Subject'] = f'Data Validation Error Log - {country}'
        msg['From'] = MAIL_FROM_ADDRESS

        if isinstance(email_receivers, list):
            msg['To'] = ', '.join(email_receivers)
        else:
            msg['To'] = email_receivers

        html_body = get_html_validation_template(country, log_content)
        pdf_path = "/tmp/validation_log.pdf"

        try:
            pdfkit.from_string(html_body, pdf_path, options=pdf_options)
        except Exception as e:
            logging.error(f"Failed to create PDF: {str(e)}")
            return

        msg.set_content("Your VALIDATION LOG IS ATTACHED AS PDF.")
        msg.add_alternative(html_body, subtype='html')

        try:
            with open(pdf_path, 'rb') as f:
                msg.add_attachment(
                    f.read(),
                    maintype='application',
                    subtype='pdf',
                    filename='validation.pdf'
                )
        except Exception as e:
            logging.error(f"Failed to attach PDF: {str(e)}")

        try:
            with smtplib.SMTP(MAIL_HOST, 587) as server:
                server.starttls()
                server.login(MAIL_USERNAME, MAIL_PASSWORD)
                server.send_message(msg)
            logging.info("Error log emailed successfully.")
        except Exception as e:
            logging.error(f"Failed to send email: {str(e)}")
