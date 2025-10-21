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

    Validates:
    - Required fields (optional=false)
    - Value ranges (minValue, maxValue)
    - Data types
    - Data quality metrics
    """
    context = gx.get_context()
    errors = []
    warnings = []
    logger = setup_logger(log_file_path)
    schema = load_json_for_comparison(script)

    if not schema:
        logger.warning(f"##### SCHEMA FOR SCRIPT {script} NOT FOUND - SKIPPING VALIDATION")
        return

    logger.info(f"\n{'='*80}")
    logger.info(f"VALIDATING SCRIPT: {script.upper()}")
    logger.info(f"{'='*80}\n")
    logger.info(f"Total rows in dataset: {len(df)}")
    logger.info(f"Total columns: {len(df.columns)}\n")

    # Create validator
    validator = context.sources.pandas_default.read_dataframe(df)

    # ============================================================================
    # 1. VALIDATE UID COLUMN (CRITICAL)
    # ============================================================================
    logger.info("="*80)
    logger.info("1. UID VALIDATION")
    logger.info("="*80)

    try:
        if 'uid' in df.columns:
            validator.expect_column_values_to_not_be_null(column="uid")

            # Check for duplicate UIDs
            duplicate_uids = df[df.duplicated(subset=['uid'], keep=False)]
            if not duplicate_uids.empty:
                dup_count = len(duplicate_uids)
                unique_dup = duplicate_uids['uid'].nunique()
                logger.error(f"❌ ERROR: Found {dup_count} duplicate UID entries ({unique_dup} unique UIDs)")
                logger.error(f"   Sample duplicate UIDs: {duplicate_uids['uid'].unique()[:5].tolist()}")
                errors.append(f"Duplicate UIDs found: {dup_count} rows")
            else:
                logger.info("✓ UID column: All values are unique and non-null")
        else:
            logger.error("❌ ERROR: UID column is missing from dataset")
            errors.append("UID column missing")
    except Exception as e:
        err_msg = f"Error validating 'uid' column: {str(e)}\n{traceback.format_exc()}"
        logger.error(err_msg)
        errors.append(err_msg)

    # ============================================================================
    # 2. DROP KEYWORDS VALIDATION (SENSITIVE/UNWANTED COLUMNS)
    # ============================================================================
    logger.info(f"\n{'='*80}")
    logger.info("2. SENSITIVE/UNWANTED COLUMN NAMES CHECK")
    logger.info("="*80)

    drop_keywords = ['surname', 'firstname', 'dobtob', 'column_name', 'mothcell',
                     'dob.value', 'dob.label', 'kinaddress', 'kincell', 'kinname']

    found_sensitive_columns = []
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in drop_keywords:
            found_sensitive_columns.append(col)

    if found_sensitive_columns:
        logger.error(f"❌ CRITICAL WARNING: Found {len(found_sensitive_columns)} column(s) with sensitive/unwanted names!")
        logger.error(f"   These columns should be dropped from the dataset:")
        for col in found_sensitive_columns:
            logger.error(f"   - {col}")
        warnings.append(f"Found {len(found_sensitive_columns)} sensitive/unwanted columns: {', '.join(found_sensitive_columns)}")
    else:
        logger.info("✓ No sensitive/unwanted column names detected")

    # Create field lookup dictionary
    field_info = {f['key']: f for f in schema}

    # ============================================================================
    # 3. VALIDATE REQUIRED FIELDS (optional=false)
    # ============================================================================
    logger.info(f"\n{'='*80}")
    logger.info("3. REQUIRED FIELD VALIDATION")
    logger.info("="*80)

    required_fields_checked = 0
    required_field_errors = 0

    for value_col in [col for col in df.columns if col.endswith('.value')]:
        base_key = value_col[:-6]

        if base_key not in field_info:
            continue

        field = field_info[base_key]
        is_optional = field.get('optional', True)

        # Only validate if field is required (optional=false)
        if not is_optional:
            required_fields_checked += 1

            # Check for NULL/empty values
            temp_series = (
                df[value_col]
                .astype(str)
                .replace(['nan', '<NA>', 'None', 'null', 'NAT', 'NaT'], '')
                .str.strip()
                .replace('', np.nan)
            )

            null_count = temp_series.isna().sum()
            total_count = len(df)

            if null_count > 0:
                null_pct = (null_count / total_count) * 100
                logger.error(f"❌ ERROR: Required field '{base_key}' has {null_count}/{total_count} ({null_pct:.1f}%) NULL/empty values")
                errors.append(f"Required field '{base_key}' has {null_count} NULL values")
                required_field_errors += 1
            else:
                logger.info(f"✓ Required field '{base_key}': All values present")

    logger.info(f"\nRequired fields summary: {required_fields_checked} checked, {required_field_errors} with errors")

    # ============================================================================
    # 4. VALIDATE VALUE RANGES (minValue, maxValue)
    # ============================================================================
    logger.info(f"\n{'='*80}")
    logger.info("4. VALUE RANGE VALIDATION")
    logger.info("="*80)

    range_checks_performed = 0
    range_violations = 0

    for value_col in [col for col in df.columns if col.endswith('.value')]:
        base_key = value_col[:-6]

        if base_key not in field_info:
            continue

        field = field_info[base_key]
        min_val = field.get('minValue')
        max_val = field.get('maxValue')
        data_type = field.get('dataType', field.get('type', ''))

        # Only check if min or max value is specified
        if min_val is not None or max_val is not None:
            range_checks_performed += 1

            # Get non-null values
            non_null_mask = df[value_col].notna()
            non_null_values = df.loc[non_null_mask, value_col]

            if len(non_null_values) == 0:
                continue

            # Check each value
            out_of_range_values = []
            for idx, val in non_null_values.items():
                is_valid, error_msg = check_value_range(val, min_val, max_val, data_type)
                if not is_valid:
                    out_of_range_values.append((idx, val, error_msg))

            if out_of_range_values:
                violation_count = len(out_of_range_values)
                total = len(non_null_values)
                violation_pct = (violation_count / total) * 100

                logger.error(f"❌ ERROR: Field '{base_key}' has {violation_count}/{total} ({violation_pct:.1f}%) out-of-range values")
                logger.error(f"   Valid range: [{min_val}, {max_val}]")

                # Show sample violations
                sample_violations = out_of_range_values[:5]
                for idx, val, err_msg in sample_violations:
                    logger.error(f"   - Row {idx}: {err_msg}")

                errors.append(f"Field '{base_key}': {violation_count} out-of-range values")
                range_violations += 1
            else:
                logger.info(f"✓ Field '{base_key}': All values within range [{min_val}, {max_val}]")

    logger.info(f"\nRange validation summary: {range_checks_performed} fields checked, {range_violations} with violations")

    # ============================================================================
    # 5. DATA TYPE VALIDATION
    # ============================================================================
    logger.info(f"\n{'='*80}")
    logger.info("5. DATA TYPE VALIDATION")
    logger.info("="*80)

    type_checks_performed = 0
    type_errors = 0

    for value_col in [col for col in df.columns if col.endswith('.value')]:
        base_key = value_col[:-6]
        label_col = f"{base_key}.label"

        if base_key not in field_info:
            continue

        field = field_info[base_key]
        expected_label = field['label']
        field_type = field.get('type', '')
        data_type = field.get('dataType', '')

        type_checks_performed += 1

        try:
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
                logger.warning(f"⚠ WARNING: Field '{base_key}' has all NULL values")
                continue

            # Validate based on field type
            if data_type in ['number', 'integer', 'float', 'timer']:
                # Use GE to validate numeric format
                numeric_regex = r"^\s*$|^-?\d+(\.\d+)?([eE][+-]?\d+)?$"
                result = validator.expect_column_values_to_match_regex(
                    column=value_col,
                    regex=numeric_regex,
                    mostly=1.0
                )

                if not result['success']:
                    invalid_count = result['result'].get('unexpected_count', 0)
                    logger.error(f"❌ ERROR: Field '{base_key}' has {invalid_count} non-numeric values")
                    type_errors += 1

            elif data_type in ['datetime', 'timestamp', 'date']:
                # Validate datetime format (ISO 8601)
                datetime_regex = r"^\s*$|^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$"
                result = validator.expect_column_values_to_match_regex(
                    column=value_col,
                    regex=datetime_regex,
                    mostly=1.0
                )

                if not result['success']:
                    invalid_count = result['result'].get('unexpected_count', 0)
                    logger.error(f"❌ ERROR: Field '{base_key}' has {invalid_count} invalid datetime values")
                    type_errors += 1

            elif data_type in ['boolean', 'yesno']:
                # Validate boolean values
                pattern = r"^\s*$|^(?i)(true|false|1|0|y|n|yes|no)$"
                result = validator.expect_column_values_to_match_regex(
                    column=value_col,
                    regex=pattern,
                    mostly=1.0
                )

                if not result['success']:
                    invalid_count = result['result'].get('unexpected_count', 0)
                    logger.error(f"❌ ERROR: Field '{base_key}' has {invalid_count} invalid boolean values")
                    type_errors += 1

            # Validate label column matches expected label
            if expected_label is not None and label_col in df.columns:
                pattern = rf"^\s*$|^(?i){re.escape(expected_label)}$"
                result = validator.expect_column_values_to_match_regex(
                    column=label_col,
                    regex=pattern,
                    mostly=1.0
                )

                if not result['success']:
                    invalid_count = result['result'].get('unexpected_count', 0)
                    logger.warning(f"⚠ WARNING: Field '{base_key}' label mismatch in {invalid_count} rows")

        except Exception as e:
            err_msg = f"Type validation failed for {base_key}: {str(e)}"
            logger.error(f"❌ ERROR: {err_msg}")
            errors.append(err_msg)
            type_errors += 1

    logger.info(f"\nData type validation summary: {type_checks_performed} fields checked, {type_errors} with errors")

    # ============================================================================
    # 6. DATA QUALITY CHECKS
    # ============================================================================
    logger.info(f"\n{'='*80}")
    logger.info("6. COMPREHENSIVE DATA QUALITY CHECKS")
    logger.info("="*80)

    # 6.1 Completeness Check
    logger.info("\n6.1 Completeness Analysis:")
    total_cells = df.shape[0] * df.shape[1]
    null_cells = df.isnull().sum().sum()
    completeness_pct = ((total_cells - null_cells) / total_cells) * 100
    logger.info(f"   Overall completeness: {completeness_pct:.2f}%")
    logger.info(f"   Total cells: {total_cells}, Null cells: {null_cells}")

    # Show columns with high NULL rates
    null_rates = (df.isnull().sum() / len(df)) * 100
    high_null_cols = null_rates[null_rates > 50].sort_values(ascending=False)
    if not high_null_cols.empty:
        logger.warning(f"⚠ WARNING: {len(high_null_cols)} columns have >50% NULL values:")
        for col, rate in high_null_cols.head(10).items():
            logger.warning(f"   - {col}: {rate:.1f}% NULL")

    # 6.2 Consistency Checks
    logger.info("\n6.2 Consistency Checks:")

    # Check for inconsistent value-label pairs
    inconsistencies = 0
    for value_col in [col for col in df.columns if col.endswith('.value')]:
        base_key = value_col[:-6]
        label_col = f"{base_key}.label"

        if label_col in df.columns:
            # Find rows where value is null but label is not (inconsistent)
            inconsistent_mask = df[value_col].isna() & df[label_col].notna()
            if inconsistent_mask.sum() > 0:
                inconsistencies += 1
                logger.warning(f"⚠ WARNING: Field '{base_key}' has {inconsistent_mask.sum()} rows with NULL value but non-NULL label")

    if inconsistencies == 0:
        logger.info("   ✓ No value-label inconsistencies found")
    else:
        logger.warning(f"   ⚠ Found inconsistencies in {inconsistencies} fields")

    # 6.3 Outlier Detection (for numeric fields)
    logger.info("\n6.3 Outlier Detection (Numeric Fields):")
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

                if len(numeric_values) > 10:  # Only check if we have enough data
                    Q1 = numeric_values.quantile(0.25)
                    Q3 = numeric_values.quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 3 * IQR
                    upper_bound = Q3 + 3 * IQR

                    outliers = numeric_values[(numeric_values < lower_bound) | (numeric_values > upper_bound)]

                    if len(outliers) > 0:
                        outlier_pct = (len(outliers) / len(numeric_values)) * 100
                        if outlier_pct > 5:  # Only report if >5% are outliers
                            logger.warning(f"⚠ WARNING: Field '{base_key}' has {len(outliers)} ({outlier_pct:.1f}%) potential outliers")
                            logger.warning(f"   Expected range: [{lower_bound:.2f}, {upper_bound:.2f}]")
                            logger.warning(f"   Outlier examples: {outliers.head().tolist()}")
                            outlier_fields += 1
            except Exception as e:
                pass  # Skip fields that can't be converted to numeric

    if outlier_fields == 0:
        logger.info("   ✓ No significant outliers detected")
    else:
        logger.info(f"   Found potential outliers in {outlier_fields} fields")

    # 6.4 Referential Integrity (if applicable)
    logger.info("\n6.4 Referential Integrity:")

    # Check if UIDs exist across related tables (example check)
    if 'uid' in df.columns:
        unique_uids = df['uid'].nunique()
        total_rows = len(df)
        logger.info(f"   Unique UIDs: {unique_uids}")
        logger.info(f"   Total rows: {total_rows}")

        if unique_uids < total_rows:
            avg_records_per_uid = total_rows / unique_uids
            logger.info(f"   Average records per UID: {avg_records_per_uid:.2f}")

    # ============================================================================
    # 7. FINAL SUMMARY
    # ============================================================================
    logger.info(f"\n{'='*80}")
    logger.info("VALIDATION SUMMARY")
    logger.info("="*80)
    logger.info(f"Script: {script}")
    logger.info(f"Total Rows: {len(df)}")
    logger.info(f"Total Columns: {len(df.columns)}")
    logger.info(f"")
    logger.info(f"Validation Results:")
    logger.info(f"  - Errors: {len(errors)}")
    logger.info(f"  - Warnings: {len(warnings)}")
    logger.info(f"")

    if errors:
        logger.error(f"❌ VALIDATION FAILED - {len(errors)} ERRORS FOUND")
        logger.error("\nError Summary:")
        for i, error in enumerate(errors[:10], 1):  # Show first 10 errors
            logger.error(f"  {i}. {error}")
        if len(errors) > 10:
            logger.error(f"  ... and {len(errors) - 10} more errors")
    else:
        logger.info("✓ VALIDATION PASSED - NO ERRORS")

    if warnings:
        logger.warning(f"\n⚠ {len(warnings)} WARNINGS:")
        for i, warning in enumerate(warnings[:10], 1):  # Show first 10 warnings
            logger.warning(f"  {i}. {warning}")
        if len(warnings) > 10:
            logger.warning(f"  ... and {len(warnings) - 10} more warnings")

    logger.info(f"\n{'='*80}\n")


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
