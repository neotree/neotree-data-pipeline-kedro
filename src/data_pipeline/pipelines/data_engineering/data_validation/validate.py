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
from conf.common.scripts import get_script,merge_script_data
from conf.common.logger import setup_logger
from typing import  Dict
from datetime import datetime
from conf.base.catalog import params,hospital_conf
import re
import pdfkit
from data_pipeline.pipelines.data_engineering.utils.field_info import load_json_for_comparison
from difflib import SequenceMatcher

STATUS_FILE = "logs/validation_status.json"


def set_status(status: str):
    with open(STATUS_FILE, "w") as f:
        json.dump({"status": status,"time":datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, f)


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
        log_file_path="logs/validation.log"
        if 'mail_receivers' in params:
            email_recipients= params['MAIL_RECEIVERS'.lower()]
            if email_recipients:
                send_log_via_email(log_file_path,email_receivers=email_recipients)


def validate_dataframe_with_ge(df: pd.DataFrame, script: str, log_file_path="logs/validation.log"):
    """
    Validate DataFrame using Great Expectations with proper data type validation
    and sample invalid values logging.
    """
    context = gx.get_context()
    errors = []
    logger = setup_logger(log_file_path)
    schema = load_json_for_comparison(script)

    if not schema:
        logger.warning(f"##### SCHEMA FOR SCRIPT {script} NOT FOUND")
        return

    logger.info(f"\n VALIDATING ::::::::::::::::{script}:::::::::::::::::::::::::::: \n")

    # Create validator
    validator = context.sources.pandas_default.read_dataframe(df)
    
    # Validate UID column
    try:
        validator.expect_column_values_to_not_be_null(column="uid")
    except Exception as e:
        err_msg = f"Error validating 'uid' column: {str(e)}\n{traceback.format_exc()}"
        logger.error(err_msg)
        errors.append(err_msg)

    # Create field lookup dictionary
    field_info = {f['key']: f for f in schema}

    # Process each .value column
    for value_col in [col for col in df.columns if col.endswith('.value')]:
        base_key = value_col[:-6]
        label_col = f"{base_key}.label"
        
        if base_key not in field_info:
            continue
            
        field = field_info[base_key]
        expected_label = field['label']
        field_type = field.get('type', '')

        try:
            # Check for all NULL values
            temp_base_series = (
                df[value_col]
                .astype(str)
                .replace(['nan', '<NA>', 'None', 'null', 'NAT'], '')
                .str.strip()
                .replace('', np.nan)
            )
            
            if temp_base_series.isna().all():
                logger.warning(f"Column '{base_key}' of {script} script has all NULL values")

            # Validate based on field type
            if field_type == 'number':
                # Use GE to validate numeric format
                numeric_regex = r"^\s*$|^-?\d+(\.\d+)?([eE][+-]?\d+)?$"
                validator.expect_column_values_to_match_regex(
                    column=value_col,
                    regex=numeric_regex,
                    mostly=1.0  # Expect all non-null values to match
                )
                
                # Convert to numeric (will be validated in results section)
                df[value_col] = pd.to_numeric(df[value_col], errors='coerce')

            elif field_type in ['dropdown', 'single_select', 'period', 'multi_select', 'text', 'string', 'uid']:
                # Validate string/object type
                validator.expect_column_values_to_be_of_type(
                    column=value_col,
                    type_='object'
                )

            elif field_type in ['datetime', 'timestamp', 'date']:
                # Validate datetime format (ISO 8601)
                datetime_regex = r"^\s*$|^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$"
                validator.expect_column_values_to_match_regex(
                    column=value_col,
                    regex=datetime_regex,
                    mostly=1.0
                )

            elif field_type in ['boolean', 'yesno']:
                # Validate boolean values
                accepted_values = [
                    "True", "False", "true", "false",
                    "1", "0",
                    "y", "n", "Y", "N",
                    "Yes", "No", "yes", "no"
                ]
                # Create case-insensitive pattern
                pattern = r"^\s*$|^(?i)(true|false|1|0|y|n|yes|no)$"
                validator.expect_column_values_to_match_regex(
                    column=value_col,
                    regex=pattern,
                    mostly=1.0
                )

            else:
                # Default to object type for unknown types
                validator.expect_column_values_to_be_of_type(
                    column=value_col,
                    type_='object'
                )

            # Validate label column matches expected label (CORRECTED)
            if expected_label is not None and label_col in df.columns:
                # Use exact match with case-insensitive flag
                pattern = rf"^\s*$|^(?i){re.escape(expected_label)}$"
                validator.expect_column_values_to_match_regex(
                    column=label_col,
                    regex=pattern,
                    mostly=1.0
                )

        except Exception as e:
            err_msg = f"Validation setup failed for {base_key} of {script} script: {str(e)}\n{traceback.format_exc()}"
            logger.error(err_msg)
            errors.append(err_msg)

    # Execute all validations and process results
    try:
        results = validator.validate()
        
        # Parse results if string
        if isinstance(results, str):
            try:
                results = json.loads(results)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse validation results as JSON: {str(e)}")
                return errors

        if results is not None and isinstance(results, dict):
            validation_results = results.get("results", [])
            
            for result in validation_results:
                if not isinstance(result, dict):
                    continue
                
                # Extract expectation details
                expectation_config = result.get("expectation_config", {})
                column = expectation_config.get("kwargs", {}).get("column")
                expectation_type = expectation_config.get("expectation_type", "")
                success = result.get("success", True)
                
                # Log failures with sample invalid values
                if not success and column:
                    result_details = result.get("result", {})
                    
                    # Get sample invalid values
                    unexpected_list = result_details.get("partial_unexpected_list", [])
                    if unexpected_list is None:
                        unexpected_list = []
                    
                    unexpected_count = result_details.get("unexpected_count")
                    if unexpected_count is None:
                        unexpected_count = len(unexpected_list) if isinstance(unexpected_list, list) else 0
                    
                    element_count = result_details.get("element_count", 0)
                    
                    # Get up to 5 sample values
                    sample_vals = unexpected_list[:5] if isinstance(unexpected_list, list) else []
                    
                    # Create detailed error message
                    msg = (
                        f"❌ Validation failed for column '{column}'\n"
                        f"   Expectation: {expectation_type}\n"
                        f"   Invalid values: {unexpected_count} out of {element_count} rows\n"
                        f"   Sample invalid values: {sample_vals}"
                    )
                    
                    logger.error(msg)
                    errors.append(msg)
                    
                    # Additional context for specific expectation types
                    if expectation_type == "expect_column_values_to_match_regex":
                        regex_pattern = expectation_config.get("kwargs", {}).get("regex", "")
                        logger.error(f"   Expected pattern: {regex_pattern}")
                    
                    elif expectation_type == "expect_column_values_to_be_of_type":
                        expected_type = expectation_config.get("kwargs", {}).get("type_", "")
                        logger.error(f"   Expected type: {expected_type}")

            # Log validation summary
            total_expectations = len(validation_results)
            failed_expectations = sum(1 for r in validation_results if not r.get("success", True))
            success_rate = ((total_expectations - failed_expectations) / total_expectations * 100) if total_expectations > 0 else 0
            
            logger.info(f"\nValidation Summary for {script}:")
            logger.info(f"  Total expectations: {total_expectations}")
            logger.info(f"  Failed expectations: {failed_expectations}")
            logger.info(f"  Success rate: {success_rate:.2f}%")

    except Exception as e:
        err_msg = f"Validation execution error: {str(e)}\n{traceback.format_exc()}"
        logger.error(err_msg)
        errors.append(err_msg)

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
