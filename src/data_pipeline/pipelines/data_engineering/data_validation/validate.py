import os
import json
import pandas as pd
import numpy as np
import great_expectations as gx
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


def validate_dataframe_with_ge(df: pd.DataFrame,script:str, log_file_path="logs/validation.log"):
    context = gx.get_context()
    errors = []
    logger =setup_logger(log_file_path)
    schema=get_schema(script)

    if not schema:
        logger.warning(f"#####SCHEMA FOR SCRIPT {script} NOT FOUND")
        return
    # Setup logging   
    logger.info(f" \n VALIDATING ::::::::::::::::{script}:::::::::::::::::::::::::::: \n")

    validator = context.sources.pandas_default.read_dataframe(df)
    try:
        validator.expect_column_values_to_not_be_null(column="uid")
    except Exception as e:
        err_msg = f"Error validating 'uid' column: {str(e)}\n{traceback.format_exc()}"
        logger.error(err_msg)
        errors.append(err_msg)

    for base_col, meta in schema.items():
        value_col = f"{base_col}.value"
        dtype = (meta.get('dataType') or '').lower()

        try:
            if value_col in df.columns:
                temp_base_series = (
                    df[col]
                    .astype(str)
                    .replace(['nan', '<NA>', 'None', 'null'], '')
                    .str.strip()
                    .replace('', np.nan) 
                )
                
                if temp_base_series.isna().all():
                    logger.info(f"Skipping {col} - all values are null/empty.")
                    continue
                if dtype == 'number':
                    temp_col = f"_{value_col}_as_number"
                    df[temp_col] = pd.to_numeric(df[value_col], errors='coerce')  # non-convertible → NaN
                    validator = context.sources.pandas_default.read_dataframe(df)
                    invalid_mask = (~df[value_col].isnull()) & (df[temp_col].isnull())
                    invalid_sample = df.loc[invalid_mask, value_col].dropna().unique()[:3].tolist()
                    if invalid_sample:
                        logger.error(f"❌ Column '{value_col}' has values that are not numeric or empty: {invalid_sample}")
                        errors.append(f"Non-numeric values in '{value_col}': {invalid_sample}")
                    df.drop(columns=[temp_col], inplace=True) 

                elif dtype in ['dropdown', 'single_select_option', 'period','multi_select_option','text','string','uid']:
                    validator.expect_column_values_to_be_of_type(value_col, 'object')
                elif dtype in ['datetime', 'timestamp', 'date']:
                    datetime_regex = r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$"
                    validator.expect_column_values_to_match_regex(value_col, datetime_regex)

                elif dtype == 'boolean':
                    validator.expect_column_values_to_be_in_set(
                    value_col,
                    ["True", "False", "true", "false", True, False, 1, 0,"y","n","Y","N","Yes","No","yes","no"]
)
                else:
                    validator.expect_column_values_to_be_of_type(value_col, 'object')
            else:
                continue
        except Exception as e:
            err_msg = f"Error validating column '{value_col}' {dtype}: {str(e)}\n"
            logger.error(err_msg)
            errors.append(err_msg)

    forbidden = ['who', 'when', 'where', 'is', '?', 'what', 'do', 'how', 'date', 'reason', 'readmission', 'did', 'which', 'if', 'age category', 'were']
    escaped_values = ['Chest is clear']
    pattern = r"(?i)\b(" + "|".join(map(re.escape, forbidden)) + r")\b"
    for col in df.columns:
        if col.endswith(('.value', '.label')):
            try:
                # Create a clean series with consistent null handling
                temp_series = (
                    df[col]
                    .astype(str)
                    .replace(['nan', '<NA>', 'None', 'null'], '')
                    .str.strip()
                    .replace('', np.nan) 
                )
                
                if temp_series.isna().all():
                    logger.info(f"Skipping {col} - all values are null/empty.")
                    continue
                    
                # Skip if column not in validator
                if col not in validator.columns():
                    logger.warning(f"Skipping {col} - not found in validator.")
                    continue

                content_result = validator.expect_column_values_to_not_match_regex(
                    column=col,
                    regex=pattern,
                    mostly=1.0
                )
                
                # Use the validation result to get problematic values
                if not content_result.success:
                    bad_count = content_result.result['unexpected_count']
                    bad_values = content_result.result['partial_unexpected_list'][:3]
                    logger.warning(
                        f"Content check failed for {col}: "
                        f"{bad_count} violations. Sample: {bad_values}"
                    )
                    
                    # Additional check to exclude escaped values
                    actual_bad_values = [
                        val for val in bad_values 
                        if val not in escaped_values
                    ]
                    if actual_bad_values:
                        logger.warning(
                            f"After escaping, found {len(actual_bad_values)} "
                            f"true violations in {col}. Sample: {actual_bad_values[:3]}"
                        )

            except Exception as e:
                logger.error(
                    f"Validation failed for {col}: {str(e)}\n"
                    f"Data sample: {temp_series.dropna().head(3).tolist()}"
                )
    try:
        results = validator.validate()
        for result in results.get("results", []):
                expectation_type = result.get("expectation_config", {}).get("expectation_type")
                column = result.get("expectation_config", {}).get("kwargs", {}).get("column")
                success = result.get("success")

                if (not success and column in df.columns and ('expect_column_values_to_be' in expectation_type
                                                               or 'expect_column_values_to_match' in expectation_type)):
                    unexpected_list = result.get("result", {}).get("partial_unexpected_list", [])
                    sample_vals = unexpected_list[:3]
                    msg = f"❌ Expectation failed for column '{column}' :: Sample invalid values: {sample_vals}"
                    logger.error(msg)
                    errors.append(msg)
    except Exception as e:
        err_msg = f"Validation execution error on : {str(e)}"
        logger.error(err_msg)
        errors.append(err_msg)

def send_log_via_email(log_file_path: str, email_receivers):  # type: ignore
    with open(log_file_path, 'r') as f:
        log_content = f.read()
    if 'ERROR' in log_content or "WARN" in log_content:
        MAIL_HOST = str('smtp.'+params['mail_host'])
        MAIL_USERNAME = params['MAIL_USERNAME'.lower()]
        MAIL_PASSWORD = params['MAIL_PASSWORD'.lower()]
        MAIL_FROM_ADDRESS = params['MAIL_FROM_ADDRESS'.lower()]
        country = params['country']

        pdf_options = {
            'page-size': 'A4',
            'margin-top': '10mm',
            'margin-right': '10mm',
            'margin-bottom': '10mm',
            'margin-left': '10mm',
            'encoding': "UTF-8",
            'no-outline': None,
            'dpi': 300,
            'enable-local-file-access': None, 
            'disable-smart-shrinking': ''
             
            # Required in some environments
}

        msg = EmailMessage()
        msg['Subject'] = 'Data Validation Error Log'
        msg['From'] = MAIL_FROM_ADDRESS
        
        if isinstance(email_receivers, list):
            msg['To'] = ', '.join(email_receivers)
        else:
            msg['To'] = email_receivers
        html_body = get_html_validation_template(country, log_content)
        pdf_path = "/tmp/validation_log.pdf"
        pdfkit.from_string(html_body, pdf_path,options=pdf_options)
        msg.set_content("Your VALDATION LOG IS ATTACHED AS PDF.")
        msg.add_alternative(html_body, subtype='html')
        try:
            with open(pdf_path, 'rb') as f:
                msg.add_attachment(f.read(), maintype='application', subtype='pdf', filename='validation.pdf')

        except:
            logging.error(f">>>>>>>>>Failed TO ATTACH LOG PDF: {str(e)}") 
        
        try:
            with smtplib.SMTP(MAIL_HOST, 587) as server:
                server.starttls()
                server.login(MAIL_USERNAME, MAIL_PASSWORD)
                server.send_message(msg)
            logging.info("Error log emailed successfully.")
        except Exception as e:
            logging.error(f"Failed to send email: {str(e)}")

def get_schema(script: str):
    hospital_scripts = hospital_conf()
    merged_script_data = None

    if not hospital_scripts:
        return None

    for hospital in hospital_scripts:
        script_id_entry = hospital_scripts[hospital].get(script, '')
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
