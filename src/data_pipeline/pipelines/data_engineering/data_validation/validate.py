import os
import json
import pandas as pd
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
                    datetime_regex = r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$"
                    validator.expect_column_values_to_match_regex(value_col, datetime_regex)

                elif dtype == 'boolean':
                    validator.expect_column_values_to_be_of_type(value_col, 'bool')
                else:
                    validator.expect_column_values_to_be_of_type(value_col, 'object')
            else:
                continue
        except Exception as e:
            err_msg = f"Error validating column '{value_col}' {dtype}: {str(e)}\n"
            logger.error(err_msg)
            errors.append(err_msg)

    forbidden = ['who', 'when', 'where', 'is', '?', 'what', 'do', 'how', 'date', 'reason','readmission', 'did', 'which', 'if', 'age category', 'were']
    pattern = r"(?i)\b(" + "|".join(map(re.escape, forbidden)) + r")\b"
    escaped_values =['Chest is clear']

    for col in df.columns:
        if col.endswith(('.value', '.label')):
            try:
                # Skip if column is completely NaN
                if df[col].dropna().empty:
                    continue

                df[col] = df[col].astype(str).fillna("")

                # Skip if column not present in validator (sometimes true with ephemeral batches)
                logging.info(f"#####--RRREE---{validator.active_batch.data}")
                if col not in validator.active_batch.data:
                    logger.warning(f"Skipping {col} — not found in validator batch.")
                    continue

                # Only apply expectation if at least one non-null value
                validator.expect_column_values_to_not_match_regex(col, pattern)

                bad_vals = df[df[col].astype(str).str.contains(pattern, na=False, regex=True)]

                # Exclude escaped values
                bad_vals_filtered = bad_vals[~bad_vals[col].isin(escaped_values)]
                sample = bad_vals_filtered[col].dropna().head(3).tolist()

                if sample:
                    logger.error(f"Forbidden content in {col}: {sample}")

            except Exception as e:
                err_msg = f"Error applying content check to '{col}': {str(e)}\n{traceback.format_exc()}"
                logger.error(err_msg)
                errors.append(err_msg)
                logging.error(f"GE ERROR::{err_msg}")


    try:
        results = validator.validate()
        for result in results.get("results", []):
                expectation_type = result.get("expectation_config", {}).get("expectation_type")
                column = result.get("expectation_config", {}).get("kwargs", {}).get("column")
                success = result.get("success")

                if (not success and column in df.columns and ('expect_column_values_to_be_of_type' in expectation_type
                                                               or 'expect_column_values_to_match_regex' in expectation_type)):
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

        msg = EmailMessage()
        msg['Subject'] = 'Data Validation Error Log'
        msg['From'] = MAIL_FROM_ADDRESS
        
        if isinstance(email_receivers, list):
            msg['To'] = ', '.join(email_receivers)
        else:
            msg['To'] = email_receivers
        html_body = get_html_validation_template(country, log_content)
        pdf_path = "/tmp/validation_log.pdf"
        pdfkit.from_string(html_body, pdf_path)
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
