import os
import json
import pandas as pd
import great_expectations as gx
import smtplib
from email.message import EmailMessage
import logging
import traceback
from .templates import get_html_validation_template
from typing import  Dict
from datetime import datetime
from conf.base.catalog import params
import re

STATUS_FILE = "./logs/validation_status.json"


def set_status(status: str):
    with open(STATUS_FILE, "w") as f:
        json.dump({"status": status,"time":datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, f)


def get_status():
    if not os.path.exists(STATUS_FILE):
        return "unknown"
    with open(STATUS_FILE, "r") as f:
        return json.load(f).get("status")


def reset_log(log_file_path="./logs/validation.log"):
    with open(log_file_path, "w") as f:
        f.write("")


def begin_validation_run(log_file_path="./logs/validation.log"):
    set_status("running")
    reset_log(log_file_path)


def finalize_validation():
    if get_status() == "running":
        set_status("done")
        country = params['country']
        log_file_path="./logs/validation.log"
        email_recipients= params["MAIL_RECEIVERS"]
        if email_recipients:
            send_log_via_email(log_file_path, country,email_recipients)


def validate_dataframe_with_ge(df: pd.DataFrame,script:str,schema: Dict[str, Dict[str, str]], log_file_path="./logs/validation.log"):
    context = gx.get_context()
    errors = []

    # Setup logging
    logging.basicConfig(filename=log_file_path, level=logging.INFO, filemode='a')
    logger = logging.getLogger()
    logger.info(f" \n VALIDATING ::::::::::::::::{script} \n")
    suite_name = "dynamic_expectation_suite"
    context.create_expectation_suite(suite_name, overwrite_existing=True)
    validator = context.sources.pandas_default.read_dataframe(df)

    try:
        validator.expect_column_values_to_not_be_null(column="uid")
    except Exception as e:
        err_msg = f"Error validating 'uid' column: {str(e)}\n{traceback.format_exc()}"
        logger.error(err_msg)
        errors.append(err_msg)

    for base_col in schema.items():
        value_col = f"{base_col}.value"
        meta = schema.get(base_col)
        dtype = (meta.get('dataType') or '').lower()

        try:
            if value_col in validator.columns:
                if dtype in ['dropdown', 'single_select_option', 'period','multi_select_option']:
                    validator.expect_column_values_to_be_of_type(value_col, 'object')
                elif dtype in ['datetime', 'timestamp', 'date']:
                    datetime_regex = r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$"
                    validator.expect_column_values_to_match_regex(value_col, datetime_regex)
                elif dtype == 'number':
                    validator.expect_column_values_to_be_in_type_list(value_col, ['int64', 'float64'])
                else:
                    logger.warning(f"No rule for type '{dtype}' for column '{value_col}'")
            else:
                logger.warning(f"Column '{value_col}' not found in dataframe.")
        except Exception as e:
            err_msg = f"Error validating column '{value_col}': {str(e)}\n{traceback.format_exc()}"
            logger.error(err_msg)
            errors.append(err_msg)

    forbidden = ['who', 'when', 'where', 'was', 'is', '?', 'what', 'do', 'how', 'date', 'reason','readmission', 'did', 'which', 'if', 'age category', 'were']
    pattern = r"(?i)\b(" + "|".join(map(re.escape, forbidden)) + r")\b"


    for col in validator.columns:
        if col.endswith(('.value', '.label')):
            try:
                validator.expect_column_values_to_not_match_regex(col, pattern)
                if col in df.columns:
                    bad_vals = df[df[col].astype(str).str.contains(pattern, na=False, regex=True)]
                    sample = bad_vals[col].dropna().head(3).tolist()
                    if sample:
                        logger.error(f"Forbidden content in {col}: {sample}")
            except Exception as e:
                err_msg = f"Error applying content check to '{col}': {str(e)}\n{traceback.format_exc()}"
                logger.error(err_msg)
                errors.append(err_msg)

    try:
        results = validator.validate()
        for result in results.get("results", []):
                expectation_type = result.get("expectation_config", {}).get("expectation_type")
                column = result.get("expectation_config", {}).get("kwargs", {}).get("column")
                success = result.get("success")

                if not success and column in df.columns:
                    unexpected_list = result.get("result", {}).get("partial_unexpected_list", [])
                    sample_vals = unexpected_list[:3]
                    msg = f"❌ Expectation failed for column '{column}' (type: {expectation_type}). Sample invalid values: {sample_vals}"
                    logger.error(msg)
                    errors.append(msg)
    except Exception as e:
        err_msg = f"Validation execution error: {str(e)}\n{traceback.format_exc()}"
        logger.error(err_msg)
        errors.append(err_msg)

def send_log_via_email(log_file_path: str, country: str, email_receivers):  # type: ignore
    with open(log_file_path, 'r') as f:
        log_content = f.read()

    MAIL_HOST = params['MAIL_HOST']
    MAIL_PORT = params['MAIL_PORT']
    MAIL_USERNAME = params['MAIL_USERNAME']
    MAIL_PASSWORD = params['MAIL_PASSWORD']
    MAIL_FROM_ADDRESS = params['MAIL_FROM_ADDRESS']
    MAIL_FROM_NAME = "NeoTree"

    msg = EmailMessage()
    msg['Subject'] = 'Data Validation Error Log'
    msg['From'] = f"{MAIL_FROM_NAME} <{MAIL_FROM_ADDRESS}>"
    
    if isinstance(email_receivers, list):
        msg['To'] = ', '.join(email_receivers)
    else:
        msg['To'] = email_receivers

    html_body = get_html_validation_template(country, log_content)
    msg.set_content("Validation errors occurred. See the HTML version.")
    msg.add_alternative(html_body, subtype='html')

    try:
        with smtplib.SMTP(MAIL_HOST, int(MAIL_PORT)) as server:
            server.starttls()
            server.login(MAIL_USERNAME, MAIL_PASSWORD)
            server.send_message(msg)
        logging.info("Error log emailed successfully.")
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")

