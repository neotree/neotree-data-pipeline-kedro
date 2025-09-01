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


def validate_dataframe_with_ge(df: pd.DataFrame,script:str, log_file_path="logs/validation.log"):
    context = gx.get_context()
    errors = []
    logger =setup_logger(log_file_path)
    schema=load_json_for_comparison(script)

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

    field_info = {f['key']: f for f in schema}

    for value_col in [col for col in df.columns if col.endswith('.value')]:
        base_key = value_col[:-6]  
        label_col = f"{base_key}.label"
        if  base_key not in field_info:
            pass;
        else:
            field = field_info[base_key]
            expected_label = field['label']
            field_type = field.get('type', '')
            try:
                temp_base_series = (
                    df[value_col]
                    .astype(str)
                    .replace(['nan', '<NA>', 'None', 'null','NAT'], '')
                    .str.strip()
                    .replace('', np.nan) 
                )
                if temp_base_series.isna().all():
                    logger.warning(f"Column '{base_key}' of {script} script is showing all NULL values")


                if field_type == 'number':
                    temp_col = f"_{value_col}_as_number"
                    df[temp_col] = pd.to_numeric(df[value_col], errors='coerce')  # non-convertible → NaN
                    invalid_mask = df[temp_col].isnull()
                    invalid_sample = df.loc[invalid_mask, value_col].unique()[:5].tolist()    
                    if invalid_sample:
                        logger.error(f"❌ Column '{value_col}' has values that are not numeric: {invalid_sample}")
                        errors.append(f"Non-numeric values in '{value_col}': {invalid_sample}")        
                    df.drop(columns=[temp_col], inplace=True)

                elif field_type in ['dropdown', 'single_select', 'period','multi_select','text','string','uid']:
                    validator.expect_column_values_to_be_of_type(value_col, 'object')
                elif field_type in ['datetime', 'timestamp', 'date']:
                    datetime_regex = r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$"
                    validator.expect_column_values_to_match_regex(value_col, datetime_regex)
                elif field_type == 'boolean' or field_type=='yesno':
                    accepted_values = [
                            "True", "False", "true", "false",
                            "1", "0",
                            "y", "n", "Y", "N",
                            "Yes", "No", "yes", "no"
                            ]
                    values = set(str(v).lower() for v in accepted_values)
                    pattern = r"^(?i)(" + "|".join(map(re.escape, values)) + r")$"
                    validator.expect_column_values_to_match_regex(
                    column=value_col,
                    regex=pattern,
                    mostly=1.0
                    )
                else:
                    validator.expect_column_values_to_be_of_type(value_col, 'object') 

                if expected_label is not None and label_col in df.columns:
                   pattern = rf"(?i)^{re.escape(expected_label)}$"
                   validator.expect_column_values_to_not_match_regex(
                   column=label_col,
                   regex=pattern,
                   mostly=1.0
                   )
                 
            except Exception as e:
                logger.error(
                    f"Validation failed for {base_key} of {script} script: {str(e)}\n")
                    
    try:
        results = validator.validate()
        for result in results.get("results", []):
                column = result.get("expectation_config", {}).get("kwargs", {}).get("column")
                success = result.get("success")

                if (not success and column in df.columns):
                    unexpected_list = result.get("result", {}).get("partial_unexpected_list", [])
                    sample_vals = unexpected_list[:3]
                    msg = f"❌ Expectation failed for column '{column}'.I got {len(unexpected_list)} invalid values :: Sample invalid values: {sample_vals}"
                    logger.error(msg)
                    errors.append(msg)
    except Exception as e:
        err_msg = f"Validation execution error on : {str(e)}"
        logger.error(err_msg)
        errors.append(err_msg)

def not_90_percent_similar_to_label(x, reference_value):
    if x is None:
        return True
    ratio = SequenceMatcher(None, str(x).lower(), str(reference_value).lower()).ratio()
    return ratio < 0.9

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
            'margin-right': '15mm',
            'margin-bottom': '10mm',
            'margin-left': '15mm',
            'encoding': "UTF-8",
            'no-outline': None,
            'enable-local-file-access': None,
             
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


