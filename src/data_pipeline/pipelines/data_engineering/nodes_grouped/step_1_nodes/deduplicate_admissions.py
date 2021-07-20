from conf.base.catalog import deduplicate_admissions_query, params, cron_log_file
from pathlib import Path, PureWindowsPath
from datetime import datetime
import time
import logging
from conf.common.format_error import formatError
from conf.common.sql_functions import inject_sql
import os
import sys
sys.path.append(os.getcwd())


start = time.time()
cron_time = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
mode = params['env']
# Not passing any Input To Allow Concurrent running of independent Nodes


def deduplicate_admissions(data_import_output):
    try:
        if data_import_output is not None:
            sql_script = deduplicate_admissions_query
            inject_sql(sql_script, "deduplicate-admissions")
            # Add Return Value For Kedro Not To Throw Data Error
            return dict(
                status='Success',
                message="Admissions Deduplication Complete"
            )
        else:
            logging.error(
                "Data Importation Did Not Execute To Completion")
            return None

    except Exception as e:
        logging.error(
            "!!! An error occured deduplicating discharges: ")
        logging.error(e.with_traceback())
        # Only Open This File When Need Be To Write To It
        cron_log = open(cron_log_file, "a+")
        cron_log.write(
            "StartTime: {0}   Instance: {1}   Status: Failed Stage: Deduplicating Admissions ".format(cron_time, mode))
        cron_log.close()
        sys.exit(1)
