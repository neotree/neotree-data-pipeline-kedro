import os, sys
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
import logging
from conf.common.config import config
from pathlib import Path,PureWindowsPath
import time
from datetime import datetime


start = time.time()
params = config()
cron_time = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
mode = params['env']
#Not passing any Input To Allow Concurrent running of independent Nodes
def deduplicate_admissions():
    cwd = os.getcwd()
    try:
        file_name = Path(cwd+"/src/data_pipeline/pipelines/data_engineering/queries/1-deduplicate-admissions.sql");
        sql_file = open(file_name, "r")
        sql_script = sql_file.read()
        sql_file.close()
        inject_sql(sql_script, "deduplicate-admissions")
        #Add Return Value For Kedro Not To Throw Data Error
        return dict(
            status='Success',
            message = "Admissions Deduplication Complete"
        )

    except Exception as e:
        logging.error(
            "!!! An error occured deduplicating discharges: ")
        logging.error(formatError(e))
        #Only Open This File When Need Be To Write To It
        cron_log = open("/var/log/data_pipeline_cron.log","a+")
        #cron_log = open("C:\/Users\/morris\/Documents\/BRTI\/logs\/data_pipeline_cron.log","a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed Stage: Deduplicating Admissions ".format(cron_time,mode))
        cron_log.close()
        sys.exit(1)