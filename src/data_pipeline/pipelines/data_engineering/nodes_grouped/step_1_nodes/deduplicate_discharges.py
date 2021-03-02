import os, sys
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
import logging
from pathlib import Path,PureWindowsPath
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time

#Not passing any Input To Allow Concurrent running of independent Nodes
def deduplicate_discharges():
    cwd = os.getcwd()
    try:
        file_name = Path(cwd+ "/src/data_pipeline/pipelines/data_engineering/queries/1-deduplicate-discharges.sql");
        
        sql_file = open(file_name, "r")
        sql_script = sql_file.read()
        sql_file.close()
        inject_sql(sql_script, "deduplicate-discharges")
        #Add Return Value For Kedro Not To Throw Data Error And To Be Used As Input For Step 2
        return dict(
            status='Success',
            message = "Discharges Deduplication Complete"
        )
    except Exception as e:
        logging.error(
            "!!! An error occured deduplicating discharges: ")
        cron_log = open("/var/log/data_pipeline_cron.log","a+")
        #cron_log = open("C:\/Users\/morris\/Documents\/BRTI\/logs\/data_pipeline_cron.log","a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed Stage: Deduplicating Discharges ".format(cron_time,mode))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)