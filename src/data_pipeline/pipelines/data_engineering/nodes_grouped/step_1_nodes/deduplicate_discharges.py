import os, sys
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
from conf.base.catalog import deduplicate_discharges,cron_log_file
import logging
from pathlib import Path,PureWindowsPath
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time

#Not passing any Input To Allow Concurrent running of independent Nodes
def deduplicate_discharges(data_import_output):
    try:
        if data_import_output is not None:
            sql_script = deduplicate_discharges
            inject_sql(sql_script, "deduplicate-discharges")
            #Add Return Value For Kedro Not To Throw Data Error And To Be Used As Input For Step 2
            return dict(
                status='Success',
                message = "Discharges Deduplication Complete"
            )
        else:
            logging.error(
                    "Data Importation Did Not Execute To Completion")
            return None
            
    except Exception as e:
        logging.error(
            "!!! An error occured deduplicating discharges: ")
        cron_log = open(cron_log_file,"a+")
        #cron_log = open("C:\/Users\/morris\/Documents\/BRTI\/logs\/data_pipeline_cron.log","a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed Stage: Deduplicating Discharges ".format(cron_time,mode))
        cron_log.close()
        logging.error(e.with_traceback())
        sys.exit(1)