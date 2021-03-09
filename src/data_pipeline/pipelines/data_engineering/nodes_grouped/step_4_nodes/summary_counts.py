import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
from pathlib import Path,PureWindowsPath
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time

cwd = os.getcwd()

#Pass Convinience Views Output
def create_summary_counts(convinience_views_output):    
    try:
        #Test If Previous Node Has Completed Successfully
        if convinience_views_output is not None:
            file_name = Path(
             cwd+"/src/data_pipeline/pipelines/data_engineering/queries/create-summary-counts.sql");

            sql_file = open(file_name, "r")
            sql_script = sql_file.read()
            sql_file.close()
            inject_sql(sql_script, "create-summary-counts")
            #Add Return Value For Kedro Not To Throw Data Error
            return dict(
            status='Success',
            message = "Creating Summary counts Complete"
            )
        else:
            logging.error(
                "Creating Summary counts Did Not Execute To Completion")
            return None

    except Exception as e:
        logging.error("!!! An error occured creating summary counts: ")
        cron_log = open("/var/log/data_pipeline_cron.log","a+")
        #cron_log = open("C:\/Users\/morris\/Documents\/BRTI\/logs\/data_pipeline_cron.log","a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Summary Counts ".format(cron_time,mode))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)