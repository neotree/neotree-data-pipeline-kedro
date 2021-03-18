import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql_procedure
from conf.common.format_error import formatError
from conf.common.config import config
from pathlib import Path,PureWindowsPath
from datetime import datetime
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time, start
import logging
import time

#Pass Convinience Views Output as it is the last step in the data pipeline process


cwd = os.getcwd()
logs_dir = str(cwd+"/logs")


#Prefered Log Var for Ubuntu
ubuntu_log_dir = "/var/log"
if Path(ubuntu_log_dir).exists():
    logs_dir = ubuntu_log_dir
    
cron_log_file = Path(logs_dir+'/data_pipeline_cron.log')
cron_log_file.touch(exist_ok=True);
cron_log = open(cron_log_file,"a+")
def grant_privileges(create_summary_counts_output):
    try:
        #Test If Previous Node Has Completed Successfully
        if create_summary_counts_output:
            
            file_name = Path(
            cwd+"/src/data_pipeline/pipelines/data_engineering/queries/3-grant-usage-on-tables.sql");

            sql_file = open(file_name, "r")
            sql_script = sql_file.read()
            sql_file.close()
            inject_sql_procedure(sql_script, "grant-usage-on-tables")
            
            end = time.time()
            execution_time = end-start
            execution_time_seconds = 0
            execution_time_minutes = 0
            if execution_time > 0:
                execution_time_minutes = round(execution_time//60)
                execution_time_seconds = round(execution_time % 60)
                cron_log.write("StartTime: {0}   Instance: {1}   Status: Success  ExecutionTime: {2} mins {3} seconds \n".format(cron_time,mode,execution_time_minutes,execution_time_seconds))
                cron_log.close()

            #Add Return Value For Kedro Not To Throw Data Error
            return dict(
            status='Success',
            message = "Granting Priviledges Complete"
            )
        else:
            logging.error(
                "Granting Priviledges Complete Did Not Execute To Completion")

            return None

    except Exception as e:
        logging.error(
            "!!! An error occured Granting Priviledges: ")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed Stage: Granting Privileges".format(cron_time,mode))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)