import logging
import sys,os
sys.path.insert(1,os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
from pathlib import Path,PureWindowsPath
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time



cwd = os.getcwd()
#Passing The Same Input with Manually Fixing Admissions To Allow For Concurrency
def manually_fix_discharges(tidy_data_output):
    try:
        #Test If Previous Node Has Completed Successfully
        if tidy_data_output is not None:
            file_name = Path(
            cwd+"/src/data_pipeline/pipelines/data_engineering/queries/2b-discharges-manually-fix-records.sql");
        
            sql_file = open(file_name, "r")
            sql_script = sql_file.read()
            sql_file.close()
            inject_sql(sql_script, "manually-fix-discharges")
            #Add Return Value For Kedro Not To Throw Data Error
            return dict(
            status='Success',
            message = "Manual Fixing Of Discharges Complete"
            )
        else:
            logging.error(
                "Manual Fixing Of Disharges Did Not Execute To Completion")
            return None

    except Exception as e:
        logging.error(
            "!!! An error occured manually fixing discharges: ")
        cron_log = open("/var/log/data_pipeline_cron.log","a+")
        #cron_log = open("C:\/Users\/morris\/Documents\/BRTI\/logs\/data_pipeline_cron.log","a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed Stage: Manually Fixing Discharges".format(cron_time,mode))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)