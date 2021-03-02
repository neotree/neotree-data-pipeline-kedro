import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
from pathlib import Path,PureWindowsPath
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time




#Passing Step 2 Output So That It can Wait For Step 2 Data Tyding To Happen
def manually_fix_admissions(tidy_data_output):
    cwd = os.getcwd()
    try:
        #Test If Previous Node Has Completed Successfully
        if tidy_data_output is not None:
            file_name = Path(cwd+"/src/data_pipeline/pipelines/data_engineering/queries/2a-admissions-manually-fix-records.sql");
       
            sql_file = open(file_name, "r")
            sql_script = sql_file.read()
            sql_file.close()
            inject_sql(sql_script, "manually-fix-admissions")
            #Add Return Value For Kedro Not To Throw Data Error
            return dict(
            status='Success',
            message = "Manual Fixing Of Admissions Complete"
            )
        else:
            logging.error(
                "Manual Fixing Of Admissions Did Not Execute To Completion")
            return None

    except Exception as e:
        logging.error(
            "!!! An error occured manually fixing admissions: ")
        cron_log = open("/var/log/data_pipeline_cron.log","a+")
        #cron_log = open("C:\/Users\/morris\/Documents\/BRTI\/logs\/data_pipeline_cron.log","a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed Stage: Manually Fixing Admissions ".format(cron_time,mode))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)