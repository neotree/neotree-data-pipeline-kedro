import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
from conf.base.catalog import cron_log_file,env,cron_time
from data_pipeline.pipelines.data_engineering.queries.create_summary_discharge_diagnosis_sql import summary_discharge_diagnosis_query


#Pass Join Table Output To Create Convenience View From Joined Tables
def create_summary_diagnosis(join_tables_output):    
    try:

        if env=='demo':
            return dict(
            status='Success',
            message = "Skippable Task"
            )
        
        #Test If Previous Node Has Completed Successfully
        elif join_tables_output is not None:
           
            sql_script = summary_discharge_diagnosis_query()
            inject_sql(sql_script, "create-summary-diagnosis")
            #Add Return Value For Kedro Not To Throw Data Error
            return dict(
            status='Success',
            message = "Creating Summary Diagnosis Complete"
            )
        else:
            logging.error(
                "Creating Summary Diagnosis Did Not Execute To Completion")
            return None

    except Exception as e:
        logging.error("!!! An error occured creating summary diagnosis: ")
        cron_log = open(cron_log_file,"a+")
        #cron_log = open("C:\/Users\/morris\/Documents\/BRTI\/logs\/data_pipeline_cron.log","a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Summary Diagnosis ".format(cron_time,env))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)