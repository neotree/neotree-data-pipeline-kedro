import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql_procedure
from conf.common.format_error import formatError
from conf.base.catalog import cron_log_file,cron_time,start,env
from data_pipeline.pipelines.data_engineering.queries.grant_usage_on_tables_sql import grant_usage_query 
import logging
import time

#This file calls the query to grant privileges to users on the generated tables
cron_log = open(cron_log_file,"a+")
def grant_privileges(create_summary_counts_output):
    try:
        #Test If Previous Node Has Completed Successfully
        if create_summary_counts_output:
         
            return dict(
            status='Success',
            message = "Data Cleanup Complete"
            )
        else:
            logging.error(
                "Granting Priviledges Complete Did Not Execute To Completion")

            return None

    except Exception as e:
        logging.error(
            "!!! An error occured Granting Priviledges: ")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed Stage: Granting Privileges".format(cron_time,env))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)