from conf.base.catalog import env,cron_time,cron_log_file
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.queries.check_table_count_sql import table_data_count
from data_pipeline.pipelines.data_engineering.queries.create_summary_discharges_sql import summary_discharges_query
from conf.common.sql_functions import inject_sql
import logging
import sys

def create_summary_discharges():
    discharges_count = 0
    tble_exists = False
    try:
        tble_exists = (table_exists('derived','discharges')) ;
    
        if tble_exists:
                discharges_count = table_data_count('derived','discharges')

        if (discharges_count > 0):
            summary_discharges_script = summary_discharges_query()

            # Run  Summary Admissions Query
            inject_sql(summary_discharges_script, "create-summary-discharges")
        else:
            pass;

    except Exception as e:
        logging.error("!!! An error occured creating summary discharges: ")
        cron_log = open(cron_log_file,"a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Summary Discharges ".format(cron_time,env))
        cron_log.close()
        logging.error(e)
        sys.exit(1)
   