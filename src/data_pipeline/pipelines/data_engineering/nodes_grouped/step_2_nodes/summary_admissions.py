from conf.base.catalog import catalog,cron_log_file
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.queries.check_table_count_sql import table_data_count
from data_pipeline.pipelines.data_engineering.queries.create_summary_admissions_sql import summary_admissions_query
from conf.common.sql_functions import inject_sql
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time
import logging
import sys

def create_summary_admissions():
    admissions_count = 0
    tble_exists = False
    try:
        tble_exists = (table_exists('derived','admissions')) ;
    
        if tble_exists:
                admissions_count = table_data_count('derived','admissions')

        if (admissions_count > 0):
            summary_admissions_script = summary_admissions_query()

            # Run  Summary Admissions Query
            inject_sql(summary_admissions_script, "create-summary-admissions")
        else:
            pass;

    except Exception as e:
        logging.error("!!! An error occured creating summary admissions: ")
        cron_log = open(cron_log_file,"a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Summary Admissions ".format(cron_time,mode))
        cron_log.close()
        logging.error(e)
        sys.exit(1)
   