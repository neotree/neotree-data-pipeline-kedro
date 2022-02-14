from conf.base.catalog import catalog,cron_log_file
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.queries.check_table_count_sql import table_data_count
from data_pipeline.pipelines.data_engineering.queries.create_summary_discharges_sql import summary_discharges_query
from conf.common.sql_functions import inject_sql
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_discharges import mode,cron_time
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
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Summary Discharges ".format(cron_time,mode))
        cron_log.close()
        logging.error(e.with_traceback())
        sys.exit(1)
   