from conf.base.catalog import catalog,cron_log_file
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.queries.check_table_count_sql import table_data_count
from data_pipeline.pipelines.data_engineering.queries.create_summary_joined_vitals_sql import summary_joined_vitals_query
from conf.common.sql_functions import inject_sql
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time
import logging
import sys

def create_summary_joined_vitalsigns():
    vital_signs_count = 0
    tble_exists = False
    try:
        tble_exists = (table_exists('derived','vitalsigns') and table_exists('derived','summary_day1_vitals') 
                      and table_exists('derived','summary_day2_vitals') and table_exists('derived','summary_day3_vitals')) ;
    
        if tble_exists:
                vital_signs_count = table_data_count('derived','vitalsigns')

        if (vital_signs_count> 0):
            summary_joined_vitals_script = summary_joined_vitals_query()

            # Run  Summary Joined Vital Signs Query
            inject_sql(summary_joined_vitals_script, "create-summary-joined-vital-signs")
        else:
            pass;

    except Exception as e:
        logging.error("!!! An error occured creating joined vital signs: ")
        cron_log = open(cron_log_file,"a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Summary Maternal Outcomes ".format(cron_time,mode))
        cron_log.close()
        logging.error(e)
        sys.exit(1)
   