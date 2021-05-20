from conf.base.catalog import catalog,cron_log_file
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.queries.check_table_count_sql import table_data_count
from data_pipeline.pipelines.data_engineering.queries.create_summary_joined_vitals_sql import summary_joined_vitals_query
from conf.common.sql_functions import inject_sql
import logging
import sys

def create_summary_joined_vitalsigns(create_summary_vitalsigns_output):
    vital_signs_count = 0
    tble_exists = False
    try:
        tble_exists = (table_exists('derived','vitalsigns') and table_exists('derived','summary_day1_vitals') 
                      and table_exists('derived','summary_day2_vitals') and table_exists('derived','summary_day3_vitals')) ;
        if tble_exists:
                vital_signs_count = table_data_count('derived','vitalsigns')

    except Exception as e:
        raise e
    if (vital_signs_count> 0):
        try:
             #Test If Previous Node Has Completed Successfully
            if create_summary_vitalsigns_output is not None:
                summary_joined_vitals_script = summary_joined_vitals_query()

                # Run  Summary Joined Vital Signs Query
                inject_sql(summary_joined_vitals_script, "create-summary-joined-vital-signs")

                #Add Return Value For Kedro Not To Throw Data Error
                return dict(
                status='Success',
                message = "Creating Summary Joined Vital Signs Complete"
                )
            else:
                logging.error(
                "Creating Summary Joined Vital Signs Did Not Execute To Completion")
                return None

        except Exception as e:
            logging.error("!!! An error occured creating joined vital signs: ")
            cron_log = open(cron_log_file,"a+")
            cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Summary Maternal Outcomes ".format(cron_time,mode))
            cron_log.close()
            logging.error(e.with_traceback())
            sys.exit(1)
    else:
        return dict(
                status='Skipped',
                message = "Creating Vital Signs Summary Skipped"
                )