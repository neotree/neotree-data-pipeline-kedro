from conf.base.catalog import catalog,cron_log_file
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.queries.create_summary_vitalsigns_sql import summary_vital_signs_query
from conf.common.sql_functions import inject_sql
import logging
import sys

def create_summary_vitalsigns(tidy_data_output):
    vital_signs_count = 0
    tble_exists = False
    try:
        tble_exists = table_exists('derived','vitalsigns');
        if table_exists:
            vital_signs_count_df = catalog.load('vital_signs_count')
            if 'count' in vital_signs_count_df:
                vital_signs_count = vital_signs_count_df['count'].values[0]

    except Exception as e:
        raise e
    if (vital_signs_count> 0):
        try:
             #Test If Previous Node Has Completed Successfully
            if tidy_data_output is not None:
                sql_script = summary_vital_signs_query()
                inject_sql(sql_script, "create-summary-vital-signs")
                #Add Return Value For Kedro Not To Throw Data Error
                return dict(
                status='Success',
                message = "Creating Summary Vital Signs Complete"
                )
            else:
                logging.error(
                "Creating Vit Did Not Execute To Completion")
                return None

        except Exception as e:
            logging.error("!!! An error occured creating summary Maternal Outcomes: ")
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