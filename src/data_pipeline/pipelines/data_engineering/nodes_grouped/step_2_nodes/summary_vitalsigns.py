from conf.base.catalog import catalog,cron_log_file
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.queries.check_table_count_sql import table_data_count
from data_pipeline.pipelines.data_engineering.queries.create_summary_vitalsigns_sql import summary_vital_signs_query
from data_pipeline.pipelines.data_engineering.queries.create_summary_day_one_vitals_sql import summary_day_one_vitals_query
from data_pipeline.pipelines.data_engineering.queries.create_summary_day_three_vitals_sql import summary_day_three_vitals_query
from data_pipeline.pipelines.data_engineering.queries.create_summary_day_two_vitals_sql import summary_day_two_vitals_query
from conf.common.sql_functions import inject_sql
import logging
import sys

def create_summary_vitalsigns(tidy_data_output):
    vital_signs_count = 0
    tble_exists = False
    try:
        tble_exists = table_exists('derived','vitalsigns');
        if tble_exists:
                vital_signs_count = table_data_count('derived','vitalsigns')

    except Exception as e:
        raise e
    if (vital_signs_count> 0):
        try:
             #Test If Previous Node Has Completed Successfully
            if tidy_data_output is not None:
                summary_vitals_script = summary_vital_signs_query()
                summary_vitals_day1_script = summary_day_one_vitals_query()
                summary_vitals_day2_script = summary_day_two_vitals_query()
                summary_vitals_day3_script = summary_day_three_vitals_query()
               
                # Run Summary Vital Signs Query
                inject_sql(summary_vitals_script, "create-summary-vital-signs")
                # Run Day1 Summary Vital Signs Query
                inject_sql(summary_vitals_day1_script, "create-summary-day1-vital-signs")

                # Run Day2 Summary Vital Signs Query
                inject_sql(summary_vitals_day2_script, "create-summary-day2-vital-signs")

                # Run Day3 Summary Vital Signs Query
                inject_sql(summary_vitals_day3_script, "create-summary-day3-vital-signs")


                #Add Return Value For Kedro Not To Throw Data Error
                return dict(
                status='Success',
                message = "Creating  Vital Signs Summaries Complete"
                )
            else:
                logging.error(
                "Creating Vital Signs Summaries Did Not Execute To Completion")
                return None

        except Exception as e:
            logging.error("!!! An error occured creating Vital Signs Summaries: ")
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