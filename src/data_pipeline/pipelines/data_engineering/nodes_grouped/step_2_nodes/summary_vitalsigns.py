from conf.base.catalog import cron_log_file,cron_time,env
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.queries.check_table_count_sql import table_data_count
from data_pipeline.pipelines.data_engineering.queries.create_summary_vitalsigns_sql import summary_vital_signs_query
from data_pipeline.pipelines.data_engineering.queries.create_summary_day_one_vitals_sql import summary_day_one_vitals_query
from data_pipeline.pipelines.data_engineering.queries.create_summary_day_three_vitals_sql import summary_day_three_vitals_query
from data_pipeline.pipelines.data_engineering.queries.create_summary_day_two_vitals_sql import summary_day_two_vitals_query
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
import logging
import sys

def create_summary_vitalsigns():
    vital_signs_count = 0
    tble_exists = False
    try:
        tble_exists = table_exists('derived','vitalsigns');
        if tble_exists:
                vital_signs_count = table_data_count('derived','vitalsigns')

        if (vital_signs_count> 0):
            failures = []
            for name, query_builder in (
                ('vital-signs', summary_vital_signs_query),
                ('day1-vital-signs', summary_day_one_vitals_query),
                ('day2-vital-signs', summary_day_two_vitals_query),
                ('day3-vital-signs', summary_day_three_vitals_query),
            ):
                try:
                    inject_sql(query_builder(), f'create-summary-{name}')
                except Exception as ex:
                    failures.append(name)
                    logging.error('Failed summary %s; continuing with other vital summaries: %s', name, ex)
            if failures:
                raise RuntimeError(f"Failed vital summaries: {', '.join(failures)}")
        else:
            pass;

    except Exception as e:
        logging.error("!!! An error occured creating Vital Signs Summaries: ")
        cron_log = open(cron_log_file,"a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Summary Maternal Outcomes ".format(cron_time,env))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)

