import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from conf.base.catalog import catalog,cron_log_file
from data_pipeline.pipelines.data_engineering.queries.create_summary_maternal_outcomes_sql import summary_maternal_outcomes_query
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time

cwd = os.getcwd()


#Pass Convinience Views Output
def create_summary_maternal_outcomes(tidy_data_output):
    maternal_outcomes_count = 0
    tble_exists = False;
    try:
        tble_exists = table_exists('derived','maternal_outcomes');
        if table_exists:
            mat_outcomes_count_df = catalog.load('count_maternal_outcomes')
            if 'count' in mat_outcomes_count_df:
                maternal_outcomes_count = mat_outcomes_count_df['count'].values[0]
    except Exception as e:
        raise e
    if (maternal_outcomes_count> 0):
        try:
             #Test If Previous Node Has Completed Successfully
            if tidy_data_output is not None:
                sql_script = summary_maternal_outcomes_query()
                inject_sql(sql_script, "create-summary-maternal-outcomes")
                #Add Return Value For Kedro Not To Throw Data Error
                return dict(
                status='Success',
                message = "Creating Summary Maternal Outcomes Complete"
                )
            else:
                logging.error(
                "Creating Summary Maternal Outcomes Did Not Execute To Completion")
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
                message = "Creating Summary Maternal Outcomes Skipped"
                )