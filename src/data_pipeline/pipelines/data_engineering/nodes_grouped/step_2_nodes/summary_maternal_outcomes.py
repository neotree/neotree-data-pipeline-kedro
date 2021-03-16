import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
from pathlib import Path,PureWindowsPath
from conf.base.catalog import catalog
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time

cwd = os.getcwd()

#Pass Convinience Views Output
def create_summary_maternal_outcomes(tidy_data_output):
    maternal_outcomes_count = 0
    try:
        mat_outcomes_count_df = catalog.load('count_maternal_outcomes')
        if 'count' in mat_outcomes_count_df:
            maternal_outcomes_count = mat_outcomes_count_df['count'].values[0]
    except Exception as e:
        raise e
    if (maternal_outcomes_count> 0):
        try:
             #Test If Previous Node Has Completed Successfully
            if tidy_data_output is not None:
                file_name = Path(
                cwd+"/src/data_pipeline/pipelines/data_engineering/queries/create-summary-maternal-outcomes.sql");
                sql_file = open(file_name, "r")
                sql_script = sql_file.read()
                sql_file.close()
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
            cron_log = open("/var/log/data_pipeline_cron.log","a+")
            #cron_log = open("C:\/Users\/morris\/Documents\/BRTI\/logs\/data_pipeline_cron.log","a+")
            cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Summary Maternal Outcomes ".format(cron_time,mode))
            cron_log.close()
            logging.error(formatError(e))
            sys.exit(1)
    else:
        return dict(
                status='Skipped',
                message = "Creating Summary Maternal Outcomes Skipped"
                )