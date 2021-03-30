import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
from conf.base.catalog import cron_log_file
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time
from data_pipeline.pipelines.data_engineering.queries.create_summary_counts_sql import summary_counts_query;


#Pass Convinience Views Output
def create_summary_counts(convinience_views_output):    
    try:
        #Test If Previous Node Has Completed Successfully
        if convinience_views_output is not None:
            sql_script = summary_counts_query();
            inject_sql(sql_script, "create-summary-counts")
            #Add Return Value For Kedro Not To Throw Data Error
            return dict(
            status='Success',
            message = "Creating Summary counts Complete"
            )
        else:
            logging.error(
                "Creating Summary counts Did Not Execute To Completion")
            return None

    except Exception as e:
        logging.error("!!! An error occured creating summary counts: ")
        cron_log = open(cron_log_file,"a+")
        #cron_log = open("C:\/Users\/morris\/Documents\/BRTI\/logs\/data_pipeline_cron.log","a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Summary Counts ".format(cron_time,mode))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)