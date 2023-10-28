import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
from conf.base.catalog import cron_log_file
from data_pipeline.pipelines.data_engineering.queries.create_summary_baselines_sql import summary_baseline_query
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists 


#Pass Join Table Output To Create Convenience View From Joined Tables
def create_summary_baseline(join_tables_output):  
    tble_exists = False;
    try:
        # Test if table exist before executing query
        tble_exists = table_exists('derived','baseline');
        #Test If Previous Node Has Completed Successfully
        if tble_exists:
            if join_tables_output is not None:
            
                sql_script = summary_baseline_query()
                inject_sql(sql_script, "create-summary-baseline")
                #Add Return Value For Kedro Not To Throw Data Error
                return dict(
                status='Success',
                message = "Creating Summary Baseline Complete"
                )
            else:
                logging.error(
                    "Creating Summary Baseline Did Not Execute To Completion")
                return None
        else:
           return dict(
               status='Skipped'
              )

    except Exception as e:
        logging.error("!!! An error occured creating summary baseline: ")
        cron_log = open(cron_log_file,"a+")
        #cron_log = open("C:\/Users\/morris\/Documents\/BRTI\/logs\/data_pipeline_cron.log","a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Summary Baseline ".format(cron_time,mode))
        cron_log.close()
        raise e
        