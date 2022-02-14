import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.queries.check_table_count_sql import table_data_count
from conf.base.catalog import cron_log_file
from data_pipeline.pipelines.data_engineering.queries.create_summary_maternal_outcomes_sql import summary_maternal_outcomes_query
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time

cwd = os.getcwd()


#Pass Convinience Views Output
def create_summary_maternal_outcomes():
    maternal_outcomes_count = 0
    mat_outcomes_exists = False;
    try:
        mat_outcomes_exists = table_exists('derived','maternal_outcomes');
       
        if mat_outcomes_exists:
            maternal_outcomes_count = table_data_count('derived','maternal_outcomes')
       
        if (maternal_outcomes_count> 0):
      
            
            sql_script = summary_maternal_outcomes_query()
            inject_sql(sql_script, "create-summary-maternal-outcomes")
        else:
            pass;

    except Exception as e:
        logging.error("!!! An error occured creating summary Maternal Outcomes: ")
        cron_log = open(cron_log_file,"a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Summary Maternal Outcomes ".format(cron_time,mode))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)
   