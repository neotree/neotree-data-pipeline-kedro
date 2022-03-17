import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.queries.check_table_count_sql import table_data_count
from conf.base.catalog import cron_log_file
from data_pipeline.pipelines.data_engineering.queries.create_summary_neolab_sql import summary_neolab_query
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time

cwd = os.getcwd()


def create_summary_neolabs():
    neolab_count = 0
    neolab_exists = False;
    try:
        neolab_exists = table_exists('derived','neolab');
       
        if neolab_exists:
            neolab_count = table_data_count('derived','neolab')
       
        if (neolab_count> 0):
      
            
            sql_script = summary_neolab_query()
            inject_sql(sql_script, "create-summary-neolabs")
        else:
            pass;

    except Exception as e:
        logging.error("!!! An error occured creating summary Neolab")
        cron_log = open(cron_log_file,"a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Summary Neolab".format(cron_time,mode))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)
   