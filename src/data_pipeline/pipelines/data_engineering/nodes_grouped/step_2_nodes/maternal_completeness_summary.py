import logging
import sys
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.queries.check_table_count_sql import table_data_count
from data_pipeline.pipelines.data_engineering.queries.create_summary_maternal_completeness_sql import summary_maternal_completeness_query
from conf.common.sql_functions import inject_sql
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time
from conf.base.catalog import cron_log_file
from conf.common.format_error import formatError


def create_maternal_completeness_summary():
    try: 
        maternal_completeness_count = 0;
        mat_completeness_exists = False;
        mat_completeness_exists = table_exists('derived','maternity_completeness');

        if mat_completeness_exists:
            maternal_completeness_count = table_data_count('derived','maternity_completeness')

        if (maternal_completeness_count> 0):
         
            sql_script = summary_maternal_completeness_query()
            inject_sql(sql_script, "create-summary-maternal-completeness")
        else:
            pass;


    except Exception as e:
         logging.error("!!! An error occured creating summary maternal completeness: ")
         cron_log = open(cron_log_file,"a+")
         cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Summary Maternal Completeness ".format(cron_time,mode))
         cron_log.close()
         logging.error(formatError(e))
         sys.exit(1)