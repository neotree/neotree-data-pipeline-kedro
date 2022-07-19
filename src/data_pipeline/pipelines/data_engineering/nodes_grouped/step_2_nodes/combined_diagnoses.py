import logging
import sys
from conf.base.catalog import cron_log_file
from conf.common.sql_functions import inject_sql
from data_pipeline.pipelines.data_engineering.queries.create_combined_diagnoses_sql import combined_diagnoses_query
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time
from conf.common.format_error import formatError

def create_combined_diagnoses():
    try:     
        sql_script = combined_diagnoses_query()
        inject_sql(sql_script, "create-combined-diagnoses")

    except Exception as e:
        logging.error("!!! An error occured combined diagnoses: ")
        cron_log = open(cron_log_file,"a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Combined Diagnoses ".format(cron_time,mode))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)