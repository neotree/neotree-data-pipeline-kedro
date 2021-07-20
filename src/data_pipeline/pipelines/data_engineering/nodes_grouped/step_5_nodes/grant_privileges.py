from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode, cron_time, start
from data_pipeline.pipelines.data_engineering.queries.grant_usage_on_tables_sql import grant_usage_query
import time
from datetime import datetime
from conf.base.catalog import cron_log_file
from conf.common.config import config
from conf.common.format_error import formatError
from conf.common.sql_functions import inject_sql_procedure
import logging
import sys
import os
sys.path.append(os.getcwd())

# This file calls the query to grant privileges to users on the generated tables
cron_log = open(cron_log_file, "a+")


def grant_privileges(create_summary_counts_output):
    try:
        # Test If Previous Node Has Completed Successfully
        if create_summary_counts_output:

            sql_script = grant_usage_query()
            inject_sql_procedure(sql_script, "grant-usage-on-tables")
            end = time.time()
            execution_time = end-start
            execution_time_seconds = 0
            execution_time_minutes = 0
            if execution_time > 0:
                execution_time_minutes = round(execution_time//60)
                execution_time_seconds = round(execution_time % 60)
                cron_log.write("StartTime: {0}   Instance: {1}   Status: Success  ExecutionTime: {2} mins {3} seconds \n".format(
                    cron_time, mode, execution_time_minutes, execution_time_seconds))
                cron_log.close()

            # Add Return Value For Kedro Not To Throw Data Error
            return dict(
                status='Success',
                message="Granting Priviledges Complete"
            )
        else:
            logging.error(
                "Granting Priviledges Complete Did Not Execute To Completion")

            return None

    except Exception as e:
        logging.error(
            "!!! An error occured Granting Priviledges: ")
        cron_log.write(
            "StartTime: {0}   Instance: {1}   Status: Failed Stage: Granting Privileges".format(cron_time, mode))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)
