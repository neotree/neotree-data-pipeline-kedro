from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode, cron_time
from data_pipeline.pipelines.data_engineering.queries.discharges_manually_fix_records_sql import manually_fix_discharges_query
from conf.base.catalog import cron_log_file
from conf.common.format_error import formatError
from conf.common.sql_functions import inject_sql
import logging
import sys
import os
sys.path.insert(1, os.getcwd())


# Passing The Same Input with Manually Fixing Admissions To Allow For Concurrency
def manually_fix_discharges(tidy_data_output):
    try:
        # Test If Previous Node Has Completed Successfully
        if tidy_data_output is not None:

            sql_script = manually_fix_discharges_query()
            inject_sql(sql_script, "manually-fix-discharges")
            # Add Return Value For Kedro Not To Throw Data Error
            return dict(
                status='Success',
                message="Manual Fixing Of Discharges Complete"
            )
        else:
            logging.error(
                "Manual Fixing Of Disharges Did Not Execute To Completion")
            return None

    except Exception as e:
        logging.error(
            "!!! An error occured manually fixing discharges: ")
        cron_log = open(cron_log_file, "a+")
        cron_log.write(
            "StartTime: {0}   Instance: {1}   Status: Failed Stage: Manually Fixing Discharges".format(cron_time, mode))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)
