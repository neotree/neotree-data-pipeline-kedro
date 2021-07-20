import logging
import sys
from conf.common.sql_functions import inject_sql
from data_pipeline.pipelines.data_engineering.queries.drop_views_sql import drop_views_query
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode, cron_time
from conf.base.catalog import cron_log_file


def drop_views(data_import_output):
    try:
        if data_import_output is not None:
            sql_script = drop_views_query()
            inject_sql(sql_script, "drop-views")
            # Add Return Value For Kedro Not To Throw Data Error
            return dict(
                status='Success',
                message="Views Dropping Complete"
            )
        else:
            logging.error(
                "Data Importation Did Not Execute To Completion")
            return None
    except Exception as e:
        logging.error(
            "!!! An error occured dropping views: ")
        logging.error(e.with_traceback())
        # Only Open This File When Need Be To Write To It
        cron_log = open(cron_log_file, "a+")
        cron_log.write(
            "StartTime: {0}   Instance: {1}   Status: Failed Stage: Dropping Views ".format(cron_time, mode))
        cron_log.close()
        sys.exit(1)
