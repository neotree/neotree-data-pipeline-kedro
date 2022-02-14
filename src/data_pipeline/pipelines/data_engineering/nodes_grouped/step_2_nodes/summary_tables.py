import logging
import sys
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time
from conf.base.catalog import cron_log_file
from .summary_vitalsigns import create_summary_vitalsigns
from .maternal_completeness_summary import create_maternal_completeness_summary
from .summary_joined_vitalsigns import create_summary_joined_vitalsigns
from .summary_maternal_outcomes import create_summary_maternal_outcomes
from .summary_admissions import create_summary_admissions
from .summary_discharges import create_summary_discharges


def create_summary_tables(tidy_data_output):
    try:
        if tidy_data_output is not None:
            create_summary_vitalsigns()
            create_maternal_completeness_summary() 
            create_summary_joined_vitalsigns() 
            create_summary_maternal_outcomes()
            create_summary_admissions()
            create_summary_discharges()

            # Add Return Value For Kedro Not To Throw Data Error
            return dict(
                status='Success',
                message="Summary Tables Complete"
            )
        else:
            logging.error(
                "Data Tidying Did Not Execute To Completion")
            return None
    except Exception as e:
        logging.error(
            "!!! An error occured in creating summary tables: ")
        logging.error(e.with_traceback())
        # Only Open This File When Need Be To Write To It
        cron_log = open(cron_log_file, "a+")
        cron_log.write(
            "StartTime: {0}   Instance: {1}   Status: Failed Stage: Summary Tables ".format(cron_time, mode))
        cron_log.close()
        sys.exit(1)
