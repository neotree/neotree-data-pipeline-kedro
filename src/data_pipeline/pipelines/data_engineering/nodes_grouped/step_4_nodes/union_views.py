import logging
import sys
from conf.base.catalog import cron_log_file
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time
from data_pipeline.pipelines.data_engineering.derive_data.create_union_views import union_views
# Pass The Output Of Tidying Data, As Input 
def create_union_views(joined_tables_output):
    try:
        # Test If Previous Node Has Completed Successfully
        if joined_tables_output is not None:
            union_views()
            # Add Return Value For Kedro Not To Throw Data Error
            return dict(
                status='Success',
                message="Union Views Creation Complete"
            )
        else:
            logging.error(
                "Union Views Not To Completion")
            return None

    except Exception as e:
        logging.error("!!! An error occured creating union views")
        logging.error(e)
        cron_log = open(cron_log_file, "a+")
        cron_log.write(
            "StartTime: {0}   Instance: {1}   Status: Failed Stage: Union Views Creation ".format(cron_time, mode))
        cron_log.close()
        sys.exit
