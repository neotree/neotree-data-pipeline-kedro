import logging
import sys,os
from  data_pipeline.pipelines.data_engineering.data_tyding.tidy_admissions_discharges_and_create_mcl_tables import tidy_tables
from  conf.common.format_error import formatError
from conf.base.catalog import catalog
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time




# Pass The Output Of Step One, As Input To Avoid Concurrent running of Step 1 and This Step
def tidy_data(deduplicate_discharges_output):
    try:
        #Test If Previous Node Has Completed Successfully
        if deduplicate_discharges_output is not None:
            tidy_tables()
            #Add Return Value For Kedro Not To Throw Data Error
            return dict(
            status='Success',
            message = "Tidying Admissions and Discharges Complete"
             )
        else:
            logging.error(
                "Data Tyding Did Not Execute To Completion")
            return None
        
    except Exception as e:
        logging.error("!!! An error occured tidying or creating MCL tables: ")
        logging.error(e.with_traceback())
        cron_log = open("/var/log/data_pipeline_cron.log","a+")
        #cron_log = open("C:\/Users\/morris\/Documents\/BRTI\/logs\/data_pipeline_cron.log","a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed Stage: Tyding Data ".format(cron_time,mode))
        cron_log.close()
        sys.exit