import logging
import sys
from  data_pipeline.pipelines.data_engineering.data_tyding.tidy_admissions_discharges_and_create_mcl_tables import tidy_tables
from conf.base.catalog import cron_time,env,cron_log_file


# Pass The Output Of Step One, As Input To Avoid Concurrent running of Step 1 and This Step
def tidy_data(deduplicate_data_output):
    try:
        #Test If Previous Node Has Completed Successfully
        if (deduplicate_data_output is not None):
            tidy_tables()
            
            #Add Return Value For Kedro Not To Throw Data Error
            return dict(
            status='Success',
            message = "Tidying Admissions and Discharges Complete"
             )
        else:
            logging.error(
                "Data Deduplication Did Not Execute To Completion")
            return None
        
    except Exception as e:
        logging.error("!!! An error occured tidying or creating MCL tables: ")
        logging.error(e)
        cron_log = open(cron_log_file,"a+")
        #cron_log = open("C:\/Users\/morris\/Documents\/BRTI\/logs\/data_pipeline_cron.log","a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed Stage: Tyding Data ".format(cron_time,env))
        cron_log.close()
        sys.exit