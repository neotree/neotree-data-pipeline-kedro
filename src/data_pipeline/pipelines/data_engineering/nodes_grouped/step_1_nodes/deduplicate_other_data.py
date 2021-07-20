import os, sys
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
from conf.base.catalog import deduplicate_baseline_query,deduplicate_maternal_query,deduplicate_neolab_query,deduplicate_vitals_query,cron_log_file
import logging
from pathlib import Path,PureWindowsPath
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time

#Not passing any Input To Allow Concurrent running of independent Nodes
def deduplicate_other_data(data_import_output):
    try:
        if data_import_output is not None:
            maternal_script = deduplicate_maternal_query
            vitals_script = deduplicate_vitals_query
            baseline_script = deduplicate_baseline_query
            neolab_script = deduplicate_neolab_query
            inject_sql(maternal_script, "deduplicate-maternal")
            inject_sql(vitals_script, "deduplicate-vitals")
            inject_sql(baseline_script, "deduplicate-baseline")
            inject_sql(neolab_script, "deduplicate-neolabs")
            #Add Return Value For Kedro Not To Throw Data Error And To Be Used As Input For Step 2
            return dict(
                status='Success',
                message = "Deduplication Complete"
            )
        else:
            logging.error(
                    "Data Deduplication Did Not Execute To Completion")
            return None
            
    except Exception as e:
        logging.error(
            "!!! An error occured deduplicating data: ")
        cron_log = open(cron_log_file,"a+")
        #cron_log = open("C:\/Users\/morris\/Documents\/BRTI\/logs\/data_pipeline_cron.log","a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed Stage: Deduplicating Data ".format(cron_time,mode))
        cron_log.close()
        logging.error(e.with_traceback())
        sys.exit(1)