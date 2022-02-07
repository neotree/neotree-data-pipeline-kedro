import os, sys
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.base.catalog import (deduplicate_baseline,deduplicate_maternal,
                              deduplicate_neolab,deduplicate_vitals,cron_log_file,
                              deduplicate_mat_completeness)
import logging
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time

#Not passing any Input To Allow Concurrent running of independent Nodes
def deduplicate_other_data(data_import_output):
    try:
        if data_import_output is not None:
            maternal_script = deduplicate_maternal
            vitals_script = deduplicate_vitals
            baseline_script = deduplicate_baseline
            neolab_script = deduplicate_neolab
            mat_completeness_script = deduplicate_mat_completeness
            inject_sql(maternal_script, "deduplicate-maternal")
            inject_sql(vitals_script, "deduplicate-vitals")
            inject_sql(baseline_script, "deduplicate-baseline")
            inject_sql(neolab_script, "deduplicate-neolabs")
            inject_sql(mat_completeness_script, "deduplicate-mat-completeness")
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