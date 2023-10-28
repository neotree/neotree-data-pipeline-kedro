import os, sys
sys.path.append(os.getcwd())                         
import logging
from conf.common.sql_functions import inject_sql
from conf.base.catalog import (dedup_baseline,dedup_maternal,
                              dedup_neolab,dedup_vitals,cron_log_file,
                              dedup_mat_completeness,generic_dedup_queries)
from data_pipeline.pipelines.data_engineering.data_tyding.maternal_data_duplicates_cleanup import maternal_data_duplicates_cleanup
from data_pipeline.pipelines.data_engineering.data_tyding.fix_data_labels import data_labels_cleanup 
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time

#Not passing any Input To Allow Concurrent running of independent Nodes
def deduplicate_other_data(data_import_output):
    try:
        if data_import_output is not None:
            maternal_script = dedup_maternal
            vitals_script = dedup_vitals
            baseline_script = dedup_baseline
            neolab_script = dedup_neolab
            mat_completeness_script = dedup_mat_completeness
            data_labels_cleanup('maternals')
            maternal_data_duplicates_cleanup()
            data_labels_cleanup('baselines')
            if(maternal_script):
                inject_sql(maternal_script, "deduplicate-maternal")
            if(vitals_script):
                inject_sql(vitals_script, "deduplicate-vitals")
            if(baseline_script):
                inject_sql(baseline_script, "deduplicate-baseline")
            if(neolab_script):
                inject_sql(neolab_script, "deduplicate-neolabs")
            if(mat_completeness_script):
                inject_sql(mat_completeness_script, "deduplicate-mat-completeness")
            ###DEDUPLICATE DYNAMICALLY ADDED SCRIPTS
            for index,dedup_query in enumerate(generic_dedup_queries):
                inject_sql(dedup_query, f'''deduplicate-generic_{index}''')
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
        logging.error(e)
        sys.exit(1)