import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.scripts import get_script,merge_script_data,merge_two_script_outputs,process_dataframe_with_types
from conf.common.hospital_config import hospital_conf
from conf.common.format_error import formatError
from conf.common.sql_functions import run_query_and_return_df
from conf.base.catalog import cron_log_file,cron_time,start,env
from data_pipeline.pipelines.data_engineering.queries.grant_usage_on_tables_sql import grant_usage_query 
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import (read_derived_data_query)
import logging
import time

#This file calls the query to grant privileges to users on the generated tables
cron_log = open(cron_log_file,"a+")
def clean_data_for_research(create_summary_counts_output):
    try:
        #Test If Previous Node Has Completed Successfully
        if create_summary_counts_output:

            logging.info("...........Cleaning Joined Admissions Discharges.............")
            hospital_scripts = hospital_conf()
            merged_admissions= None
            merged_discharges= None
            if hospital_scripts:
                for hospital in hospital_scripts:
                    ids = hospital_scripts[hospital]
                    for script in ids.keys():
                        if script =='admissions':
                            script_ids = str(ids[script]).split(',') 
                            for script_id in script_ids:
                                script_id = script_id.strip()
                                if not script_id:
                                    continue
                                else:
                                    script_json = get_script(script_id)
                                    if(script_json is not None):
                                        merged_admissions=merge_script_data(merged_admissions,script_json)
                        if script =='discharges':
                            script_ids = str(ids[script]).split(',') 
                            for script_id in script_ids:
                                script_id = script_id.strip()
                                if not script_id:
                                    continue
                                else:
                                    script_json = get_script(script_id)
                                    if(script_json is not None):
                                        merged_discharges=merge_script_data(merged_discharges,script_json)
                ####################################################################################################
                logging.info("###### GETTING JOINED ADMISSIONS DISCHARGES ##############################")
                merged_keys = merge_two_script_outputs(merged_admissions,merged_discharges)
                if (merged_keys is not None and bool(merged_keys)):
                    joined_admission_discharges = run_query_and_return_df(read_derived_data_query('joined_admissions_discharges','clean_joined_adm_discharges'))
                    cleaned_df = process_dataframe_with_types(joined_admission_discharges,merged_keys)
                    if(cleaned_df is not None and not cleaned_df.empty):
                        logging.info(f"################GENERATED DF ==={cleaned_df.head()}")
         
            return dict(
            status='Success',
            message = "Data Cleanup Complete"
            )
        else:
            logging.error(
                "Granting Priviledges Complete Did Not Execute To Completion")

            return None

    except Exception as e:
        logging.error(
            "!!! An error occured Granting Priviledges: ")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed Stage: Granting Privileges".format(cron_time,env))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)