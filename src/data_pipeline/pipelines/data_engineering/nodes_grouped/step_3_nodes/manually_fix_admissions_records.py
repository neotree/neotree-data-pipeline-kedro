import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql,run_bulky_query,run_query_and_return_df
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_label_cleanup_data
from conf.common.format_error import formatError
from data_pipeline.pipelines.data_engineering.queries.admissions_manually_fix_records_sql import manually_fix_admissions_query
from data_pipeline.pipelines.data_engineering.utils.field_info import transform_matching_labels_for_update_queries
from conf.base.catalog import cron_log_file,cron_time,env


#Passing Step 2 Output So That It can Wait For Step 2 Data Tyding To Happen
def manually_fix_admissions(tidy_data_output):
    try:
        #Test If Previous Node Has Completed Successfully
        if env=='demo':
            return dict(
            status='Success',
            message = "Skippable Task"
            )
        
        elif tidy_data_output is not None:
            try:
                #FIX OLD DATA 
                admissions_fix_query = manually_fix_admissions_query()
                inject_sql(admissions_fix_query)
                current_scripts = ['admissions','discharges','maternal_outcomes'
                                ,'daily_review','infections','neolab','vitalsigns'
                                ,'maternal_completeness','baseline'
                                ,'joined_admissions_discharges','twenty_8_day_follow_up']
                for script in current_scripts:
                    logging.info(f"@@@@@START@@@@@---{script}")
                    query = read_label_cleanup_data(script)
                    if query is not None:
                        df = run_query_and_return_df(query)
                        if df is not None:
                            transformed = transform_matching_labels_for_update_queries(df,script)
                            if transformed is not None:
                                run_bulky_query(script,transformed) 
                        logging.info(f"@@@@@DONE@@@@@---{script}")             
            
                return dict(
                status='Success',
                message = "Manual Fixing Of Admissions Complete"
                )
            except Exception as ex:
                logging.error(f'LABELS FIX FAILED:: {formatError(ex)}')
        else:
            logging.error(
                "Manual Fixing Of Admissions Did Not Execute To Completion")
            return None
        # FIX LABELS
        

    except Exception as e:
        logging.error(
            "!!! An error occured manually fixing admissions: ")
        cron_log = open(cron_log_file,"a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed Stage: Manually Fixing Admissions ".format(cron_time,env))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)