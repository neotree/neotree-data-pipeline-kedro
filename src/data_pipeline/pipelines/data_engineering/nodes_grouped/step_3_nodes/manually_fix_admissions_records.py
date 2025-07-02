import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
from data_pipeline.pipelines.data_engineering.queries.admissions_manually_fix_records_sql import manually_fix_admissions_query
from data_pipeline.pipelines.data_engineering.queries.data_fix import (update_age_category
                                                                       ,update_admission_weight
                                                                       ,update_mode_delivery
                                                                       ,update_refferred_from
                                                                       ,update_signature
                                                                       ,update_cause_death,
                                                                       update_disdiag, 
                                                                       update_hive_result,
                                                                       fix_broken_dates_combined,
                                                                       update_gender,
                                                                       deduplicate_combined
                                                                       )
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
            # sql_script = manually_fix_admissions_query()
            # inject_sql(sql_script, "manually-fix-admissions")
            deduplicate_combined()
            update_mode_delivery()
            update_admission_weight()
            update_refferred_from()
            update_age_category()
            update_signature()
            update_cause_death()
            update_disdiag()
            update_hive_result()
            logging.info("################### NOW FIXING BROKEN DATE;THIS MIGHT TAKE A WHILE!!#####")
            fix_broken_dates_combined()
            logging.info("################### DONE FIXING BROKEN DATE;THAT WAS QUICK HEY!!#####")
            update_gender()
            return dict(
            status='Success',
            message = "Manual Fixing Of Admissions Complete"
            )
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