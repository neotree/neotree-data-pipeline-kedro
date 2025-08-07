import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql,run_bulky_query,run_query_and_return_df,generate_label_fix_updates
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_all_from_derived_table
from conf.common.format_error import formatError
from data_pipeline.pipelines.data_engineering.queries.admissions_manually_fix_records_sql import manually_fix_admissions_query
from data_pipeline.pipelines.data_engineering.queries.data_fix import deduplicate_combined
# from data_pipeline.pipelines.data_engineering.queries.data_fix import (update_age_category,update_admission_weight,update_mode_delivery,            
#                                                                        update_refferred_from,update_ANVDRL,update_signature,update_cause_death,
#                                                                        update_disdiag, update_hive_result,fix_broken_dates_combined,update_gender,
#                                                                        deduplicate_combined,update_puurine,update_haart,update_lengthhaart,
#                                                                        update_stools,update_admreason,update_reason,update_anmatsyphtreat,
#                                                                        update_patnsyph ,update_birthfac,update_ageestimate,
#                                                                        update_anster,update_ansteroids,update_transfusion,update_transtype,
#                                                                        update_specrev,update_specrevtype,update_matadmit,update_matdisc,
#                                                                        update_troward,update_readmission,update_vomiting,update_passedmec,
#                                                                        update_puurine_nb,update_IRON,update_TTV,update_ROMLENGTH,update_ROM,
#                                                                        update_CryBirth,update_VitK,update_TEO,update_DateVDRLSameHIV,
#                                                                        update_AnvdrlResult,update_BSUnit,update_BsMonyn,update_VRLKnown
    
#                                                                        )
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
                deduplicate_combined()
                current_scripts = ['admissions','discharges','maternal_outcomes'
                                ,'daily_review','infections','neolab','vitalsigns'
                                ,'maternal_completeness','baseline'
                                ,'joined_admissions_discharges','twenty_8_day_follow_up']
                for script in current_scripts:
                    query = read_all_from_derived_table(script)
                    if query is not None:
                        df = run_query_and_return_df(query)
                        if df is not None:
                            transformed = transform_matching_labels_for_update_queries(df,script)
                            if transformed is not None:
                                run_bulky_query(script,transformed)                
            
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