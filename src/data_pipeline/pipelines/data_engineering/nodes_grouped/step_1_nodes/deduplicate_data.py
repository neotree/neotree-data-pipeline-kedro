import os, sys
sys.path.append(os.getcwd())                         
import logging
from conf.common.sql_functions import inject_sql
from conf.base.catalog import cron_log_file,generic_dedup_queries,cron_time,env
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import insert_sessions_data
from data_pipeline.pipelines.data_engineering.data_tyding.fix_data_labels import data_labels_cleanup, fix_data_errors 
from conf.common.config import config
from data_pipeline.pipelines.data_engineering.data_tyding.regenerate_unique_key import regenerate_unique_key


#Not passing any Input To Allow Concurrent running of independent Nodes
def deduplicate_data(data_import_output):
    params = config()
    try:
        
        if data_import_output is not None:
            logging.info("******START DATA CLEANING*********")
            inject_sql(insert_sessions_data(),"Sessions Data")
            logging.info("******DONE INSERTING INTO CLEAN SESSIONS*********")
            
            #regenerate_unique_key()
            # fix_data_errors()
            
            if('data_fix' in params and str(params['data_fix']).lower()=='true'):
                logging.info("************FIXING ADM LABELS******************************")
                data_labels_cleanup('admissions')
                logging.info("************FIXING DIS LABELS******************************")
                data_labels_cleanup('discharges')
                logging.info("************FIXING MATER LABELS******************************")
                data_labels_cleanup('maternals')
                logging.info("************FIXING BL LABELS******************************")
                data_labels_cleanup('baselines')
                # maternal_data_duplicates_cleanup()
            logging.info("************DONE DATA FIXES******************************")  
            ###DEDUPLICATE DYNAMICALLY
            for index,dedup_query in enumerate(generic_dedup_queries):  
                current_dedup = f'''deduplicate-generic_{index}'''  
                inject_sql(dedup_query, current_dedup)
            #Add Return Value For Kedro Not To Throw Data Error And To Be Used As Input For Step 2
            logging.info("*****************DONE DEDUPLICATING ALL DATA************************")
            
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
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed Stage: Deduplicating Data ".format(cron_time,env))
        cron_log.close()
        logging.error(e)
        sys.exit(1)