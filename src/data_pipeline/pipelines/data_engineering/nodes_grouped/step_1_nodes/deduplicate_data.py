import os, sys
sys.path.append(os.getcwd())                         
import logging
from conf.common.sql_functions import inject_sql, inject_sql_with_return
from conf.base.catalog import cron_log_file,generic_dedup_queries,cron_time,env
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import (
    insert_sessions_data,
    clean_known_confidential_columns,
    pii_skipped_redaction_summary_query,
)
from conf.common.config import config
from data_pipeline.pipelines.data_engineering.data_tyding.regenerate_unique_key import regenerate_unique_key


def log_pii_skip_summary(schema: str, table: str):
    try:
        result = inject_sql_with_return(pii_skipped_redaction_summary_query(schema, table))
        if not result:
            return

        skipped_count, patterns, sample_rows = result[0]
        skipped_count = int(skipped_count or 0)

        if skipped_count > 0:
            logging.warning(
                "PII redaction skipped %s suspicious row(s) on %s.%s. Patterns: %s. Samples: %s",
                skipped_count,
                schema,
                table,
                patterns or "none",
                sample_rows or "none",
            )
    except Exception as ex:
        logging.warning("Could not log PII skip summary for %s.%s: %s", schema, table, ex)


#Not passing any Input To Allow Concurrent running of independent Nodes
def deduplicate_data(data_import_output):
    params = config()
    try:
        
        if data_import_output is not None:
<<<<<<< HEAD
            #Clean Up Confidential Columns From Source
            logging.info("******CLEANING KNOWN CONFIDENTIALITY COLUMNS*********")
            inject_sql(clean_known_confidential_columns("public","sessions"),"confidential-sessions")
            inject_sql(clean_known_confidential_columns("public","clean_sessions"),"confidential-clean-sessions")
            logging.info("******START DATA CLEANING*********")
            inject_sql(insert_sessions_data(),"Sessions Data")
=======
            logging.info("******START DATA CLEANING*********")
            inject_sql(insert_sessions_data(),"Sessions Data")
            logging.info("******CLEANING KNOWN CONFIDENTIALITY COLUMNS ON CLEAN SESSIONS*********")
            inject_sql(clean_known_confidential_columns("public","clean_sessions"),"confidential-clean-sessions")
            log_pii_skip_summary("public", "clean_sessions")
>>>>>>> NEOAPP1238
            logging.info("******DONE INSERTING INTO CLEAN SESSIONS*********") 
            regenerate_unique_key()
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