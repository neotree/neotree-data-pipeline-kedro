import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
from conf.base.catalog import cron_log_file,cron_time,env
from data_pipeline.pipelines.data_engineering.queries.create_convenience_views_sql import create_convinience_views_query


#Pass Join Table Output To Create Convenience View From Joined Tables
def create_convenience_views(join_tables_output):    
    try:
        #Test If Previous Node Has Completed Successfully
        if env=='demo':
            return dict(
            status='Success',
            message = "Skippable Task"
            )
        
        elif join_tables_output is not None:
           
            sql_script = create_convinience_views_query()
            inject_sql(sql_script, "create-convenience-views")
            #Add Return Value For Kedro Not To Throw Data Error
            return dict(
            status='Success',
            message = "Creating Convenience Views Complete"
            )
        else:
            logging.error(
                "Creating Convenience Views Did Not Execute To Completion")
            return None

    except Exception as e:
        logging.error("!!! An error occured creating convenience views: ")
        cron_log = open(cron_log_file,"a+")
        #cron_log = open("C:\/Users\/morris\/Documents\/BRTI\/logs\/data_pipeline_cron.log","a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Convenience Views ".format(cron_time,env))
        raise e
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)