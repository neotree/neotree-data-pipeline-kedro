import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.sql_functions import inject_sql
from conf.common.format_error import formatError
from pathlib import Path,PureWindowsPath
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time

cwd = os.getcwd()

#Pass Join Table Output To Create Convenience View From Joined Tables
def create_convenience_views(join_tables_output):    
    try:
        #Test If Previous Node Has Completed Successfully
        if join_tables_output is not None:
            file_name = Path(
             cwd+"/src/data_pipeline/pipelines/data_engineering/queries/create-convenience-views.sql");

            sql_file = open(file_name, "r")
            sql_script = sql_file.read()
            sql_file.close()
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
        cron_log = open("/var/log/data_pipeline_cron.log","a+")
        #cron_log = open("C:\/Users\/morris\/Documents\/BRTI\/logs\/data_pipeline_cron.log","a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Convenience Views ".format(cron_time,mode))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)