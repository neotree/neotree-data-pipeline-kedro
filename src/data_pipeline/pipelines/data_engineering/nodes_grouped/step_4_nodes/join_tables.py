import logging
from conf.base.catalog import cron_log_file
from data_pipeline.pipelines.data_engineering.derive_data.create_joined_table_and_derived_columns import join_table
from data_pipeline.pipelines.data_engineering.nodes_grouped.step_1_nodes.deduplicate_admissions import mode,cron_time

#Pass OutPut From Manually Fixing Admissions So That We Can use a clean derived table as Input
def join_tables(manually_Fix_admissions_output,manually_fix_discharges_output):
    try:
        #Test If Previous Node Has Completed Successfully
        if manually_Fix_admissions_output is not None and manually_fix_discharges_output is not None:
            join_table()
            #Add Return Value For Kedro Not To Throw Data Error
            return dict(
            status='Success',
            message = "Creating Joint  Tables Complete"
            )
        else:
            logging.error(
                "Manual Fixing Of Records Did Not Execute To Completion")
            return None

    except Exception as e:
        logging.error("!!! An error occured joining tables: ")
        cron_log = open(cron_log_file,"a+")
        #cron_log = open("C:\/Users\/morris\/Documents\/BRTI\/logs\/data_pipeline_cron.log","a+")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed   Stage: Creating Joint Tables ".format(cron_time,mode))
        cron_log.close()
        raise e
        
