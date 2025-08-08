import pandas as pd # type: ignore
import logging
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import update_eronous_label
from conf.base.catalog import catalog
from conf.common.sql_functions import inject_sql
from data_pipeline.pipelines.data_engineering.utils.data_label_fix_new import fix_data_label,fix_data_value,fix_multiple_data_label,bulk_fix_data_labels 
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from datetime import datetime



@DeprecationWarning
def fix_data_errors():
    
    script_df = catalog.load('script_ids')
     
    for script_index, row in script_df.iterrows():
        script_id = row['scriptid']
        
        logging.info(f'''Fixing labels for script ({script_id}): {script_index + 1} of {script_df.shape[0]}''')
        
        commands = bulk_fix_data_labels(script_id)
         
        errors = False

        for command in commands: 
            try: 
                inject_sql(command,f'''FIX {script_id} LABEL ERRORS''')
            except Exception as e:
                errors = True
                logging.error(e)
                logging.error(command)
                
        if not errors:
            command =f"UPDATE public.clean_sessions SET cleaned = true WHERE scriptid = '{script_id}';;"
            inject_sql(command, f'''FIX {script_id} LABEL ERRORS''')
                
             
    logging.info("done fixing labels for scripts")

     
                                    

        
        