import pandas as pd # type: ignore
import logging
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import update_eronous_label
from conf.base.catalog import catalog
from conf.common.sql_functions import inject_sql
from data_pipeline.pipelines.data_engineering.utils.data_label_fix_new import fix_data_label,fix_data_value,fix_multiple_data_label,bulk_fix_data_labels 
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from datetime import datetime




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

            
def data_labels_cleanup(script):
       #####IDENTIFY THE FAULTY RECORDS
        faulty_df = pd.DataFrame()
        # if table_exists('public','clean_sessions'):
        #     faulty_df = catalog.load(f'''{script}_to_fix''')
        #     logging.info(f'''loaded {script}_to_fix''')
        # if not faulty_df.empty:
        #     total_size = faulty_df.size
        #     for index,row in faulty_df.iterrows():
        #         try:
        #             if 'data' in row:
        #                 if 'key' in row['data']:
        #                     #Means Its Old Data Format Found In New Era
        #                     pass
        #                 else:
                    
        #                     for key in row['data']:
        #                         sid = row['scriptid']
        #                         uid = row['uid']  
        #                         #logging.info(f'{script} : {index} of {total_size} - {key} {sid} {uid}')
        #                         if (row['data'][key] is not None and row['data'][key]['values'] is not None
        #                             and len(row['data'][key]['values']['label'])>0 and len(row['data'][key]['values']['value'])>0):
        #                             label = row['data'][key]['values']['label'][0]
        #                             type = row['data'][key]['type'] if 'type' in row['data'][key] else None
        #                             ##FIX LABELS
        #                             if  type!='set<id>':
        #                                 value = row['data'][key]['values']['value'][0]
                                        
        #                                 if(value is None):
        #                                     label = value
        #                                 elif (type!='number' or type!='date' or type!='datetime' or type!='string'):
                                    
        #                                     label = fix_data_label(key,value,row['scriptid']) 
                                                   
        #                                     if(key=='MatOutcome'):
        #                                         logging.info(f'{key} {label} {uid} {sid}')       
                                                        
        #                                     # sanitise label - remove special characters
        #                                     if (label !="Undefined"):                   
        #                                         query = update_eronous_label(row['uid'],row['scriptid'],type,key,f'"{label}"',
        #                                                                             f'"{value}"')
        #                                         if(key=='MatOutcome'):
        #                                             logging.info(f'{query}') 
        #                                             logging.info('')
        #                                         inject_sql(query,f'''FIX {script} ERRORS''')
                                    
        #                             elif len(row['data'][key]['values']['value']) > 0:
                                        
        #                                 value=row['data'][key]['values']['value']
                                        
        #                                 label = fix_multiple_data_label(key,value,row['scriptid'],row['uid']) 
                                            
        #                                 if(label !="Undefined" and len(label)>0):
        #                                     processed_label =None
        #                                     if len(label)>1:
        #                                         processed_label =', '.join(f'"{x}"' for x in label)
        #                                         value = ', '.join(f'"{x}"' for x in value)
        #                                     else:
        #                                         processed_label= f'"{label[0]}"'
        #                                         value=f'"{value[0]}"'   
        #                                     query = update_eronous_label(row['uid'],row['scriptid'],type,key,processed_label,value)
        #                                     inject_sql(query,f'''FIX {script}  MULTI SELECT ERRORS''')
        #                             else:
        #                                 logging.info(f'hapana hapana {label}')  
        #         except Exception as e:
        #             logging.error(e)
                                    

        
        