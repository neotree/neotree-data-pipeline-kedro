from ast import Str
import pandas as pd
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import update_eronous_label
from conf.base.catalog import catalog
from conf.common.sql_functions import inject_sql
from data_pipeline.pipelines.data_engineering.utils.data_label_fix_new import fix_data_label,fix_data_value,fix_multiple_data_label
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from datetime import datetime
import logging
import json

def data_labels_cleanup(script):
       #####IDENTIFY THE FAULTY RECORDS
        faulty_df = pd.DataFrame()
        if table_exists('public','clean_sessions'):
            faulty_df = catalog.load(f'''{script}_to_fix''')
        if not faulty_df.empty:
            for index,row in faulty_df.iterrows():
                if 'data' in row:
                    if 'key' in row['data']:
                        #Means Its Old Data Format Found In New Era
                        pass
                    else:
                
                        for key in row['data']:
                            if (row['data'][key] is not None and row['data'][key]['values'] is not None
                                and len(row['data'][key]['values']['label'])>0 and len(row['data'][key]['values']['value'])>0):
                                label =row['data'][key]['values']['label'][0]
                                type = row['data'][key]['type'] if 'type' in row['data'][key] else None
                                ##FIX LABELS
                                if  type!='set<id>':
                                    value = row['data'][key]['values']['value'][0]
                                    
                        
                                    if(value is None):
                                        label = value
                                    elif (type!='number' or type!='date' or type!='datetime' or type!='string'):
                                        if(script=='admissions'):
                                            
                                            label = fix_data_label(key,value,'admission')
                                                    
                                        elif(script=='discharges'):
                                            label = fix_data_label(key,value,'discharge')
                                        elif(script=='maternals'):
                                            label = fix_data_label(key,value,'maternity')
                                        elif(script=='baselines'):
                                            label = fix_data_label(key,value,'baseline')
                                        if (label !="Undefined"):                   
                                            query = update_eronous_label(row['uid'],row['scriptid'],type,key,f'"{label}"',
                                                                                f'"{value}"')
                                            inject_sql(query,f'''FIX {script} ERRORS''')
                                
                                elif  len(row['data'][key]['values']['value'])>0:
                                    
                                    value=row['data'][key]['values']['value']
                                    if script =='admissions':
                                        label = fix_multiple_data_label(key,value,'admission')
                                    elif script == 'discharges':
                                        label = fix_multiple_data_label(key,value,'discharge')
                                    elif script == 'maternals':
                                        label = fix_multiple_data_label(key,value,'maternity')
                                    elif script == 'baselines':
                                        label = fix_multiple_data_label(key,value,'baseline')
                                        
                                    if(label !="Undefined" and len(label)>0):
                                        processed_label =None
                                        if len(label)>1:
                                            processed_label =', '.join(f'"{x}"' for x in label)
                                            value = ', '.join(f'"{x}"' for x in value)
                                        else:
                                            processed_label= f'"{label[0]}"'
                                            value=f'"{value[0]}"'   
                                        query = update_eronous_label(row['uid'],row['scriptid'],type,key,processed_label,value)
                                        inject_sql(query,f'''FIX {script}  MULTI SELECT ERRORS''')
                                       
                                    

        
        