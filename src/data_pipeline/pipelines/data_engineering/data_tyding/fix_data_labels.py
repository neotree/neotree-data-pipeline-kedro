from ast import Str
import pandas as pd
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import update_eronous_label
from conf.base.catalog import catalog
from conf.common.sql_functions import inject_sql
from data_pipeline.pipelines.data_engineering.utils.data_label_fixes import fix_disharge_label,fix_maternal_label,fix_admissions_label,fix_baseline_label
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from datetime import datetime

def data_labels_cleanup(script):
       #####IDENTIFY THE FAULTY RECORDS
        faulty_df = pd.DataFrame()
        if table_exists('public','sessions'):
            faulty_df = catalog.load(f'''{script}_to_fix''')
        if not faulty_df.empty:
            for index,row in faulty_df.iterrows():
                for key in row['data']:
                    if row['data'][key] is not None and row['data'][key]['values'] is not None and len(row['data'][key]['values']['value'])>0:
                        value = row['data'][key]['values']['value'][0]
                        type = row['data'][key]['type'] if 'type' in row['data'][key] else None
                        
                        label = 'Undefined'
                        if(script=='admissions'):
                            label = fix_admissions_label(key,value)
                        elif(script=='discharges'):
                            label = fix_disharge_label(key,value)
                        elif(script=='maternals'):
                            label= fix_maternal_label(key,value)
                        elif(script=='baselines'):
                            label= fix_baseline_label(key,value)   
                        if label !='Undefined':                     
                            query = update_eronous_label(row['uid'],row['scriptid'],type,key,label,value)
                            inject_sql(query,f'''FIX {script} ERRORS''')

    
    