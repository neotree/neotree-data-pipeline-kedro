from conf.base.catalog import catalog
from conf.common.sql_functions import inject_sql,run_query_and_return_df
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import regenerate_unique_key_query,read_data_with_no_unique_key
from conf.common.format_error import formatError
import logging
import pandas as pd


#Method To Make Use of Either Date of Admission or Date of Death As Part Of The Unique Key
def regenerate_unique_key():
    
    try:
        #Read Data From The Kedro Catalog
        raw_data = run_query_and_return_df(read_data_with_no_unique_key())
        if isinstance(raw_data, dict):
            raw_data = list(raw_data.items())
        for index, row in raw_data.iterrows():
           
            id = row['id']
            unique_key = row['completed_at']
            query = regenerate_unique_key_query(id,unique_key)
            inject_sql(query,f'''UNIQUE-KEYS- {id}''')
        #FIX UNIQUE KEYS WITH WRONG DATE FORMATS        
        fix_regenerated_unique_keys()
    except Exception as ex:
        logging.error("UNIQUE KEY GENERATION ERROR:-")
        logging.error(ex)

def fix_regenerated_unique_keys():
    query= f'''UPDATE public.clean_sessions SET unique_key =to_char(to_timestamp(unique_key,'DD Mon, YYYY HH24:MI'),
                        'YYYY-MM-DD HH24:MI') WHERE  unique_key ~ '^[0-9]{{1,2}} [A-Za-z]{{3}}, [0-9]{{4}} [0-9]{{2}}:[0-9]{{2}}$';;'''
    inject_sql(query,f"UPDATING UNIQUE KEYS ")