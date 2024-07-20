from conf.base.catalog import catalog
from conf.common.sql_functions import inject_sql
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import regenerate_unique_key_query
from conf.common.format_error import formatError
import logging
import pandas as pd


#Method To Make Use of Either Date of Admission or Date of Death As Part Of The Unique Key
def regenerate_unique_key():
    
    try:
        #Read Data From The Kedro Catalog
        raw_data = catalog.load('no_unique_keys_data')
        for index, row in raw_data.iterrows():
           
            app_version = None
            id = row['id']
            if 'appVersion' in row:
                app_version = row['appVersion']
           
            values = []
            possible_unique_keys = ['dateadmission','datetimeadmission','datetimedeath']
            value = pd.DataFrame(row['entries'])
            for prefix in possible_unique_keys:
                #Check If It Is Old Format Or New Format
                if(isinstance(value,list)):
                    item = value.loc[(value['key'].str.lower().str.contains(prefix)) & (value['values'].apply(lambda x: x[0]['value']) is not None)]
                    if not item.empty:
                        values.append(item.iloc[0]['values'][0]['value'])         
                    # NEW FORMAT
                else:   
                    if index<2:
                        logging.info("-----RESTAST"+str(value.columns))
                    matching_data = [col for col in value.columns if col.startswith("Date")  
                             and value[col]['values']['value'][0] is not None]
                    if matching_data:
                        values.append(value[matching_data[0]]['values']['value'][0])
                
                if len(values)>0:
                    query = regenerate_unique_key_query(id,values[0])
                    if index<20 or index>285460:    
                        logging.info("-QUERY ="+query)
                    inject_sql(query,"UNIQUE-KEYS")
                break
                         
    except Exception as ex:
        logging.error("UNIQUE KEY GENERATION ERROR:-"+str(id))
        logging.error(formatError(ex))