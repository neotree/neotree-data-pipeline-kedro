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
           
            id = row['id']
            values = []
            possible_unique_keys = ['DateAdmission','DateTimeAdmission','DateTimeDeath']
            value = pd.DataFrame(row['entries'])
            for prefix in possible_unique_keys:
                #Check If It Is Old Format Or New Format
                if('key' in value):
                    matching_rows = value[value["key"].str.startswith(prefix)]
                    item=None
                    if not matching_rows.empty:
                        item = next((item["value"] for item in value[value["key"].str.startswith(prefix)]["values"].iloc[0] if item["value"] is not None), None)
                        
                    if item:
                        values.append(item)         
                    # NEW FORMAT
                else:   
                    matching_data = [col for col in value.columns if col.startswith(prefix)  
                             and value[col]['values']['value'][0] is not None]
                    if matching_data:
                        values.append(value[matching_data[0]]['values']['value'][0])
                
                if len(values)>0:
                    query = regenerate_unique_key_query(id,values[0])
                    inject_sql(query,"UNIQUE-KEYS")
                break
                         
    except Exception as ex:
        logging.error("UNIQUE KEY GENERATION ERROR:-"+str(id))
        logging.error(formatError(ex))