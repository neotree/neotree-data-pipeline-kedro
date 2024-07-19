from conf.base.catalog import catalog
from conf.common.sql_functions import inject_sql
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import regenerate_unique_key_query
from conf.common.format_error import formatError
import logging


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
            for value in row['entries']:
                labels = []
                possible_unique_keys = ['dateadmission','datetimeadmission','datetimedeath']
                unique_values = None
                for prefix in possible_unique_keys:
                     #Check If It Is Old Format Or New Format
                    if('key' not in value) or (app_version!='' and app_version!=None and (app_version>454 or int(str(app_version)[:1])>=5)): 
                        for key, entry in value.items():
                            if str(key).lower().startswith(prefix) and any(value is not None for value in entry['values']['value']):
                                labels.append(entry['values']['label'][0])
                                break     
                    # NEW FORMAT
                    else:
                        unique_values= value[str(value['key']).lower().startswith(prefix) 
                        & (~value['values'].apply(lambda x: x[0]['value']).isna())]['values'].apply(lambda x: x[0]['value']).tolist()
                        if unique_values:
                            labels.append(unique_values[0])
                            break
                if len(labels)>0:
                    query = regenerate_unique_key_query(id,labels[0])
                    inject_sql(query,"UNIQUE-KEYS")
                break
                         
    except Exception as ex:
         logging.error("UNIQUE KEY GENERATION ERROR:-")
         logging.error(formatError(ex))