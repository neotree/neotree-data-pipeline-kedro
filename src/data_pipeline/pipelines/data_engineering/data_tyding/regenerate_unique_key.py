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
            logging.info("-MY ID ="+str(id))
            if 'appVersion' in row:
                app_version = row['appVersion']
           
            values = []
            possible_unique_keys = ['dateadmission','datetimeadmission','datetimedeath']
            value = row['entries']
            for prefix in possible_unique_keys:
                #Check If It Is Old Format Or New Format
                if('key' not in row['entries']) or (app_version!='' and app_version!=None and (app_version>454 or int(str(app_version)[:1])>=5)):
                    item = value.loc[str(value.index).startswith(prefix) & ~value['values'].apply(lambda x: x['value'][0] is not None)].iloc[0]
                    if item is not None:
                        values.append(item['values']['value'][0])            
                    # OLD FORMAT
                else:
                    item = value.loc[(str(value['key']).lower().startswith(prefix)) & (value['values'].apply(lambda x: x[0]['value']) is not None)]
                    if not item.empty:
                        values.append(item.iloc[0]['values'][0]['value'])
                
                if len(values)>0:
                    query = regenerate_unique_key_query(id,values[0])
                else:
                    query = regenerate_unique_key_query(id,None)
                logging.info("-QUERY ="+query)
                inject_sql(query,"UNIQUE-KEYS")
                break
                         
    except Exception as ex:
        logging.error("UNIQUE KEY GENERATION ERROR:-")
        logging.error(formatError(ex))