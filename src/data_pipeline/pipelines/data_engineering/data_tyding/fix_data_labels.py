import logging
import pandas as pd
from conf.common.format_error import formatError
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import update_eronous_label
from conf.base.catalog import catalog
from conf.common.sql_functions import inject_sql
from data_pipeline.pipelines.data_engineering.utils.data_label_fixes import fix_disharge_label
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists

def data_labels_cleanup():
    try:
        #####IDENTIFY THE FAULTY DISCHARGES
        faulty_discharges_df = pd.DataFrame()
        if table_exists('public','sessions'):
            faulty_discharges_df = catalog.load('discharges_to_fix')
        if not faulty_discharges_df.empty:
            for index,row in faulty_discharges_df.iterrows():
                for key in row['data']:
                    value = row['data'][key]['values']['value'][0]
                    type = row['data'][key]['type']
                    label = fix_disharge_label(key,value)
                    if value is not None and label is not None:
                        query = update_eronous_label(row['uid'],row['scriptid'],type,key,label,value)
                        inject_sql(query,"FIX DISCHARGE ERRORS")
    
    except Exception as e:
        logging.error(formatError(e))


    
    