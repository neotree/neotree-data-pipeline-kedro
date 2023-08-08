from ast import Str
import logging
import pandas as pd
from conf.common.format_error import formatError
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import update_eronous_label
from conf.base.catalog import catalog
from conf.common.sql_functions import inject_sql
from data_pipeline.pipelines.data_engineering.utils.data_label_fixes import fix_disharge_label
,fix_maternal_label,fix_admissions_label,fix_baseline_label
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists

def discharge_data_cleanup():
    try:
        #####IDENTIFY THE FAULTY DISCHARGES
        faulty_discharges_df = pd.DataFrame()
        if table_exists('public','sessions'):
            faulty_discharges_df = catalog.load('discharges_to_fix')
        if not faulty_discharges_df.empty:
            for index,row in faulty_discharges_df.iterrows():
                for key in row['data']:
                    if row['data'][key] is not None and row['data'][key]['values'] is not None and len(row['data'][key]['values']['value'])>0:
                        value = row['data'][key]['values']['value'][0]
                        type = row['data'][key]['type'] if 'type' in row['data'][key] else None
                        label = fix_disharge_label(key,value)
                        if value is not None and label is not None:
                            query = update_eronous_label(row['uid'],row['scriptid'],type,key,label,value)
                            inject_sql(query,"FIX DISCHARGE ERRORS")
    
    except Exception as e:
        logging.error(formatError(e))

def maternal_data_cleanup():
    try:
        #####IDENTIFY THE FAULTY MATERNAL DATA
        faulty_maternal_df = pd.DataFrame()
        if table_exists('public','sessions'):
            faulty_maternal_df = catalog.load('maternals_to_fix')
        if not faulty_maternal_df.empty:
            for index,row in faulty_maternal_df.iterrows():
                for key in row['data']:
                    if row['data'][key] is not None and row['data'][key]['values'] is not None and len(row['data'][key]['values']['value'])>0:
                        value = row['data'][key]['values']['value'][0]
                        type = row['data'][key]['type'] if 'type' in row['data'][key] else None
                        label = fix_maternal_label(key,value)
                        if value is not None and label is not None:
                            query = update_eronous_label(row['uid'],row['scriptid'],type,key,label,value)
                            inject_sql(query,"FIX MATERNITY DATA ERRORS")
    
    except Exception as e:
        logging.error(formatError(e))

def admissions_data_cleanup():
    try:
        #####IDENTIFY THE FAULTY ADMISSIONS DATA
        faulty_admin_df = pd.DataFrame()
        if table_exists('public','sessions'):
            faulty_admin_df = catalog.load('admissions_to_fix')
        if not faulty_admin_df.empty:
            for index,row in faulty_admin_df.iterrows():
                for key in row['data']:
                    if row['data'][key] is not None and row['data'][key]['values'] is not None and len(row['data'][key]['values']['value'])>0:
                        value = row['data'][key]['values']['value'][0]
                        type = row['data'][key]['type'] if 'type' in row['data'][key] else None
                        label = fix_admissions_label(key,value)
                        if value is not None and label is not None:
                            query = update_eronous_label(row['uid'],row['scriptid'],type,key,label,value)
                            inject_sql(query,"FIX ADMISSIONS DATA ERRORS")

def baseline_data_cleanup():
    try:
        #####IDENTIFY THE FAULTY BASELINE DATA
        faulty_baseline_df = pd.DataFrame()
        if table_exists('public','sessions'):
            faulty_baseline_df = catalog.load('baselines_to_fix')
        if not faulty_baseline_df.empty:
            for index,row in faulty_baseline_df.iterrows():
                for key in row['data']:
                    if row['data'][key] is not None and row['data'][key]['values'] is not None and len(row['data'][key]['values']['value'])>0:
                        value = row['data'][key]['values']['value'][0]
                        type = row['data'][key]['type'] if 'type' in row['data'][key] else None
                        label = fix_baseline_label(key,value)
                        if value is not None and label is not None:
                            query = update_eronous_label(row['uid'],row['scriptid'],type,key,label,value)
                            inject_sql(query,"FIX BASELINE DATA ERRORS")
    
    except Exception as e:
        logging.error(formatError(e))

    
    