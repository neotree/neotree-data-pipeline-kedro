# This module extracts the key-value pairs within a raw json file.
import logging
import re
import pandas as pd # type: ignore
from conf.common.format_error import formatError
from .json_restructure import restructure, restructure_new_format, restructure_array
from functools import  reduce
from datetime import datetime
from typing import Dict, Any
from collections import defaultdict


def get_key_values(data_raw):
   
    if data_raw.empty:
        return [], set()

    mcl = []
     
    # Will store the final list of uid, ingested_at & reformed key-value pairs
    data_new = []
    for index, row in data_raw.iterrows():
        # to store all the restructured keys & values for each row
        try:
            new_entry = {}
            # add uid and ingested_at first
            app_version = None
            script_version = None
            ingested_at =None
            if 'appVersion' in row:
                app_version = row['appVersion']
            if 'scriptid' in row:
                new_entry['scriptid']=row['scriptid']
            if(app_version!=None and app_version!=''):
                #Remove any Other Characters that are non-numeric
                app_version = int(''.join(d for d in app_version if d.isdigit()))
            if 'scriptVersion' in row:
                new_entry['script_version'] = row['scriptVersion']
                
            if 'facility' in row:
                new_entry['facility'] = row['facility']
                
            if 'unique_key' in row:
                new_entry['unique_key'] = row['unique_key']
            
            # Convert All UIDS TO UPPER CASE
            new_entry['uid'] = str(row['uid']).upper()
                
            if 'ingested_at_admission' in row:
                new_entry['ingested_at'] = row['ingested_at_admission']
            if 'ingested_at_discharge' in row:
                new_entry['ingested_at'] = row['ingested_at_discharge']

            if 'started_at' in row:
                new_entry['started_at'] = row['started_at']

            if 'completed_at' in row:
                new_entry['completed_at'] = row['completed_at']

            if 'completed_time' in row:
                new_entry['completed_time'] = row['completed_time']

            if 'ingested_at' in row:
                new_entry['ingested_at'] = row['ingested_at']
                new_entry['ingested_at'] = pd.to_datetime(row['ingested_at'], format='%Y-%m-%dT%H:%M:%S').tz_localize(None)
            if 'review_number' in row:
                new_entry['review_number'] = row['review_number']


        # iterate through key, value and add to dict
            for c in row['entries']:
                # Initialize k and v to avoid unbound variable errors
                k = None
                v = None

                #RECORDS FORMATTED WITH NEW FORMAT, CONTAINS THE jsonFormat Key and C is the Key
                if('key' not in c) or (app_version!='' and app_version!=None and (app_version>454 or int(str(app_version)[:1])>=5)):
                    try:
                        # Check if row['entries'][c] is not None before processing
                        entry_data = row['entries'][c]
                        if entry_data is not None:
                            k, v, mcl = restructure_new_format(c, entry_data, mcl)
                        #SET UID FOR ZIM DISCHARGES WHICH COME WITH NULL UID NEW FORMAT
                            if 'uid' not in new_entry or new_entry['uid'] is None:
                                if((k=='NeoTreeID' or k=='NUID_BC' or k=='NUID_M' or k=='NUID_S') and new_entry['uid'] is None):
                                    if hasattr(v, 'value'):
                                        new_entry['uid'] = v.value
                                    else:
                                        new_entry['uid'] = v
                    except Exception as e:
                        logging.info("RESTRUCTURING ERROR "+str(e))

            #ELSE USE THE OLD FORMAT
                else:
                    k, v, mcl = restructure(c, mcl)

                #SET UID FOR ZIM DISCHARGES WHICH COME WITH NULL UID OLD FORMAT
                if 'uid' not in new_entry or new_entry['uid'] is None:
                    if k is not None and ((k=='NeoTreeID' or k=='NUID_BC'or k=='NUID_M' or k=='NUID_S') and new_entry['uid'] is None):
                        if hasattr(v, 'value'):
                            new_entry['uid'] = v.value
                        else:
                            new_entry['uid'] = v
                if k is not None and v is not None:
                    if (k=='completed_at' and 'completed_at' not in new_entry) or k!='completed_at':
                        new_entry[k] = v
        
            data_new.append(new_entry)
            
        except Exception as ex:
            logging.error(formatError(ex))
        
    return data_new, set(mcl)

def get_diagnoses_key_values(data_raw):
    # Will store the final list of uid, ingested_at & reformed key-value pairs
    data_new = []
   
    for index, row in data_raw.iterrows():
        if "diagnoses" in row:
            
            # add uid and ingested_at first
            

            #Convert List to dictionary
            if row['diagnoses'] is not None and len(row['diagnoses'])> 0:
                parent_keys=reduce(lambda a, b: {**a, **b}, row['diagnoses'])
                
                # iterate through parent keys
                for parent_key in parent_keys:
                    new_entry = {}
                    values = parent_keys[parent_key]
                    new_entry['diagnosis']=parent_key
                    app_version = None
                    if 'appVersion' in row:
                        app_version = row['appVersion']
                    if(app_version!=None and app_version!=''):
                #   Remove any Other Characters that are non-numeric
                        app_version = int(''.join(d for d in app_version if d.isdigit()))
                    new_entry['appVersion'] = app_version
                    if 'facility' in row:
                        new_entry['facility'] = row['facility']
            
                    new_entry['uid'] = row['uid']

                    if 'ingested_at_admission' in row:
                        new_entry['ingested_at'] = row['ingested_at_admission']

                    if 'ingested_at' in row:
                        new_entry['ingested_at'] = row['ingested_at']
                    
                    
                    # iterate through child/inner keys
                    for child_key in values:
                        k, v = restructure_array(child_key,values[child_key])
                        new_entry[k] = v
                     # for each row add all the keys & values to a list
                    data_new.append(new_entry)
    return data_new

def get_fluids_key_values(data_raw):
    # Will store the final list of uid, ingested_at & reformed key-value pairs
    data_new = []
   
    for index, row in data_raw.iterrows():
        if "fluids" in row:
            
            # add uid and ingested_at first
            

            #Convert List to dictionary
            if row['fluids'] is not None and len(row['fluids'])> 0:
                parent_keys=reduce(lambda a, b: {**a, **b}, row['fluids'])
                
                # iterate through parent keys
                for parent_key in parent_keys:
                    new_entry = {}
                    values = parent_keys[parent_key]
                    new_entry['value']=parent_key
                    new_entry['is_fluid']=True
                    new_entry['is_drug']=False
                    app_version = None
                    if 'appVersion' in row:
                        app_version = row['appVersion']
                    if(app_version!=None and app_version!=''):
                #   Remove any Other Characters that are non-numeric
                        app_version = int(''.join(d for d in app_version if d.isdigit()))
                    new_entry['appVersion'] = app_version
                    if 'facility' in row:
                        new_entry['facility'] = row['facility']
            
                    new_entry['uid'] = row['uid']

                    if 'ingested_at_admission' in row:
                        new_entry['ingested_at'] = row['ingested_at_admission']

                    if 'ingested_at' in row:
                        new_entry['ingested_at'] = row['ingested_at']
                    
                    
                    # iterate through child/inner keys
                    for child_key in values:
                        k, v = restructure_array(child_key,values[child_key])
                        new_entry[k] = v
                     # for each row add all the keys & values to a list
                    data_new.append(new_entry)
    return data_new

def get_drugs_key_values(data_raw):
    # Will store the final list of uid, ingested_at & reformed key-value pairs
    data_new = []
   
    for index, row in data_raw.iterrows():
        if "drugs" in row:
            
            # add uid and ingested_at first
            

            #Convert List to dictionary
            if row['drugs'] is not None and len(row['drugs'])> 0:
                parent_keys=reduce(lambda a, b: {**a, **b}, row['drugs'])
                
                # iterate through parent keys
                for parent_key in parent_keys:
                    new_entry = {}
                    values = parent_keys[parent_key]
                    new_entry['value']=parent_key
                    new_entry['is_fluid']= False
                    new_entry['is_drug']= True
                    app_version = None
                    if 'appVersion' in row:
                        app_version = row['appVersion']
                    if(app_version!=None and app_version!=''):
                #   Remove any Other Characters that are non-numeric
                        app_version = int(''.join(d for d in app_version if d.isdigit()))
                    new_entry['appVersion'] = app_version
                    if 'facility' in row:
                        new_entry['facility'] = row['facility']
            
                    new_entry['uid'] = row['uid']

                    if 'ingested_at_admission' in row:
                        new_entry['ingested_at'] = row['ingested_at_admission']

                    if 'ingested_at' in row:
                        new_entry['ingested_at'] = row['ingested_at']
                    
                    
                    # iterate through child/inner keys
                    for child_key in values:
                        k, v = restructure_array(child_key,values[child_key])
                        new_entry[k] = v
                     # for each row add all the keys & values to a list
                    data_new.append(new_entry)
    return data_new

def sanitize_key(key: str) -> str:
    return re.sub(r'\W+', '_', key).strip('_')

def normalize_table_name(script:str,name: str) -> str:
    return re.sub(r'\s+', '', (script+'_'+name).strip().lower())

def format_repeatables_to_rows(df: pd.DataFrame, script: str) -> Dict[str, pd.DataFrame]:
    if df is None or df.empty:
        return {}

    tables = defaultdict(list)

    try:
        for _, row in df.iterrows():
            uid = row.get("uid")
            facility = row.get("facility")
            review_number = row.get("review_number")
            repeatables = row.get("repeatables")

            if not isinstance(repeatables, dict) or not repeatables:
                continue

            for table_name, entries in repeatables.items():
                if not isinstance(entries, list) or not entries:
                    continue

                normalized_table = normalize_table_name(script,table_name)

                for entry in entries:
                    if not isinstance(entry, dict):
                        continue

                    # Filter out entries missing 'id' or 'createdAt'
                    if not entry.get("id") or not entry.get("createdAt"):
                        continue

                    flat_row = {
                        "uid": uid,
                        "form_id": entry.get("id"),
                        "facility": facility,
                        "created_at": entry.get("createdAt"),
                        "review_number": review_number,
                        "script_table": f"{script}_{normalized_table}"
                    }

                    for key, value in entry.items():
                        if key in ["id", "createdAt", "requiredComplete", "hasCollectionField"]:
                            continue

                        sanitized_key = sanitize_key(key)
                        label_key = f"{sanitized_key}_label"

                        if isinstance(value, dict):
                            flat_row[sanitized_key] = value.get("value")
                            flat_row[label_key] = value.get("label")
                        else:
                            flat_row[sanitized_key] = value
                            flat_row[label_key] = value

                    tables[normalized_table].append(flat_row)

        return {table: pd.DataFrame(rows) for table, rows in tables.items()}

    except Exception as ex:
        logging.error(f"Error processing repeatables to table dict: {ex}")
        return {}