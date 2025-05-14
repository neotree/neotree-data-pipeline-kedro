# This module extracts the key-value pairs within a raw json file.
import logging
from conf.common.format_error import formatError
from .json_restructure import restructure, restructure_new_format, restructure_array
from functools import  reduce
import pandas as pd
from datetime import datetime
import re


def get_key_values(data_raw):
   
    if not data_new:
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

            if 'started_at' in row:
                new_entry['started_at'] = row['started_at']

            if 'completed_at' in row:
                new_entry['completed_at'] = row['completed_at']
                
            if 'unique_key' in row:
                new_entry['unique_key'] = row['unique_key']
            
            if 'ingested_at' in row:
                new_entry['ingested_at'] = row['ingested_at']
                new_entry['ingested_at'] = pd.to_datetime(row['ingested_at'], format='%Y-%m-%dT%H:%M:%S').tz_localize(None)
            if 'review_number' in row:
                new_entry['review_number'] = row['review_number']


        # iterate through key, value and add to dict
            for c in row['entries']:
                 
                #RECORDS FORMATTED WITH NEW FORMAT, CONTAINS THE jsonFormat Key and C is the Key
                if('key' not in c) or (app_version!='' and app_version!=None and (app_version>454 or int(str(app_version)[:1])>=5)): 
                    try:            
                        k, v, mcl = restructure_new_format(c,row['entries'][c], mcl)
                    #SET UID FOR ZIM DISCHARGES WHICH COME WITH NULL UID NEW FORMAT
                        if((k=='NeoTreeID' or k=='NUID_BC' or k=='NUID_M' or k=='NUID_S') and new_entry['uid'] is None):
                            new_entry['uid'] = v.value;
                    except Exception:
                        logging.info("RESTRUCTURING ERROR ON RECORD WITH UID"+str(row['uid']))

            #ELSE USE THE OLD FORMAT
                else:
                    k, v, mcl = restructure(c, mcl)
                        
                #SET UID FOR ZIM DISCHARGES WHICH COME WITH NULL UID OLD FORMAT
                if((k=='NeoTreeID' or k=='NUID_BC'or k=='NUID_M' or k=='NUID_S') and new_entry['uid'] is None):
                        new_entry['uid'] = v.value;
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

from typing import Dict, Any

from typing import Dict, Any
import re

def sanitize_key(key: str) -> str:
    return re.sub(r'\W+', '_', key).strip('_')

def format_repeatables_to_rows(data: Dict[str, Any],script) -> Dict[str, list]:
    result = {}
    uid = data.get("uid")
    hospital_id = data.get("hospital_id")
    facility = data.get("facility")
    review_number = data.get("review_number")
    repeatables = data.get("repeatables", {})

    for table_name, entries in repeatables.items():
        result[table_name] = []
        for entry in entries:
            row = {
                "uid": uid,
                "hospital_id": hospital_id,
                "form_id": entry.get("id"),
                "facility": facility,
                "created_at": entry.get("createdAt"),
                "review_number": review_number
            }

            for key, value in entry.items():
                if key in ["id", "createdAt", "requiredComplete", "hasCollectionField"]:
                    continue

                if isinstance(value, dict):
                    val = value.get("value")
                    label = value.get("label")
                    row[sanitize_key(key)] = val       
                    label_key = sanitize_key(f"{key}_label")
                    row[label_key] = label
                else:
                    row[sanitize_key(key)] = value
                    label_key = sanitize_key(f"{key}_label")
                    row[label_key] = value

            result[script+"_"+table_name].append(row)
    return result

def sanitize_key(key: str) -> str:
    return re.sub(r'\W+', '_', key).strip('_').lower()