# This module extracts the key-value pairs within a raw json file.
import logging
from conf.common.format_error import formatError
from .json_restructure import restructure, restructure_new_format, restructure_array
from functools import  reduce
import sys
import traceback

def get_key_values(data_raw):
    mcl = []
    # Will store the final list of uid, ingested_at & reformed key-value pairs
    data_new = []
    for index, row in data_raw.iterrows():
        # to store all the restructured keys & values for each row
        try:
            new_entry = {}
            # add uid and ingested_at first
            app_version = None
            if 'appVersion' in row:
                app_version = row['appVersion']
            if(app_version!=None and app_version!=''):
                #Remove any Other Characters that are non-numeric
                app_version = int(''.join(d for d in app_version if d.isdigit()))
            if 'facility' in row:
                new_entry['facility'] = row['facility']
            
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
            
            if 'ingested_at' in row:
             new_entry['ingested_at'] = row['ingested_at']


        # iterate through key, value and add to dict
            for c in row['entries']:
           
                #RECORDS FORMATTED WITH NEW FORMAT, CONTAINS THE jsonFormat Key and C is the Key
                if(app_version!='' and app_version!=None and (app_version>454 or int(str(app_version)[:1])>=5)):             
                    k, v, mcl = restructure_new_format(c,row['entries'][c], mcl)
                    #SET UID FOR ZIM DISCHARGES WHICH COME WITH NULL UID NEW FORMAT
                    if((k=='NeoTreeID' or k=='NUID_BC' or k=='NUID_M' or k=='NUID_S') and new_entry['uid'] is None):
                        new_entry['uid'] = v.value;

            #ELSE USE THE OLD FORMAT
                else:
                    try:
                        k, v, mcl = restructure(c, mcl)
                    except Exception:
                        logging.info("===CORRUPT==="+str(row['uid']))
                        traceback.print_exc()
                        sys.exit(1)
                #SET UID FOR ZIM DISCHARGES WHICH COME WITH NULL UID OLD FORMAT
                if((k=='NeoTreeID' or k=='NUID_BC'or k=='NUID_M' or k=='NUID_S') and new_entry['uid'] is None):
                        new_entry['uid'] = v.value;
                new_entry[k] = v
        # for each row add all the keys & values to a list
         
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