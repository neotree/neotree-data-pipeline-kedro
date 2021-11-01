# This module extracts the key-value pairs within a raw json file.
import logging
from .json_restructure import restructure, restructure_new_format, restructure_array
from functools import reduce

def get_key_values(data_raw):
    mcl = []
    # Will store the final list of uid, ingested_at & reformed key-value pairs
    data_new = []
    for index, rows in data_raw.iterrows():
        # to store all the restructured keys & values for each row
        new_entries = {}
        # add uid and ingested_at first
        app_version = None
        if 'appVersion' in rows:
            app_version = rows['appVersion']
        if(app_version!=None and app_version!=''):
            #Remove any Other Characters that are non-numeric
            app_version = int(''.join(d for d in app_version if d.isdigit()))
        if 'facility' in rows:
            new_entries['facility'] = rows['facility']
        
        new_entries['uid'] = rows['uid']
        if 'ingested_at_admission' in rows:
            new_entries['ingested_at'] = rows['ingested_at_admission']
        if 'ingested_at_discharge' in rows:
            new_entries['ingested_at'] = rows['ingested_at_discharge']

        if 'started_at' in rows:
            new_entries['started_at'] = rows['started_at']

        if 'started_at' in rows:
            new_entries['started_at'] = rows['started_at']

        if 'completed_at' in rows:
             new_entries['completed_at'] = rows['completed_at']


        # iterate through key, value and add to dict
        for c in rows['entries']:
           
            #RECORDS FORMATTED WITH NEW FORMAT, CONTAINS THE jsonFormat Key and C is the Key
            if(app_version!='' and app_version!=None and app_version>454):   
                k, v, mcl = restructure_new_format(c,rows['entries'][c], mcl)
                #SET UID FOR ZIM DISCHARGES WHICH COME WITH NULL UID NEW FORMAT
                if((k=='NeoTreeID' or k=='NUID_BC' or k=='NUID_M' or k=='NUID_S') and new_entries['uid'] is None):
                     new_entries['uid'] = v.value;

            #ELSE USE THE OLD FORMAT
            else:
               k, v, mcl = restructure(c, mcl)
               if(k== 'DateBCT' or k == 'DateBCR') and rows['uid'] == '0028-0386':
                   logging.info('---EDDSS---'+str(k)+ "--sRT--"+str(v))
               #SET UID FOR ZIM DISCHARGES WHICH COME WITH NULL UID OLD FORMAT
               if((k=='NeoTreeID' or k=='NUID_BC'or k=='NUID_M' or k=='NUID_S') and new_entries['uid'] is None):
                     new_entries['uid'] = v.value;
            new_entries[k] = v
        # for each row add all the keys & values to a list
          
        data_new.append(new_entries)

    return data_new, set(mcl)

def get_diagnoses_key_values(data_raw):
    # Will store the final list of uid, ingested_at & reformed key-value pairs
    data_new = []
   
    for index, rows in data_raw.iterrows():
        if "diagnoses" in rows:
            new_entries = {}
            # add uid and ingested_at first
            app_version = None
            if 'appVersion' in rows:
                app_version = rows['appVersion']
            if(app_version!=None and app_version!=''):
                #Remove any Other Characters that are non-numeric
                app_version = int(''.join(d for d in app_version if d.isdigit()))
            if 'facility' in rows:
                new_entries['facility'] = rows['facility']
            
            new_entries['uid'] = rows['uid']
            if 'ingested_at_admission' in rows:
                new_entries['ingested_at'] = rows['ingested_at_admission']

            if 'ingested_at' in rows:
                new_entries['ingested_at'] = rows['ingested_at']

            #Convert List to dictionary
            if rows['diagnoses'] is not None and len(rows['diagnoses'])> 0:
                values_dict=reduce(lambda a, b: {**a, **b}, rows['diagnoses'])

                # iterate through parent keys
                for parent_key in values_dict:
                    
                    values = values_dict[parent_key]
                    # iterate through child/inner keys
                    for child_key in values:
                        k, v = restructure_array(parent_key,values[child_key],child_key)

                        new_entries[k] = v
                # for each row add all the keys & values to a list
                
                data_new.append(new_entries)
    return data_new