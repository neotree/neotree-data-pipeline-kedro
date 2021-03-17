# This module extracts the key-value pairs within a raw json file.
from .json_restructure import restructure, restructure_new_format

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
        
        new_entries['uid'] = rows['uid']
        if 'ingested_at_admission' in rows:
            new_entries['ingested_at'] = rows['ingested_at_admission']
        if 'ingested_at_discharge' in rows:
            new_entries['ingested_at'] = rows['ingested_at_discharge']

        # iterate through key, value and add to dict
        for c in rows['entries']:
           
            #RECORDS FORMATTED WITH NEW FORMAT, CONTAINS THE jsonFormat Key and C is the Key
            if(app_version!='' and app_version!=None and app_version>454):   
                k, v, mcl = restructure_new_format(c,rows['entries'][c], mcl)
                #SET UID FOR ZIM DISCHARGES WHICH COME WITH NULL UID NEW FORMAT
                if(k=='NeoTreeID' and new_entries['uid'] is None):
                     new_entries['uid'] = v.value;

            #ELSE USE THE OLD FORMAT
            else:
               k, v, mcl = restructure(c, mcl)
               #SET UID FOR ZIM DISCHARGES WHICH COME WITH NULL UID OLD FORMAT
               if(k=='NeoTreeID' and new_entries['uid'] is None):
                     new_entries['uid'] = v.value;
            new_entries[k] = v
        # for each row add all the keys & values to a list
        data_new.append(new_entries)

    return data_new, set(mcl)


