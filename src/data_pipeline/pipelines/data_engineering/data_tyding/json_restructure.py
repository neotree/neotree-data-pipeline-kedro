# This module manages different value cases
# A number of if statements have been used to manage different scenarios
# These include: MCL, empty lists and list with one element
# The input is the raw json data and the output is a reformed json (newentrieslist)
import logging
from conf.common.format_error import formatError
import re
import traceback


def restructure(c, mcl):
    # branch to manage MCL
    c= dict(c)
    if len(c['values']) > 1:
        v = {}
        current_v = c['values']
        
        # code to restructure MCL json file to key value pairs format
        for k in current_v:
            logging.info("====K==="+str(k))
            logging.info("====KIN==="+str(k in v.keys()))
            if k not in v.keys():
                v[k] = [current_v[k]]
            else:
                v[k].append(current_v[k])
        k = c['key']
        mcl.append(k)

    # branch to cater for empty values
    elif len(c['values']) == 0:
        k = c['key']
        v = c['values']

    # branch to extract single entry values
    else:
        k = c['key']
        v = c['values'][0]
        #Add Other Values T MCL Columns For Exploding and Adm Reason
        if str(k).endswith('Oth') or k=="AdmReason":
            mcl.append(k)
    return k, v, mcl

    #Restructure New Formated Data
def restructure_new_format(k,v,mcl):
    try:
        #Check If Multi Value Column 
        if len(v['values']['label']) > 1:
            k = k
            v = v['values']
            mcl.append(k)

        else :
            if len(v['values']['label'])>0 and len(v['values']['value'])>0:
                k = str(k).strip()
                #  Unpack The Values Object To Get Single Values
                v = {'label':v['values']['label'][0],'value':v['values']['value'][0]}
                # #Add Other Values T MCL Columns For Exploding
                if str(k).endswith('Oth') or k=="AdmReason":
                    mcl.append(k) 
        

        return k, v, mcl
    except Exception as ex:
        logging.error(formatError(ex))

def restructure_array(key,value):
    
    # Create columns acceptable by postgres

    if key is not None:
    
        k = re.sub('[^A-Za-z0-9_ ]+', '',str(key).replace('-','_').replace(" ",""))
        v = value

    return k, v
