# This module manages different value cases
# A number of if statements have been used to manage different scenarios
# These include: MCL, empty lists and list with one element
# The input is the raw json data and the output is a reformed json (newentrieslist)
import logging
from conf.common.format_error import formatError
import re

def restructure(c, mcl):
    # branch to manage MCL
    if len(dict(c['values'])) > 1:
        v = {}
        current_v = dict(c['values'])
        logging.info("=====I HAVE PASSED===="+str(current_v))
        
        # code to restructure MCL json file to key value pairs format
        for k, val in [(key, d[key]) for d in current_v for key in d]:
            logging.info("=====I KEYED ===="+str(k)+"==VAL-")
            if k not in v:
                v[k] = [val]
            else:
                v[k].append(val)
        k = dict(c)['key']
        mcl.append(k)

    # branch to cater for empty values
    elif len(dict(c)['values']) == 0:
        k = dict(c)['key']
        v = dict(c)['values']

    # branch to extract single entry values
    else:
        k = dict(c)['key']
        v = dict(c)['values'][0]
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
        logging.error(v)
        logging.error(formatError(ex))

def restructure_array(key,value):
    
    # Create columns acceptable by postgres

    if key is not None:
    
        k = re.sub('[^A-Za-z0-9_ ]+', '',str(key).replace('-','_').replace(" ",""))
        v = value

    return k, v
