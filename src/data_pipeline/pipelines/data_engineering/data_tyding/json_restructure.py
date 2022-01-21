# This module manages different value cases
# A number of if statements have been used to manage different scenarios
# These include: MCL, empty lists and list with one element
# The input is the raw json data and the output is a reformed json (newentrieslist)
import re

def restructure(c, mcl):
    # branch to manage MCL
    if len(c['values']) > 1:
        v = {}
        current_v = c['values']
        # code to restructure MCL json file to key value pairs format
        for k, val in [(key, d[key]) for d in current_v for key in d]:
            if k not in v:
                v[k] = [val]
            else:
                v[k].append(val)
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

    #Check If Multi Value Column 
    if len(v['values']['label']) > 1:
        k = k
        v = v['values']
        mcl.append(k)

    else :
        k = str(k).strip()
        #  Unpack The Values Object To Get Single Values
        v = {'label':v['values']['label'][0],'value':v['values']['value'][0]}
        # #Add Other Values T MCL Columns For Exploding
        if str(k).endswith('Oth') or k=="AdmReason":
            mcl.append(k) 

    return k, v, mcl

def restructure_array(key,value):
    
    # Create columns acceptable by postgres

    if key is not None:
    
        k = re.sub('[^A-Za-z0-9_ ]+', '',str(key).replace('-','_').replace(" ",""))
        v = value

    return k, v
