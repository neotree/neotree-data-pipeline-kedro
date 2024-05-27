
from  conf.common.scripts import get_script
import logging

def fix_data_label(key,value,script): 
    try: 
        script_json = get_script(script) 
        if(script_json is not None): 
            item = script_json.get(key) 
            if item: 
                if value in item["values"]: 
                    value_index = item["values"].index(value) 
                    if isinstance(value_index, int) and value_index>=0: 
                        label = item["labels"][value_index]
                        return label
            else: 
                return "Undefined"
            
        return "Undefined"
    except Exception as ex: 
        logging.error("**********"+str(key)+"-----"+str(value)+"+++++"+str(script))
        return "Undefined"
        
def fix_data_value(key,label,script): 
    try:
        script_json = get_script(script)
        if(script_json is not None):
            item = script_json.get(key)
            if item:
                if label in item["labels"]:
                    value_index = item["labels"].index(label)
                    if isinstance(value_index, int) and value_index>=0:
                        value = item["values"][value_index]
                        return value
        return None
    except Exception as ex:
        logging.error("**********"+str(key)+"-----"+str(value)+"+++++"+str(script))

def fix_multiple_data_label(key,value,script): 
    try:
        script_json = get_script(script)
        if(script_json is not None):
            item = script_json.get(key)
            if item:
                values = []
                for v in value:
                    if v in item["values"]:
                        value_index = item["values"].index(v)
                        if isinstance(value_index, int) and value_index>=0:
                            values.append(item["labels"][value_index])
                return values
        return "Undefined"
    except Exception as ex:
        logging.error("**********"+str(key))
        return "Undefined"