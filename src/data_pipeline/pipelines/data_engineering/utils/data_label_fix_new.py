
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
                    if isinstance(value_index, int):
                        label = item["labels"][value_index]
                        return label
        return None
    except Exception as ex:
        logging.error("**********"+str(key)+"-----"+str(value)+"+++++"+str(script))