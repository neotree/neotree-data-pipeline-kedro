
from  conf.common.scripts import get_script

def fix_data_label(key,value,script):
    
    script = get_script(script)
    if(script is not None):
        item = script.get(key)
        if item:
            value_index = item["values"].index(value)
            label = item["labels"][value_index]
            return label
    return None