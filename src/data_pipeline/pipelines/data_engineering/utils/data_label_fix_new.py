
from  conf.common.scripts import get_script
import logging

def bulk_fix_data_labels(script_id):
    entries = get_script(script_id) 
    
    commands = []
    
    if(entries is not None): 
          
        for key, value in entries.items():
            values = value['values']
            labels = value['labels']
            
            for index,label in enumerate(labels):
                value =  str(values[index]).replace("'","''")
                label = str(label).replace("'","''").strip()
               
                command = f'''UPDATE public.clean_sessions
                    SET data = jsonb_set(
                        data,
                        '{{entries,{key},values,label}}',
                        '["{label}"]',
                        true
                    )
                    WHERE data->'entries'->'{key}'->'values'->>'label' = '["None"]' 
                    AND data->'entries'->'{key}'->'values'->>'value' = '["{value}"]' 
                    AND scriptid = '{script_id}';;'''
                
                commands.append(command)
                
                command = f'''UPDATE public.clean_sessions
                    SET data = jsonb_set(
                        data,
                        '{{entries,{key},values,label}}',
                        '["{label}"]',
                        true
                    )
                    WHERE data->'entries'->'{key}'->'values'->>'label' = '["None"]' 
                    AND data->'entries'->'{key}'->'values'->>'value' = '["{label}"]' 
                    AND scriptid = '{script_id}';;'''
                
                commands.append(command)
            
    return commands

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
                # new code    
                else: 
                    return value 
                      
            else: 
                return value
            
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
                # new code
                else:
                    return label
            else:
                return label
            
        return None
    except Exception as ex:
        logging.error("**********"+str(key)+"-----"+str(value)+"+++++"+str(script))

def fix_multiple_data_label(key,value,script,uid): 
    #logging.info(f'fix_multiple_data_label >>>>>>>>>>>>>>>>>>>>> {key} {value} {uid}<<<<<<<<<<<<<<<<<<<<<<<<')
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
                #logging.info(f'Returning values: {values}')
                return values
        return "Undefined"
    except Exception as ex:
        logging.error("**********"+str(key))
        return "Undefined"