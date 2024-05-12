import json


def get_script(script_type):

    if(script_type=='admission'):
        return convert_json_to_dict('conf/common/admission.json')   
    elif(script_type=='discharge'):
        return convert_json_to_dict('conf/common/discharge.json')
    elif(script_type=='maternity'):
        return convert_json_to_dict('conf/common/maternity.json')
    elif(script_type=='baseline'):
        return convert_json_to_dict('conf/common/baseline.json')
    elif(script_type=='lab'):
        return convert_json_to_dict('conf/common/lab.json')
    else:
        return None

def convert_json_to_dict(file_path):
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
            
            converted_data = {}
            for item in data:
                key = item["key"]
                del item["key"]
                converted_data[key] = item
            
            return converted_data
        except FileNotFoundError:
            print(f">>>>>>>>>>> File '{file_path}' not found.")
            return None
