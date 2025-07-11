import json
import logging 
import requests
import os
from conf.common.config import config
import os
import json
from collections import OrderedDict
from pathlib import Path


def download_file(url: str, filename: str) -> bool:
    """Download a file from URL and save it locally."""
    
    try:
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()
        
        # Ensure directory exists
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive chunks
                    f.write(chunk)
        return True
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Download failed: {type(e).__name__} - {e}")
        return False


def convert_json_to_dict(filename):
    with open(filename, 'r') as file:
        return json.load(file)

def get_script(script_type):
    filename = f'conf/local/scripts/{script_type}.json'
    
    if os.path.exists(filename): 
        script_data = convert_json_to_dict(filename)
        # Process the data to extract unique fields with data types
        fields_dict = OrderedDict()
        
        for screen in script_data.get('screens', []):
            for field in screen.get('fields', []):
                if 'dataType' in field and 'key' in field:
                    # Use the key as the dictionary key to avoid duplicates
                    fields_dict[field['key']] = {
                        'key': field['key'],
                        'dataType': field['dataType']
                    }
        
        # Convert the ordered dictionary values to a list
        return list(fields_dict.values())
    else:
        params = config() 
        webeditor = params['webeditor'] 
        url = f'{webeditor}/scripts/{script_type}/metadata'
         
        download_file(url, filename)
        # After downloading, process it the same way
        return get_script(script_type)
