import json
import logging 
import requests
import os
from conf.common.config import config

def get_script(script_type):
    
    # filename = f'conf/common/scripts/{script_type}.json'
    filename = f'conf/local/scripts/{script_type}.json'
     
    if os.path.exists(filename): 
        
        return convert_json_to_dict(filename)
    else:
        params = config()
        logging.info(f'The file {filename} does not exist.')
        
        webeditor = params['webeditor']
        logging.info(f'deownload file from server {webeditor}')
        # url=f'https://zim-webeditor.neotree.org:10243/script-labels?scriptId={script_type}'
        url=f'{webeditor}/script-labels?scriptId={script_type}'
        logging.info(url)
        download_file(url, filename)

    return None

def convert_json_to_dict(filename):
        try:
            with open(filename, 'r') as file:
                data = json.load(file)
            
            converted_data = {}
            for item in data:
                key = item["key"]
                del item["key"]
                converted_data[key] = item
            
            return converted_data
        except FileNotFoundError:
            print(f">>>>>>>>>>> File '{filename}' not found.")
            return None
        except Exception:
            logging.info(f'error in {filename}')
            return None

def download_file(url, filename):
    try:
        # Send a GET request to the URL
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Check for HTTP errors

        # Open the file in binary-write mode and write the content
        with open(filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        print(f"File downloaded successfully and saved to {filename}")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")  