import pandas as pd
import json
import logging 
import requests
import os
from conf.common.config import config
import json
from collections import OrderedDict
from pathlib import Path
from typing import Optional, OrderedDict as OrderedDictType,Dict


# def download_file(url: str, filename: str) -> bool:
#     """Download a file from URL and save it locally."""
    
#     try:
#         response = requests.get(url, stream=True, timeout=10)
#         response.raise_for_status()
        
#         # Ensure directory exists
#         Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
#         with open(filename, 'wb') as f:
#             for chunk in response.iter_content(chunk_size=8192):
#                 if chunk:  # filter out keep-alive chunks
#                     f.write(chunk)
#         return True
    
#     except requests.exceptions.RequestException as e:
#         logging.error(f"Download failed: {type(e).__name__} - {e}")
#         return False
    
def download_file(url: str, filename: str, api_key: str) -> bool:
    """Download a JSON response and save it properly (not raw HTML)."""
    headers = {
        'x-api-key': api_key,
        'Accept': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status() # Raises HTTP errors (4xx/5xx)
        json_data = response.json()

        # Ensure directory exists
        Path(filename).parent.mkdir(parents=True, exist_ok=True)

        # Save pretty-printed JSON to file
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)

        return True

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {type(e).__name__} - {e}")
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON: {e} (Is the server returning HTML instead?)")
    return False


def load_processed_script(script_type: str) -> OrderedDictType[str, Dict[str, str]]:
    filename = f'conf/local/scripts/{script_type}.json'
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            items = json.load(file)
            return OrderedDict(items)
    return None
# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_and_save_script(script_type: str, raw_data: dict) -> OrderedDictType[str, Dict[str, str]]:
    filename = f'conf/local/scripts/{script_type}.json'
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    # Process the data
    fields_dict = OrderedDict()
    if raw_data.get('data'):
        for entry in raw_data['data']:
            screens = entry.get('screens', [])
            for screen in screens:
                fields = screen.get('fields', [])
                for field in fields:
                    # Ensure it's a dict and has necessary keys
                    if isinstance(field, dict) and 'key' in field and 'dataType' in field:
                        key = field['key']
                        data_type = field['dataType']
                        # Only add the key if it's not already in the dict
                        if key not in fields_dict:
                            fields_dict[key] = {
                                'key': key,
                                'dataType': data_type
                            }
    # Convert to JSON and validate before saving
    try:
        json_data = list(fields_dict.items())
        # Validate by serializing and deserializing
        json_string = json.dumps(json_data)
        json.loads(json_string)  # This will raise ValueError if invalid
        
        # Save to file
        with open(filename, 'w') as file:
            file.write(json_string)
        logger.info(f"Successfully saved script data to {filename}")
        
    except ValueError as e:
        logger.error(f"Invalid JSON data generated for {script_type}: {str(e)}")
        logger.error("Skipping file save due to invalid JSON data")
    except Exception as e:
        logger.error(f"Unexpected error while processing {script_type}: {str(e)}")
        logger.error("Skipping file save due to unexpected error")
    
    return fields_dict

def download_script(script_type: str) -> OrderedDictType[str, Dict[str, str]]:
    """Download script and return processed data."""
    params = config()
    data = {
    'scriptsIds': [script_type.strip('"')],
    'returnDraftsIfExist': True
   }
    api_key = params['webeditor_api_key']
    url = f"{params['webeditor']}/api/scripts/metadata?data={json.dumps(data)}"
    filename = f'conf/local/scripts/{script_type}.json'
    # Download directly to the file
    download_file(url, filename,api_key)
    
    # Now process the downloaded file
    with open(filename, 'r') as file:
        raw_data = json.load(file)
    
    return process_and_save_script(script_type, raw_data)

def get_script(script_type: str) -> OrderedDictType[str, Dict[str, str]]:
  
    return download_script(script_type)

def merge_script_data(
    existing_data: Optional[OrderedDictType[str, Dict[str, str]]], 
    new_data: OrderedDictType[str, Dict[str, str]]
) -> OrderedDictType[str, Dict[str, str]]:
    """
    Merge script data with priority to existing data.
    
    Args:
        existing_data: Output from previous merge (None if first run)
        new_data: Output from get_script() (contains fresh data)
        
    Returns:
        Merged OrderedDict with existing data taking precedence
    """
    if existing_data is None:
        return new_data
    
    # Create merged result (existing_data has priority)
    merged = OrderedDict(existing_data)
    for key, value in new_data.items():
        if key not in merged:
            merged[key] = value
    
    return merged

def merge_two_script_outputs(
    output1: OrderedDictType[str, Dict[str, str]],
    output2: OrderedDictType[str, Dict[str, str]]
) -> OrderedDictType[str, Dict[str, str]]:
    """
    Merge two merge_script_data outputs into one.
    Priority is given to output1's values when keys conflict.
    
    Args:
        output1: First merged script data (has priority)
        output2: Second merged script data
        
    Returns:
        Combined OrderedDict with output1's values taking precedence
    """
    merged = OrderedDict(output1)
    for key, value in output2.items():
        if key not in merged:
            merged[key] = value
    return merged


def process_dataframe_with_types(
    df: pd.DataFrame, 
    merged_data: Dict[str, Dict[str, str]]
) -> pd.DataFrame:
    """
    Process dataframe columns using metadata from merged_data.
    Handles .value and .label suffixes and preserves base columns (no dot).
    
    Args:
        df: Input dataframe
        merged_data: Dictionary {key: {'dataType': type}}
        
    Returns:
        Processed dataframe with renamed and type-coerced columns
    """
    processed_df = df.copy()
    columns_to_process = {}
    columns_to_drop = set()

    for col in processed_df.columns:
        if '.' in col:
            base_key, suffix = col.split('.', 1)
            meta = merged_data.get(base_key)
            if not meta:
                continue  # skip if key not in metadata
            data_type = (meta.get('dataType') or '').lower()
            new_key = base_key.lower()

            if suffix == 'value':
                if data_type in ['dropdown', 'single_select_option', 'period']:
                    columns_to_process[new_key] = processed_df[col].astype(str)
                elif data_type == 'multi_select_option':
                    columns_to_process[new_key] = df[col].astype(str).apply(clean_to_jsonb_array)  

                elif data_type == 'boolean':
                    bool_map = {
                        'y': True, 'yes': True, 'true': True, '1': True, True: True,
                        'n': False, 'no': False, 'false': False, '0': False, False: False
                    }
                    columns_to_process[new_key] = (
                        processed_df[col].astype(str).str.strip().str.lower().map(bool_map).fillna(False)
                    )
                elif data_type in ['number', 'integer', 'float']:
                    columns_to_process[new_key] = pd.to_numeric(processed_df[col], errors='coerce')
                elif data_type in ['datetime', 'timestamp', 'date']:
                    columns_to_process[new_key] = pd.to_datetime(processed_df[col], errors='coerce')
                else:
                    columns_to_process[new_key] = processed_df[col].astype(str)
                columns_to_drop.add(col)

            elif suffix == 'label' and data_type in ['dropdown', 'single_select_option','multi_select_option']:
                label_key = f"{new_key}_label"
                if data_type == 'multi_select_option':
                    columns_to_process[label_key] = processed_df[col].astype(str).apply(clean_to_jsonb_array) 
                else:
                    columns_to_process[label_key] = processed_df[col].astype(str)
                columns_to_drop.add(col)

            ### DROP COLUMN NAMES WITH NONE AS COLUMN NAME
            if 'none' in str(col).lower():
                columns_to_drop.add(col)
        else:
            # If it's a base column and not marked for drop, include it
            if col not in columns_to_drop:
                logging.info(f"###MY BASE {col}")
                columns_to_process[col.lower().strip()] = processed_df[col]

    return pd.DataFrame(columns_to_process)

def clean_to_jsonb_array(val):
 
    if isinstance(val, str) and val.startswith("{") and val.endswith("}"):
        inner = val[1:-1]
        return f"[{','.join(x.strip() for x in inner.split(','))}]"
    return val