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


def load_processed_script(script_type: str) -> OrderedDictType[str, Dict[str, str]]:
    filename = f'conf/local/scripts/{script_type}.json'
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            items = json.load(file)
            return OrderedDict(items)
    return None

import os
import json
from collections import OrderedDict
from typing import Dict, OrderedDict as OrderedDictType
import logging

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_and_save_script(script_type: str, raw_data: dict) -> OrderedDictType[str, Dict[str, str]]:
    filename = f'conf/local/scripts/{script_type}.json'
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    # Process the data
    fields_dict = OrderedDict()
    for screen in raw_data.get('screens', []):
        for field in screen.get('fields', []):
            if 'dataType' in field and 'key' in field:
                fields_dict[field['key']] = {
                    'key': field['key'],
                    'dataType': field['dataType']
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
    url = f"{params['webeditor']}/scripts/{script_type}/metadata"
    filename = f'conf/local/scripts/{script_type}.json'
    logging.info(f"MY URL=={url}")
    # Download directly to the file
    download_file(url, filename)
    
    # Now process the downloaded file
    with open(filename, 'r') as file:
        raw_data = json.load(file)
    
    return process_and_save_script(script_type, raw_data)

def get_script(script_type: str) -> OrderedDictType[str, Dict[str, str]]:
    processed_data = load_processed_script(script_type)
    if processed_data is not None:
        return processed_data
  
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
    Process dataframe columns with special handling for .value/.label columns,
    while preserving original columns exactly as they are (just lowercasing names).
    
    Args:
        df: Input dataframe with potential .value/.label columns
        merged_data: Dictionary with type information {key: {'dataType': type}}
        
    Returns:
        Processed dataframe with formatted columns and values
    """
    processed_df = df.copy()
    columns_to_process = {}
    columns_to_drop = set()
    
    # Only process columns containing dots (.)
    for col in processed_df.columns:
        if '.' in col:
            base_key, suffix = col.split('.', 1)
            if base_key in merged_data:
                data_type = merged_data[base_key].get('dataType', '').lower()
                
                if suffix == 'value':
                    new_key = base_key.lower()
                    if data_type in ['dropdown', 'single_select_option', 'multi_select_option', 'period']:
                        columns_to_process[new_key] = processed_df[col].astype(str)
                    elif data_type == 'boolean':
                        bool_map = {'y': True, 'yes': True, 'true': True, True: True,
                                  'n': False, 'no': False, 'false': False, False: False}
                        columns_to_process[new_key] = (
                            processed_df[col].astype(str).str.lower().map(bool_map).fillna(False)
                        )
                    elif data_type in ['number', 'integer', 'float']:
                        columns_to_process[new_key] = pd.to_numeric(processed_df[col], errors='coerce')
                    elif data_type in ['datetime', 'timestamp', 'date']:
                        columns_to_process[new_key] = pd.to_datetime(processed_df[col], errors='coerce')
                    else:
                        columns_to_process[new_key] = processed_df[col].astype(str)
                    columns_to_drop.add(col)
                    
                elif suffix == 'label':
                    if data_type in ['dropdown', 'single_select_option', 'multi_select_option']:
                        new_key = f"{base_key.lower()}_label"
                        columns_to_process[new_key] = processed_df[col]
                    columns_to_drop.add(col)
    
    # For regular columns (no dots), keep exactly as they are (just lowercase names)
    for col in processed_df.columns:
        if '.' not in col and col not in columns_to_drop:
            columns_to_process[col.lower()] = processed_df[col]
    
    # Create new dataframe with processed columns
    result_df = pd.DataFrame(columns_to_process)

    return result_df