import json
import os
from conf.base.catalog import params,hospital_conf
from conf.common.scripts import get_raw_json
import pandas as pd
import logging

def process_and_save_field_info(script, json_data):

    filename =  filename = f'conf/local/scripts/{script}.json'
    
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            result = json.load(f)
        # Convert list back to dictionary for easier processing
        result = {item['key']: item for item in result}
    else:
        result = {}

    for screen in json_data:
        logging.info(f'MY SCRIPT::{script}::MY JSON: {json_data}')
        if "fields" not in screen:
            continue
            
        for field in screen["fields"]:
            key = field["key"]
            field_type = field["type"]
            label = field["label"]
            
            # Initialize the entry if it doesn't exist
            if key not in result:
                result[key] = {
                    "key": key,
                    "type": field_type,
                    "label": label,
                    "options": []
                }
            
            # Add options if they exist
            if "value" in field and "valueLabel" in field:
                value = field["value"]
                value_label = field["valueLabel"]
                
                if value is not None and value_label is not None:
                    # Check if this option already exists
                    option_exists = any(
                        opt["value"] == value and opt["valueLabel"] == value_label 
                        for opt in result[key]["options"]
                    )
                    
                    if not option_exists:
                        result[key]["options"].append({
                            "value": value,
                            "valueLabel": value_label
                        })
    
    # Convert result back to list for saving
    final_result = list(result.values())
    
    # Save to file
    with open(filename, 'w') as f:
        json.dump(final_result, f, indent=2)
    

def update_fields_info(script: str):
    hospital_scripts = hospital_conf()

    if not hospital_scripts:
        return None

    for hospital in hospital_scripts:
        script_id_entry = hospital_scripts[hospital].get(script, '')
        if not script_id_entry:
            continue

        script_ids = str(script_id_entry).split(',')

        for script_id in script_ids:
            script_id = script_id.strip()
            if not script_id:
                continue

            script_json = get_raw_json(script_id)
            process_and_save_field_info(script,script_json)


def load_json_for_comparison(filename):

    try:
        file_path = f'conf/scripts/{filename}.json'
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                data = json.load(f)
            return data
        return None
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: File '{filename}' contains invalid JSON.")
        return None


def merge_json_files(file1_path, file2_path):
    file_path1= f'conf/scripts/{file1_path}.json'
    file_path2= f'conf/scripts/{file2_path}.json'
    if os.path.exists(file_path1) and  os.path.exists(file2_path):
        with open(file_path1, 'r') as f1, open(file_path2, 'r') as f2:
            data1 = json.load(f1)
            data2 = json.load(f2)

        merged = dict(data1)
        for k, v in data2.items():
            if k not in merged:
                merged[k] = v

        return merged
    return None


def transform_matching_labels(df, script):
    json_data = load_json_for_comparison(script)
    if json_data is None:
        return df
    
    field_info = {item['key']: item for item in json_data}
    transformed_df = df.copy()
   
    for value_col in [col for col in df.columns if col.endswith('.value')]:
        base_key = value_col[:-6]
        label_col = f"{base_key}.label"
        
        if label_col in df.columns:
            # Set label to null where value is null
            null_mask = transformed_df[value_col].isna()
            transformed_df.loc[null_mask, label_col] = None
    
    # Existing transformation logic
    for value_col in [col for col in df.columns if col.endswith('.value')]:
        base_key = value_col[:-6]
        label_col = f"{base_key}.label"
        
        if label_col not in df.columns:
            continue
            
        field = field_info.get(base_key)
        if field:
            json_label = field['label']
            field_type = field.get('type', '')
            options = field.get('options', [])
            value_to_label = {opt['value']: opt['valueLabel'] for opt in options}
            
            # Skip rows where value is null (already handled in critical section)
            non_null_mask = (transformed_df[value_col].notna()) & (transformed_df[label_col] == json_label)
            
            if field_type in ('multi_select', 'checklist'):
                transformed_df.loc[non_null_mask, label_col] = transformed_df.loc[non_null_mask, value_col].apply(
                    lambda x: ','.join([
                        value_to_label.get(v.strip(), v.strip()) 
                        for v in str(x).split(',') if v.strip()
                    ]) if pd.notna(x) else x
                )
            elif options:
                transformed_df.loc[non_null_mask, label_col] = transformed_df.loc[non_null_mask, value_col].map(value_to_label)
            else:
                transformed_df.loc[non_null_mask, label_col] = transformed_df.loc[non_null_mask, value_col]
    
    return transformed_df


def transform_matching_labels_for_update_queries(df, script):

    json_data = load_json_for_comparison(script)
    if 'joined_admissions_discharges' in script:
        json_data=merge_json_files('admissions','discharges')
    if not json_data:
        return pd.DataFrame(columns=['uid', 'unique_key'])  # empty fallback
    
    field_info = {f['key']: f for f in json_data}
    df_transformed = df.copy()
    label_cols_changed = []

    for value_col in [col for col in df.columns if col.endswith('.value')]:
        base_key = value_col[:-6]
        label_col = f"{base_key}.label"

        if label_col not in df.columns or base_key not in field_info:
            continue

        null_mask = df[value_col].isna()
        df_transformed.loc[null_mask, label_col] = None
        
        field = field_info[base_key]
        value_to_label = {opt['value']: opt['valueLabel'] for opt in field.get('options', [])}
        expected_label = field['label']
        field_type = field.get('type', '')

        # Build mask of rows to update
        mask = df[value_col].notna() & (df[label_col] == expected_label)
        if not mask.any():
            continue

        # Store original label values to compare later
        original_labels = df[label_col].copy()

        # Apply transformation
        if field_type in ('multi_select', 'checklist'):
            df_transformed.loc[mask, label_col] = df.loc[mask, value_col].apply(
                lambda x: ','.join(
                    [value_to_label.get(v.strip(), v.strip()) for v in str(x).split(',') if v.strip()]
                )
            )
        elif value_to_label:
            df_transformed.loc[mask, label_col] = df.loc[mask, value_col].map(value_to_label)
        else:
            df_transformed.loc[mask, label_col] = df.loc[mask, value_col]

        # Determine which rows changed
        changed = df_transformed[label_col] != original_labels
        if changed.any():
            label_cols_changed.append(label_col)
        else:
            # No actual changes, revert
            df_transformed[label_col] = original_labels

    # Filter only updated rows
    if not label_cols_changed:
        return pd.DataFrame(columns=['uid', 'unique_key'])  # no changes
    
    update_mask = df_transformed[label_cols_changed] != df[label_cols_changed]
    changed_rows = update_mask.any(axis=1)
    result = df_transformed.loc[changed_rows, ['uid', 'unique_key'] + label_cols_changed]

    marker = '__UNTOUCHED__'
    prepared = result.fillna(marker)

    records = prepared.to_dict(orient='records')

    filtered_records = [
        {k: (None if v == marker else v) for k, v in row.items() if v != marker}
        for row in records
    ]

    return filtered_records
