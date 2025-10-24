import json
import os
from conf.base.catalog import hospital_conf
from conf.common.scripts import get_raw_json
import pandas as pd
import logging


def process_and_save_field_info(script, json_data):
    # Ensure directory exists
    os.makedirs('conf/local/scripts', exist_ok=True)

    filename = f'conf/local/scripts/{script}.json'
    # Load existing data (simple key-value dict)
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            result = json.load(f)
        # Convert from list to dict if needed
        if isinstance(result, list):
            result = {item['key']: item for item in result}
    else:
        result = {}

    # Process screens from the input data
    for script_entry in json_data.get('data', []):
        for screen in script_entry.get('screens', []):
            if "fields" not in screen:
                continue
                
            for field in screen["fields"]:
                key = field["key"]
                field_type = field.get("type")
                label = field.get("label")
                data_type = field.get("dataType")
                optional = field.get("optional", True)  # Default to optional if not specified
                min_value = field.get("minValue")
                max_value = field.get("maxValue")

                # Initialize if doesn't exist
                if key not in result:
                    result[key] = {
                        "key": key,
                        "type": field_type,
                        "dataType": data_type,
                        "label": label,
                        "optional": optional,
                        "minValue": min_value,
                        "maxValue": max_value,
                        "options": []
                    }
                else:
                    # Update optional, minValue, maxValue if they exist in new field
                    # (keep the most restrictive: required over optional)
                    if not optional:  # If new field is required, mark as required
                        result[key]["optional"] = False
                    if min_value is not None:
                        result[key]["minValue"] = min_value
                    if max_value is not None:
                        result[key]["maxValue"] = max_value
                    if data_type and not result[key].get("dataType"):
                        result[key]["dataType"] = data_type

                # Add options if they exist
                if "value" in field and "valueLabel" in field:
                    value = field["value"]
                    value_label = field["valueLabel"]

                    if value and value_label:
                        if not any(opt["value"] == value and opt["valueLabel"] == value_label
                                 for opt in result[key]["options"]):
                            result[key]["options"].append({
                                "value": value,
                                "valueLabel": value_label
                            })
    # Convert to list of field objects before saving
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
        file_path = f'conf/local/scripts/{filename}.json'

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
    file_path1= f'conf/local/scripts/{file1_path}.json'
    file_path2= f'conf/local/scripts/{file2_path}.json'

    # Fix: Check the correct file path variable
    if os.path.exists(file_path1) and os.path.exists(file_path2):
        with open(file_path1, 'r') as f1, open(file_path2, 'r') as f2:
            data1 = json.load(f1)
            data2 = json.load(f2)

        # Handle case where data is a list (expected format) or dict
        if isinstance(data1, list) and isinstance(data2, list):
            # Convert lists to dicts using 'key' field
            dict1 = {item['key']: item for item in data1 if isinstance(item, dict) and 'key' in item}
            dict2 = {item['key']: item for item in data2 if isinstance(item, dict) and 'key' in item}

            # Merge dictionaries
            merged = dict1.copy()
            for k, v in dict2.items():
                if k not in merged:
                    merged[k] = v

            # Convert back to list format
            return list(merged.values())
        elif isinstance(data1, dict) and isinstance(data2, dict):
            # Handle dict format
            merged = data1.copy()
            for k, v in data2.items():
                if k not in merged:
                    merged[k] = v
            return merged
        else:
            logging.warning(f"Unexpected data format in merge_json_files: {type(data1)}, {type(data2)}")
            return None
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
            
            try:
                inverted_mask = (transformed_df[value_col].isin(value_to_label.values()) & transformed_df[label_col].isin(value_to_label.keys()))
                # Fix inverted rows FIRST (only for select field types)
                if options and inverted_mask.any() and field_type in ('single_select_option', 'dropdown', 'multi_select_option'):
                    # Safely swap values using temporary column to avoid KeyError
                    temp_vals = transformed_df.loc[inverted_mask, value_col].copy()
                    transformed_df.loc[inverted_mask, value_col] = transformed_df.loc[inverted_mask, label_col]
                    transformed_df.loc[inverted_mask, label_col] = temp_vals

                non_null_mask = (transformed_df[value_col].notna()) & (transformed_df[label_col] == json_label)
            except KeyError as e:
                # Skip this field if columns don't exist (defensive check)
                logging.warning(f"Column access error for field {base_key}: {str(e)}")
                continue
            
            if field_type in ('multi_select', 'checklist'):
                transformed_df.loc[non_null_mask, label_col] = transformed_df.loc[non_null_mask, value_col].apply(
                    lambda x: ','.join([
                        value_to_label.get(v.strip(), v.strip())
                        for v in str(x).split(',') if v.strip()
                    ]) if pd.notna(x) else x
                )
            elif non_null_mask.any():
                if options:

                    transformed_df.loc[non_null_mask, label_col] = transformed_df.loc[non_null_mask, value_col].map(value_to_label)
                else:
                    transformed_df.loc[non_null_mask, label_col] = transformed_df.loc[non_null_mask, value_col]
    transformed_df['transformed'] = True
    return transformed_df


def transform_matching_labels_for_update_queries(df, script):
    json_data = load_json_for_comparison(script)
    if 'joined_admissions_discharges' in script:
        json_data = merge_json_files('admissions', 'discharges')
    if not json_data:
        return []  # empty fallback - return empty list, not DataFrame

    # Defensive check: ensure json_data is a list or dict
    if not isinstance(json_data, (list, dict)):
        logging.warning(f"Unexpected json_data type for script '{script}': {type(json_data)}")
        return []

    # Handle both list and dict formats
    if isinstance(json_data, list):
        field_info = {f['key']: f for f in json_data if isinstance(f, dict) and 'key' in f}
    else:
        field_info = json_data

    if not field_info:
        return []

    df_transformed = df.copy()
    label_cols_changed = []

    for value_col in [col for col in df.columns if col.endswith('.value')]:
        base_key = value_col[:-6]  # removes '.value' suffix
        label_col = f"{base_key}.label"
        
        # Skip if label column doesn't exist or field not in JSON
        if label_col not in df.columns or base_key not in field_info:
            continue

        # Get field metadata
        field = field_info[base_key]
        value_to_label = {opt['value']: opt['valueLabel'] for opt in field.get('options', [])}
        expected_label = field['label']
        field_type = field.get('type', '')

        # Initialize NULL handling - set label to NULL where value is NULL
        null_value_mask = df[value_col].isna()
        df_transformed.loc[null_value_mask, label_col] = None

        # Build mask of rows to process (including cases where value is NULL but label isn't)
        process_mask = (
            # Cases where value exists and label matches expected
            (df[value_col].notna() & (df[label_col] == expected_label)) | (
            # OR cases where value is NULL but label is not NULL (the problematic cases)
            (df[value_col].isna() & df[label_col].notna())
        ))

        if not process_mask.any():
            continue

        # Store original labels for comparison
        original_labels = df[label_col].copy()

        # Apply transformations
        try:
            if field_type in ('multi_select', 'checklist'):
                # For NULL values, we've already set label to NULL above
                non_null_mask = process_mask & df[value_col].notna()
                df_transformed.loc[non_null_mask, label_col] = df.loc[non_null_mask, value_col].apply(
                    lambda x: ','.join(
                        [value_to_label.get(v.strip(), v.strip()) for v in str(x).split(',') if v.strip()]
                    )
                )
            elif value_to_label:
                # For NULL values, we've already set label to NULL above
                non_null_mask = process_mask & df[value_col].notna()
                df_transformed.loc[non_null_mask, label_col] = df.loc[non_null_mask, value_col].map(value_to_label)
            else:
                # For NULL values, we've already set label to NULL above
                non_null_mask = process_mask & df[value_col].notna()
                df_transformed.loc[non_null_mask, label_col] = df.loc[non_null_mask, value_col]
        except Exception as e:
            print(f"Error processing {base_key}: {str(e)}")
            continue

        # Track changed columns (including NULL to NULL changes)
        changed = df_transformed[label_col].ne(original_labels)
        if changed.any():
            label_cols_changed.append(label_col)
        else:
            df_transformed[label_col] = original_labels  # revert if no changes

    # Prepare final output
    if not label_cols_changed:
        return []  # No changes - return empty list, not DataFrame

    # Find changed rows (including NULL to NULL changes)
    changed_rows = df_transformed[label_cols_changed].ne(df[label_cols_changed]).any(axis=1)
    result = df_transformed.loc[changed_rows, ['uid', 'unique_key'] + label_cols_changed]

    # Convert to records with proper NULL handling
    marker = '__UNTOUCHED__'
    prepared = result.fillna(marker)
    records = prepared.to_dict(orient='records')
    
    filtered_records = [
        {k: (None if v == marker else v) for k, v in row.items() if v != marker}
        for row in records
    ]
   
    return filtered_records
