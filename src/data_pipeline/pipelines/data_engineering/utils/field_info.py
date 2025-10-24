import json
import os
from conf.base.catalog import hospital_conf
from conf.common.scripts import get_raw_json
import pandas as pd
import logging


def process_and_save_field_info(script, json_data):
    """
    Process and save field metadata organized by scriptid.

    NEW STRUCTURE:
    {
        "scriptid1": {
            "fieldKey1": {field metadata},
            "fieldKey2": {field metadata},
            ...
        },
        "scriptid2": {
            "fieldKey1": {field metadata},
            ...
        }
    }

    Args:
        script: Script name (e.g., 'admissions')
        json_data: Raw JSON from API containing script metadata
    """
    # Ensure directory exists
    os.makedirs('conf/local/scripts', exist_ok=True)

    filename = f'conf/local/scripts/{script}.json'

    # Load existing data (dict of scriptid -> field dicts)
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            all_scripts = json.load(f)
        # Handle legacy format (convert list to new structure)
        if isinstance(all_scripts, list):
            logging.warning(f"Converting legacy format for {script}.json to scriptid-based structure")
            all_scripts = {"legacy": {item['key']: item for item in all_scripts}}
    else:
        all_scripts = {}

    # Process screens from the input data
    for script_entry in json_data.get('data', []):
        # Extract scriptid from the script entry
        script_id = script_entry.get('scriptid') or script_entry.get('_id', 'unknown')

        # Initialize storage for this scriptid as a dict
        if script_id not in all_scripts:
            all_scripts[script_id] = {}

        result = all_scripts[script_id]

        # Convert list to dict if needed
        if isinstance(result, list):
            result = {item['key']: item for item in result}
            all_scripts[script_id] = result

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
                confidential = field.get("confidential", False)

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
                        "confidential": confidential,
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
                    if confidential:
                        result[key]["confidential"] = True

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

    # Save as-is (keep as dict structure)
    with open(filename, 'w') as f:
        json.dump(all_scripts, f, indent=2)

    logging.info(f"Saved metadata for {len(all_scripts)} scriptid(s) to {filename}")

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


def load_json_for_comparison(filename, script_id=None):
    """
    Load field metadata from JSON file.

    NEW STRUCTURE SUPPORT:
    - If script_id is provided: returns field dict for that specific scriptid
    - If script_id is None: returns entire dict of {scriptid: {fieldKey: field}}
    - Handles legacy format (array) by converting to dict

    Args:
        filename: Script name (e.g., 'admissions')
        script_id: Optional scriptid to load specific metadata

    Returns:
        dict: {scriptid: {fieldKey: field}} if script_id is None
        dict: {fieldKey: field} if script_id is provided
        None: if file not found or error
    """
    try:
        file_path = f'conf/local/scripts/{filename}.json'

        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                data = json.load(f)

            # Handle legacy format (flat array) - convert to dict
            if isinstance(data, list):
                logging.warning(f"Legacy format detected for {filename}.json - converting to dict")
                legacy_dict = {item['key']: item for item in data if isinstance(item, dict) and 'key' in item}
                return legacy_dict

            # New format: dict of {scriptid: {fieldKey: field}}
            if script_id:
                # Return field dict for specific scriptid
                return data.get(script_id)
            else:
                # Return entire structure
                return data

        return None
    except FileNotFoundError:
        logging.error(f"Error: File '{filename}' not found.")
        return None
    except json.JSONDecodeError:
        logging.error(f"Error: File '{filename}' contains invalid JSON.")
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
    """
    Transform value-label pairs based on metadata.

    NEW: Supports scriptid-based transformation - splits dataframe by scriptid
    and transforms each subset with its corresponding metadata.

    Args:
        df: DataFrame to transform
        script: Script name (e.g., 'admissions')

    Returns:
        Transformed DataFrame with corrected labels
    """
    metadata = load_json_for_comparison(script)
    if metadata is None:
        return df

    # Check if we have new scriptid-based structure and scriptid column
    if isinstance(metadata, dict) and 'scriptid' in df.columns:
        # NEW FORMAT: Split by scriptid and transform each subset
        logging.info(f"Using scriptid-based transformation for {script}")

        transformed_subsets = []
        unique_script_ids = df['scriptid'].dropna().unique()

        for script_id in unique_script_ids:
            script_id_str = str(script_id)
            subset_df = df[df['scriptid'] == script_id].copy()
            schema = metadata.get(script_id_str)

            if not schema:
                logging.warning(f"No metadata found for scriptid: {script_id_str} - skipping transformation")
                transformed_subsets.append(subset_df)
                continue

            # Transform this subset (schema is already a dict)
            transformed_subset = _transform_subset(subset_df, schema)
            transformed_subsets.append(transformed_subset)

        # Handle rows with NULL scriptid (no transformation)
        null_script_id_df = df[df['scriptid'].isna()].copy()
        if not null_script_id_df.empty:
            logging.warning(f"{len(null_script_id_df)} rows have NULL scriptid - no transformation")
            transformed_subsets.append(null_script_id_df)

        # Recombine all subsets
        transformed_df = pd.concat(transformed_subsets, ignore_index=False)
        transformed_df['transformed'] = True
        return transformed_df

    # LEGACY FORMAT or no scriptid column
    if isinstance(metadata, dict):
        # Check if this is a flat field dict (legacy converted) or scriptid-based dict
        first_key = next(iter(metadata.keys()))
        first_value = metadata[first_key]

        if isinstance(first_value, dict) and 'key' in first_value:
            # This is a legacy format converted to dict {fieldKey: field}
            logging.info(f"Using legacy transformation format (converted from array)")
            field_info = metadata
        else:
            # This is scriptid-based format but no scriptid column
            logging.warning(f"No scriptid column in dataframe - using first available schema")
            if len(metadata) == 1:
                field_info = list(metadata.values())[0]
                logging.info(f"Using single available schema: {list(metadata.keys())[0]}")
            else:
                logging.warning(f"Multiple schemas available but no scriptid column - using first schema")
                field_info = list(metadata.values())[0]
    else:
        logging.error(f"Unexpected metadata type: {type(metadata)}")
        return df

    # Transform entire dataframe (legacy)
    transformed_df = _transform_subset(df, field_info)
    transformed_df['transformed'] = True
    return transformed_df


def _transform_subset(df, field_info):
    """
    Transform value-label pairs for a single dataframe subset.

    This function contains the core transformation logic extracted from transform_matching_labels.
    It can be called for the entire dataframe (legacy) or for scriptid subsets (new).

    Args:
        df: DataFrame to transform
        field_info: Dict of {field_key: field_metadata}

    Returns:
        Transformed DataFrame
    """
    transformed_df = df.copy()

    # First pass: Set labels to null where values are null
    for value_col in [col for col in df.columns if col.endswith('.value')]:
        base_key = value_col[:-6]
        label_col = f"{base_key}.label"

        if label_col in df.columns:
            null_mask = transformed_df[value_col].isna()
            transformed_df.loc[null_mask, label_col] = None

    # Second pass: Transform labels based on field metadata
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

    return transformed_df


def transform_matching_labels_for_update_queries(df, script):
    """
    Transform value-label pairs and generate update query records.

    NEW: Supports scriptid-based transformation - splits dataframe by scriptid
    and transforms each subset with its corresponding metadata.

    Args:
        df: DataFrame to transform
        script: Script name (e.g., 'admissions')

    Returns:
        List of update records (dicts) for changed rows only
    """
    # Special case: joined_admissions_discharges uses merged metadata
    if 'joined_admissions_discharges' in script:
        json_data = merge_json_files('admissions', 'discharges')
        if not json_data:
            return []
        # Use legacy processing for merged data
        if isinstance(json_data, list):
            field_info = {f['key']: f for f in json_data if isinstance(f, dict) and 'key' in f}
        else:
            field_info = json_data
        return _transform_for_update_queries_subset(df, field_info)

    # Load metadata
    metadata = load_json_for_comparison(script)
    if not metadata:
        return []

    # Defensive check
    if not isinstance(metadata, (list, dict)):
        logging.warning(f"Unexpected metadata type for script '{script}': {type(metadata)}")
        return []

    # Check if we have new scriptid-based structure and scriptid column
    if isinstance(metadata, dict) and 'scriptid' in df.columns:
        # NEW FORMAT: Split by scriptid and transform each subset
        logging.info(f"Using scriptid-based update queries for {script}")

        all_update_records = []
        unique_script_ids = df['scriptid'].dropna().unique()

        for script_id in unique_script_ids:
            script_id_str = str(script_id)
            subset_df = df[df['scriptid'] == script_id].copy()
            schema = metadata.get(script_id_str)

            if not schema:
                logging.warning(f"No metadata found for scriptid: {script_id_str} - skipping")
                continue

            # Generate update records for this subset (schema is already a dict)
            subset_records = _transform_for_update_queries_subset(subset_df, schema)
            all_update_records.extend(subset_records)

        return all_update_records

    # LEGACY FORMAT or no scriptid column
    if isinstance(metadata, dict):
        # Check if this is a flat field dict (legacy converted) or scriptid-based dict
        first_key = next(iter(metadata.keys()))
        first_value = metadata[first_key]

        if isinstance(first_value, dict) and 'key' in first_value:
            # This is a legacy format converted to dict {fieldKey: field}
            logging.info(f"Using legacy update queries format (converted from array)")
            field_info = metadata
        else:
            # This is scriptid-based format but no scriptid column
            logging.warning(f"No scriptid column in dataframe - using first available schema")
            if len(metadata) == 1:
                field_info = list(metadata.values())[0]
                logging.info(f"Using single available schema: {list(metadata.keys())[0]}")
            else:
                logging.warning(f"Multiple schemas available but no scriptid column - using first schema")
                field_info = list(metadata.values())[0]
    else:
        logging.error(f"Unexpected metadata type: {type(metadata)}")
        return []

    return _transform_for_update_queries_subset(df, field_info)


def _transform_for_update_queries_subset(df, field_info):
    """
    Transform labels and generate update query records for a single subset.

    This function contains the core logic extracted from transform_matching_labels_for_update_queries.
    It can be called for the entire dataframe (legacy) or for scriptid subsets (new).

    Args:
        df: DataFrame to transform
        field_info: Dict of {field_key: field_metadata}

    Returns:
        List of update records (dicts) for changed rows only
    """
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
