import pandas as pd # type: ignore
from datetime import datetime as dt
from .date_validator import is_date_formatable
import numpy as np
from dateutil.parser import parse
import logging
import re

def format_date(df:pd.DataFrame,fields):
    """
    Return A formated Date.

    :param df: dataframe, 
    :param field_name: field in dataframe
    """
    try: 
        for field_name in fields:
            if field_name in df:
                 df[field_name].map(lambda x: str(x).rstrip('.') if is_date_formatable(x) else '')
                 df[field_name] = pd.to_datetime(df[field_name], errors='coerce', format='%Y-%m-%d') 
            
        return format_date_without_timezone(df,fields,format='%Y-%m-%d')
    except Exception as e:
        pass
       

def format_date_without_timezone(df, fields,format=None):
    """
    Return a formatted date while properly handling nulls
    
    :param df: dataframe
    :param fields: list of field names in dataframe
    """
    if format is None:
        format = '%Y-%m-%d %H:%M:%S'
        
    for field_name in fields:
        if field_name in df:
            # Handle nulls first
            df[field_name] = df[field_name].replace([
                'None', 'none', 'null', 'NULL', 'nan', 'NaT', '<NA>', ''
            ], pd.NA)
            
            # Apply your convert_to_date function to all non-null values
            def convert_value(x):
                if pd.isna(x):
                    return pd.NA
                return convert_to_date(x,format)  # Your function from option 4
            
            df[field_name] = df[field_name].apply(convert_value)
            
        else:
            df[field_name] = pd.NA  
    
    return df

def convert_column_to_date(df: pd.DataFrame,column):
    if column in df:
        df[column] = df[column]

def convert_to_date(value, format_str):
    if value is None or pd.isna(value):
        return None
        
    clean_value = str(value).strip().rstrip(",")
    
    try:
        # Return datetime object, NOT formatted string
        return pd.to_datetime(clean_value, errors='raise', infer_datetime_format=True)
        
    except (ValueError, TypeError):
        # Enhanced regex pattern that includes time for textual formats
        timestamp_pattern = re.compile(
            r'^('
            r'\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}'                       
            r'(?:\.\d+)?'                                                   
            r'(?:Z|[+-]\d{2}:?\d{2})?'                                       
            r'|'                                                             
            r'\d{1,2}[ -](?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*[ ,-]*\d{4}'
            r'(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?'  # Added optional time part
            r'|'                                                             
            r'\d{1,2}[/-]\d{1,2}[/-]\d{4}'                                  
            r')$',
            re.IGNORECASE
        )

        if re.fullmatch(timestamp_pattern, clean_value):
            try:
                # Return datetime object, NOT formatted string
                return pd.to_datetime(clean_value, errors='raise')
            except:
                # Basic cleaning for known patterns
                if '.' in clean_value:
                    clean_value = clean_value.split('.')[0]
                clean_value = clean_value.replace('T', ' ')
                return pd.to_datetime(clean_value, errors='coerce')
                
        return None