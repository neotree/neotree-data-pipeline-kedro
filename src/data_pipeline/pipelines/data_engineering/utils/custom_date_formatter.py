import pandas as pd # type: ignore
from datetime import datetime as dt
from .date_validator import is_date_formatable
import numpy as np
from dateutil.parser import parse

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
            
        return df
    except Exception as e:
        pass
       

def format_date_without_timezone(df, fields):
    """
    Return a formatted date while properly handling nulls
    
    :param df: dataframe
    :param fields: list of field names in dataframe
    """
    for field_name in fields:
        if field_name in df:
            # Convert to string, clean, and handle nulls properly
            df[field_name] = df[field_name].astype(str).replace(['None', 'none', 'null', 'NULL', 'nan'], pd.NA)
            
            # Only attempt conversion on date-formatable values
            mask = df[field_name].apply(is_date_formatable)
            df.loc[mask, field_name] = pd.to_datetime(
                df.loc[mask, field_name],
                errors='coerce',
                format='%Y-%m-%dT%H:%M:%S'
            )
            df[field_name] = df[field_name].where(mask, pd.NA)
        else:
            df[field_name] = pd.NA  
    
    return df

def convert_column_to_date(df: pd.DataFrame,column):
    if column in df:
        df[column] = df[column]