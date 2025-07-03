import pandas as pd
from datetime import datetime as dt
from .date_validator import is_date_formatable
import numpy as np

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
       
def format_date_without_timezone(df,fields):
    """
    Return A formated Date.

    :param df: dataframe, 
    :param field_name: field in dataframe
    """
    try: 
        for field_name in fields:
            if field_name in df:
                df[field_name] = df[field_name].map(lambda x: str(x).rstrip('.')  if is_date_formatable(x) else '')
                df[field_name] = pd.to_datetime(df[field_name], errors='coerce', format='%Y-%m-%dT%H:%M:%S')
            else:
                df[field_name]= None
        return df
    except Exception as e:
        pass

def convert_column_to_date(df: pd.DataFrame,column):
    if column in df:
        df[column] = df[column]