import pandas as pd
from datetime import datetime as dt
from .date_validator import is_date_formatable

def format_date(df:pd.DataFrame,field_name):
    """
    Return A formated Date.

    :param df: dataframe, 
    :param field_name: field in dataframe
    """
    try: 
        if field_name in df and df[field_name] is not None:
            df[field_name] =df[field_name].map(lambda x: str(x)[:-4] if is_date_formatable(x) else None) 
            df[field_name]=pd.to_datetime(df[field_name], errors='coerce')
    except Exception as e:
        raise (e)
       
def format_date_without_timezone(df,field_name):
    """
    Return A formated Date.

    :param df: dataframe, 
    :param field_name: field in dataframe
    """
    try: 
        if field_name in df and df[field_name] is not None:
            df[field_name] = df[field_name].map(lambda x: str(x)[:-4])
            df[field_name] = pd.to_datetime(df[field_name],errors='coerce', format='%Y-%m-%dT%H:%M:%S')
        else:
            df[field_name]= None;
    except Exception as e:
        pass

def convert_column_to_date(df: pd.DataFrame,column):
    if column in df:
        df[column] = df[column]