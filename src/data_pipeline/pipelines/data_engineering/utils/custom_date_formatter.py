import pandas as pd
from datetime import datetime as dt

def format_date(df,field_name):
    """
    Return A formated Date.

    :param df: dataframe, 
    :param field_name: field in dataframe
    """
    try: 
        if field_name in df and df[field_name] is not None and pd.notnull(df[field_name]):
            #df[field_name] = dt.strftime(df[field_name].map(lambda x: str(x)), format='%Y-%m-%dT%H:%M:%S.%f')
            df[field_name] = pd.to_datetime(df[field_name].map(lambda x: str(x)[:-4]), format='%Y-%m-%dT%H:%M:%S')
        else:
            df[field_name]= None;
    except Exception as e:
        pass

def format_date_without_timezone(df,field_name):
    """
    Return A formated Date.

    :param df: dataframe, 
    :param field_name: field in dataframe
    """
    try: 
        if field_name in df and df[field_name] is not None:
            df[field_name] = df[field_name].map(lambda x: str(x)[:-4])
            df[field_name] = pd.to_datetime(df[field_name], format='%Y-%m-%dT%H:%M:%S').astype('datetime64[ns]')
        else:
            df[field_name]= None;
    except Exception as e:
        pass

def convert_column_to_date(df: pd.DataFrame,column):
    if column in df:
        df[column] = df[column]