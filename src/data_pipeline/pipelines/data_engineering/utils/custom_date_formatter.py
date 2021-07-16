import pandas as pd

def format_date(df,field_name):
    """
    Return A formated Date.

    :param df: dataframe, 
    :param field_name: field in dataframe
    """
    try: 
        if field_name in df:
            df[field_name] = df[field_name].map(lambda x: str(x)[:-4])
            df[field_name] = pd.to_datetime(df[field_name], format='%Y-%m-%dT%H:%M:%S', utc=True)
    except Exception as e:
        pass