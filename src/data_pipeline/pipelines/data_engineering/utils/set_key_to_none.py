import pandas as pd
import logging

def set_key_to_none(df: pd.DataFrame,keys):
    for key in keys:
        if key not in df and not df.empty: 
            df[key] = pd.NA # None
    return df