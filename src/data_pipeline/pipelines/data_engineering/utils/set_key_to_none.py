import pandas as pd

def set_key_to_none(df: pd.DataFrame,key):
    if key not in df.columns:
        df[key] = None