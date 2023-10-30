import pandas as pd
import logging

def set_key_to_none(df: pd.DataFrame,key):
    logging.info("=========,=="+str(df.empty))
    if key not in df and not df.empty:
        df[key] = None