import pandas as pd
import logging

def set_key_to_none(df: pd.DataFrame,key):
    if key not in df and not df.empty:
        logging.info("Setting key to none: " + key)
        df[key] = None