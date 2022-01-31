import logging
import pandas as pd
import sys

def key_change(df: pd.DataFrame,row,position,old_key,new_key):
    try:
        if old_key in row and (row[old_key]) != 'nan' and new_key not in row:
            df.at[position,new_key] = df.at[position,old_key] 
    except Exception as ex:
        logging.info("---CANT CONVERT--"+ex.with_traceback())
        sys.exit();