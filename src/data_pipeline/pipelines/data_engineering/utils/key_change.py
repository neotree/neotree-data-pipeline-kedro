import pandas as pd

def key_change(df: pd.DataFrame,row,position,old_key,new_key):
    if old_key in row and (row[old_key]) != 'nan':
        df.at[position,new_key] = df.at[position,old_key] 