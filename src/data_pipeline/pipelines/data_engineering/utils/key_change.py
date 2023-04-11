import pandas as pd

def key_change(df: pd.DataFrame,row,position,old_key,new_key):
    try:
        if old_key in row and (str(df.at[position,old_key])!= 'nan' or df.at[position,old_key] is not None or str(df.at[position,old_key])!='None'):
            df.at[position,new_key] = df.at[position,old_key] 
    except Exception as ex:
        #logging.info(f'''---CANT CONVERT--{old_key} to {new_key}''')
        pass;
       