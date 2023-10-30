# Import created modules (need to be stored in the same directory as notebook)
from conf.common.format_error import formatError
from .extract_key_values import get_key_values
from .explode_mcl_columns import explode_column
from conf.base.catalog import catalog,new_scripts
from data_pipeline.pipelines.data_engineering.utils.custom_date_formatter import format_date_without_timezone

from conf.base.catalog import params
# Import libraries
import pandas as pd
from datetime import datetime as dt
import logging


def tidy_dynamic_tables():
   
    logging.info("... Fetching Raw Data From Dynamic Tables")
    
    try:
        #Read Data From The Kedro Catalog
        for script in new_scripts:
            catalog_query = f'''read_{script}'''
            logging.info("*******CATALOG QUERY====",catalog_query)
            script_raw = catalog.load(catalog_query)
        
            try:
                script_new_entries, script_mcl = get_key_values(script_raw)
                logging.info("... Creating normalized dataframes for Dynamic Scripts")
                try:
                    script_df = pd.json_normalize(script_new_entries)
                    if "uid" in script_df:
                        script_df.set_index(['uid'])
                     # ADD TIME SPENT TO ALL DFs
                    if "started_at" in script_df and 'completed_at' in script_df :
                        format_date_without_timezone(script_df,'started_at'); 
                        format_date_without_timezone(script_df,'completed_at'); 
                        script_df['time_spent'] = (script_df['completed_at'] - script_df['started_at']).astype('timedelta64[m]')
                    else:
                        script_df['time_spent'] = None
                        
                    ########### FORMAT OTHER DATES
                    # Get the date columns
                    datetime_types = ["datetime","datetime64", "datetime64[ns]", "datetimetz"]
                    date_columns = script_df.select_dtypes(include=datetime_types).columns
                    for date_column in date_columns:
                        format_date_without_timezone(script_df,date_column);
                    # Now write the cleaned up admission and discharge tables back to the database
                    logging.info("... Writing the Generics to the database")
                    try:
                        if not script_df.empty:
                            ##### REMOVE INVALID CHARACTERS FROM DATAFRAMES 
                            script_df.columns = script_df.columns.str.replace(r"[()-]", "_")
                            catalog_save_name = f'''create_derived_{script}'''
                            logging.info("****************SAVING***************"+catalog_save_name)
                            catalog.save(catalog_save_name,script_df)
                            logging.info("... Creating MCL count tables for Generic Scripts")
                            explode_column(script_df,script_mcl,script+'_')      
                    except Exception as e:                            
                        logging.error("!!! An error occured writing admissions and discharge output back to the database: ")
                        logging.error(formatError(e))
                            
                except Exception as e:
                    logging.error( "!!! An error occured normalized dataframes/changing data types for generic scripts: ")   
                    logging.error(formatError(e))     
            except Exception as e:
                logging.error("!!! An error occured extracting keys: ")
                logging.error(formatError(e))
    except Exception as e:
        logging.error("!!! An error occured fetching generic scripts data: ")
        logging.error(formatError(e))
    