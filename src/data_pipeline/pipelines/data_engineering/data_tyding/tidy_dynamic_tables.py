# Import created modules (need to be stored in the same directory as notebook)
import pandas as pd
from datetime import datetime as dt
import logging
from conf.common.format_error import formatError
from .extract_key_values import get_key_values,format_repeatables_to_rows
from .explode_mcl_columns import explode_column
from conf.base.catalog import catalog,new_scripts
from conf.common.sql_functions import create_new_columns,get_table_column_names,generate_upsert_queries_and_create_table,generate_create_insert_sql
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.utils.custom_date_formatter import format_date_without_timezone
from data_pipeline.pipelines.data_engineering.utils.data_label_fixes import convert_false_numbers_to_text

from conf.base.catalog import params
# Import libraries



def tidy_dynamic_tables():
   
    logging.info("... Fetching Raw Data From Dynamic Tables")
    
    try:
        #Read Data From The Kedro Catalog
         
        for script in new_scripts:
            catalog_query = f'''read_{script}'''
            
            script_raw = safe_load(catalog,catalog_query)
            if 'daily_review' in script:
                logging.info("...#######....."+str(catalog_query))
        
            try:
                script_new_entries, script_mcl = get_key_values(script_raw)
                logging.info("... Creating normalized dataframes for Dynamic Scripts")
                try:
                    script_df = pd.json_normalize(script_new_entries)
                    if 'uid' in script_df:
                        script_df.set_index(['uid'])
                     # ADD TIME SPENT TO ALL DFs
                    if "started_at" in script_df and 'completed_at' in script_df :
                        if('completed_time' in script_df):
                            try:
                                script_df=format_date_without_timezone(script_df,['started_at','completed_time']); 
                                script_df['started_at'] = pd.to_datetime(script_df['started_at']).tz_localize(None)
                                script_df['completed_time'] = pd.to_datetime(script_df['completed_time']).tz_localize(None)
                                script_df['time_spent'] = (script_df['completed_time'] - script_df['started_at']).dt.total_seconds() / 60
                            except:
                                pass
                        else:
                            try:
                                script_df=format_date_without_timezone(script_df,['started_at','completed_at']); 
                                script_df['started_at'] = pd.to_datetime(script_df['started_at']).tz_localize(None)
                                script_df['completed_at'] = pd.to_datetime(script_df['completed_at']).tz_localize(None)
                                script_df['time_spent'] = (script_df['completed_at'] - script_df['started_at']).dt.total_seconds() / 60
                            except:
                                pass
                    # FORMAT DATE ADMISSION FROM TEXT DATE   
                    else:
                        script_df['time_spent'] = None
                        
                        
                    ########### FORMAT OTHER DATES
                    # Get the date columns
                    datetime_types = ["datetime","datetime64", "datetime64[ns]", "datetimetz"]
                    date_columns = script_df.select_dtypes(include=datetime_types).columns
                    for date_column in date_columns:
                        script_df= format_date_without_timezone(script_df,date_column);
                    # Now write the cleaned up admission and discharge tables back to the database
                    logging.info("... Writing the Generics to the database")
                    try:
                        if not script_df.empty:
                            ##### REMOVE INVALID CHARACTERS FROM DATAFRAMES 
                            script_df.columns = script_df.columns.str.replace(r"[()-]", "_",regex=True)
                            catalog_save_name = f'''create_derived_{script}'''
                            if table_exists('derived',script):
                                cols = pd.DataFrame(get_table_column_names(f'{script}', 'derived'), columns=["column_name"])
                                new_columns = set(script_df.columns) - set(cols.columns) 
                      
                                if new_columns:
                                    column_pairs =  [(col, str(script_df[col].dtype)) for col in new_columns]
                                    if column_pairs:
                                        create_new_columns(f'{script}','derived',column_pairs)
                            script_df=convert_false_numbers_to_text(script_df,'derived',script); 
                            generate_create_insert_sql(script_df,'derived',script)
                            #catalog.save(catalog_save_name,script_df)
                            logging.info("... Creating MCL count tables for Generic Scripts")
                            explode_column(script_df,script_mcl,script+'_') 
                        

                    except Exception as e:                            
                        logging.error("!!! An error occured writing admissions and discharge output back to the database: ")
                        logging.error(formatError(e))
                    try:
                        repeatables = format_repeatables_to_rows(script_raw, script)
                        for table_name, df in (repeatables or {}).items():
                            if not df.empty:
                                generate_upsert_queries_and_create_table(table_name,df)
                    except Exception as e:
                        logging.error( "!!! An error whilest formatting repeatables ")   
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

def safe_load(catalog, dataset_name):
    try:
        return catalog.load(dataset_name)
    except Exception as e:
        logging.warning(f"Failed to load dataset '{dataset_name}': {formatError(e)}")
        return None