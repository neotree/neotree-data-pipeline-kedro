from conf.base.catalog import catalog,params
import pandas as pd
from data_pipeline.pipelines.data_engineering.utils.date_validator import is_date, is_date_formatable
from data_pipeline.pipelines.data_engineering.utils.custom_date_formatter import format_date_without_timezone
from conf.common.sql_functions import create_new_columns,get_table_column_names,inject_sql
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from datetime import datetime, date
from conf.common.format_error import formatError

# Import libraries
import logging
from datetime import datetime as dt


def join_table():

    logging.info("... Starting script to create joined table")

    # Read the raw admissions and discharge data into dataframes
    logging.info("... Fetching admissions and discharges data")
    try:
    
        #Load Derived Admissions From Kedro Catalog
        adm_df = catalog.load('read_derived_admissions')
      
 
        #Load Derived Discharges From Kedro Catalog
        dis_df = catalog.load('read_derived_discharges')
    except Exception as e:
        logging.error("!!! An error occured fetching the data: ")
        raise e

    # Create join of admissions & discharges (left outter join)
    logging.info("... Creating joined admissions and discharge table")
    try:
        
        # join admissions and discharges using uid and facility
       
        jn_adm_dis =createJoinedDataSet(adm_df,dis_df)

    except Exception as e:
        logging.error("!!! An error occured creating joined dataframe: ")
        raise e

    # Now write the table back to the database
    logging.info("... Writing the output back to the database")
    try:
        #Create Table Using Kedro
        if jn_adm_dis and not jn_adm_dis.empty:
            catalog.save('create_joined_admissions_discharges',jn_adm_dis)
        #MERGE DISCHARGES CURRENTLY ADDED TO THE NEW DATA SET
        discharge_exists = table_exists('derived','discharges')
        joined_exists = table_exists('derived','joined_admissions_discharges')
        if(discharge_exists and joined_exists):
            #Load Derived Admissions Withoud Discharges From Kedro Catalog
            adm_df_2 = catalog.load('admissions_without_discharges')
            #Load Derived Discharges Not Yet Joined From Kedro Catalog
            dis_df_2 = catalog.load('discharges_not_joined')

            if( adm_df_2 and dis_df_2):
                jn_adm_dis_2 = createJoinedDataSet(adm_df_2,dis_df_2)
                if jn_adm_dis_2 and not jn_adm_dis_2.empty:
                    generateAndRunUpdateQuery('derived.joined_admissions_discharges',jn_adm_dis_2)

    except Exception as e:
        logging.error(
            "!!! An error occured writing join output back to the database: ")
        raise e

    logging.info("... Join script completed!")

def createJoinedDataSet(adm_df:pd.DataFrame,dis_df:pd.DataFrame)->pd.DataFrame:
        jn_adm_dis = pd.DataFrame()
        if adm_df and dis_df:
            jn_adm_dis = adm_df.merge(
            dis_df, 
            how='left', 
            on=['uid', 'facility'], 
            suffixes=('', '_discharge')
            )
            if 'unique_key' in jn_adm_dis:
                jn_adm_dis['DEDUPLICATER'] =jn_adm_dis['unique_key'].map(lambda x: str(x)[:10] if len(str(x))>=10 else None) 
                # FURTHER DEDUPLICATION ON UNIQUE KEY
                grouped = jn_adm_dis.groupby(["uid", "facility", "DEDUPLICATER"])
                duplicates = grouped.filter(lambda x: len(x) > 1)
                # Identify the index of the first record in each group
                to_drop = duplicates.groupby(["uid", "facility", "DEDUPLICATER"]).head(1).index
                # Drop the identified records from the original DataFrame
                jn_adm_dis = jn_adm_dis.drop(index=to_drop)

                # FURTHER DEDUPLICATION ON UID,FACILITY,OFC-DISCHARGE
                # THIS FIELD HELPS IN ISOLATING DIFFERENT ADMISSIONS MAPPED TO THE SAME DISCHARGE
                if "OFCDis.value" in jn_adm_dis:
                    grouped = jn_adm_dis.groupby(["uid", "facility", "OFCDis.value"])
                    duplicates = grouped.filter(lambda x: len(x) > 1)
                    # Identify the index of the first record in each group
                    to_drop = duplicates.groupby(["uid", "facility", "OFCDis.value"]).head(1).index
                    # Drop the identified records from the original DataFrame
                    jn_adm_dis = jn_adm_dis.drop(index=to_drop)

                # FURTHER DEDUPLICATION ON UID,FACILITY,BIRTH-WEIGHT-DISCHARGE
                # THIS FIELD HELPS IN ISOLATING DIFFERENT ADMISSIONS MAPPED TO THE SAME DISCHARGE
                if "BirthWeight.value_discharge" in jn_adm_dis:
                    grouped = jn_adm_dis.groupby(["uid", "facility", "BirthWeight.value_discharge"])
                    duplicates = grouped.filter(lambda x: len(x) > 1)
                    # Identify the index of the first record in each group
                    to_drop = duplicates.groupby(["uid", "facility", "BirthWeight.value_discharge"]).head(1).index
                    # Drop the identified records from the original DataFrame
                    jn_adm_dis = jn_adm_dis.drop(index=to_drop)

                

            # Drop helper columns if needed
            if table_exists('derived','joined_admissions_discharges'):
                    adm_cols = pd.DataFrame(get_table_column_names('joined_admissions_discharges', 'derived'))
                    new_adm_columns = set(jn_adm_dis.columns) - set(adm_cols.columns) 
                        
                    if new_adm_columns:
                        column_pairs =  [(col, str(jn_adm_dis[col].dtype)) for col in new_adm_columns]
                        if column_pairs:
                            create_new_columns('joined_admissions_discharges','derived',column_pairs)

            # else:
            #     # Merge for non-null Dates (exact match)
            #     jn_adm_dis = adm_df.merge(
            #     dis_df_with_date, 
            #     how='left', 
            #     on=['uid', 'facility','Date_only'], 
            #     suffixes=('', '_discharge')
            #     )
            #     # Drop helper columns if needed
            #     jn_adm_dis.drop(columns=['Date_only'],inplace=True)

            if 'Gestation.value' in jn_adm_dis:
                jn_adm_dis['Gestation.value'] =  pd.to_numeric(jn_adm_dis['Gestation.value'],downcast='integer', errors='coerce')
            
            #Length of Life and Length of Stay
            date_format = "%Y-%m-%d"

            jn_adm_dis=format_date_without_timezone(jn_adm_dis,['DateTimeAdmission.value','DateTimeDischarge.value'])

            for index, row in jn_adm_dis.iterrows():

                jn_adm_dis.loc[index,'LengthOfStay.label'] ="Length of Stay"
                if (is_date(str(row['DateTimeDischarge.value']))
                    and is_date(str(row['DateTimeAdmission.value']))):
                    DateTimeDischarge = dt.strptime(str(str(row['DateTimeDischarge.value']))[:10].strip(),date_format)
                    DateTimeAdmission = dt.strptime(str(str(row['DateTimeAdmission.value']))[:10].strip(),date_format)
                    delta_los = DateTimeDischarge -DateTimeAdmission
                    jn_adm_dis.loc[index,'LengthOfStay.value']= delta_los.days
                    
                else:
                    jn_adm_dis.loc[index,'LengthOfStay.value'] = None
            
                jn_adm_dis.loc[index,'LengthOfLife.label'] ="Length of Life"
                if ('DateTimeDeath.value' in row 
                    and is_date_formatable(str(row['DateTimeDeath.value']).strip()) and is_date(str(row['DateTimeAdmission.value']))):
                
                    DateTimeDeath = dt.strptime(str(str(row['DateTimeDeath.value']))[:10].strip(), date_format)
                    DateTimeAdmission = dt.strptime(str(row['DateTimeAdmission.value'])[:10].strip(), date_format)
                    delta_lol = DateTimeDeath - DateTimeAdmission
                    jn_adm_dis.loc[index,'LengthOfLife.value'] = delta_lol.days;
                else:
                    jn_adm_dis.loc[index, 'LengthOfLife.value'] = None
        
        return jn_adm_dis


def generateAndRunUpdateQuery(table:str,df:pd.DataFrame):
    try:
        if(table is not None and df is not None):

            update_queries = []

            # Iterate over each row in the DataFrame
            for index, row in df.iterrows():
                # Start building the UPDATE query
                query = f'''UPDATE "{table}" SET '''
                
                # Add the fields to be updated (excluding the key fields)
                updates = []
                for col in df.columns:
                    if col not in ['uid', 'facility', 'unique_key']:
                        # Handle different data types appropriately
                        if isinstance(row[col], str):
                            updates.append(f"{col} = '{row[col]}'")
                        elif isinstance(row[col], (datetime, date)):
                            # Format the date or datetime as a string in PostgreSQL-compatible format
                            if isinstance(row[col], datetime):
                                # Include time component for datetime objects
                                updates.append(f"{col} = '{row[col].strftime('%Y-%m-%d %H:%M:%S')}'")
                            else:
                                # Exclude time component for date objects
                                updates.append(f"{col} = '{row[col].strftime('%Y-%m-%d')}'")
                        else:
                            updates.append(f"{col} = {row[col]}")
                
                # Join the updates with commas
                query += ", ".join(updates)
                
                # Add the WHERE clause to specify which records to update
                query += f" WHERE uid = {row['uid']} AND facility = '{row['facility']}' AND unique_key = '{row['unique_key']}';;"
                
                # Append the query to the list
                update_queries.append(query)

            for query in update_queries:
                inject_sql(query,query)
    except Exception as ex:
        logging.error(
            "!!! An error occured whilest JOINING DATA THAT WAS UNJOINED ")
        logging.error(formatError(ex))