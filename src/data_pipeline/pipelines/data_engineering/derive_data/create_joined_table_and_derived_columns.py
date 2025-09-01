import pandas as pd # type: ignore
from conf.base.catalog import catalog,params
from data_pipeline.pipelines.data_engineering.utils.date_validator import is_date, is_date_formatable
from data_pipeline.pipelines.data_engineering.utils.custom_date_formatter import format_date_without_timezone
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import (
                                                                               read_dicharges_not_joined,
                                                                               read_admissions_not_joined,
                                                                               admissions_without_discharges,
                                                                               discharges_not_matched,read_all_from_derived_table)
from conf.common.sql_functions import (create_new_columns
                                       ,get_table_column_names
                                       ,generateAndRunUpdateQuery
                                       ,generate_create_insert_sql,
                                       get_date_column_names,run_query_and_return_df)
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.data_validation.validate import reset_log
from data_pipeline.pipelines.data_engineering.queries.data_fix import deduplicate_table



# Import libraries
import logging
from datetime import datetime as dt


def join_table():

    logging.info("... Starting script to create joined table")

    # Read the raw admissions and discharge data into dataframes
    logging.info("... Fetching admissions and discharges data")
    reset_log('logs/queries.log')
    try:
    
        #Load Derived Admissions From Kedro Catalog
        read_admissions_query=''
        read_discharges_query=''
        if table_exists('derived','joined_admissions_discharges'):
            read_admissions_query= read_admissions_not_joined()
            read_discharges_query = read_dicharges_not_joined()
        else:
            read_admissions_query= read_all_from_derived_table('admissions') 
            read_discharges_query = read_all_from_derived_table('discharges') 
     
        adm_df = run_query_and_return_df(read_admissions_query)  
        #Load Derived Discharges From Kedro Catalog
        dis_df = run_query_and_return_df(read_discharges_query)    
        jn_adm_dis =createJoinedDataSet(adm_df,dis_df)

    except Exception as e:
        logging.error("!!! An error occured creating joined dataframe: ")
        raise e

    # Now write the table back to the database
    logging.info("... Writing the output back to the database")
    try:
        #Create Table Using Kedro
        if jn_adm_dis is not None and not jn_adm_dis.empty:
            if table_exists('derived','joined_admissions_discharges'):
                    adm_cols = pd.DataFrame(get_table_column_names('joined_admissions_discharges', 'derived'))
                    new_adm_columns = set(jn_adm_dis.columns) - set(adm_cols.columns) 
                        
                    if new_adm_columns:
                        column_pairs =  [(col, str(jn_adm_dis[col].dtype)) for col in new_adm_columns]
                        if column_pairs:
                            create_new_columns('joined_admissions_discharges','derived',column_pairs)  

            date_column_types = pd.DataFrame(get_date_column_names('joined_admissions_discharges', 'derived'))
            if not date_column_types.empty:
                jn_adm_dis = format_date_without_timezone(jn_adm_dis,date_column_types)
                jn_adm_dis.columns = jn_adm_dis.columns.astype(str) 
                jn_adm_dis = jn_adm_dis.loc[:, ~jn_adm_dis.columns.str.match(r'^\d+$|^[a-zA-Z]$', na=False)]

            logging.info(f"##########JDS DATAFRAME SIZE={len(jn_adm_dis)}")
            generate_create_insert_sql(jn_adm_dis,"derived","joined_admissions_discharges")


        #MERGE DISCHARGES CURRENTLY ADDED TO THE NEW DATA SET
        discharge_exists = table_exists('derived','discharges')
        joined_exists = table_exists('derived','joined_admissions_discharges')
        if(discharge_exists and joined_exists):
            read_admissions_query_2 = admissions_without_discharges()
            adm_df_2 = run_query_and_return_df(read_admissions_query_2)
            read_discharges_query_2 = discharges_not_matched()
            dis_df_2 = run_query_and_return_df(read_discharges_query_2)
            if( adm_df_2 is not None and dis_df_2 is not None and not adm_df_2.empty):
                jn_adm_dis_2 = createJoinedDataSet(adm_df_2,dis_df_2)
                jn_adm_dis_2.columns = jn_adm_dis_2.columns.astype(str) 
                jn_adm_dis_2 = jn_adm_dis_2.loc[:, ~jn_adm_dis_2.columns.str.match(r'^\d+$|^[a-zA-Z]$', na=False)]
        
                if not jn_adm_dis_2.empty:
                    filtered_df = jn_adm_dis_2[jn_adm_dis_2['NeoTreeOutcome.value'].notna() & (jn_adm_dis_2['NeoTreeOutcome.value'] != '')]           
                    generateAndRunUpdateQuery('derived.joined_admissions_discharges',filtered_df)
        deduplicate_table('joined_admissions_discharges')

    except Exception as e:
        logging.error(
            "!!! An error occured writing join output back to the database: ")
        raise e

    logging.info("... Join script completed!")

def createJoinedDataSet(adm_df:pd.DataFrame,dis_df:pd.DataFrame)->pd.DataFrame:
        jn_adm_dis = pd.DataFrame()
        if not adm_df.empty and not dis_df.empty:
            jn_adm_dis = adm_df.merge(
            dis_df, 
            how='left', 
            on=['uid', 'facility'], 
            suffixes=('', '_discharge')
            )
            if 'unique_key' in jn_adm_dis:
                jn_adm_dis['DEDUPLICATER'] =jn_adm_dis['unique_key'].map(lambda x: str(x)[:10] if len(str(x))>=10 else None) 
                jn_adm_dis = jn_adm_dis.drop_duplicates(
                    subset=["uid", "facility", "DEDUPLICATER"], 
                    keep='first'
                )
                # FURTHER DEDUPLICATION ON UID,FACILITY,OFC-DISCHARGE
                # THIS FIELD HELPS IN ISOLATING DIFFERENT ADMISSIONS MAPPED TO THE SAME DISCHARGE
                if "OFCDis.value" in jn_adm_dis:
                    jn_adm_dis = jn_adm_dis.drop_duplicates(
                    subset=["uid", "facility", "OFCDis.value"], 
                    keep='first'
                   )

                # FURTHER DEDUPLICATION ON UID,FACILITY,BIRTH-WEIGHT-DISCHARGE
                # THIS FIELD HELPS IN ISOLATING DIFFERENT ADMISSIONS MAPPED TO THE SAME DISCHARGE
                if "BirthWeight.value_discharge" in jn_adm_dis:
                      jn_adm_dis = jn_adm_dis.drop_duplicates(
                    subset=["uid", "facility", "BirthWeight.value_discharge"], 
                    keep='first'
                   )    

            # Drop helper columns if needed
            if table_exists('derived','joined_admissions_discharges'):
                    adm_cols = pd.DataFrame(get_table_column_names('joined_admissions_discharges', 'derived'))
                    new_adm_columns = set(jn_adm_dis.columns) - set(adm_cols.columns) 
                        
                    if new_adm_columns:
                        column_pairs =  [(col, str(jn_adm_dis[col].dtype)) for col in new_adm_columns]
                        if column_pairs:
                            create_new_columns('joined_admissions_discharges','derived',column_pairs)

       
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




