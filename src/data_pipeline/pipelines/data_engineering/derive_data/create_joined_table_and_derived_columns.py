from conf.base.catalog import catalog,params
import pandas as pd
from data_pipeline.pipelines.data_engineering.utils.date_validator import is_date, is_date_formatable
from data_pipeline.pipelines.data_engineering.utils.custom_date_formatter import format_date_without_timezone


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
        jn_adm_dis = pd.DataFrame()
        # Convert Date columns to datetime format
        adm_df['DateTimeAdmission.value'] = pd.to_datetime(adm_df['DateTimeAdmission.value'], errors='coerce')
        dis_df['DateTimeAdmission.value'] = pd.to_datetime(dis_df['DateTimeAdmission.value'], format='%Y-%m-%dT%H:%M:%S', errors='coerce')
        #Extract the date part (ignoring time) for duplicate handling
        adm_df['Date_only'] = adm_df['DateTimeAdmission.value'].dt.date
        dis_df['Date_only'] = dis_df['DateTimeAdmission.value'].dt.date

        # Handle cases where discharge Date is null
        # Separate rows with non-null and null Dates in dis_df
        dis_df_with_date = dis_df[dis_df['Date_only'].notna()]

        if ('country' in params and str(params['country']).lower()) =='zimbabwe':
            # Merge for non-null Dates (exact match)
            jn_adm_dis = adm_df.merge(
            dis_df_with_date, 
            how='inner', 
            on=['uid', 'facility','Date_only'], 
            suffixes=('', '_discharge')
            )
            # Drop helper columns if needed
            jn_adm_dis.drop(columns=['Date_only'],inplace=True)

        else:
            # Merge for non-null Dates (exact match)
            jn_adm_dis = adm_df.merge(
            dis_df_with_date, 
            how='left', 
            on=['uid', 'facility','Date_only'], 
            suffixes=('', '_discharge')
            )
            # Drop helper columns if needed
            jn_adm_dis.drop(columns=['Date_only'],inplace=True)

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
            if 'DateTimeDeath.value' in row and is_date_formatable(str(row['DateTimeDeath.value']).strip()):
               
                DateTimeDeath = dt.strptime(str(str(row['DateTimeDeath.value']))[:10].strip(), date_format)
                DateTimeAdmission = dt.strptime(str(row['DateTimeAdmission.value'])[:10].strip(), date_format)
                delta_lol = DateTimeDeath - DateTimeAdmission
                jn_adm_dis.loc[index,'LengthOfLife.value'] = delta_lol.days;
            else:
                jn_adm_dis.loc[index, 'LengthOfLife.value'] = None


    except Exception as e:
        logging.error("!!! An error occured creating joined dataframe: ")
        raise e

    # Now write the table back to the database
    logging.info("... Writing the output back to the database")
    try:
       
        #Create Table Using Kedro
        catalog.save('create_joined_admissions_discharges',jn_adm_dis)

    except Exception as e:
        logging.error(
            "!!! An error occured writing join output back to the database: ")
        raise e

    logging.info("... Join script completed!")
