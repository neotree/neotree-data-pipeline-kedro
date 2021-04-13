from conf.base.catalog import catalog
from dateutil.parser import parse
from data_pipeline.pipelines.data_engineering.utils.date_validator import is_date

# Import libraries
import pandas as pd
import logging
from datetime import datetime as dt
import datetime


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
        
        # join admissions and discharges
        jn_adm_dis = adm_df.merge(dis_df, how='left', on=['uid','facility'],suffixes=('','_discharge'))

        jn_adm_dis['LengthOfStay.value'] = None
        jn_adm_dis['LengthOfStay.label'] = None
        jn_adm_dis['LengthOfLife.value'] = None
        jn_adm_dis['LengthOfLife.label'] = None
        
        #Length of Life and Length of Stay
        date_format = "%Y-%m-%d"
        for index, row in jn_adm_dis.iterrows():

            jn_adm_dis['LengthOfStay.label'].iloc[index] ="Length of Stay"
            if (is_date(str(row['DateTimeDischarge.value']))
                and is_date(str(row['DateTimeAdmission.value']))):
                DateTimeDischarge = dt.strptime(str(str(row['DateTimeDischarge.value']))[:-14].strip(),date_format)
                DateTimeAdmission = dt.strptime(str(str(row['DateTimeAdmission.value']))[:-14].strip(),date_format)
                delta_los = DateTimeDischarge -DateTimeAdmission
                jn_adm_dis['LengthOfStay.value'].iloc[index] = delta_los.days

            else:
                jn_adm_dis['LengthOfStay.value'].iloc[index] = None
        
            jn_adm_dis['LengthOfLife.label'].iloc[index] ="Length of Life"
            if 'DateTimeDeath.value' in row and (is_date(str(row['DateTimeDeath.value']))
                and is_date(str(row['DateTimeAdmission.value']))): 
                DateTimeDeath = dt.strptime(str(str(row['DateTimeDeath.value']))[:-14].strip(), date_format)
                DateTimeAdmission = dt.strptime(str(str(row['DateTimeAdmission.value']))[:-14].strip(), date_format)
                delta_lol = DateTimeDeath - DateTimeAdmission
                jn_adm_dis['LengthOfLife.value'].iloc[index] = delta_lol.days;
            else:
                jn_adm_dis['LengthOfLife.value'].iloc[index] = None;


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
