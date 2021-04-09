# Import created modules (need to be stored in the same directory as notebook)
import sys
from .extract_key_values import get_key_values
from .explode_mcl_columns import explode_column
from .create_derived_columns import create_columns
from conf.base.catalog import catalog
from conf.common.sql_functions import inject_sql
from data_pipeline.pipelines.data_engineering.queries.drop_derived_tables_sql import drop_derived_tables
from data_pipeline.pipelines.data_engineering.utils.date_validator import is_date
from conf.common.sql_functions import create_table



# Import libraries
import pandas as pd
from datetime import datetime as dt
import datetime
import numpy as np
import logging


def tidy_tables():
    # Read the raw admissions and discharge data into dataframes
    logging.info("... Fetching raw admission and discharge data")
    
    try:
        #Delete Schema inorder To Avoid caching of deleted exploded_tables and duplication when appending data
        # sql_script = drop_derived_tables();
        # inject_sql(sql_script, "drop-derived-tables")
        #Read Admisiions From The Kedro Catalog
        adm_raw = catalog.load('read_admissions');
        #Read Discharges From The Kedro Catalog
        dis_raw = catalog.load('read_discharges');
        #Read Maternal OutComes from Kedro Catalog
        mat_outcomes_raw = catalog.load('read_maternal_outcomes')
        #Read Vital Signs from Kedro Catalog
        vit_signs_raw = catalog.load('read_vital_signs')
        #Read Neo Lab Data from Kedro Catalog
        neolab_raw = catalog.load('read_neolab_data')

    
    except Exception as e:
        logging.error("!!! An error occured fetching the data: ")
        raise e

    # Now let's fetch the list of properties recorded in that table
    logging.info("... Extracting keys")
    try:
        
        
        adm_new_entries, adm_mcl = get_key_values(adm_raw)
        dis_new_entries, dis_mcl = get_key_values(dis_raw)
        #Add Newly Added Scripts On The Malawi Case
        mat_outcomes_new_entries,mat_outcomes_mcl = get_key_values(mat_outcomes_raw)
        vit_signs_new_entries,vit_signs_mcl = get_key_values(vit_signs_raw)
        neolab_new_entries,noelab_mcl = get_key_values(neolab_raw)
        

    except Exception as e:
        logging.error("!!! An error occured extracting keys: ")
        raise e

    # Create the dataframe (df) where each property is pulled out into its own colum
    logging.info(
        "... Creating normalized dataframes - one for admissions and one for discharges")
    try:

        adm_df = pd.json_normalize(adm_new_entries)
        if "uid" in adm_df:
            adm_df.set_index(['uid'])
        dis_df = pd.json_normalize(dis_new_entries)
        if "uid" in dis_df:
            dis_df.set_index(['uid'])
        mat_outcomes_df =pd.json_normalize(mat_outcomes_new_entries)
        if "uid" in mat_outcomes_df:
            mat_outcomes_df.set_index(['uid'])
        vit_signs_df = pd.json_normalize(vit_signs_new_entries)
        if "uid" in vit_signs_df:
            vit_signs_df.set_index(['uid'])
        neolab_df = pd.json_normalize(neolab_new_entries)
        if "uid" in neolab_df:
            neolab_df.set_index(['uid'])

        if adm_df.empty and dis_df.empty:
            logging.error(
            "Admissions and Discharges Can Not Be Empty::Please Check If You Have Set The Correct Country In database.ini ")
            sys.exit(1)
 
        # watch out for time zone (tz) issues if you change code (ref: https://github.com/pandas-dev/pandas/issues/25571)
        # admissions tables
        #Fix Missing Data Issues Emanating from eronous version published
        

        if 'DateHIVtest.value' not in adm_df.columns:
             adm_df['DateHIVtest.value']=None
             adm_df['DateHIVtest.label']=None

        if 'HIVtestResult.value' not in adm_df.columns:
            adm_df['HIVtestResult.value']=None
            adm_df['HIVtestResult.label']=None

        if 'ANVDRLDate.value' not in adm_df.columns:
            adm_df['ANVDRLDate.value']=None
            adm_df['ANVDRLDate.label']=None

        if 'HAART.value' not in adm_df.columns:
            adm_df['HAART.value']=None
            adm_df['HAART.label']=None

        if 'LengthHAART.value' not in adm_df.columns:
            adm_df['LengthHAART.value']=None
            adm_df['LengthHAART.label']=None

        if 'NVPgiven.value' not in adm_df.columns:
            adm_df['NVPgiven.value']=None
            adm_df['NVPgiven.label']=None
        # remove timezone in string to fix issues caused by converting to UTC
        if 'DateTimeAdmission.value' not in adm_df.columns:
            adm_df['DateTimeAdmission.value']=None
            adm_df['DateTimeAdmission.label']=None

        if adm_df['DateTimeAdmission.value'] is not None:
            adm_df['DateTimeAdmission.value'] = adm_df['DateTimeAdmission.value'].map(
            lambda x: str(x)[:-4])
            adm_df['DateTimeAdmission.value'] = pd.to_datetime(
            adm_df['DateTimeAdmission.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        if 'EndScriptDatetime.value' in adm_df and adm_df['EndScriptDatetime.value'] is not None:
            adm_df['EndScriptDatetime.value'] = adm_df['EndScriptDatetime.value'].map(
            lambda x: str(x)[:-4])
            adm_df['EndScriptDatetime.value'] = pd.to_datetime(
            adm_df['EndScriptDatetime.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        if  adm_df['DateHIVtest.value'] is not None:
            adm_df['DateHIVtest.value'] = adm_df['DateHIVtest.value'].map(lambda x: str(x)[
                                                                      :-4])
            adm_df['DateHIVtest.value'] = pd.to_datetime(
            adm_df['DateHIVtest.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
            
        if adm_df['ANVDRLDate.value'] is not None:
            adm_df['ANVDRLDate.value'] = adm_df['ANVDRLDate.value'].map(lambda x: str(x)[
                                                                    :-4])
            adm_df['ANVDRLDate.value'] = pd.to_datetime(
            adm_df['ANVDRLDate.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)

        # Remove Space From BW.Value :: Issue Was Affecting Dev Database
        if 'BW .label' in adm_df.columns: 
            adm_df['BW.label'] = adm_df['BW .label']
            adm_df.drop('BW .label',axis='columns',inplace=True)

        if 'BW .value' in adm_df.columns:
            adm_df['BW.value'] = adm_df['BW .value']
            adm_df.drop('BW .value', axis='columns', inplace=True)

        if 'ROMLength.label' not in adm_df.columns:
            adm_df['ROMLength.label'] = None;
        if  'ROMLength.value' not in adm_df.columns:
            adm_df['ROMLength.value'] = None;

        # discharges tables
        if 'DateAdmissionDC.value' in dis_df:
            dis_df['DateAdmissionDC.value'] = dis_df['DateAdmissionDC.value'].map(
            lambda x: str(x)[:-4])
            dis_df['DateAdmissionDC.value'] = pd.to_datetime(
            dis_df['DateAdmissionDC.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        if 'DateDischVitals.value'  in dis_df and dis_df['DateDischVitals.value'] is not None :
            dis_df['DateDischVitals.value'] = dis_df['DateDischVitals.value'].map(
            lambda x: str(x)[:-4])
            dis_df['DateDischVitals.value'] = pd.to_datetime(
            dis_df['DateDischVitals.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        if 'DateDischWeight.value' in dis_df and dis_df['DateDischWeight.value'] is not None:
            dis_df['DateDischWeight.value'] = dis_df['DateDischWeight.value'].map(
            lambda x: str(x)[:-4])
            dis_df['DateDischWeight.value'] = pd.to_datetime(
            dis_df['DateDischWeight.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        if 'DateTimeDischarge.value' in dis_df and  dis_df['DateTimeDischarge.value'] is not None:
            dis_df['DateTimeDischarge.value'] = dis_df['DateTimeDischarge.value'].map(
            lambda x: str(x)[:-4])
            dis_df['DateTimeDischarge.value'] = pd.to_datetime(
            dis_df['DateTimeDischarge.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        if 'EndScriptDatetime.value' in dis_df:
            dis_df['EndScriptDatetime.value'] = dis_df['EndScriptDatetime.value'].map(
            lambda x: str(x)[:-4])
       
            dis_df['EndScriptDatetime.value'] = pd.to_datetime(
            dis_df['EndScriptDatetime.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        if 'DateWeaned.value' in dis_df and  dis_df['DateWeaned.value']  is not None :
            dis_df['DateWeaned.value'] = dis_df['DateWeaned.value'].map(lambda x: str(x)[
                                                                    :-4])
            dis_df['DateWeaned.value'] = pd.to_datetime(
            dis_df['DateWeaned.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
        if 'DateTimeDeath.value' in dis_df and dis_df['DateTimeDeath.value'] is not None:
            dis_df['DateTimeDeath.value'] = dis_df['DateTimeDeath.value'].map(
            lambda x: str(x)[:-4])
            dis_df['DateTimeDeath.value'] = pd.to_datetime(
            dis_df['DateTimeDeath.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)

            
        #Maternal OutComes Table
        if 'DateAdmission.value' in mat_outcomes_df:
            mat_outcomes_df['DateAdmission.value'] =  pd.to_datetime(
            mat_outcomes_df['DateAdmission.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
            mat_outcomes_df['DateAdmission.value'] = mat_outcomes_df['DateAdmission.value'].map(
            lambda x: str(x)[:-4])
        if 'BirthDateDis.value' in mat_outcomes_df  and "BirthDateDis.vale" is not None:
            mat_outcomes_df['BirthDateDis.value'] =  pd.to_datetime(
            mat_outcomes_df['BirthDateDis.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
            mat_outcomes_df['BirthDateDis.value'] = mat_outcomes_df['BirthDateDis.value'].map(
            lambda x: str(x)[:-4])
        else:
            mat_outcomes_df["BirthDateDis.value"] = None
        if "TypeBirth.label" not in mat_outcomes_df:
            mat_outcomes_df["TypeBirth.label"] = None
        if "Presentation.label" not in mat_outcomes_df:
            mat_outcomes_df["Presentation.label"] = None
        if "BabyNursery.label" not in mat_outcomes_df:
            mat_outcomes_df["BabyNursery.label"] = None
        if "Reason.label" not in mat_outcomes_df:
            mat_outcomes_df["Reason.label"] = None
        if "ReasonOther.label" not in mat_outcomes_df:
            mat_outcomes_df["ReasonOther.label"] = None
        if "CryBirth.label" not in mat_outcomes_df:
            mat_outcomes_df["CryBirth.label"] = None
        if "Apgar1.value" not in mat_outcomes_df:
                mat_outcomes_df["Apgar1.value"] = None
        if "Apgar5.value" not in mat_outcomes_df:
                mat_outcomes_df["Apgar5.value"] = None
        if "Apgar10.value" not in mat_outcomes_df:
                mat_outcomes_df["Apgar10.value"] = None
        if "PregConditions.label" not in mat_outcomes_df:
                mat_outcomes_df["PregConditions.label"] = None
        #Vital Signs Table
        if 'D1Date.value' in vit_signs_df :
            vit_signs_df['D1Date.value'] =  pd.to_datetime(
            vit_signs_df['D1Date.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
            vit_signs_df['D1Date.value'] = vit_signs_df['D1Date.value'].map(
            lambda x: str(x)[:-4])

    
       
        if 'TimeTemp1.value' in vit_signs_df:
            vit_signs_df['TimeTemp1.value'] =  pd.to_datetime(
            vit_signs_df['TimeTemp1.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
            vit_signs_df['TimeTemp1.value'] = vit_signs_df['TimeTemp1.value'].map(
            lambda x: str(x)[:-4])
            
        if 'TimeTemp2.value' in vit_signs_df:
            vit_signs_df['TimeTemp2.value'] =  pd.to_datetime(
            vit_signs_df['TimeTemp2.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
            vit_signs_df['TimeTemp2.value'] = vit_signs_df['TimeTemp2.value'].map(
            lambda x: str(x)[:-4])

        if 'EndScriptDatetime.value' in vit_signs_df:
            vit_signs_df['EndScriptDatetime.value'] =  pd.to_datetime(
            vit_signs_df['EndScriptDatetime.value'], format='%Y-%m-%dT%H:%M:%S', utc=True)
            vit_signs_df['EndScriptDatetime.value'] = vit_signs_df['EndScriptDatetime.value'].map(
            lambda x: str(x)[:-4])


        # Make changes to admissions to match fields in power bi

        adm_df = create_columns(adm_df)

    except Exception as e:
        logging.error(
            "!!! An error occured normalized dataframes/changing data types: ")
        raise e

    # Now write the cleaned up admission and discharge tables back to the database
    logging.info(
        "... Writing the tidied admission and discharge back to the database")
    try:
       
    
        #Save Derived Admissions To The DataBase Using Kedro
        catalog.save('create_derived_admissions',adm_df)
        #Save Derived Admissions To The DataBase Using Kedro
        catalog.save('create_derived_discharges',dis_df)
        #Save Derived Maternal Outcomes To The DataBase Using Kedro
        
        catalog.save('create_derived_maternal_outcomes',mat_outcomes_df)
         #Save Derived Vital Signs To The DataBase Using Kedro
        
        catalog.save('create_derived_vital_signs',vit_signs_df)
        #Save Derived NeoLab To The DataBase Using Kedro
        
        catalog.save('create_derived_neolab',neolab_df)



    except Exception as e:
        logging.error(
            "!!! An error occured writing admissions and discharge output back to the database: ")
        raise e

    logging.info("... Creating MCL count tables")
    try:
        explode_column(adm_df, adm_mcl)
        explode_column(dis_df, dis_mcl)
        explode_column(mat_outcomes_df,mat_outcomes_mcl)
        explode_column(vit_signs_df,vit_signs_mcl)
       
    except Exception as e:
        logging.error("!!! An error occured exploding MCL  columns: ")
        raise e

