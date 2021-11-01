# Import created modules (need to be stored in the same directory as notebook)
import sys
from .extract_key_values import get_key_values, get_diagnoses_key_values
from .explode_mcl_columns import explode_column
from .create_derived_columns import create_columns
from conf.base.catalog import catalog
from conf.common.sql_functions import inject_sql
from data_pipeline.pipelines.data_engineering.utils.date_validator import is_date
from conf.common.sql_functions import create_table
from data_pipeline.pipelines.data_engineering.utils.custom_date_formatter import format_date,format_date_without_timezone



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
        #Read Baseline Data from Kedro Catalog
        baseline_raw = catalog.load('read_baseline_data')
        #Read Diagnoses Data from Kedro Catalog
        diagnoses_raw = catalog.load('read_diagnoses_data') 
        #Read Maternity Completeness Data from Kedro Catalog
        mat_completeness_raw = catalog.load('read_mat_completeness_data') 


    
    except Exception as e:
        logging.error("!!! An error occured fetching the data: ")
        raise e

    # Now let's fetch the list of properties recorded in that table
    logging.info("... Extracting keys")
    try:
        
        
        adm_new_entries, adm_mcl = get_key_values(adm_raw)
        dis_new_entries, dis_mcl = get_key_values(dis_raw)
        mat_outcomes_new_entries,mat_outcomes_mcl = get_key_values(mat_outcomes_raw)
        vit_signs_new_entries,vit_signs_mcl = get_key_values(vit_signs_raw)
        neolab_new_entries,noelab_mcl = get_key_values(neolab_raw)
        baseline_new_entries,baseline_mcl = get_key_values(baseline_raw)
        diagnoses_new_entries = get_diagnoses_key_values(diagnoses_raw)
        mat_completeness_new_entries,mat_completeness_mcl = get_key_values(mat_completeness_raw)

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

        baseline_df = pd.json_normalize(baseline_new_entries)
        if "uid" in baseline_df:
            baseline_df.set_index(['uid'])

        diagnoses_df = pd.json_normalize(diagnoses_new_entries)
        if "uid" in diagnoses_df:
            diagnoses_df.set_index(['uid'])

        mat_completeness_df = pd.json_normalize(mat_completeness_new_entries)
        if "uid" in mat_completeness_df:
            mat_completeness_df.set_index(['uid'])

        # INITIALISE THE EPISODE COLUMN ON NEOAB DF SO THAT THE COLUMN GETS CREATED
        
 
        # ADD TIME SPENT TO ALL DFs
        if "started_at" in diagnoses_df and 'completed_at' in diagnoses_df :
           format_date_without_timezone(diagnoses_df,'time_spent'); 
           format_date_without_timezone(diagnoses_df,'completed_at'); 
           diagnoses_df['time_spent']= (diagnoses_df['completed_at']-diagnoses_df['started_at']).astype('timedelta64[m]')
        else:
            diagnoses_df['time_spent'] = None

        if "started_at" in adm_df and 'completed_at' in adm_df :
            format_date_without_timezone(adm_df,'started_at'); 
            format_date_without_timezone(adm_df,'completed_at'); 
            adm_df['time_spent'] = (adm_df['completed_at'] - adm_df['started_at']).astype('timedelta64[m]')
        else:
            adm_df['time_spent'] = None
        
        if "started_at" in dis_df and 'completed_at' in dis_df :
            format_date_without_timezone(dis_df,'started_at'); 
            format_date_without_timezone(dis_df,'completed_at'); 
            dis_df['time_spent'] = (dis_df['completed_at'] -dis_df['started_at']).astype('timedelta64[m]')
        else:
            dis_df['time_spent'] = None
        
        if "started_at" in mat_outcomes_df and 'completed_at' in mat_outcomes_df :
            format_date_without_timezone(mat_outcomes_df,'started_at'); 
            format_date_without_timezone(mat_outcomes_df,'completed_at'); 
            mat_outcomes_df['time_spent'] = (mat_outcomes_df['completed_at'] - mat_outcomes_df['started_at']).astype('timedelta64[m]')
        else:
            mat_outcomes_df['time_spent'] = None

        if "started_at" in vit_signs_df and 'completed_at' in vit_signs_df :
            format_date_without_timezone(vit_signs_df,'started_at'); 
            format_date_without_timezone(vit_signs_df,'completed_at'); 
            vit_signs_df['time_spent'] = (vit_signs_df['completed_at']-vit_signs_df['started_at']).astype('timedelta64[m]')
        else:
            vit_signs_df['time_spent'] = None
        
        if "started_at" in neolab_df and 'completed_at' in neolab_df :
            format_date_without_timezone(neolab_df,'started_at'); 
            format_date_without_timezone(neolab_df,'completed_at'); 
            neolab_df['time_spent'] = (neolab_df['completed_at'] - neolab_df['started_at']).astype('timedelta64[m]')
        else:
            neolab_df['time_spent'] = None

        if "started_at" in baseline_df and 'completed_at' in baseline_df :
            format_date_without_timezone(baseline_df,'started_at'); 
            format_date_without_timezone(baseline_df,'completed_at'); 
            baseline_df['time_spent'] = (baseline_df['completed_at'] -baseline_df['started_at']).astype('timedelta64[m]')
        else:
            baseline_df['time_spent'] = None

        if "started_at" in diagnoses_df and 'completed_at' in diagnoses_df :
            format_date_without_timezone(diagnoses_df,'started_at'); 
            format_date_without_timezone(diagnoses_df,'completed_at'); 
            diagnoses_df['time_spent'] =  (diagnoses_df['completed_at'] - diagnoses_df['started_at']).astype('timedelta64[m]')
        else:
            diagnoses_df['time_spent'] = None

        baseline_df['LengthOfStay.value'] = None
        baseline_df['LengthOfStay.label'] = None
        baseline_df['LengthOfLife.value'] = None
        baseline_df['LengthOfLife.label'] = None
        
        #Length of Life and Length of Stay on Baseline Data
        date_format = "%Y-%m-%d"
        for index, row in baseline_df.iterrows():

            baseline_df['LengthOfStay.label'].iloc[index] ="Length of Stay"
            if (is_date(str(row['DateTimeDischarge.value']))
                and is_date(str(row['DateTimeAdmission.value']))):
                DateTimeDischarge = dt.strptime(str(str(row['DateTimeDischarge.value']))[:10].strip().replace('T',''),date_format)
                DateTimeAdmission = dt.strptime(str(str(row['DateTimeAdmission.value']))[:10].strip().replace('T',''),date_format)
                delta_los = DateTimeDischarge-DateTimeAdmission
                baseline_df['LengthOfStay.value'].iloc[index] = delta_los.days

            else:
                baseline_df['LengthOfStay.value'].iloc[index] = None
        
            baseline_df['LengthOfLife.label'].iloc[index] ="Length of Life"
            if 'DateTimeDeath.value' in row and (is_date(str(row['DateTimeDeath.value']))
                and is_date(str(row['DateTimeAdmission.value']))): 
                DateTimeDeath = dt.strptime(str(str(row['DateTimeDeath.value']))[:10].strip().replace('T',''), date_format)
                DateTimeAdmission = dt.strptime(str(str(row['DateTimeAdmission.value']))[:10].strip().replace('T',''), date_format)
                delta_lol = DateTimeDeath - DateTimeAdmission
                baseline_df['LengthOfLife.value'].iloc[index] = delta_lol.days;
            else:
                baseline_df['LengthOfLife.value'].iloc[index] = None;



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

        if 'DateTimeAdmission.value' in adm_df:
            format_date(adm_df,'DateTimeAdmission.value')
        if 'EndScriptDatetime.value' in adm_df:
            format_date(adm_df,'EndScriptDatetime.value')
        if  'DateHIVtest.value' in adm_df:
            format_date(adm_df,'DateHIVtest.value')
            
        if 'ANVDRLDate.value' in adm_df:
            format_date(adm_df,'ANVDRLDate.value')

        # Remove Space From BW.Value :: Issue Was Affecting Dev Database
        # if 'BW .label' in adm_df.columns and adm_df['BW .label'].notnull(): 
        #     adm_df['BW.label'] = adm_df['BW .label']
        #     adm_df.drop('BW .label',axis='columns',inplace=True)

        # if 'BW .value' in adm_df.columns and  adm_df['BW .value'].notnull():
        #     adm_df['BW.value'] = adm_df['BW .value']
        #     adm_df.drop('BW .value', axis='columns', inplace=True)

        if 'ROMLength.label' not in adm_df.columns:
            adm_df['ROMLength.label'] = None;
        if  'ROMLength.value' not in adm_df.columns:
            adm_df['ROMLength.value'] = None;

        # discharges tables
        if 'DateAdmissionDC.value' in dis_df:
            format_date(dis_df,'DateAdmissionDC.value')
        if 'DateDischVitals.value'  in dis_df and dis_df['DateDischVitals.value'] is not None :
            format_date(dis_df,'DateDischVitals.value')
        if 'DateDischWeight.value' in dis_df and dis_df['DateDischWeight.value'] is not None:
            format_date(dis_df,'DateDischWeight.value')
        if 'DateTimeDischarge.value' in dis_df and  dis_df['DateTimeDischarge.value'] is not None:
            format_date(dis_df,'DateTimeDischarge.value')
        if 'EndScriptDatetime.value' in dis_df:
            format_date(dis_df,'EndScriptDatetime.value')
        if 'DateWeaned.value' in dis_df and  dis_df['DateWeaned.value']  is not None :
           format_date(dis_df,'DateWeaned.value')
        if 'DateTimeDeath.value' in dis_df and dis_df['DateTimeDeath.value'] is not None:
            format_date(dis_df,'DateTimeDeath.value')
            
        #Maternal OutComes Table
        if 'DateAdmission.value' in mat_outcomes_df:
            format_date(dis_df,'DateAdmission.value')
        if 'BirthDateDis.value' in mat_outcomes_df :
            format_date(dis_df,'BirthDateDis.value')
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

        # Baselines Tables
        if "DateTimeAdmission.value" in baseline_df:
            format_date(baseline_df,'DateTimeAdmission.value')
        if "DateTimeDischarge.value" in baseline_df:
            format_date(baseline_df,'DateTimeDischarge.value')
        if "DateTimeDeath.value" in baseline_df:
            format_date(baseline_df,'DateTimeDeath.value')
        #Vital Signs Table
        if 'D1Date.value' in vit_signs_df :
            format_date(vit_signs_df,'D1Date.value')

    
       
        if 'TimeTemp1.value' in vit_signs_df:
            format_date(vit_signs_df,'TimeTemp1.value')
            
        if 'TimeTemp2.value' in vit_signs_df:
            format_date(vit_signs_df,'TimeTemp2.value')

        if 'EndScriptDatetime.value' in vit_signs_df:
            format_date(vit_signs_df,'EndScriptDatetime.value')

        # Make changes to admissions and baseline data to match fields in power bi
        adm_df = create_columns(adm_df)

        if not baseline_df.empty:
            baseline_df = create_columns(baseline_df)

        # Create Episode Column for Neolab Data

        if not neolab_df.empty:
            # Initialise the column
            neolab_df['episode'] = 0

            for index, rows in neolab_df.iterrows():
                logging.info("--MY INDEX---",index)
                control_df = rows.loc[(rows['uid'] == rows[index,'uid'])]
                if rows[index,'episode'] == 0 and control_df:
                    for innerIndex in control_df.index:
                        # Set Episode For first Row
                        if innerIndex == 0:
                            rows[index,'episode']= 1
                            # Set It Also On New DF For Reference In Future Episodes
                            control_df[innerIndex,'episode'] = 1
                        else:
                            if control_df[innerIndex,'DateBCT.value'] == control_df[innerIndex-1,'DateBCT.value']:
                                control_df[innerIndex, 'episode'] = control_df[innerIndex-1,'episode'];
                                
                            else:
                                control_df[innerIndex, 'episode'] = control_df[innerIndex-1,'episode']+1;
                        # Set The Episode Value For All Related Episodes        
                        rows.loc[(rows['uid']
                                ==control_df[innerIndex,'uid'] & rows['DateBCT.value']
                                ==control_df[innerIndex,'DateBCT.value']& rows['DateBCR.value']
                                == control_df[innerIndex,'DateBCR.value'])][0]['episode'] = control_df[innerIndex, 'episode']


 

    except Exception as e:
        logging.error(
            "!!! An error occured normalized dataframes/changing data types: ")
        raise e

    # Now write the cleaned up admission and discharge tables back to the database
    logging.info(
        "... Writing the tidied admission and discharge back to the database")
    try:
       
    
        #Save Derived Admissions To The DataBase Using Kedro
        if not adm_df.empty:
            catalog.save('create_derived_admissions',adm_df)
        #Save Derived Admissions To The DataBase Using Kedro
        if not dis_df.empty:
            catalog.save('create_derived_discharges',dis_df)
        #Save Derived Maternal Outcomes To The DataBase Using Kedro
        if not mat_outcomes_df.empty:
            catalog.save('create_derived_maternal_outcomes',mat_outcomes_df)
         #Save Derived Vital Signs To The DataBase Using Kedro
        if not vit_signs_df.empty:
            catalog.save('create_derived_vital_signs',vit_signs_df)
        #Save Derived NeoLab To The DataBase Using Kedro
        if not neolab_df.empty:
            catalog.save('create_derived_neolab',neolab_df)
        #Save Derived Baseline To The DataBase Using Kedro
        if not baseline_df.empty:
            catalog.save('create_derived_baselines',baseline_df)

         #Save Derived Diagnoses To The DataBase Using Kedro
        if not diagnoses_df.empty:
            catalog.save('create_derived_diagnoses',diagnoses_df)

         #Save Derived Maternity Completeness To The DataBase Using Kedro
        if not mat_completeness_df.empty:
            catalog.save('create_derived_maternity_completeness',mat_completeness_df)


    except Exception as e:
        logging.error(
            "!!! An error occured writing admissions and discharge output back to the database: ")
        raise e

    logging.info("... Creating MCL count tables")
    try:
        if not adm_df.empty:
            explode_column(adm_df, adm_mcl,"")
        if not dis_df.empty:
            explode_column(dis_df, dis_mcl,"disc_")
        if not mat_outcomes_df.empty:
            explode_column(mat_outcomes_df,mat_outcomes_mcl,"mat_")
        if not vit_signs_df.empty:
            explode_column(vit_signs_df,vit_signs_mcl,"vit_")

        if not baseline_df.empty:
            explode_column(baseline_df,baseline_mcl,"bsl_")
        
        if not mat_completeness_df.empty:
            explode_column(mat_completeness_df,mat_completeness_mcl,"matcomp_")
       
    except Exception as e:
        logging.error("!!! An error occured exploding MCL  columns: ")
        raise e

