# Import created modules (need to be stored in the same directory as notebook)
from conf.common.format_error import formatError
from .extract_key_values import get_key_values, get_diagnoses_key_values
from .explode_mcl_columns import explode_column
from .create_derived_columns import create_columns
from conf.common.sql_functions import create_new_columns,get_table_column_names
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from conf.base.catalog import catalog
from data_pipeline.pipelines.data_engineering.utils.date_validator import is_date
from data_pipeline.pipelines.data_engineering.utils.assorted_fixes import extract_years
from data_pipeline.pipelines.data_engineering.utils.custom_date_formatter import format_date,format_date_without_timezone
from data_pipeline.pipelines.data_engineering.queries.fix_duplicate_uids_for_diff_records import fix_duplicate_uid
from data_pipeline.pipelines.data_engineering.queries.update_uid import update_uid
from data_pipeline.pipelines.data_engineering.utils.key_change import key_change
from data_pipeline.pipelines.data_engineering.utils.set_key_to_none import set_key_to_none
from data_pipeline.pipelines.data_engineering.utils.data_label_fixes import format_column_as_numeric, convert_false_numbers_to_text
from .neolab_data_cleanup import neolab_cleanup
from .tidy_dynamic_tables import tidy_dynamic_tables

import datetime
# Import libraries
import pandas as pd
from datetime import datetime as dt
import logging

def tidy_tables():

    # try:
    #     tuples = fix_duplicate_uid()
    #     duplicate_df = pd.DataFrame(tuples,columns=['id','uid','DateAdmission'])
    #     if not duplicate_df.empty:
    #         unique_uids = duplicate_df['uid'].copy().unique()
           
    #         alphabet = "0A1B2C3D4E5F6789"
    #         for ind in unique_uids:
    #            dup_df = duplicate_df[(duplicate_df['uid'] == str(ind))].copy().reset_index(drop=True)

    #            if not dup_df.empty and len(dup_df)>1:
    #                prev_record = None
    #                for dup_index, dup in dup_df.iterrows():
    #                    if dup_index >=1 and dup['DateAdmission'] is not None:
    #                        adm_date = str(dup['DateAdmission'])
    #                        prev_adm_date = None
    #                        if prev_record is not None and prev_record['DateAdmission'] is not None:
    #                             prev_adm_date = str(prev_record['DateAdmission'])
    #                        if adm_date == prev_adm_date:
    #                            # RECORD IS A DUPLICATE AND WILL BE DELT WITH DURING DEDUPLICATION PROCESS ON NEXT RUN OF PIPELINE
    #                            pass
                        
    #                        else:
    #                         #    #GENERATE NEW UID
    #                             uid = '78'.join((random.choice(alphabet)) for x in range(2))+'-'+str(random.randint(1000,9999))
    #                             update_uid('public','sessions',dup['id'],uid) 
    #                    prev_record = dup    
    #     logging.info("...DONE WITH UPDATE......")
    #     sys.exit()
        
    # except Exception as ex:
    #     raise ex

    # Read the raw admissions and discharge data into dataframes
    try:
        tidy_dynamic_tables()
        
    except Exception as e:
        logging.error("!!! An error occured processing Dynamic Scripts ")
        logging.error(formatError(e))
        
    logging.info("... Fetching raw admission and discharge data")
    
    try:
        #Read Admisiions From The Kedro Catalog
        adm_raw = catalog.load('read_admissions')
        #Read Discharges From The Kedro Catalog
        dis_raw = catalog.load('read_discharges')
        #Read Maternal OutComes from Kedro Catalog
        mat_outcomes_raw = catalog.load('read_maternal_outcomes')
        #Read Vital Signs from Kedro Catalog
        vit_signs_raw = catalog.load('read_vitalsigns')
        #Read Neo Lab Data from Kedro Catalog
        neolab_raw = catalog.load('read_neolab')
        #Read Baseline Data from Kedro Catalog
        baseline_raw = catalog.load('read_baseline')
        #Read Diagnoses Data from Kedro Catalog
        diagnoses_raw = catalog.load('read_diagnoses_data') 
        #Read Maternity Completeness Data from Kedro Catalog
        mat_completeness_raw = catalog.load('read_maternity_completeness') 


    except Exception as e:
        logging.error("!!! An error occured fetching the data: ")
        logging.error(formatError(e))

    # Now let's fetch the list of properties recorded in that table
    logging.info("... Extracting keys")
    try:  
        adm_new_entries, adm_mcl = get_key_values(adm_raw)
        dis_new_entries, dis_mcl = get_key_values(dis_raw)
        mat_outcomes_new_entries,mat_outcomes_mcl = get_key_values(mat_outcomes_raw)
        vit_signs_new_entries,vit_signs_mcl = get_key_values(vit_signs_raw)
        
        baseline_new_entries,baseline_mcl = get_key_values(baseline_raw)
        diagnoses_new_entries = get_diagnoses_key_values(diagnoses_raw)
        mat_completeness_new_entries,mat_completeness_mcl = get_key_values(mat_completeness_raw)

        neolab_new_entries,noelab_mcl = get_key_values(neolab_raw)

    except Exception as e:
        logging.error("!!! An error occured extracting keys: ")
        logging.error(formatError(e))

    # Create the dataframe (df) where each property is pulled out into its own colum
    logging.info(
        "... Creating normalized dataframes - one for admissions and one for discharges")
    try:
        adm_df = pd.json_normalize(adm_new_entries)
        admissions_date_time_fields =[]
        if not adm_df.empty:
            adm_df.set_index(['uid'])
            adm_df = format_date_without_timezone(adm_df,['started_at', 'completed_at','EndScriptDatetime.value','DateTimeAdmission.value'])
            adm_df= format_date(adm_df,['DateHIVtest.value','ANVDRLDate.value'])
              # ADD TIME SPENT TO ALL DFs
            if "started_at" in adm_df and 'completed_at' in adm_df :
                adm_df['time_spent'] = (adm_df['completed_at'] - adm_df['started_at']).astype('timedelta64[m]')
            else:
                adm_df['time_spent'] = None 
            adm_df=  set_key_to_none(adm_df,['DateHIVtest.value','DateHIVtest.label','HIVtestResult.value','HIVtestResult.label',
            'ANVDRLDate.value','ANVDRLDate.label','HAART.value','HAART.label','LengthHAART.value','LengthHAART.label',
            'NVPgiven.value','NVPgiven.label','ROMlength.label','ROMlength.value','ROMLength.label','ROMLength.value',
            'TempThermia.value','AWGroup.value'])
            
            # CREATE AGE CATEGORIES
            for position,admission in adm_df.iterrows():

                age_list =[]
                period = 0

                if 'Age.value' in admission:
                    if len(str(admission['Age.value']))>10 and 'T' in str(admission['Age.value']):
                        if "DateTimeAdmission.value" in admission and admission["DateTimeAdmission.value"] is not None:
                            #FIX BUG WHERE DOB IS GREATER THAN DATE OF ADMISSIONS
                            if (pd.to_datetime(admission['DateTimeAdmission.value'], format='%Y-%m-%dT%H:%M:%S',utc=True,errors='coerce')<
                            pd.to_datetime(admission['Age.value'], format='%Y-%m-%dT%H:%M:%S',utc=True,errors='coerce')):
                                admission['Age.value'] = pd.to_datetime(admission['Age.value'], format='%Y-%m-%dT%H:%M:%S',utc=True,errors='coerce')-pd.Timedelta(hours=24)

                            admission['Age.value']=(pd.to_datetime(admission['DateTimeAdmission.value'], format='%Y-%m-%dT%H:%M:%S',utc=True,errors='coerce') -
                                        pd.to_datetime(admission['Age.value'], format='%Y-%m-%dT%H:%M:%S',utc=True,errors='coerce'))/ pd.Timedelta(hours=1)
                                        
                        if admission['Age.value']>0:
                            if admission['Age.value'] <1:
                                admission['Age.value'] = 1
                            else:
                               admission['Age.value']= round(admission['Age.value'])

                if 'Age.value' in admission and str(admission['Age.value']).isdigit():
                    period=admission['Age.value']

                else:
                    if 'Age.value' in admission and str(admission['Age.value']) != 'nan':
                    # Get The Value which is a string e.g  3 days, 4 hours
                        age_list = str(admission['Age.value']).split(",")
                    else:
                        if 'AgeB.value' in admission and str(admission['AgeB.value']) != 'nan':
                            age_list = str(admission['AgeB.value']).split(",")
                    # Initialise Hours
                    hours = 0
                    
                    # If size of List is 1 it either means its days only or hours only
                
                    if len(age_list) == 1:
                        age = age_list[0]
                        # Check if hours or Days
                        if 'hour' in age:
                        
                            hours= [int(s) for s in age.replace("-","").split() if s.isdigit()]
                            # Check if value contains figures
                            if len(hours) >0:
                                period = hours[0]
                            else:
                                if "an" in age:
                                    # IF AN HOUR 
                                    period = 1

                        elif 'day' in age:
                            hours = [int(s) for s in age.replace("-","").split() if s.isdigit()]
                            if len(hours) >0:
                                period = hours[0] * 24
                        elif 'second' in age:
                            # FEW SECONDS CAN BE ROUNDED OFF 1 HOUR
                            period = 1
                        elif 'minute' in age:
                            # MINUTES CAN BE ROUNDED OFF 1 HOUR
                            period = 1
                            pass     
                    # Contains Both Hours and Days        
                    elif len(age_list) == 2:
                        age_days = age_list[0]
                        age_hours = age_list[1]
                        if 'day' in age_days and 'hour' in age_hours:
                            number_hours_days= [int(s) for s in age_days.split() if s.isdigit()]
                            number_hours = [int(s) for s in age_hours.split() if s.isdigit()]
                            if (len(number_hours) >0 and len(number_hours_days)>0):
                                period = (number_hours_days[0]) * 24 +(number_hours[0])

                    else:
                        pass  
                if(type(period)!=int):
                    period=0
                    
                if period>0:
                    adm_df.loc[position,'Age.value'] = period
                    if period< 2:
                        adm_df.loc[position,'AgeCategory'] = 'Fresh Newborn (< 2 hours old)'
                    elif period>2 and period<=23:
                        adm_df.loc[position,'AgeCategory'] = 'Newborn (2 - 23 hrs old)'
                    elif period>23 and period<=47:
                        adm_df.loc[position,'AgeCategory']= 'Newborn (1 day - 1 day 23 hrs old)'
                    elif period>47 and period<= 71:
                        adm_df.loc[position,'AgeCategory']= 'Infant (2 days - 2 days 23 hrs old)' 
                    else:
                        adm_df.loc[position,'AgeCategory'] = 'Infant (> 3 days old)' 
                ########################## UPDATE ADMISSION SCRIPT WITH NEW KEYS ########################
                if  "BirthWeight.value" in admission and str(admission["BirthWeight.value"]) != 'nan' and admission["BirthWeight.value"] is not None:
                    pass
                else:
                    if('BW.value' in admission and str(admission["BW.value"]) != 'nan' and admission["BW.value"] is not None):
                        key_change(adm_df,admission,position,'BW.value','BirthWeight.value')
                    else:
                        if('BW .value' in admission and str(admission["BW .value"]) != 'nan' and admission["BW .value"] is not None):
                            key_change(adm_df,admission,position,'BW .value','BirthWeight.value')
                if "Convulsions.value" in admission and str(admission["Convulsions.value"]) != 'nan' and admission["Convulsions.value"] is not None:
                    pass
                else:
                    key_change(adm_df,admission,position,'Conv.value','Convulsions.value')
                if ('SymptomReviewNeurology.value' in admission and str(admission["SymptomReviewNeurology.value"]) != 'nan' 
                    and admission["SymptomReviewNeurology.value"] is not None):
                    pass
                else:
                    key_change(adm_df,admission,position,'SRNeuroOther.value','SymptomReviewNeurology.value')
                if 'LowBirthWeight.value' in admission and str(admission["LowBirthWeight.value"]) !='nan' and admission["LowBirthWeight.value"] is not None:
                    pass
                else:
                    key_change(adm_df,admission,position,'LBW.value','LowBirthWeight.value')
                if 'AdmissionWeight.value' in admission and str(admission["AdmissionWeight.value"]) != 'nan' and admission["AdmissionWeight.value"] is not None :
                    pass
                else:
                    key_change(adm_df,admission,position,'AW.value','AdmissionWeight.value')
                #Fix differences in Column data type definition
                if 'BSUnitmg.value' in admission and str(admission["BSUnitmg.value"]) !='nan' and admission["BSUnitmg.value"] is not None:
                    pass
                else:
                    key_change(adm_df,admission,position,'BSmgdL.value','BSUnitmg.value')
                if 'BSmmol.value' in admission and str(admission["BSmmol.value"])!='nan' and admission["BSmmol.value"] is not None:
                    
                    key_change(adm_df,admission,position,'BSmmol.value','BloodSugarmmol.value')

                if 'BSmg.value' in admission and str(admission["BSmg.value"])!='nan' and admission["BSmg.value"] is not None:
                    key_change(adm_df,admission,position,'BSmg.value','BloodSugarmg.value')
                    
                if  "ROMlength.value" in admission and str(admission["ROMlength.value"]) != 'nan' and admission["ROMlength.value"] is not None:
                    key_change(adm_df,admission,position,'ROMlength.value','ROMLength.value')
                
                if  "ROMlength.label" in admission and str(admission["ROMlength.label"]) != 'nan' and admission["ROMlength.label"] is not None:
                    key_change(adm_df,admission,position,'ROMlength.label','ROMLength.label')

            if 'AdmissionWeight.value' in adm_df:
                adm_df['AdmissionWeight.value'] = pd.to_numeric(adm_df['AdmissionWeight.value'],downcast='integer', errors='coerce')

            ## DROP UNNECESSARY COLUMNS
            if 'BW.value' in adm_df:
                adm_df = adm_df.drop(columns=['BW.value'])
            if 'BW .value' in adm_df:
                adm_df=adm_df.drop(columns=['BW .value'])
                
        # Make changes to admissions and baseline data to match fields in power bi  
            adm_df = create_columns(adm_df)
            adm_df= adm_df[adm_df['uid'] != 'Unknown']
            #Format Dates Admissions Tables
            adm_df.columns = adm_df.columns.str.replace(r"[()-]", "_", regex=True)
            #Save Derived Admissions To The DataBase Using Kedro
            
            # fomarting columns
            
            numeric_fields = ['Temperature','RR','HR','Age','PAR','OFC',
              'Apgar1','Apgar5','Length','SatsO2','SatsAir','BalScore',
              'VLNumber','Gestation','MatAgeYrs','BalScoreWks','BirthWeight',
              'DurationLab','BloodSugarmg','AntenatalCare','BloodSugarmmol',
              'AdmissionWeight']
            
            adm_df = format_column_as_numeric(adm_df, numeric_fields)


            if 'MatAgeYrs' in adm_df:
                adm_df['MatAgeYrs'] = adm_df['MatAgeYrs'].apply(extract_years)
            #Check For Addition of New Columns
            if table_exists('derived','admissions'):
                adm_cols = pd.DataFrame(get_table_column_names('admissions', 'derived'))
                new_adm_columns = set(adm_df.columns) - set(adm_cols.columns) 
                      
                if new_adm_columns:
                    column_pairs =  [(col, str(adm_df[col].dtype)) for col in new_adm_columns]
                    if column_pairs:
                        create_new_columns('admissions','derived',column_pairs)
            adm_df=convert_false_numbers_to_text(adm_df,'derived','admissions');         
            catalog.save('create_derived_admissions',adm_df)
            logging.info("... Creating MCL count tables for Admissions DF") 
            explode_column(adm_df, adm_mcl,"")   
        
        ##################################DISCHARGES SCRIPT#################################################### 
        dis_df = pd.json_normalize(dis_new_entries)
        if not dis_df.empty:
    
            if "started_at" in dis_df and 'completed_at' in dis_df :
                dis_df = format_date_without_timezone(dis_df,['started_at','completed_at'])
                dis_df['time_spent'] = (dis_df['completed_at'] -dis_df['started_at']).astype('timedelta64[m]')
            else:
                dis_df['time_spent'] = None
             #Format Dates Discharge Table
            dis_df =  format_date(dis_df,['DateAdmissionDC.value','DateDischVitals.value','DateDischWeight.value',
                                          'DateTimeDischarge.value','EndScriptDatetime.value','DateWeaned.value',
                                          'DateTimeDeath.value','DateAdmission.value','BirthDateDis.value','DateHIVtest.value'
                                          ,'DateVDRLSameHIV.value'])
            
            for position,discharge in dis_df.iterrows():
                if 'BirthWeight.value' in discharge and str(discharge['BirthWeight.value'])!='nan' and discharge['BirthWeight.value'] is not None:
                    pass
                else:
                    key_change(dis_df,discharge,position,'BWTDis.value','BirthWeight.value')
                if 'DOBTOB.value' in discharge and str(discharge['DOBTOB.value'])!='nan' and discharge['DOBTOB.value'] is not None:
                    pass
                else:
                    key_change(dis_df,discharge,position,'BirthDateDis.value','DOBTOB.value')
                if 'ModeDelivery.value' in discharge and str(discharge['ModeDelivery.value'])!='nan' and discharge['ModeDelivery.value'] is not None:
                    pass
                else:
                    key_change(dis_df,discharge,position,'Delivery.value','ModeDelivery.value')
                if 'Temperature.value' in discharge and str(discharge['Temperature.value'])!='nan' and discharge['Temperature.value'] is not None:
                    pass
                else: 
                    key_change(dis_df,discharge,position,'NNUAdmTemp.value','Temperature.value') 
                if  'Gestation.value' in discharge and str(discharge['Gestation.value'])!='nan' and discharge['Gestation.value'] is not None:
                    pass
                else:
                    key_change(dis_df,discharge,position,'GestBirth.value','Gestation.value')
                if 'AdmReason.value' in discharge and str(discharge['AdmReason.value'])!='nan' and discharge['AdmReason.value'] is not None:
                    pass
                else:
                    key_change(dis_df,discharge,position,'PresComp.value','AdmReason.value')
             #Save Derived Discharges To The DataBase Using Kedro
            dis_df = create_columns(dis_df) 
            dis_df= dis_df[(dis_df['uid'] != 'Unknown')]
            #Add New Columns
            if table_exists('derived','discharges'):
                disc_cols = pd.DataFrame(get_table_column_names('discharges', 'derived'))
                new_disc_columns = set(dis_df.columns) - set(disc_cols.columns) 
                      
                if new_disc_columns:
                    column_pairs =  [(col, str(dis_df[col].dtype)) for col in new_disc_columns]
                    if column_pairs:
                        create_new_columns('discharges','derived',column_pairs)
            dis_df=convert_false_numbers_to_text(dis_df,'derived','discharges'); 
            catalog.save('create_derived_discharges',dis_df)
            logging.info("... Creating MCL count tables for Discharge DF") 
            explode_column(dis_df, dis_mcl,"disc_")
            
            
        ##################################MATERNAL OUTCOMES########################################33
        mat_outcomes_df =pd.json_normalize(mat_outcomes_new_entries)
        if not mat_outcomes_df.empty:
            logging.info("...##########MAT DF IS NOT EMPTY################")
            mat_outcomes_df.set_index(['unique_key'])
            mat_outcomes_df=format_date_without_timezone(mat_outcomes_df,['started_at','completed_at','DateAdmission.value']) 
            if "started_at" in mat_outcomes_df and 'completed_at' in mat_outcomes_df :

                mat_outcomes_df['time_spent'] = (mat_outcomes_df['completed_at'] - mat_outcomes_df['started_at']).astype('timedelta64[m]')
            else:
                mat_outcomes_df['time_spent'] = None
            mat_outcomes_df =  set_key_to_none(mat_outcomes_df,['Presentation.label','BabyNursery.label','Reason.label',
            'ReasonOther.label','CryBirth.label','Apgar1.value','Apgar5.value','Apgar10.value','PregConditions.label',
            'BirthDateDis.value'])
              
        #Save Derived Maternal Outcomes To The DataBase Using Kedro
            mat_outcomes_df = create_columns(mat_outcomes_df)
            mat_outcomes_df= mat_outcomes_df[(mat_outcomes_df['uid'] != 'Unknown')]
            set_key_to_none(mat_outcomes_df,['TypeBirth.label','TypeBirth.value'])
            if table_exists('derived','maternal_outcomes'):
                cols = pd.DataFrame(get_table_column_names('maternal_outcomes', 'derived'), columns=["column_name"])
                new_columns = set(mat_outcomes_df.columns) - set(cols.columns) 
                      
                if new_columns:
                    column_pairs =  [(col, str(mat_outcomes_df[col].dtype)) for col in new_columns]
                    if column_pairs:
                        create_new_columns('maternal_outcomes','derived',column_pairs)
            mat_outcomes_df=convert_false_numbers_to_text(mat_outcomes_df,'derived','maternal_outcomes'); 
            catalog.save('create_derived_maternal_outcomes',mat_outcomes_df)
            logging.info("... Creating MCL count tables for Maternal Outcomes DF") 
            explode_column(mat_outcomes_df,mat_outcomes_mcl,"mat_")
        
        vit_signs_df = pd.json_normalize(vit_signs_new_entries)
        if not vit_signs_df.empty:
            vit_signs_df.set_index(['uid'])
            vit_signs_df = format_date_without_timezone(vit_signs_df,['started_at','completed_at',]) 
            if "started_at" in vit_signs_df and 'completed_at' in vit_signs_df :
                   
                vit_signs_df['time_spent'] = (vit_signs_df['completed_at']-vit_signs_df['started_at']).astype('timedelta64[m]')
            else:
                vit_signs_df['time_spent'] = None
                 #Vital Signs Table
            vit_signs_df = format_date(vit_signs_df,['D1Date.value','TimeTemp1.value','TimeTemp2.value','EndScriptDatetime.value']) 
              #Save Derived Vital Signs To The DataBase Using Kedro 
            vit_signs_df= vit_signs_df[(vit_signs_df['uid'] != 'Unknown')]
            if table_exists('derived','vitalsigns'):
                cols = pd.DataFrame(get_table_column_names('vitalsigns', 'derived'), columns=["column_name"])
                new_columns = set(vit_signs_df.columns) - set(cols.columns) 
                      
                if new_columns:
                    column_pairs =  [(col, str(vit_signs_df[col].dtype)) for col in new_columns]
                    if column_pairs:
                        create_new_columns('vitalsigns','derived',column_pairs)
            vit_signs_df= convert_false_numbers_to_text(vit_signs_df,'derived','vitalsigns'); 
            catalog.save('create_derived_vitalsigns',vit_signs_df)
            logging.info("... Creating MCL count tables for Vital Signs DF")
            explode_column(vit_signs_df,vit_signs_mcl,"vit_")
    ##################### NEOLAB ###################################################################################    
        neolab_df = pd.json_normalize(neolab_new_entries)
        if not neolab_df.empty:
            if ("DateBCR.value" in neolab_df and 'DateBCT.value' in neolab_df and 
                neolab_df['DateBCR.value'] is not None and neolab_df['DateBCT.value'] is not None):
            
                neolab_df['BCReturnTime'] = (pd.to_datetime(neolab_df['DateBCR.value'], format='%Y-%m-%dT%H:%M:%S',utc=True,errors='coerce') -
                                        pd.to_datetime(neolab_df['DateBCT.value'], format='%Y-%m-%dT%H:%M:%S',utc=True,errors='coerce')).astype('timedelta64[h]')
            else:
                neolab_df['BCReturnTime'] = None
            neolab_df= format_date_without_timezone(neolab_df,['started_at','completed_at']) 
            if "started_at" in neolab_df and 'completed_at' in neolab_df :
                neolab_df['time_spent'] = (neolab_df['completed_at'] - neolab_df['started_at']).astype('timedelta64[m]')
            else:
                neolab_df['time_spent'] = None
            ###################CREATING EPISODES###################################   
             # Initialise the column
            neolab_df['episode'] = 0
            # Initialise BCR TYPE
            neolab_df['BCType']= None
            neolab_df['DateBCT.value']=pd.to_datetime(neolab_df['DateBCT.value'],errors='coerce')
            
            for index,row in neolab_df.iterrows():
                # Data Cleaning
                neolab_cleanup(neolab_df,index)  
                #Set Episodes
                control_df = neolab_df[neolab_df['uid'] == row['uid']].copy().sort_values(by=['DateBCT.value']).reset_index(drop=True)
                if not control_df.empty:
                    episode =1
                    if neolab_df.at[index,'episode'] ==0:
                        for innerIndex, innerRow in control_df.iterrows() :
                            
                            if innerIndex == 0:
                            #Episode Remains 1 
                                pass
                            else:
                                control_df_date_bct = control_df.at[innerIndex,'DateBCT.value']
                                prev_control_df_date_bct = control_df.at[innerIndex-1,'DateBCT.value']
                                if len(str(control_df_date_bct)) >9 and len(str(prev_control_df_date_bct)) > 9 :
                                    if str(control_df_date_bct)[:10] == str(prev_control_df_date_bct)[:10]:
                                        # Episode Remains the same as previous Episode
                                        pass
                                    
                                    else:
                                        episode = episode+1
                            # Set The Episode Value For All Related Episodes in the Main DF 
                            control_df.loc[innerIndex,'episode']= episode
                            neolab_df.loc[(neolab_df['uid']
                                ==control_df.at[innerIndex,'uid']) & (neolab_df['DateBCT.value']
                                ==control_df.at[innerIndex,'DateBCT.value']) & (neolab_df['DateBCR.value']
                                == control_df.at[innerIndex,'DateBCR.value']),'episode'] = episode                              

                        #Add BCR TYPE TO CONTROL DF
                        # Loop is necessary since BCType is dependant on the set episodes

                        for control_index, bct_row in control_df.iterrows() :  
                            bct_type_df = control_df[(control_df['uid'] == bct_row['uid']) & (control_df['episode'] == bct_row['episode'])].copy().sort_values(by=['DateBCR.value']).reset_index(drop=True)
                            
                            if not bct_type_df.empty:
                                preliminary_index= 1
                                for bct_index, row in bct_type_df.iterrows():
                                    bct_value = None
                                    bct_values_from_df = neolab_df.loc[(neolab_df['uid']
                                            ==bct_type_df.at[bct_index,'uid']) & (neolab_df['DateBCT.value']
                                            ==bct_type_df.at[bct_index,'DateBCT.value']) & (neolab_df['DateBCR.value']
                                            == bct_type_df.at[bct_index,'DateBCR.value'])]['BCType'].values
                                    if len(bct_values_from_df) >0:
                                        bct_value = bct_values_from_df[0]

                                    if bct_value is None:
                                        if (bct_type_df.at[bct_index,'BCResult.value'] != 'Pos' and bct_type_df.at[bct_index,'BCResult.value'] != 'Neg'
                                            and bct_type_df.at[bct_index,'BCResult.value'] != 'PC'):
                                            bct_type_df.loc[bct_index,'BCType'] = "PRELIMINARY-"+str(preliminary_index)
                                            preliminary_index=preliminary_index+1
        
                                        else:
                                            if bct_index == len(bct_type_df)-1:
                                                bct_type_df.loc[bct_index,'BCType'] = "FINAL"
                                            else:
                                                bct_type_df.loc[bct_index,'BCType'] = "PRELIMINARY-"+str(preliminary_index)
                                                preliminary_index = preliminary_index+1

                                    # Set The BCR Type For All Related Records in the Main DFclear
                                    if bct_type_df.at[bct_index,'BCType'] is not None:
                                        neolab_df.loc[(neolab_df['uid']
                                            ==bct_type_df.at[bct_index,'uid']) & (neolab_df['DateBCT.value']
                                            ==bct_type_df.at[bct_index,'DateBCT.value']) & (neolab_df['DateBCR.value']
                                            == bct_type_df.at[bct_index,'DateBCR.value']),'BCType'] = bct_type_df.at[bct_index,'BCType']
            ####CREATING COLUMNS                          
            neolab_df = create_columns(neolab_df)
              #SET INDEX 
            if "uid" in neolab_df:
                neolab_df.set_index(['uid'])
                if ("episode" in neolab_df):
                    neolab_df.sort_values(by=['uid','episode']) 
            neolab_df= neolab_df[(neolab_df['uid'] != 'Unknown')] 
            ########SAVE DATA#####################################
            if table_exists('derived','neolab'):
                cols = pd.DataFrame(get_table_column_names('neolab', 'derived'), columns=["column_name"])
                new_columns = set(neolab_df.columns) - set(cols.columns) 
                      
                if new_columns:
                    column_pairs =  [(col, str(neolab_df[col].dtype)) for col in new_columns]
                    if column_pairs:
                        create_new_columns('neolab','derived',column_pairs)

            catalog.save('create_derived_neolab',neolab_df)
            
        #########################BASELINE###############################################################    
        baseline_df = pd.json_normalize(baseline_new_entries) 
        if not baseline_df.empty:
            baseline_df = format_date_without_timezone(baseline_df,['started_at','completed_at']) 
            if "started_at" in baseline_df and 'completed_at' in baseline_df :
                baseline_df['time_spent'] = (baseline_df['completed_at'] -baseline_df['started_at']).astype('timedelta64[m]')
            else:
                baseline_df['time_spent'] = None
            baseline_df['LengthOfStay.value'] = None
            baseline_df['LengthOfStay.label'] = None
            baseline_df['LengthOfLife.value'] = None
            baseline_df['LengthOfLife.label'] = None
            if 'AdmissionWeight.value' in baseline_df:
                 baseline_df['AdmissionWeight.value'] = pd.to_numeric(baseline_df['AdmissionWeight.value'],downcast='integer', errors='coerce')
            if 'BirthWeight.value' in baseline_df:
                baseline_df['BirthWeight.value'] = pd.to_numeric(baseline_df['BirthWeight.value'],downcast='integer', errors='coerce')
                
                
            if 'Temperature.value' in baseline_df:
                baseline_df['Temperature.value'] = pd.to_numeric(baseline_df['Temperature.value'],downcast='integer', errors='coerce')
           
             #Length of Life and Length of Stay on Baseline Data
            date_format = "%Y-%m-%d"
            for index, row in baseline_df.iterrows():

                baseline_df.loc[index,'LengthOfStay.label'] ="Length of Stay"
                if (is_date(str(row['DateTimeDischarge.value']))
                    and is_date(str(row['DateTimeAdmission.value']))):
                    DateTimeDischarge = dt.strptime(str(str(row['DateTimeDischarge.value']))[:10].strip().replace('T',''),date_format)
                    DateTimeAdmission = dt.strptime(str(str(row['DateTimeAdmission.value']))[:10].strip().replace('T',''),date_format)
                    delta_los = DateTimeDischarge-DateTimeAdmission
                    baseline_df.loc[index,'LengthOfStay.value'] = delta_los.days

                else:
                    baseline_df.loc[index,'LengthOfStay.value']= None
            
                baseline_df.loc[index,'LengthOfLife.label'] ="Length of Life"
                if 'DateTimeDeath.value' in row and (is_date(str(row['DateTimeDeath.value']))
                    and is_date(str(row['DateTimeAdmission.value']))): 
                    DateTimeDeath = dt.strptime(str(str(row['DateTimeDeath.value']))[:10].strip().replace('T',''), date_format)
                    DateTimeAdmission = dt.strptime(str(str(row['DateTimeAdmission.value']))[:10].strip().replace('T',''), date_format)
                    delta_lol = DateTimeDeath - DateTimeAdmission
                    baseline_df.loc[index,'LengthOfLife.value']= delta_lol.days
                else:
                    baseline_df.loc[index,'LengthOfLife.value'] = None
                 # Baselines Tables
            baseline_df = format_date(baseline_df,['DateTimeAdmission.value','DateTimeDischarge.value','DateTimeDeath.value'])  
            baseline_df= set_key_to_none(baseline_df,['AWGroup.value','BWGroup.value','AdmittedFrom.label'
                                                      ,'AdmittedFrom.value','ReferredFrom2.label','ReferredFrom2.value',
                                                      'ReferredFrom.label','ReferredFrom.value','TempThermia.label','TempThermia.value',
                                                      'TempGroup.label','TempGroup.value','GestGroup.label','GestGroup.value'])
            ###############CREATE COLUMNS#################################
            baseline_df = create_columns(baseline_df)  
            if 'Gestation.value' in baseline_df:
                baseline_df['Gestation.value'] = pd.to_numeric(baseline_df['Gestation.value'], errors='coerce')      
            #Save Derived Baseline To The DataBase Using Kedro
            baseline_df= baseline_df[(baseline_df['uid'] != 'Unknown')] 
            if table_exists('derived','baseline'):
                cols = pd.DataFrame(get_table_column_names('baseline', 'derived'), columns=["column_name"])
                new_columns = set(baseline_df.columns) - set(cols.columns) 
                      
                if new_columns:
                    column_pairs =  [(col, str(baseline_df[col].dtype)) for col in new_columns]
                    if column_pairs:
                        create_new_columns('baseline','derived',column_pairs)

            catalog.save('create_derived_baseline',baseline_df)
            logging.info("... Creating MCL count tables for Baseline DF")
            explode_column(baseline_df,baseline_mcl,"bsl_")
        
        #########################################DIAGNOSES####################    
        diagnoses_df = pd.json_normalize(diagnoses_new_entries)
        if not diagnoses_df.empty:
            if 'uid' in diagnoses_df:
                diagnoses_df= diagnoses_df[(diagnoses_df['uid'] != 'Unknown')] 
            catalog.save('create_derived_diagnoses',diagnoses_df)
         
        ###################MATERNAL COMPLETENES CASE OF MALAWI###################   
        mat_completeness_df = pd.json_normalize(mat_completeness_new_entries)
        if not mat_completeness_df.empty:
            mat_completeness_df= format_date_without_timezone(mat_completeness_df,['DateAdmission.value','DateTimeAdmission.value'])
            # Join Maternal Completeness and Maternal Outcomes /A Case For Malawi
            if not mat_outcomes_df.empty: 
                previous_mat_outcomes_df = mat_outcomes_df[pd.to_datetime(mat_outcomes_df['DateAdmission.value'],errors='coerce') >='2021-10-01']
                latest_mat_outcomes_df= mat_completeness_df[pd.to_datetime(mat_completeness_df['DateAdmission.value'],errors='coerce') >='2021-09-30']
                previous_mat_outcomes_df = previous_mat_outcomes_df.reset_index(drop=True) 
                latest_mat_outcomes_df = latest_mat_outcomes_df.reset_index(drop=True) 
                mat_completeness_df = pd.concat([latest_mat_outcomes_df, previous_mat_outcomes_df],axis=0,ignore_index=True)
                ##########SAVING DATA####################################
            if table_exists('derived','maternity_completeness'):
                cols = pd.DataFrame(get_table_column_names('maternity_completeness', 'derived'), columns=["column_name"])
                new_columns = set(mat_completeness_df.columns) - set(cols.columns) 
                      
                if new_columns:
                    column_pairs =  [(col, str(mat_completeness_df[col].dtype)) for col in new_columns]
                    if column_pairs:
                        create_new_columns('maternity_completeness','derived',column_pairs)

            catalog.save('create_derived_maternity_completeness',mat_completeness_df)
            explode_column(mat_completeness_df,mat_completeness_mcl,"matcomp_")


    except Exception as e:
        logging.error(
            "!!! An error occured writing dataframe output back to the database: ")
        logging.error(formatError(e))

  