import pandas as pd
import json
from conf.common.sql_functions import inject_sql,get_table_columns
import logging
from conf.base.catalog import catalog
from data_pipeline.pipelines.data_engineering.utils.key_change import key_change
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.utils.custom_date_formatter import format_date
from conf.base.catalog import params
from conf.common.format_error import formatError

def union_views():
    if('country' in params and str(params['country']).lower()) =='zimbabwe':
        try:
            adm_cols = pd.DataFrame(get_table_columns(
                'admissions', 'derived'), columns=["column_name", "data_type"])
            old_adm_cols = pd.DataFrame(get_table_columns(
                'old_smch_admissions', 'derived'), columns=["column_name", "data_type"])
            old_disc_cols = pd.DataFrame(get_table_columns(
                'old_smch_discharges', 'derived'), columns=["column_name", "data_type"])
            disc_cols = pd.DataFrame(get_table_columns(
                'discharges', 'derived'), columns=["column_name", "data_type"])
            old_matched_cols = pd.DataFrame(get_table_columns(
                'old_smch_matched_admissions_discharges', 'derived'), columns=["column_name", "data_type"])
            matched_cols = pd.DataFrame(get_table_columns(
                'joined_admissions_discharges', 'derived'), columns=["column_name", "data_type"])

            # Match Data Types For Admissions
            for index, row in adm_cols.iterrows():
                col_name = str(row['column_name']).strip()
                data_type = row['data_type']
                using = ''
                for index2, row2 in old_adm_cols.iterrows():
                    if col_name == row2['column_name']:
                        try:
                            if str(data_type) != str(row2['data_type']):
                                using = f'''USING "{col_name}"::{data_type}'''
                                query = f'''ALTER table derived.old_smch_admissions ALTER column "{col_name}" TYPE {data_type}  {using};;'''
                                inject_sql(query,"OLD ADMISSIONS")
                        except Exception as ex:
                            query = f'''ALTER table derived.old_smch_admissions DROP column "{col_name}" '''
                            inject_sql(query,f'''DROPPING ADMISSIONS {col_name}''')

            
            # Match Data Types For Discharges
            for index, row in disc_cols.iterrows():
                col_name = str(row['column_name']).strip()
                data_type = row['data_type']
                using = ''
                for index2, row2 in old_disc_cols.iterrows():
                    if col_name == row2['column_name']: 
                        try:
                            if str(data_type) != str(row2['data_type']):
                                using = f'''USING "{col_name}"::{data_type}'''
                                query = f''' ALTER table derived.old_smch_discharges ALTER column "{col_name}" TYPE {data_type} {using};;'''
                                inject_sql(query,"OLD DISCHARGES")
                        except Exception as ex:
                            query = f'''ALTER table derived.old_smch_discharges DROP column "{col_name}";; '''
                            inject_sql(query,f'''DROPPING DISCHARGE COLL {col_name}''')

            # Match Data Types For Matched Data
            for index, row in matched_cols.iterrows():
                col_name = str(row['column_name']).strip()
                data_type = row['data_type']
                using = ''
                for index2, row2 in old_matched_cols .iterrows():
                    if col_name == row2['column_name']:
                        try:
                            if str(data_type) != str(row2['data_type']):
                                using = f'''USING "{col_name}"::{data_type}'''
                                query = f''' ALTER table derived.old_smch_matched_admissions_discharges ALTER column "{col_name}" TYPE {data_type} {using};;'''
                                inject_sql(query,"Union Views")
                        except Exception as ex:
                            query = f'''ALTER table derived.old_smch_matched_admissions_discharges DROP column "{col_name}";; '''
                            inject_sql(query,f'''DROPPING MATCHED COLL {col_name} ''')

            old_smch_admissions =  None
            old_smch_discharges =  None
            old_matched_smch_data = None
            new_smch_admissions =   None
            new_smch_discharges =   None
            new_smch_matched_data = None            
            if table_exists('derived','old_smch_admissions'):
                old_smch_admissions =   catalog.load('read_old_smch_admissions')
            if table_exists('derived','old_smch_discharges'):
                old_smch_discharges =   catalog.load('read_old_smch_discharges')
            if table_exists('derived','old_smch_matched_admissions_discharges'):
                old_matched_smch_data = catalog.load('read_old_smch_matched_data')
            if table_exists('derived','admissions'):
                new_smch_admissions =   catalog.load('read_new_smch_admissions')
            if table_exists('derived','discharges'):
                new_smch_discharges =   catalog.load('read_new_smch_discharges')
            if table_exists('derived','joined_admissions_discharges'):
                new_smch_matched_data = catalog.load('read_new_smch_matched')

            if old_smch_admissions  is not None and not old_smch_admissions.empty:
            
                for position,admission in old_smch_admissions.iterrows():

                    age_list =[]
                
                    if 'AgeB.value' in admission and str(admission['AgeB.value']) != 'nan':
                        age_list = str(admission['AgeB.value']).split(",")
                    # Initialise Hours
                    hours = 0
                    period = 0
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
                            pass;     
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
                        pass;  

                    if period>0:
                        old_smch_admissions.loc[position,'Age.value'] = period
                        if period< 2:
                            old_smch_admissions.loc[position,'AgeCategory'] = 'Fresh Newborn (< 2 hours old)'
                        elif period>2 and period<=23:
                            old_smch_admissions.loc[position,'AgeCategory'] = 'Newborn (2 - 23 hrs old)'
                        elif period>23 and period<=47:
                            old_smch_admissions.loc[position,'AgeCategory']= 'Newborn (1 day - 1 day 23 hrs old)'
                        elif period>47 and period<= 71:
                            old_smch_admissions.loc[position,'AgeCategory']= 'Infant (2 days - 2 days 23 hrs old)' 
                        else:
                            old_smch_admissions.loc[position,'AgeCategory'] = 'Infant (> 3 days old)' 
                    ########################## UPDATE ADMISSION SCRIPT WITH NEW KEYS ########################
                
                    key_change(old_smch_admissions,admission,position,'BW.value','BirthWeight.value')              
                    key_change(old_smch_admissions,admission,position,'Conv.value','Convulsions.value')  
                    key_change(old_smch_admissions,admission,position,'SRNeuroOther.value','SymptomReviewNeurology.value')
                    key_change(old_smch_admissions,admission,position,'LBW.value','LowBirthWeight.value')
                    key_change(old_smch_admissions,admission,position,'AW.value','AdmissionWeight.value')
                    key_change(old_smch_admissions,admission,position,'BSmgdL.value','BSUnitmg.value')
                    key_change(old_smch_admissions,admission,position,'BSmmol.value','BloodSugarmmol.value')
                    key_change(old_smch_admissions,admission,position,'BSmg.value','BloodSugarmg.value')
              
                if 'AdmissionWeight.value' in old_smch_admissions:
                    old_smch_admissions['AdmissionWeight.value'] = pd.to_numeric(old_smch_admissions['AdmissionWeight.value'],downcast='integer', errors='coerce')
                if 'BirthWeight.value' in old_smch_admissions:
                    old_smch_admissions['BirthWeight.value'] = pd.to_numeric(old_smch_admissions['BirthWeight.value'],downcast='integer', errors='coerce')
                
                if 'Gestation.value' in old_smch_admissions:
                    old_smch_admissions['Gestation.value'] = pd.to_numeric(old_smch_admissions['Gestation.value'],downcast='integer', errors='coerce')
                
                if 'Temperature.value' in old_smch_admissions:
                    old_smch_admissions['Temperature.value'] = pd.to_numeric(old_smch_admissions['Temperature.value'],downcast='integer', errors='coerce')
           
                
                format_date(old_smch_admissions,'DateTimeAdmission.value')
                format_date(old_smch_admissions,'EndScriptDatetime.value')
                format_date(old_smch_admissions,'DateHIVtest.value')
                format_date(old_smch_admissions,'ANVDRLDate.value')


            if old_smch_discharges  is not None and not old_smch_discharges.empty:
                for position,discharge in old_smch_discharges.iterrows():
                    key_change(old_smch_discharges,discharge,position,'BWTDis.value','BirthWeight.value')
                    key_change(old_smch_discharges,discharge,position,'BirthDateDis.value','DOBTOB.value')
                    key_change(old_smch_discharges,discharge,position,'Delivery.value','ModeDelivery.value')
                    key_change(old_smch_discharges,discharge,position,'NNUAdmTemp.value','Temperature.value') 
                    key_change(old_smch_discharges,discharge,position,'GestBirth.value','Gestation.value')
                    key_change(old_smch_discharges,discharge,position,'PresComp.value','AdmReason.value')
                #Format Dates Discharge Table
                format_date(old_smch_discharges,'DateAdmissionDC.value')  
                format_date(old_smch_discharges,'DateDischVitals.value')
                format_date(old_smch_discharges,'DateDischWeight.value')
                format_date(old_smch_discharges,'DateTimeDischarge.value')
                format_date(old_smch_discharges,'EndScriptDatetime.value')
                format_date(old_smch_discharges,'DateWeaned.value')
                format_date(old_smch_discharges,'DateTimeDeath.value')
                format_date(old_smch_discharges,'DateAdmission.value')
                format_date(old_smch_discharges,'BirthDateDis.value')

            if old_matched_smch_data  is not None and not old_matched_smch_data.empty:
                for position,matched_admission in old_matched_smch_data.iterrows():

                    age_list =[]

                
                    if 'AgeB.value' in matched_admission and str(matched_admission['AgeB.value']) != 'nan':
                        age_list = str(matched_admission['AgeB.value']).split(",")
                    # Initialise Hours
                    hours = 0
                    period = 0
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
                            pass;     
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
                        pass;  

                    if period>0:
                        old_matched_smch_data.loc[position,'Age.value'] = period
                        if period< 2:
                            old_matched_smch_data.loc[position,'AgeCategory'] = 'Fresh Newborn (< 2 hours old)'
                        elif period>2 and period<=23:
                            old_matched_smch_data.loc[position,'AgeCategory'] = 'Newborn (2 - 23 hrs old)'
                        elif period>23 and period<=47:
                            old_matched_smch_data.loc[position,'AgeCategory']= 'Newborn (1 day - 1 day 23 hrs old)'
                        elif period>47 and period<= 71:
                            old_matched_smch_data.loc[position,'AgeCategory']= 'Infant (2 days - 2 days 23 hrs old)' 
                        else:
                            old_matched_smch_data.loc[position,'AgeCategory'] = 'Infant (> 3 days old)' 
                    ########################## UPDATE ADMISSION SCRIPT WITH NEW KEYS ########################
                    key_change(old_matched_smch_data,matched_admission,position,'BW.value','BirthWeight.value')              
                    key_change(old_matched_smch_data,matched_admission,position,'Conv.value','Convulsions.value')  
                    key_change(old_matched_smch_data,matched_admission,position,'SRNeuroOther.value','SymptomReviewNeurology.value')
                    key_change(old_matched_smch_data,matched_admission,position,'LBW.value','LowBirthWeight.value')
                    key_change(old_matched_smch_data,matched_admission,position,'AW.value','AdmissionWeight.value')
                    key_change(old_matched_smch_data,matched_admission,position,'BSmgdL.value','BSUnitmg.value')
                    key_change(old_matched_smch_data,matched_admission,position,'BSmmol.value','BloodSugarmmol.value')
                    key_change(old_matched_smch_data,matched_admission,position,'BSmg.value','BloodSugarmg.value')
                    key_change(old_matched_smch_data,matched_admission,position,'BWTDis.value','BirthWeight.value')
                    key_change(old_matched_smch_data,matched_admission,position,'BirthDateDis.value','DOBTOB.value')
                    key_change(old_matched_smch_data,matched_admission,position,'Delivery.value','ModeDelivery.value')
                    key_change(old_matched_smch_data,matched_admission,position,'NNUAdmTemp.value','Temperature.value') 
                    key_change(old_matched_smch_data,matched_admission,position,'GestBirth.value','Gestation.value')
                    key_change(old_matched_smch_data,matched_admission,position,'PresComp.value','AdmReason.value')
                
                if 'AdmissionWeight.value' in old_matched_smch_data:
                    old_matched_smch_data['AdmissionWeight.value'] = pd.to_numeric(old_matched_smch_data['AdmissionWeight.value'],downcast='integer', errors='coerce')

                if 'BirthWeight.value' in old_matched_smch_data:
                    old_matched_smch_data['BirthWeight.value'] = pd.to_numeric(old_matched_smch_data['BirthWeight.value'],downcast='integer', errors='coerce')
                if 'BirthWeight.value_discharge' in old_matched_smch_data:
                    old_matched_smch_data['BirthWeight.value_discharge'] = pd.to_numeric(old_matched_smch_data['BirthWeight.value_discharge'],downcast='integer', errors='coerce')
                    
                format_date(old_matched_smch_data,'DateTimeAdmission.value')
                format_date(old_matched_smch_data,'EndScriptDatetime.value')
                format_date(old_matched_smch_data,'DateHIVtest.value')
                format_date(old_matched_smch_data,'ANVDRLDate.value')
                #Format Dates Discharge Table
                format_date(old_matched_smch_data,'DateAdmissionDC.value')  
                format_date(old_matched_smch_data,'DateDischVitals.value')
                format_date(old_matched_smch_data,'DateDischWeight.value')
                format_date(old_matched_smch_data,'DateTimeDischarge.value')
                format_date(old_matched_smch_data,'EndScriptDatetime.value')
                format_date(old_matched_smch_data,'DateWeaned.value')
                format_date(old_matched_smch_data,'DateTimeDeath.value')
                format_date(old_matched_smch_data,'DateAdmission.value')
                format_date(old_matched_smch_data,'BirthDateDis.value')
            # SAVE OLD NEW ADMISSIONS
            try:
                combined_adm_df = pd.DataFrame()
                if new_smch_admissions is not None and old_smch_admissions  is not None:
                    if new_smch_admissions.index.is_unique and old_smch_admissions.index.is_unique:
                        combined_adm_df = pd.concat([new_smch_admissions, old_smch_admissions],ignore_index=True)
                    else:
                        new_smch_admissions = new_smch_admissions.reset_index(drop=True)
                        old_smch_admissions= old_smch_admissions.reset_index(drop=True)
                        combined_adm_df = pd.concat([new_smch_admissions, old_smch_admissions]).drop_duplicates().reset_index(drop=True)
                    if not combined_adm_df.empty:   
                        catalog.save('create_derived_old_new_admissions_view',combined_adm_df)  
            except Exception as e:
                logging.error("*******AN EXCEPTIONS HAPPENED WHILEST CONCATENATING COMBINED ADMISSIONS")
                logging.error(formatError(e))
                pass   
            # SAVE OLD NEW DISCHARGES
            try:
                if new_smch_discharges  is not None and old_smch_discharges  is not None:
                    new_smch_discharges.set_index(['uid'])
                    old_smch_discharges.set_index(['uid'])
                    combined_dis_df = pd.concat([new_smch_discharges, old_smch_discharges],axis=0).reset_index(drop=True)
                    if not combined_dis_df.empty:   
                        catalog.save('create_derived_old_new_discharges_view',combined_dis_df)  
            except Exception as e:
                logging.error("*******AN EXCEPTIONS HAPPENED WHILEST CONCATENATING COMBINED DISCHARGES")
                logging.error(formatError(e))
                pass  

            # SAVE MATCHED DATA 
            try:
                if new_smch_matched_data  is not None and old_matched_smch_data  is not None:
                    #Correct UID column to suit the lower case uid in new_smch_matched_data
                    if 'UID' in old_matched_smch_data.columns:
                        new_smch_matched_data = new_smch_matched_data.reset_index(drop=True)
                        old_matched_smch_data = old_matched_smch_data.rename(columns = {'UID': 'uid'})  
                    combined_matched_df = pd.concat([new_smch_matched_data, old_matched_smch_data],axis=0).reset_index(drop=True)
                    if not combined_matched_df.empty:   
                        catalog.save('create_derived_old_new_matched_view',combined_matched_df)   
            except Exception as e:
                logging.error("*******AN EXCEPTIONS HAPPENED WHILEST CONCATENATING COMBINED MATCHED")
                logging.error(formatError(e))
                pass

        except Exception as ex:
            logging.error("!!! An error occured creating union views: ")
            logging.error(ex)
            exit()
