import pandas as pd
from conf.base.catalog import params
from data_pipeline.pipelines.data_engineering.utils.set_key_to_none import set_key_to_none



def create_columns(table: pd.DataFrame):
    '''
    This function replicates some of the fields created in power bi.
    Create derived fields based on DAX power bi expressions:
    e.g. simple IF(<logical_test>,<value_if_true> [,<value_if_false>])
    e.g. nested IF([Calls]<200,"low",IF([Calls]<300,"medium","high")) 
    Comments indicate which DAX formulas have been used and which expressession have not be replicated due to complexity issues
    For things that couldn't easily be on here the recommendation is to try and use Metabase
    '''

    # Create AdmissionSource = IF(ISBLANK(Admissions[AdmittedFrom]); "External Referral"; Admissions[AdmittedFrom])
     #Fix Lost Data From One Of The Published Versions
    try:
            table = set_key_to_none(table,['AdmittedFrom.value','AdmittedFrom.label','ReferredFrom.label'
                                           ,'ReferredFrom.value','ReferredFrom2.label','ReferredFrom2.value','AgeCat.label'])
            if 'AdmittedFrom.value' in table: 
                  table['AdmittedFrom.value'].fillna("ER", inplace=True)
                  table['AdmittedFrom.label'].fillna("External Referral", inplace=True)

                  # Cascading fallback: AdmittedFrom -> ReferredFrom -> ReferredFrom2
                  table['EXTERNALSOURCE.label'] = table['AdmittedFrom.label'].fillna(
                        table['ReferredFrom.label']).fillna(table['ReferredFrom2.label'])
                  table['EXTERNALSOURCE.value'] = table['AdmittedFrom.value'].fillna(
                        table['ReferredFrom.value']).fillna(table['ReferredFrom2.value'])

            # order of statements matters
                  
            if('country' in params and str(params['country']).lower()) =='zimbabwe':
                  try:
                        if not table.empty and 'Gestation.value' in table:
                              table['Gestation.value'] = pd.to_numeric(table['Gestation.value'], errors='coerce')
                              table.loc[table['Gestation.value'].isnull(), 'GestGroup.value'] = "Unknowwn"
                              table.loc[table['Gestation.value'] >= 42, 'GestGroup.value'] = "42 wks or above"
                              table.loc[table['Gestation.value'] < 42, 'GestGroup.value'] = "37-41 wks"
                              table.loc[table['Gestation.value'] < 37, 'GestGroup.value'] = "33-36 wks"
                              table.loc[table['Gestation.value'] < 33, 'GestGroup.value'] = "28-32 wks"
                              table.loc[table['Gestation.value'] < 28, 'GestGroup.value'] = "<28"
                  except:
                        pass
            else:
                  if not table.empty  and 'Gestation.value' in table:
                        try:
                              table['Gestation.value'] = pd.to_numeric(table['Gestation.value'], errors='coerce')
                              table.loc[table['Gestation.value'].isnull(), 'GestGroup.value'] = None
                              table.loc[table['Gestation.value'] >= 37, 'GestGroup.value'] = "Term"
                              table.loc[table['Gestation.value'] < 37, 'GestGroup.value'] = "34-36+6 wks"
                              table.loc[table['Gestation.value'] < 34, 'GestGroup.value'] = "32-34 wks"
                              table.loc[table['Gestation.value'] < 32, 'GestGroup.value'] = "28-32 wks"
                              table.loc[table['Gestation.value'] < 28, 'GestGroup.value'] = "<28" 
                        except:
                              pass  


            # order of statements matters
            if 'BirthWeight.value' in table:
                  try:
                        table['BirthWeight.value'] =  pd.to_numeric(table['BirthWeight.value'], errors='coerce')
                        table.loc[table['BirthWeight.value'].isnull(), 'BWGroup.value'] = "Unknown"
                        table.loc[table['BirthWeight.value'] >= 4000, 'BWGroup.value'] = "HBW"
                        table.loc[table['BirthWeight.value'] < 4000, 'BWGroup.value'] = "NBW"
                        table.loc[table['BirthWeight.value'] < 2500, 'BWGroup.value'] = "LBW"
                        table.loc[table['BirthWeight.value'] < 1500, 'BWGroup.value'] = "VLBW"
                        table.loc[table['BirthWeight.value'] < 1000, 'BWGroup.value'] = "ELBW"
                        table['BirthWeightCategory'] =table['BWGroup.value']
                  except:
                        pass
            else:
                  if ('BW.value' in table):
                        try:
                              table['BW.value'] = pd.to_numeric(table['BW.value'], errors='coerce')
                              table.loc[table['BW.value'].isnull() , 'BWGroup.value'] = "Unknown"
                              table.loc[table['BW.value'] >= 4000, 'BWGroup.value'] = "HBW"
                              table.loc[table['BW.value'] < 4000, 'BWGroup.value'] = "NBW"
                              table.loc[table['BW.value'] < 2500, 'BWGroup.value'] = "LBW"
                              table.loc[table['BW.value'] < 1500, 'BWGroup.value'] = "VLBW"
                              table.loc[table['BW.value'] < 1000, 'BWGroup.value'] = "ELBW"
                        except:
                              pass

            # For Baseline Tables
            if 'AdmissionWeight.value' in table:
                  try:

                        table['AdmissionWeight.value'] = pd.to_numeric(table['AdmissionWeight.value'], errors='coerce')
                        table.loc[table['AdmissionWeight.value'].isnull(), 'AWGroup.value'] = "Unknown"
                        table.loc[table['AdmissionWeight.value'] >= 4000, 'AWGroup.value'] = ">4000g"
                        table.loc[table['AdmissionWeight.value'] < 4000, 'AWGroup.value'] = "2500-4000g"
                        table.loc[table['AdmissionWeight.value'] < 2500, 'AWGroup.value'] = "1500-2500g"
                        table.loc[table['AdmissionWeight.value'] < 1500, 'AWGroup.value'] = "1000-1500g"
                        table.loc[table['AdmissionWeight.value'] < 1000, 'AWGroup.value'] = "<1000g"
                  except:
                        pass

            # order of statements matters
            elif 'AW.value' in table:
                  try: 
                        table['AW.value']= pd.to_numeric(table['AdmissionWeight.value'], errors='coerce')
                        table.loc[table['AW.value'].isnull(), 'AWGroup.value'] = "Unknown"
                        table.loc[table['AW.value'] >= 4000, 'AWGroup.value'] = ">4000g"
                        table.loc[table['AW.value'] < 4000, 'AWGroup.value'] = "2500-4000g"
                        table.loc[table['AW.value'] < 2500, 'AWGroup.value'] = "1500-2500g"
                        table.loc[table['AW.value'] < 1500, 'AWGroup.value'] = "1000-1500g"
                        table.loc[table['AW.value'] < 1000, 'AWGroup.value'] = "<1000g"
                        table['AdmissionWeight.value']= table["AW.value"]
                  except:
                        pass

            else:
                  table['AdmissionWeight.value']= None
                  table['AWGroup.value']= None  

            # order of statements matters
            if 'Temperature.value' in table:
                  try:
                        table['Temperature.value'] = pd.to_numeric(table['Temperature.value'], errors='coerce')
                        table.loc[table['Temperature.value'] >= 41.5, 'TempGroup.value'] = ">41.5"
                        table.loc[table['Temperature.value'] <
                                    41.5, 'TempGroup.value'] = "40.5-41.5"
                        table.loc[table['Temperature.value'] <
                                    40.5, 'TempGroup.value'] = "39.5-40.5"
                        table.loc[table['Temperature.value'] <
                                    39.5, 'TempGroup.value'] = "38.5-39.5"
                        table.loc[table['Temperature.value'] <
                                    38.5, 'TempGroup.value'] = "37.5-38.5"
                        table.loc[table['Temperature.value'] <
                                    37.5, 'TempGroup.value'] = "36.5-37.5"
                        table.loc[table['Temperature.value'] <
                                    36.5, 'TempGroup.value'] = "35.5-36.5"
                        table.loc[table['Temperature.value'] <
                                    35.5, 'TempGroup.value'] = "34.5-35.5"
                        table.loc[table['Temperature.value'] <
                                    34.5, 'TempGroup.value'] = "33.5-34.5"
                        table.loc[table['Temperature.value'] <
                                    33.5, 'TempGroup.value'] = "32.5-33.5"
                        table.loc[table['Temperature.value'] <
                                    32.5, 'TempGroup.value'] = "31.5-32.5"
                        table.loc[table['Temperature.value'] <
                                    31.5, 'TempGroup.value'] = "30.5-31.5"
                        table.loc[table['Temperature.value'] < 30.5, 'TempGroup.value'] = "<30.5"
                  except:
                        table.loc['TempGroup.value'] = "Unknown"

            
            if('country' in params and str(params['country']).lower()) =='zimbabwe':
                  if 'Temperature.value' in table:
                        try:
                              table.loc[not isinstance(table['Temperature.value'], (int, float, complex)),'TempThermia.value'] = "Unknown"
                              table.loc[table['Temperature.value'] >37.5,
                                    'TempThermia.value'] = "Fever"
                              table.loc[(table['Temperature.value'] >= 36.5) & (table['Temperature.value'] <= 37.5),
                                    'TempThermia.value'] = "Normothermia"
                              table.loc[(table['Temperature.value'] >= 36.0) & (table['Temperature.value'] <= 36.4),
                                    'TempThermia.value'] = "Mild Hypothermia"
                              table.loc[(table['Temperature.value'] >= 32.1) & (table['Temperature.value'] <= 35.9),
                                    'TempThermia.value'] = "Moderate Hypothermia"
                              table.loc[table['Temperature.value'] <= 32,
                                    'TempThermia.value'] = "Severe Hypothermia"
                              table.loc[table['Temperature.value'].isnull(),
                              'TempThermia.value'] = "Unknown"
                        except:
                              table.loc['TempThermia.value'] = "Unknown"
                  
                  
            else:
                  try:
                        table.loc[not isinstance(table['Temperature.value'], (int, float, complex)),'TempThermia.value'] = "Unknown"
                        table.loc[table['Temperature.value'] >= 37.5,
                              'TempThermia.value'] = "Hyperthermia"
                        table.loc[table['Temperature.value'] < 37.5,
                              'TempThermia.value'] = "Normothermia"
                        table.loc[table['Temperature.value'] < 36.5,
                        'TempThermia.value'] = "Hypothermia"
                  except:
                        pass

            if 'BirthWeight.value' in table: 
                  try:
                        
                        table['LBWBinary'] = ((table['BirthWeight.value'] > 0) & (table['BirthWeight.value'] < 2500)) 

                        table['<28wks/1kg.value'] = ((table['BirthWeight.value'] > 0) &
                                                ((table['BirthWeight.value'] < 1000) |
                                                 (isinstance(table['Gestation.value'], (int, float, complex)) & (table['Gestation.value'] < 28))))
                        
                        
                  except:
                        
                        pass

            else:
                  if 'BW.value' in table:
                        try:
                              table['LBWBinary'] = ((table['BW.value'] > 0) & (table['BW.value'] < 2500)) 

                              table['<28wks/1kg.value'] = ((table['BW.value'] > 0) &
                                                ((table['BW.value'] < 1000) |
                                                 (isinstance(table['Gestation.value'], (int, float, complex)) & (table['Gestation.value'] < 28))))
                  
                        except:
                              pass
                  if 'Bw.value' in table:
                        try:
                              table['LBWBinary'] = ((table['Bw.value'] > 0) & (table['Bw.value'] < 2500)) 

                              table['<28wks/1kg.value'] = ((table['Bw.value'] > 0) &
                                                ((table['Bw.value'] < 1000) |
                                                 (isinstance(table['Gestation.value'], (int, float, complex)) & (table['Gestation.value'] < 28))))
                        except:
                              pass
                  if 'BWTDis.value' in table:
                        table['BWTDis.value'] = pd.to_numeric(table['BWTDis.value'], errors='coerce')
                        
            # Create LBWBinary = AND(Admissions[bw-2]<> Blank();(Admissions[bw-2]<2500))
            return table
    except Exception as e:
          raise e
    

            
      

