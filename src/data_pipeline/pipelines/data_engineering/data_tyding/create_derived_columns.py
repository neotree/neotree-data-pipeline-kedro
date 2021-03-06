import pandas as pd
import numpy as np
from datetime import datetime as dt
from conf.base.catalog import params


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
    if 'AdmittedFrom.value' not in table:
        table['AdmittedFrom.value'] = None
    if 'AdmittedFrom.label' not in table:
        table['AdmittedFrom.label'] = None
    if 'ReferredFrom.label' not in table:
         table['ReferredFrom.label'] = None
    if 'ReferredFrom.value' not in table:
         table['ReferredFrom.value'] = None
    if 'ReferredFrom2.label' not in table:
         table['ReferredFrom2.label'] = None
    if 'ReferredFrom2.value' not in table:
         table['ReferredFrom2.value'] = None
        
    table['AdmittedFrom.value'].fillna("ER", inplace=True)
    table['AdmittedFrom.label'].fillna("External Referral", inplace=True)

    # float("nan") used to make sure nan's are set not a string "nan"
    table['EXTERNALSOURCE.label'] = np.where(table['AdmittedFrom.label'].isnull(), table['AdmittedFrom.label'].mask(
        pd.isnull, (table['ReferredFrom.label'].mask(pd.isnull, table['ReferredFrom2.label']))), float('nan'))
    table['EXTERNALSOURCE.value'] = np.where(table['AdmittedFrom.value'].isnull(), table['AdmittedFrom.value'].mask(
        pd.isnull, (table['ReferredFrom.value'].mask(pd.isnull, table['ReferredFrom2.value']))), float('nan'))

    # order of statements matters
    if 'Gestation.value' in table.keys():
          pass;
    else:
        table['Gestation.value'] = float('nan') 
         
    if('country' in params and str(params['country']).lower()) =='zimbabwe':
            table.loc[table['Gestation.value'].isnull(
            ), 'GestGroup.value'] = float('nan')
            table.loc[table['Gestation.value'] >= 42, 'GestGroup.value'] = "42 wks or above"
            table.loc[table['Gestation.value'] < 42, 'GestGroup.value'] = "37-41 wks"
            table.loc[table['Gestation.value'] < 37, 'GestGroup.value'] = "33-36 wks"
            table.loc[table['Gestation.value'] < 33, 'GestGroup.value'] = "28-32 wks"
            table.loc[table['Gestation.value'] < 28, 'GestGroup.value'] = "<28"
    else:
            table.loc[table['Gestation.value'].isnull(
            ), 'GestGroup.value'] = float('nan')
            table.loc[table['Gestation.value'] >= 37, 'GestGroup.value'] = "Term"
            table.loc[table['Gestation.value'] < 37, 'GestGroup.value'] = "34-36+6 wks"
            table.loc[table['Gestation.value'] < 34, 'GestGroup.value'] = "32-34 wks"
            table.loc[table['Gestation.value'] < 32, 'GestGroup.value'] = "28-32 wks"
            table.loc[table['Gestation.value'] < 28, 'GestGroup.value'] = "<28"   


    # order of statements matters

    table.loc[table['BW.value'].isnull(), 'BWGroup.value'] = "Unknown"
    table.loc[table['BW.value'] >= 4000, 'BWGroup.value'] = "HBW"
    table.loc[table['BW.value'] < 4000, 'BWGroup.value'] = "NBW"
    table.loc[table['BW.value'] < 2500, 'BWGroup.value'] = "LBW"
    table.loc[table['BW.value'] < 1500, 'BWGroup.value'] = "VLBW"
    table.loc[table['BW.value'] < 1000, 'BWGroup.value'] = "ELBW"

    # order of statements matters
    table.loc[table['AW.value'] >= 4000, 'AWGroup.value'] = ">4000g"
    table.loc[table['AW.value'] < 4000, 'AWGroup.value'] = "2500-4000g"
    table.loc[table['AW.value'] < 2500, 'AWGroup.value'] = "1500-2500g"
    table.loc[table['AW.value'] < 1500, 'AWGroup.value'] = "1000-1500g"
    table.loc[table['AW.value'] < 1000, 'AWGroup.value'] = "<1000g"

    # order of statements matters
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

  
    if('country' in params and str(params['country']).lower()) =='zimbabwe':
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
        
        
    else:
        table.loc[table['Temperature.value'] >= 37.5,
              'TempThermia.value'] = "Hyperthermia"
        table.loc[table['Temperature.value'] < 37.5,
              'TempThermia.value'] = "Normothermia"
        table.loc[table['Temperature.value'] < 36.5,
              'TempThermia.value'] = "Hypothermia"


    table['<28wks/1kg.value'] = ((table['BW.value'] > 0) &
                                 ((table['BW.value'] < 1000) | (table['Gestation.value'] < 28)))

    # Create LBWBinary = AND(Admissions[bw-2]<> Blank();(Admissions[bw-2]<2500))

    table['LBWBinary'] = ((table['BW.value'] > 0) & (table['BW.value'] < 2500))

    return table
