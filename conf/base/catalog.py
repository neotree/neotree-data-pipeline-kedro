from kedro.extras.datasets.pandas import (
    SQLQueryDataSet,SQLTableDataSet)
from kedro.io import DataCatalog
from conf.common.config import config
import sys

params = config()
con = 'postgresql+psycopg2://' + \
params["user"] + ':' + params["password"] + '@' + \
params["host"] + ':' + '5432' + '/' + params["database"]

read_admissions_query = '''
            select 
            uid,
            ingested_at,
            "data"->'appVersion' as "appVersion",
            "data"->'entries' as "entries"
            from scratch.deduplicated_admissions;
            '''

read_discharges_query = '''
            select 
                uid,
                ingested_at,
                "data"->'appVersion' as "appVersion",
                "data"->'entries' as "entries"
            from scratch.deduplicated_discharges;
        '''
read_maternal_outcome_query = '''
            select 
            scriptid,
            uid,
            id,
            ingested_at,
            "data"->'appVersion' as "appVersion",
            "data"->'entries' as "entries"
            from public.sessions where scriptid ='-MOAjJ_In4TOoe0l_Gl5'
        '''
read_vitalsigns_query = '''
            select 
            scriptid,
            uid,
            id,
            ingested_at,
            "data"->'appVersion' as "appVersion",
            "data"->'entries' as "entries"
            from public.sessions where scriptid = '-LAeXX-JCxWLkIrQxVLD'
'''

derived_admissions_query = '''
                select 
                    *
                from derived.admissions;
            '''

derived_discharges_query = '''
            select 
                *
            from derived.discharges;
        '''

#Create A Kedro Data Catalog from which we can easily get a Pandas DataFrame using catalog.load('name_of_dataframe')
catalog = DataCatalog(
        {
         #Read 
         "read_admissions": SQLQueryDataSet(
            sql= read_admissions_query,
            credentials=dict(con=con)
         ),
         #Read Raw Discharges
         "read_discharges": SQLQueryDataSet(
            sql= read_discharges_query,
            credentials=dict(con=con)
         ),
         #Read Derived Admissions
        "read_derived_admissions": SQLQueryDataSet(
            sql= derived_admissions_query,
            #load_args= dict(index_col="uid"),
            credentials=dict(con=con)
         ),
         #Read Derived Discharges
         "read_derived_discharges": SQLQueryDataSet(
            sql= derived_discharges_query,
            #load_args= dict(index_col="uid"),
            credentials=dict(con=con)
         ),
         #Read Maternal Outcomes
         "read_maternal_outcomes": SQLQueryDataSet(
            sql= read_maternal_outcome_query,
            credentials=dict(con=con)
         ),
         #Read Vital Signs
         "read_vital_signs": SQLQueryDataSet(
            sql= read_vitalsigns_query,
            credentials=dict(con=con)
         ),
         #Make Use Of Save Method To Create Tables
          "create_derived_admissions": SQLTableDataSet(
            table_name='admissions',
            credentials=dict(con=con),
            save_args = dict(schema='derived',if_exists='replace')
         ),
         #Make Use Of Save Method To Create Tables
         "create_derived_discharges": SQLTableDataSet(
            table_name="discharges",
            credentials=dict(con=con),
            save_args = dict(schema="derived",if_exists="replace")
         ),
         
         #Make Use Of Save Method To Create Tables
         "create_joined_admissions_discharges": SQLTableDataSet(
            table_name='joined_admissions_discharges',
            credentials=dict(con=con),
            save_args = dict(schema='derived',if_exists='replace')
         ),
         "create_derived_maternal_outcomes": SQLTableDataSet(
            table_name='maternal_outcomes',
            credentials=dict(con=con),
            save_args = dict(schema='derived',if_exists='replace')
         ),
          "create_derived_vital_signs": SQLTableDataSet(
            table_name='vitalsigns',
            credentials=dict(con=con),
            save_args = dict(schema='derived',if_exists='replace')
         )

        }
        )
