from kedro.extras.datasets.pandas import (
    SQLQueryDataSet,SQLTableDataSet)
from kedro.io import DataCatalog
from pathlib import Path
from conf.common.config import config
import sys,os

params = config()
con = 'postgresql+psycopg2://' + \
params["user"] + ':' + params["password"] + '@' + \
params["host"] + ':' + '5432' + '/' + params["database"]
env = params['env']

cwd = os.getcwd()
logs_dir = str(cwd+"/logs")


#Prefered Log Var for Ubuntu
ubuntu_log_dir = "/var/log"
if Path(ubuntu_log_dir).exists():
    logs_dir = ubuntu_log_dir
cron_log_file = Path(logs_dir+'/data_pipeline_cron.log');

#Remove Dev Data From Production Instance:

#Take Everything (Applies To Dev And Stage Ebnvironments)
where = " "
#Else Remove All Records That Were Created In App Mode Dev
if(env=="prod"):
       where = " and \"data\"->>\'app_mode\' is null OR \"data\"->>\'app_mode\'=\'production\'"
       
       
#Country Defaults To Malawi
#Maternal Outcomes Script IDs
#REMOVE DUMMY IF TUPPLE HAS AT LEAST 2 VALID IDS
mat_outcomes_script_ids = ('-MOAjJ_In4TOoe0l_Gl5','-DUMMY-')
if(env =='dev'):
   mat_outcomes_script_ids = ('-MOAjJ_In4TOoe0l_Gl5','-MNwjrpe8Ju-BQUKFMZ-')  
#Admissions Script IDs
adm_script_ids = ('-KO1TK4zMvLhxTw6eKia','-DUMMY-')
#Discharges Script IDs
disc_script_ids = ('-KYDiO2BTM4kSGZDVXAO','-DUMMY-')
#NeoLab Script IDs
neo_lab_ids = ('-MO_MFKCgx8634jhjLId','-DUMMY-')
#Vital Signs Script IDs
vital_signs_ids = ('-LAeXX-JCxWLkIrQxVLD','-DUMMY-')



if('country' in params and str(params['country']).lower()) =='zim':
   adm_script_ids = ('-ZO1TK4zMvLhxTw6eKia','-MJBnoLY0YLDqLUhPgkK')
   disc_script_ids = ('-ZYDiO2BTM4kSGZDVXAO','-MJCntWHvPaIuxZp35ka')
   mat_outcomes_script_ids =('-MDPYzHcFVHt02D1Tz4Z','-MYk0A3-Z_QjaXYU5MsS')
   neo_lab_ids = ('-LfOH5fWtWEKk1yJPwfo','-DUMMY-')
   #To Pass The Parameter As A Tuple, it requires at least 2 items, hence the use of '-DUMMY-' As IDs
   vital_signs_ids = ('-DUMMY-','-DUMMY-')

read_admissions_query = f'''
            select 
            uid,
            ingested_at,
            "data"->'appVersion' as "appVersion",
            "data"->'entries' as "entries",
            CASE WHEN scriptid ='-ZO1TK4zMvLhxTw6eKia' THEN 'SMCH'
            WHEN scriptid ='-MJBnoLY0YLDqLUhPgkK' THEN 'CPH'
            WHEN scriptid = '-KO1TK4zMvLhxTw6eKia' THEN 'KCH'
            END AS "facility"
            from scratch.deduplicated_admissions where uid!='null' and scriptid in {adm_script_ids};
            '''
deduplicate_admissions_query =f'''
drop table if exists scratch.deduplicated_admissions cascade;
create table scratch.deduplicated_admissions as 
(
  with earliest_admissions as (
    select
      scriptid,
      uid, 
      min(id) as id -- This takes the first upload 
                    -- of the session as the deduplicated record. 
                    -- We could replace with max(id) to take the 
                    -- most recently uploaded
     from public.sessions
     where scriptid in {adm_script_ids} {where} -- only pull out admissions
    group by 1,2
  )
  select
    earliest_admissions.scriptid,
    earliest_admissions.uid,
    earliest_admissions.id,
    sessions.ingested_at,
    data
  from earliest_admissions join sessions
  on earliest_admissions.id = sessions.id
); '''

deduplicate_discharges_query = f'''
drop table if exists scratch.deduplicated_discharges cascade;
create table scratch.deduplicated_discharges as 
(
  with earliest_discharges as (
    select
      scriptid,
      uid, 
      min(id) as id -- This takes the first upload 
                    -- of the session as the deduplicated record. 
                    -- We could replace with max(id) to take the 
                    -- most recently uploaded
    from public.sessions
    where scriptid in {disc_script_ids} {where} -- only pull out discharges
    group by 1,2
  )
  select
    earliest_discharges.scriptid,
    earliest_discharges.uid,
    earliest_discharges.id,
    sessions.ingested_at,
    data
  from earliest_discharges join sessions
  on earliest_discharges.id = sessions.id
); '''
read_discharges_query = f'''
            select 
                uid,
                ingested_at,
                "data"->'appVersion' as "appVersion",
                "data"->'entries' as "entries",
                CASE WHEN scriptid ='-ZYDiO2BTM4kSGZDVXAO' THEN 'SMCH'
                WHEN scriptid ='-MJCntWHvPaIuxZp35ka' THEN 'CPH'
                WHEN scriptid = '-KYDiO2BTM4kSGZDVXAO' THEN 'KCH'
                END AS "facility"
            from scratch.deduplicated_discharges where uid!='null' and scriptid in {disc_script_ids};
        '''
read_maternal_outcome_query = f'''
            select 
            scriptid,
            uid,
            id,
            ingested_at,
            "data"->'appVersion' as "appVersion",
            "data"->'entries' as "entries",
            CASE WHEN scriptid ='-MDPYzHcFVHt02D1Tz4Z' THEN 'SMCH'
            WHEN scriptid = '-MYk0A3-Z_QjaXYU5MsS' THEN 'CPH'
            WHEN scriptid ='-MOAjJ_In4TOoe0l_Gl5' THEN 'KCH'
            WHEN scriptid ='-MNwjrpe8Ju-BQUKFMZ-' THEN 'KCH'
            END AS "facility"
            from public.sessions where scriptid in {mat_outcomes_script_ids} and uid!='null'; '''

read_vitalsigns_query = f'''
            select 
            scriptid,
            uid,
            id,
            ingested_at,
            "data"->'appVersion' as "appVersion",
            "data"->'entries' as "entries",
            CASE WHEN scriptid ='-LAeXX-JCxWLkIrQxVLD' THEN 'KCH'
            END AS "facility"
            from public.sessions where scriptid in {vital_signs_ids} and uid!='null';
'''

derived_admissions_query = '''
                select 
                    *
                from derived.admissions where uid!='null';
            '''

derived_discharges_query = '''
            select 
                *
            from derived.discharges where uid!='null';
        '''
#Query To Validate If It is necessary to run Maternal OutComes Summary        
count_maternal_outcomes = ''' select count(*) from derived.maternal_outcomes'''

vital_signs_count = ''' select count(*) from derived.vitalsigns '''

read_noelab_query = f'''
            select 
            scriptid,
            uid,
            id,
            ingested_at,
            "data"->'appVersion' as "appVersion",
            "data"->'entries' as "entries",
            CASE WHEN scriptid = '-MO_MFKCgx8634jhjLId' THEN 'KCH'
            WHEN scriptid = '-LfOH5fWtWEKk1yJPwfo' THEN 'SMCH'
            END AS "facility"
            from public.sessions where scriptid in {neo_lab_ids}
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
         "read_neolab_data": SQLQueryDataSet(
            sql= read_noelab_query,
            credentials=dict(con=con)
         ),
         #Count Maternal Outcomes
         "count_maternal_outcomes": SQLQueryDataSet(
            sql= count_maternal_outcomes,
            credentials=dict(con=con)
         ),
          #Count Vital Signs
         "vital_signs_count": SQLQueryDataSet(
            sql= vital_signs_count,
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
         ),
         "create_derived_neolab": SQLTableDataSet(
            table_name='neolab',
            credentials=dict(con=con),
            save_args = dict(schema='derived',if_exists='replace')
         )

        }
        )
