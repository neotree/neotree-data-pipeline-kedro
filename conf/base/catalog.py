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
mat_outcomes_script_id = '-MOAjJ_In4TOoe0l_Gl5'
adm_script_id = '-KO1TK4zMvLhxTw6eKia'
disc_script_id = '-KYDiO2BTM4kSGZDVXAO'


if('country' in params and str(params['country']).lower()) =='zim':
   adm_script_id = '-ZO1TK4zMvLhxTw6eKia'
   disc_script_id = '-ZYDiO2BTM4kSGZDVXAO'
   mat_outcomes_script_id ='-MDPYzHcFVHt02D1Tz4Z'

read_admissions_query = '''
            select 
            uid,
            ingested_at,
            "data"->'appVersion' as "appVersion",
            "data"->'entries' as "entries"
            from scratch.deduplicated_admissions ;
            '''
deduplicate_admissions_query ='''
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
     where scriptid = '{0}' {1} -- only pull out admissions
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
); '''.format(adm_script_id,where)

deduplicate_discharges_query = '''
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
    where scriptid = '{0}' {1} -- only pull out discharges
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
); '''.format(disc_script_id,where)
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
            from public.sessions where scriptid = '{}' '''.format(mat_outcomes_script_id)

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
#Query To Validate If It is necessary to run Maternal OutComes Summary        
count_maternal_outcomes = ''' select count(*) from derived.maternal_outcomes'''
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
         #Count Maternal Outcomes
         "count_maternal_outcomes": SQLQueryDataSet(
            sql= count_maternal_outcomes,
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
