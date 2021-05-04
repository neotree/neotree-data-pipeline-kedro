from kedro.extras.datasets.pandas import (
    SQLQueryDataSet,SQLTableDataSet)
from kedro.io import DataCatalog
from pathlib import Path
from conf.common.config import config
from conf.common.hospital_config import hospital_conf
import sys,os
import logging

params = config()
con = 'postgresql+psycopg2://' + \
params["user"] + ':' + params["password"] + '@' + \
params["host"] + ':' + '5432' + '/' + params["database"]
env = params['env']

cwd = os.getcwd()
logs_dir = str(cwd+"/logs")
log = logging.getLogger('');
#Prefered Log Var for Ubuntu
ubuntu_log_dir = "/var/log"
if Path(ubuntu_log_dir).exists():
    logs_dir = ubuntu_log_dir
cron_log_file = Path(logs_dir+'/data_pipeline_cron.log');

mat_outcomes_script_ids = ["DUMMY-"] 
#Admissions Script IDs
adm_script_ids = ["DUMMY-"] 
#Discharges Script IDs
disc_script_ids = ["DUMMY-"]
#NeoLab Script IDs
neo_lab_ids = ["DUMMY-"]
#Vital Signs Script IDs
vital_signs_ids = ["DUMMY-"]                                     



# Hospital Scripts Configs
hospital_scripts = hospital_conf()

if hospital_scripts:
       # Declare Dynamic Switch Cases
       
       admissions_case = ', CASE '
       dicharges_case = ', CASE '
       maternal_case = ', CASE '
       vitals_case = ', CASE '
       neolabs_case = ', CASE '

       
       for hospital in hospital_scripts:
            ids = hospital_scripts[hospital]
            if 'country' in ids.keys() and 'country' in params.keys():
               if str(ids['country']).lower() == str(params['country']):
                  if 'admissions' in ids.keys():   
                     adm_script_id = ids['admissions']
                     if(adm_script_id!= ''):
                        adm_script_ids.append(adm_script_id)
                        admissions_case = admissions_case+ " WHEN scriptid = '{0}' THEN '{1}' ".format(adm_script_id,hospital)
                     else:
                        #ADD DUMMY SO THAT IN WORST CASE SCENARIO WE WILL HAVE AT LEAST 2 DUMMIES IN THE TUPLE,MAKING IT A VALID TUPLE
                        adm_script_ids.append('-DUMMY-')
                     #CONVERT LIST TO TUPLE   
                     

                  if 'discharges' in ids.keys():
                     disc_script_id = ids['discharges']
                  
                     if(disc_script_id!= ''):
                        disc_script_ids.append(disc_script_id)
                        dicharges_case = dicharges_case+ " WHEN scriptid = '{0}' THEN '{1}' ".format(disc_script_id,hospital)
                     else:
                       #ADD DUMMY SO THAT IN WORST CASE SCENARIO WE WILL HAVE AT LEAST 2 DUMMIES IN THE TUPLE,MAKING IT A VALID TUPLE
                        disc_script_ids.append('-DUMMY-')
                     
                  if 'maternals' in ids.keys():
                     mat_script_id = ids['maternals']
                     if (mat_script_id!=''):
                        mat_outcomes_script_ids.append(mat_script_id)
                        maternal_case = maternal_case+ " WHEN scriptid = '{0}' THEN '{1}' ".format(mat_script_id,hospital)
                     else:
                        #ADD DUMMY SO THAT IN WORST CASE SCENARIO WE WILL HAVE AT LEAST 2 DUMMIES IN THE TUPLE,MAKING IT A VALID TUPLE
                        mat_outcomes_script_ids.append('-DUMMY-')
                     

                  ## ADD SCRIPT IDS WHERE THE DEV SCRIPTID IS DIFFERENT FROM PRODUCTION ONE (SHOULDN'T BE THE CASE)
                  if 'maternals_dev' in ids.keys() and env=='dev':
                     mat_script_id_dev = ids['maternals_dev']
                     if (mat_script_id_dev!=''):
                        mat_outcomes_script_ids.append(mat_script_id_dev)   
                        maternal_case = maternal_case+ " WHEN scriptid ='{0}' THEN '{1}' ".format(mat_script_id_dev,hospital)
                    

                  if 'vital_signs' in ids.keys():
                     vit_script_id = ids['vital_signs']
                     if (vit_script_id!=''):
                        vital_signs_ids.append(vit_script_id)
                        vitals_case = vitals_case+ " WHEN scriptid ='{0}' THEN '{1}' ".format(vit_script_id,hospital)
                     else:
                        #ADD DUMMY SO THAT IN WORST CASE SCENARIO WE WILL HAVE AT LEAST 2 DUMMIES IN THE TUPLE,MAKING IT A VALID TUPLE
                        vital_signs_ids.append('-DUMMY-')
                     

                  if 'neolabs' in ids.keys():
                     neolab_script_id = ids['neolabs']
                     if (neolab_script_id!=''):
                        neo_lab_ids.append(neolab_script_id)
                        neolabs_case = neolabs_case + " WHEN scriptid ='{0}' THEN '{1}' ".format(neolab_script_id,hospital)
                     else:
                        #ADD DUMMY SO THAT IN WORST CASE SCENARIO WE WILL HAVE AT LEAST 2 DUMMIES IN THE TUPLE,MAKING IT A VALID TUPLE
                        neo_lab_ids.append('-DUMMY-')
                     
            
            else:
               log.error("Please specify country in both `database.ini` and `hospitals.ini` files")
               sys.exit() 

       
      # RESET CASE STATEMENT TO EMPTY STRING IF NO SCRIPT IDs FOUND ELSE APPEND LAST PART FOR CASE STATEMENT
       facility_case_end = ' END AS "facility"  '
       if  admissions_case.strip() ==', CASE':
           admissions_case = ''
       else:
         admissions_case = admissions_case + facility_case_end

       if  dicharges_case.strip() ==', CASE':
           dicharges_case = ''
       else:
           dicharges_case = dicharges_case + facility_case_end 

       if  vitals_case.strip() ==', CASE':
           vitals_case = ''
       else:
           vitals_case = vitals_case + facility_case_end
       if  maternal_case.strip() ==', CASE':
           maternal_case = ''
       else:
          maternal_case = maternal_case + facility_case_end

       if  neolabs_case.strip() ==', CASE':
           neolabs_case = ''
       else:
         neolabs_case = neolabs_case + facility_case_end     
#CONVERT LISTS TO TUPLE
adm_script_ids_tuple = tuple(adm_script_ids) 
disc_script_ids_tuple = tuple(disc_script_ids)
mat_outcomes_script_ids_tuple = tuple(mat_outcomes_script_ids)
vital_signs_ids_tuple = tuple(vital_signs_ids)
neo_lab_ids_tuple = tuple(neo_lab_ids)

read_admissions_query = f'''
            select 
            uid,
            ingested_at,
            "data"->'appVersion' as "appVersion",
            "data"->'entries' as "entries" {admissions_case} 
            from scratch.deduplicated_admissions where uid!='null' and scriptid in {adm_script_ids_tuple};
            '''
#Remove Dev Data From Production Instance:
#Take Everything (Applies To Dev And Stage Environments)
where = " "
#Else Remove All Records That Were Created In App Mode Dev
if(env=="prod"):
   where = " and \"data\"->>\'app_mode\' is null OR \"data\"->>\'app_mode\'=\'production\'"

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
     where scriptid in {adm_script_ids_tuple} {where} -- only pull out admissions
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
    where scriptid in {disc_script_ids_tuple} {where} -- only pull out discharges
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
                "data"->'entries' as "entries" {dicharges_case}
            from scratch.deduplicated_discharges where uid!='null' and scriptid in {disc_script_ids_tuple};
        '''
read_maternal_outcome_query = f'''
            select 
            scriptid,
            uid,
            id,
            ingested_at,
            "data"->'appVersion' as "appVersion",
            "data"->'entries' as "entries" {maternal_case}
            from public.sessions where scriptid in {mat_outcomes_script_ids_tuple} and uid!='null'; '''

read_vitalsigns_query = f'''
            select 
            scriptid,
            uid,
            id,
            ingested_at,
            "data"->'appVersion' as "appVersion",
            "data"->'entries' as "entries" {vitals_case}
            from public.sessions where scriptid in {vital_signs_ids_tuple} and uid!='null';
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
            "data"->'entries' as "entries" {neolabs_case}
            from public.sessions where scriptid in {neo_lab_ids_tuple}
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
