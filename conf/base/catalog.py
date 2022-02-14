from kedro.extras.datasets.pandas import (
    SQLQueryDataSet,SQLTableDataSet)
from kedro.io import DataCatalog
from pathlib import Path
from  conf.common.config import config
from conf.common.hospital_config import hospital_conf
import sys,os
import logging
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import deduplicate_admissions_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import deduplicate_baseline_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import deduplicate_mat_completeness_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import deduplicate_vitals_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import deduplicate_neolab_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import deduplicate_maternal_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import deduplicate_discharges_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_admissions_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_discharges_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_maternal_outcome_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_vitalsigns_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_baselines_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_mat_completeness_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import derived_admissions_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import derived_discharges_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_noelab_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_diagnoses_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_new_smch_admissions_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_new_smch_discharges_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_old_smch_admissions_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_old_smch_discharges_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_old_smch_matched_view_query
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_new_smch_matched_query

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

maternal_script_id = ''
#Admissions Script IDs
admission_script_id = ''
#Discharges Script IDs
discharge_script_id = ''
#NeoLab Script IDs
neolab_script_id = ''
#Vital Signs Script IDs
vital_signs_script_id = ''  
#Vital Signs Script IDs
baseline_id = ''
#Maternity Data Completeness
maternity_completeness_id = ''

maternal_script_id_dev = ''


# Hospital Scripts Configs
hospital_scripts = hospital_conf()

if hospital_scripts:
       # Declare Dynamic Switch Cases
      admissions_case = ', CASE '
      dicharges_case = ', CASE '
      maternal_case = ', CASE '
      vitals_case = ', CASE '
      neolabs_case = ', CASE '
      baseline_case = ', CASE '
      maternity_completeness_case = ', CASE '

       ####LIST TO FIRST FOR EASY APPENDING THEN CONVERT TO TUPLE LATER
      adm_tuple = []
      disc_tuple = []
      mat_tuple = []
      mat_completeness_tuple = []
      neolab_tuple = []
      baseline_tuple = []
      vitals_tuple = []

       
      for hospital in hospital_scripts:
            ids = hospital_scripts[hospital]
            if 'country' in ids.keys() and 'country' in params.keys():
               if str(ids['country']).lower() == str(params['country']):
                  if 'admissions' in ids.keys():   
                     admission_script_id = ids['admissions']
                     if(admission_script_id!= ''):
                        adm_tuple.append(admission_script_id)
                        admissions_case = admissions_case+ " WHEN scriptid = '{0}' THEN '{1}' ".format(admission_script_id,hospital)

                  if 'discharges' in ids.keys():
                     discharge_script_id = ids['discharges']
                     if(discharge_script_id!= ''):
                        disc_tuple.append(discharge_script_id)
                        dicharges_case = dicharges_case+ " WHEN scriptid = '{0}' THEN '{1}' ".format(discharge_script_id,hospital)
                     
                  if 'maternals' in ids.keys():
                     maternal_script_id = ids['maternals']
                     if (maternal_script_id!=''):
                        mat_tuple.append(maternal_script_id)
                        maternal_case = maternal_case+ " WHEN scriptid = '{0}' THEN '{1}' ".format(maternal_script_id,hospital)

                  ## ADD SCRIPT IDS WHERE THE DEV SCRIPTID IS DIFFERENT FROM PRODUCTION ONE (SHOULDN'T BE THE CASE)
                  if 'maternals_dev' in ids.keys() and env=='dev':
                     maternal_script_id_dev = ids['maternals_dev']
                     if (maternal_script_id_dev!='' and maternal_script_id==''):
                        maternal_script_id = maternal_script_id_dev
                        mat_tuple.append(maternal_script_id)
                        maternal_case = maternal_case+ " WHEN scriptid ='{0}' THEN '{1}' ".format(maternal_script_id,hospital)
                    

                  if 'vital_signs' in ids.keys():
                     vital_signs_script_id = ids['vital_signs']
                     if (vital_signs_script_id!=''):
                        vitals_tuple.append(vital_signs_script_id)
                        vitals_case = vitals_case+ " WHEN scriptid ='{0}' THEN '{1}' ".format(vital_signs_script_id,hospital)

                  if 'neolabs' in ids.keys():
                     neolab_script_id = ids['neolabs']
                     if (neolab_script_id!=''):
                        neolab_tuple.append(neolab_script_id)
                        neolabs_case = neolabs_case + " WHEN scriptid ='{0}' THEN '{1}' ".format(neolab_script_id,hospital)

                  if 'baselines' in ids.keys(): 
                     baseline_id = ids['baselines']
                     if (baseline_id!=''):
                        baseline_tuple.append(baseline_id)
                        baseline_case = baseline_case + " WHEN scriptid ='{0}' THEN '{1}' ".format(baseline_id,hospital)
                  
                  if 'maternity_completeness' in ids.keys():
                     maternity_completeness_id = ids['maternity_completeness']
                     if (maternity_completeness_id != ''):
                        mat_completeness_tuple.append(maternity_completeness_id)
                        maternity_completeness_case = maternity_completeness_case+ " WHEN scriptid ='{0}' THEN '{1}' ".format(maternity_completeness_id,hospital)
               
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

      if  baseline_case.strip() ==', CASE':
           baseline_case = ''
      else:
         baseline_case = baseline_case + facility_case_end   

      if  maternity_completeness_case.strip() ==', CASE':
           maternity_completeness_case = ''
      else:
         maternity_completeness_case = maternity_completeness_case + facility_case_end     



#DEFINE FROM SECTION TO AVOID ERROR FROM NON-EXISTING OPTIONAL TABLE
generic_from = 'public.sessions'
mat_outcomes_from = 'scratch.deduplicated_maternals'
neolab_from = 'scratch.deduplicated_neolabs'
baseline_from = 'scratch.deduplicated_baseline'
vital_signs_from = 'scratch.deduplicated_vitals'
mat_completeness_from = 'scratch.deduplicated_maternity_completeness'

#If maternal outcomes id is empty the system should take data from take data directly from sessions which idearly should be empty
generic_where =  f''' where scriptid='' '''
adm_where = generic_where
disc_where = generic_where
mat_outcomes_where = generic_where
mat_completeness_where = generic_where
neolab_where = generic_where
baseline_where = generic_where
vitals_where = generic_where
#Remove Dev Data From Production Instance:
#Take Everything (Applies To Dev And Stage Environments)
additional_where = " "
#Else Remove All Records That Were Created In App Mode Dev
if(env=="prod"):
   additional_where = "  and \"data\"->>\'app_mode\' is null OR \"data\"->>\'app_mode\'=\'production\'"

if (len(mat_tuple) == 0):
   mat_outcomes_from = generic_from
else:
  if (len(mat_tuple) == 1):
      mat_outcomes_where = f''' where scriptid = '{mat_tuple[0]}' ''' 
  elif (len(mat_tuple)>1):
      mat_outcomes_where = f''' where scriptid in {tuple(mat_tuple)} '''
  else:
      pass

if (len(neolab_tuple) == 0):
   neolab_from = generic_from
else:
   if (len(neolab_tuple) == 1):
      neolab_where = f''' where scriptid = '{neolab_tuple[0]}' ''' 
   elif (len(neolab_tuple) >1):
      neolab_where = f''' where scriptid in {tuple(neolab_tuple)} '''
   else:
      pass

if (len(vitals_tuple) == 0):
   vital_signs_from = generic_from
else:
   if (len(vitals_tuple) == 1):
      vitals_where = f''' where scriptid = '{vitals_tuple[0]}' ''' 
   elif (len(vitals_tuple) >1):
      vitals_where = f''' where scriptid in {tuple(vitals_tuple)} '''
   else:
      pass

if (len(baseline_tuple) == 0):  
   baseline_from = generic_from
else:
   if (len(baseline_tuple) == 1):
      baseline_where = f''' where scriptid = '{baseline_tuple[0]}' ''' 
   elif (len(baseline_tuple) >1):
      baseline_where = f''' where scriptid in {tuple(baseline_tuple)} '''
   else:
      pass

if (len(mat_completeness_tuple) == 0):  
   mat_completeness_from = generic_from
else:
   if (len(mat_completeness_tuple) == 1):
      mat_completeness_where = f''' where scriptid = '{mat_completeness_tuple[0]}' ''' 
   elif (len(mat_completeness_tuple) >1):
      mat_completeness_where = f''' where scriptid in {tuple(mat_completeness_tuple)} '''
   else:
      pass
       
if (len(adm_tuple) ==1):
    adm_where = f''' where scriptid = '{adm_tuple[0]}'   ''' 
elif (len(adm_tuple) >1):
      adm_where = f''' where scriptid in {tuple(adm_tuple)}  ''' 
else:
   pass

if (len(disc_tuple) ==1):
    disc_where = f''' where scriptid = '{disc_tuple[0]}'   ''' 
elif (len(disc_tuple) >1):
      disc_where = f''' where scriptid in {tuple(disc_tuple)}  ''' 
else:
   pass


dedup_admissions = deduplicate_admissions_query(adm_where+additional_where)
dedup_baseline = deduplicate_baseline_query(baseline_where + additional_where)
dedup_mat_completeness = deduplicate_mat_completeness_query(mat_completeness_where + additional_where)
dedup_vitals = deduplicate_vitals_query(vitals_where + additional_where)
dedup_neolab = deduplicate_neolab_query(neolab_where + additional_where)
dedup_maternal = deduplicate_maternal_query(mat_outcomes_where + additional_where)
dedup_discharges = deduplicate_discharges_query(disc_where + additional_where)
read_admissions = read_admissions_query(admissions_case,adm_where)
read_discharges = read_discharges_query(dicharges_case,disc_where)
read_maternal_outcome = read_maternal_outcome_query(maternal_case,mat_outcomes_from,mat_outcomes_where)
read_vitalsigns = read_vitalsigns_query(vitals_case,vital_signs_from,vitals_where)
read_baselines = read_baselines_query(baseline_case,baseline_from,baseline_where)
read_mat_completeness = read_mat_completeness_query(maternity_completeness_case,mat_completeness_from,mat_completeness_where)
derived_admissions = derived_admissions_query()
derived_discharges = derived_discharges_query()
read_noelab = read_noelab_query(neolabs_case,neolab_from,neolab_where)
read_diagnoses= read_diagnoses_query(admissions_case,adm_where)
#UNION VIEWS FOR OLD SMCH AND NEW SMCH DATA
read_new_smch_admissions = read_new_smch_admissions_query()
read_new_smch_discharges = read_new_smch_discharges_query()
read_old_smch_admissions = read_old_smch_admissions_query()
read_old_smch_discharges = read_old_smch_discharges_query()
read_old_smch_matched_data = read_old_smch_matched_view_query()
read_new_smch_matched = read_new_smch_matched_query()


#Create A Kedro Data Catalog from which we can easily get a Pandas DataFrame using catalog.load('name_of_dataframe')
catalog = DataCatalog(
        {
         #Read Admissions
         "read_admissions": SQLQueryDataSet(
            sql= read_admissions,
            credentials=dict(con=con)
         ),
         #Read Raw Discharges
         "read_discharges": SQLQueryDataSet(
            sql= read_discharges,
            credentials=dict(con=con)
         ),
         #Read Derived Admissions
        "read_derived_admissions": SQLQueryDataSet(
            sql= derived_admissions,
            #load_args= dict(index_col="uid"),
            credentials=dict(con=con)
         ),
         #Read Derived Discharges
         "read_derived_discharges": SQLQueryDataSet(
            sql= derived_discharges,
            #load_args= dict(index_col="uid"),
            credentials=dict(con=con)
         ),
         #Read Maternal Outcomes
         "read_maternal_outcomes": SQLQueryDataSet(
            sql= read_maternal_outcome,
            credentials=dict(con=con)
         ),
         #Read Vital Signs
         "read_vital_signs": SQLQueryDataSet(
            sql= read_vitalsigns,
            credentials=dict(con=con)
         ),
         #Read Neolab Data
         "read_neolab_data": SQLQueryDataSet(
            sql= read_noelab,
            credentials=dict(con=con)
         ),
         #Read Baseline Data
         "read_baseline_data": SQLQueryDataSet(
            sql= read_baselines,
            credentials=dict(con=con)
         ),
         #Read  Diagnoses Data
         "read_diagnoses_data": SQLQueryDataSet(
            sql= read_diagnoses,
            credentials=dict(con=con)
         ),
          #Read Baseline Data
         "read_mat_completeness_data": SQLQueryDataSet(
            sql= read_mat_completeness,
            credentials=dict(con=con)
         ),
           #Read New SCMH Admissions Data
         "read_new_smch_admissions": SQLQueryDataSet(
            sql= read_new_smch_admissions,
            credentials=dict(con=con)
         ),
            #Read New SCMH Discharges
         "read_new_smch_discharges": SQLQueryDataSet(
            sql= read_new_smch_discharges,
            credentials=dict(con=con)
         ),

          #Read Old SCMH Admissions
         "read_old_smch_admissions": SQLQueryDataSet(
            sql= read_old_smch_admissions,
            credentials=dict(con=con)
         ),

          #Read Old SCMH Discharges
         "read_old_smch_discharges": SQLQueryDataSet(
            sql= read_old_smch_discharges,
            credentials=dict(con=con)
         ),
         #Read New Matched SCMH Data
         "read_new_smch_matched": SQLQueryDataSet(
            sql= read_new_smch_matched,
            credentials=dict(con=con)
         ),

          #Read Old Matched SCMH Data
         "read_old_smch_matched_data": SQLQueryDataSet(
            sql= read_old_smch_matched_data,
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
         ),
          
         "create_derived_baselines": SQLTableDataSet(
            table_name="baseline",
            credentials=dict(con=con),
            save_args = dict(schema="derived",if_exists="replace")
         ),

         "create_derived_diagnoses": SQLTableDataSet(
            table_name="diagnoses",
            credentials=dict(con=con),
            save_args = dict(schema="derived",if_exists="replace")
         )
         ,
          #Make Use Of Save Method To Create Tables
          "create_derived_maternity_completeness": SQLTableDataSet(
            table_name='maternity_completeness',
            credentials=dict(con=con),
            save_args = dict(schema='derived',if_exists='replace')
         ),
         #Make Use Of Save Method To Create Tables
          "create_derived_old_new_admissions_view": SQLTableDataSet(
            table_name='old_new_admissions_view',
            credentials=dict(con=con),
            save_args = dict(schema='derived',if_exists='replace')
         ),

         "create_derived_old_new_discharges_view": SQLTableDataSet(
            table_name='old_new_discharges_view',
            credentials=dict(con=con),
            save_args = dict(schema='derived',if_exists='replace')
         ),

          "create_derived_old_new_matched_view": SQLTableDataSet(
            table_name='old_new_matched_view',
            credentials=dict(con=con),
            save_args = dict(schema='derived',if_exists='replace')
         ),
        }
        )
