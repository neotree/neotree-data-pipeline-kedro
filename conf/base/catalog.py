from kedro.extras.datasets.pandas import (
    SQLQueryDataSet,SQLTableDataSet)
from kedro.io import DataCatalog
from pathlib import Path
from  conf.common.config import config
from conf.common.hospital_config import hospital_conf
import sys,os
import logging
from datetime import datetime
import time
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import (get_admissions_data_tofix_query,
                            deduplicate_neolab_query,read_deduplicated_data_query,read_new_smch_admissions_query,
                            read_new_smch_discharges_query,read_old_smch_admissions_query,read_old_smch_discharges_query,
                            read_old_smch_matched_view_query,read_new_smch_matched_query,get_duplicate_maternal_query,
                            get_discharges_tofix_query,get_maternal_data_tofix_query,get_admissions_data_tofix_query,
                            get_baseline_data_tofix_query,deduplicate_data_query,read_derived_data_query,read_diagnoses_query)

params = config()
con = 'postgresql+psycopg2://' + \
params["user"] + ':' + params["password"] + '@' + \
params["host"] + ':' + '5432' + '/' + params["database"]
env = params['env']
cron_time = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
start = time.time()

cwd = os.getcwd()
logs_dir = str(cwd+"/logs")
log = logging.getLogger('');
#Prefered Log Var for Ubuntu
ubuntu_log_dir = "/var/log"
if Path(ubuntu_log_dir).exists():
    logs_dir = ubuntu_log_dir
cron_log_file = Path(logs_dir+'/data_pipeline_cron.log');

generic_dedup_queries = []

# Hospital Scripts Configs
hospital_scripts = hospital_conf()

old_scripts = ['admissions','discharges','maternal_outcomes','maternals_dev','vitalsigns','neolab','baseline','maternity_completeness']

##INITIALISE NEW SCRIPTS
new_scripts = []

if hospital_scripts:

      generic_catalog = {}
     
      #Remove Dev Data From Production Instance:
      #Take Everything (Applies To Dev And Stage Environments)
      additional_where = " "
      #Else Remove All Records That Were Created In App Mode Dev
      if(env=="prod"):
         additional_where = "  and \"data\"->>\'app_mode\' is null OR \"data\"->>\'app_mode\'=\'production\'"
         
      ###This Assumes One Script Id Per Script, Per Hospital
      processed_scripts = []
      processed_case =[]
      processed_script_names =[] 
      for hospital in hospital_scripts:
            ids = hospital_scripts[hospital]
            if 'country' in ids.keys() and 'country' in params.keys():
               if str(ids['country']).lower() == str(params['country']):
                  for script in ids.keys():            
                     if script!='name' and script!='country':
                        script_id = ids[script]
                        if script not in processed_script_names: 
                           processed_scripts.append({script:[script_id]})
                           script_case = f''', CASE WHEN scriptid='{script_id}' then '{hospital}'  '''
                           processed_case.append({script:script_case})
                           
                           processed_script_names.append(script)
                        else:
                           ### APPEND MORE IDS OF THE SAME SCRIPT NAME
                           for dic in processed_scripts:
                              for key in dic.keys():
                                 if(key==script):
                                    existing_list =  dic[key]
                                    existing_list.append(script_id)
                                    dic[key] = existing_list
                                    break
                           ##### ADD TO THE CASE CONDITION
                           for case in processed_case:
                              for key in case.keys():
                                 if(key==script):
                                    existing_case =  case[key]
                                    existing_case = existing_case+ f''' WHEN scriptid ='{script_id}' THEN '{hospital}' '''
                                    case[key] = existing_case
                                    
      #########CLOSE CASE STATEMENTS
      for case in processed_case:
         for key in case.keys():
            case[key] = case[key] + f''' END AS "facility" '''                                                                                         
      for proc_script in processed_scripts:
         for key in proc_script.keys():
            script_name = key
            dedup_destination = 'scratch.deduplicated_'+script_name
            myIds = proc_script[key]
            if script_name not in old_scripts and script_name not in new_scripts:
               new_scripts.append(script_name)
            
            if(type(myIds) is list):
               condition =''
               if len(myIds)==1:
                  script_id = myIds[0]
                  condition = f''' = '{script_id}' '''
                  logging.info("======MY IDS====="+condition)
               else:
                  condition =  f''' in {tuple(myIds)} '''
                  logging.info("======MY LIST IDS====="+condition)
               if condition !='':
                  if(script_name=='neolab'):
                     deduplication_query= deduplicate_neolab_query(condition+additional_where)
                  else:
                     deduplication_query = deduplicate_data_query(condition+additional_where,dedup_destination)
                     logging.info("======MY DEDUP====="+deduplication_query)
                  generic_dedup_queries.append(deduplication_query)
                  case_object = [item for item in processed_case if script_name in item]
                  if case_object:
                     script_case = case_object[0][script_name]  
                  read_query = read_deduplicated_data_query(script_case,condition,dedup_destination)
                  create_query = SQLTableDataSet(
                                 table_name=script_name,
                                 credentials=dict(con=con),
                                 save_args = dict(schema='derived',if_exists='replace')
                                 )
                              #### ADD THE QUERIES TO THE GENERIC CATALOG
                  read_table = f'''read_{script_name}'''
                  create_table =f'''create_derived_{script_name}''' 
                  generic_catalog.update({read_table: SQLQueryDataSet(
                                          sql= read_query,
                                          credentials=dict(con=con)
                                          ),
                                          create_table:
                                          create_query
                                          })
                  ### ADD READ DIAGNOSIS QUERY
                  if(script_name=='admissions'):
                     diagnoses_query = read_diagnoses_query(script_case,condition)
                     generic_catalog.update({"read_diagnoses_data": SQLQueryDataSet(
                                          sql= diagnoses_query,
                                          credentials=dict(con=con)
                                          )}
                                          ) 
 #### FOR LEGACY DATA                                   
read_new_smch_admissions = read_new_smch_admissions_query()
read_new_smch_discharges = read_new_smch_discharges_query()
read_old_smch_admissions = read_old_smch_admissions_query()
read_old_smch_discharges = read_old_smch_discharges_query()
read_old_smch_matched_data = read_old_smch_matched_view_query()
read_new_smch_matched = read_new_smch_matched_query()
derived_admissions = read_derived_data_query('admissions')
derived_discharges = read_derived_data_query('discharges')

#DATA CLEANUP QUERIES
get_duplicate_maternal_data = get_duplicate_maternal_query()
get_discharges_tofix = get_discharges_tofix_query()
get_maternal_outcome_to_fix = get_maternal_data_tofix_query()
get_admissions_data_to_fix = get_admissions_data_tofix_query()
get_baseline_data_to_fix = get_baseline_data_tofix_query()

#Create A Kedro Data Catalog from which we can easily get a Pandas DataFrame using catalog.load('name_of_dataframe')-
old_catalog =  {
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
         "create_joined_admissions_discharges": SQLTableDataSet(
            table_name='joined_admissions_discharges',
            credentials=dict(con=con),
            save_args = dict(schema='derived',if_exists='replace')
         ),

         "create_derived_diagnoses": SQLTableDataSet(
            table_name="diagnoses",
            credentials=dict(con=con),
            save_args = dict(schema="derived",if_exists="replace")
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
          "duplicate_maternal_data": SQLQueryDataSet(
            sql= get_duplicate_maternal_data,
            credentials=dict(con=con)
         ),
          "discharges_to_fix": SQLQueryDataSet(
            sql= get_discharges_tofix,
            credentials=dict(con=con)
         ),
          "maternals_to_fix": SQLQueryDataSet(
            sql= get_maternal_outcome_to_fix,
            credentials=dict(con=con)
         ),
         "admissions_to_fix": SQLQueryDataSet(
            sql= get_admissions_data_to_fix,
            credentials=dict(con=con)
         ),
         "baselines_to_fix": SQLQueryDataSet(
            sql= get_baseline_data_to_fix,
            credentials=dict(con=con)
         )
        }
old_catalog.update(generic_catalog)   
catalog = DataCatalog(
         old_catalog
        )
