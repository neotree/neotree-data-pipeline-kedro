import logging
import sys,os
sys.path.append(os.getcwd())
from conf.common.scripts import get_script,merge_script_data,merge_two_script_outputs,process_dataframe_with_types
from conf.common.hospital_config import hospital_conf
from conf.common.format_error import formatError
from conf.common.sql_functions import (run_query_and_return_df
                                        ,generate_create_insert_sql
                                        ,get_table_column_names
                                        ,create_new_columns,table_exists,
                                        generateAndRunUpdateQuery)
from conf.base.catalog import cron_log_file,cron_time,start,env
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import (read_derived_data_query,
                                                                               read_clean_admissions_not_joined,
                                                                               read_clean_dicharges_not_joined,
                                                                               read_all_from_derived_table,
                                                                               read_clean_admissions_without_discharges,
                                                                               clean_discharges_not_matched
                                                                               )
from data_pipeline.pipelines.data_engineering.queries.data_fix import deduplicate_table
import pandas as pd

#This file calls the query to grant privileges to users on the generated tables
cron_log = open(cron_log_file,"a+")
def clean_data_for_research(create_summary_counts_output):
    try:
        #Test If Previous Node Has Completed Successfully
        if create_summary_counts_output:

            logging.info("...........Cleaning Joined Admissions Discharges.............")
            hospital_scripts = hospital_conf()
            if hospital_scripts:
                all_script_types = set()
                for scripts in hospital_scripts.values():
                    all_script_types.update(scripts.keys())

                for script in all_script_types:
                    merged_script_data = None
                    if script=='country' or script=='name':
                        continue
                    for hospital in hospital_scripts:
                        ids = hospital_scripts[hospital]
                        script_id_entry = ids.get(script, '')

                        if not script_id_entry:
                            continue

                        script_ids = str(script_id_entry).split(',')

                        for script_id in script_ids:
                            script_id = script_id.strip()
                            if not script_id:
                                continue

                            script_json = get_script(script_id)
                            if script_json is not None:
                                merged_script_data = merge_script_data(merged_script_data, script_json)
                    logging.info(f"@@@..TR-@@--{bool(merged_script_data)}")
                    if merged_script_data is not None and bool(merged_script_data):
                        query= read_derived_data_query(script, f'''clean_{script}''')
                        logging.info(f"KWERY..TR-@@--{query}")
                        new_data_df = run_query_and_return_df(query)
        
                        if new_data_df is not None and not new_data_df.empty:
                            cleaned_df = process_dataframe_with_types(new_data_df, merged_script_data)

                            if cleaned_df is not None and not cleaned_df.empty:
                                cleaned_df.columns = cleaned_df.columns.str.replace(r"[()-]", "_",regex=True)
                                if table_exists('derived',f'''clean_{script}'''):
                                    cols = pd.DataFrame(get_table_column_names(f'''clean_{script}''', 'derived'), columns=["column_name"])
                                    new_columns = set(cleaned_df.columns) - set(cols.columns) 
                                    if new_columns:
                                        column_pairs =  [(col, str(cleaned_df[col].dtype)) for col in new_columns]
                                        
                                        if len(column_pairs)>0:
                                            create_new_columns(f'clean_{script}','derived',column_pairs)
                                generate_create_insert_sql(cleaned_df, 'derived', f'clean_{script}')
                                deduplicate_table(f'clean_{script}')
                
                  #Load Derived Admissions From Kedro Catalog
                read_admissions_query=''
                read_discharges_query=''
                if table_exists('derived','clean_joined_adm_discharges'):
                    read_admissions_query= read_clean_admissions_not_joined()
                    read_discharges_query = read_clean_dicharges_not_joined()
                else:
                    read_admissions_query = read_all_from_derived_table('clean_admissions')
                    read_discharges_query = read_all_from_derived_table('clean_discharges')
                clean_adm_df = run_query_and_return_df(read_admissions_query)  
                clean_dis_df = run_query_and_return_df(read_discharges_query)    
                clean_jn_adm_dis =createCleanJoinedDataSet(clean_adm_df,clean_dis_df)

                if clean_jn_adm_dis is not None and not clean_jn_adm_dis.empty:
                    clean_jn_adm_dis = clean_jn_adm_dis.loc[:, ~clean_jn_adm_dis.columns.str.match(r'^\d+$|^[a-zA-Z]$', na=False)]
                    if table_exists('derived','clean_joined_adm_discharges'):
                            adm_cols = pd.DataFrame(get_table_column_names('clean_joined_adm_discharges', 'derived'))
                            new_adm_columns = set(clean_jn_adm_dis.columns) - set(adm_cols.columns)          
                            if new_adm_columns:
                                column_pairs =  [(col, str(clean_jn_adm_dis[col].dtype)) for col in new_adm_columns]
                                if column_pairs:
                                    create_new_columns('clean_joined_adm_discharges','derived',column_pairs)  
                                

                generate_create_insert_sql(clean_jn_adm_dis,"derived","clean_joined_adm_discharges")

                discharge_exists = table_exists('derived','clean_discharges')
                joined_exists = table_exists('derived','clean_joined_adm_discharges')
                if(discharge_exists and joined_exists):
                    read_admissions_query_2 = read_clean_admissions_without_discharges()
                    adm_df_2 = run_query_and_return_df(read_admissions_query_2)
                    read_discharges_query_2 = clean_discharges_not_matched()
                    dis_df_2 = run_query_and_return_df(read_discharges_query_2)
                    if( adm_df_2 is not None and dis_df_2 is not None and not adm_df_2.empty):
                        jn_adm_dis_2 = createCleanJoinedDataSet(adm_df_2,dis_df_2)
                        jn_adm_dis_2.columns = jn_adm_dis_2.columns.astype(str) 
                        jn_adm_dis_2 = jn_adm_dis_2.loc[:, ~jn_adm_dis_2.columns.str.match(r'^\d+$|^[a-zA-Z]$', na=False)]
                        if not jn_adm_dis_2.empty:
                            filtered_df = jn_adm_dis_2[jn_adm_dis_2['neotreeoutcome'].notna() & (jn_adm_dis_2['neotreeoutcome'] != '')]           
                            generateAndRunUpdateQuery('derived.clean_joined_adm_discharges',filtered_df)
                            deduplicate_table('clean_joined_adm_discharges')

            return dict(
            status='Success',
            message = "Data Cleanup Complete"
            )
        else:
            logging.error(
                "Granting Priviledges Complete Did Not Execute To Completion")

            return None

    except Exception as e:
        logging.error(
            "!!! An error occured Cleaning Derived Data: ")
        cron_log.write("StartTime: {0}   Instance: {1}   Status: Failed Stage: Data Cleaning for Reasearch".format(cron_time,env))
        cron_log.close()
        logging.error(formatError(e))
        sys.exit(1)


def createCleanJoinedDataSet(adm_df:pd.DataFrame,dis_df:pd.DataFrame)->pd.DataFrame:
        jn_adm_dis = pd.DataFrame()
        if not adm_df.empty and not dis_df.empty:
            jn_adm_dis = adm_df.merge(
            dis_df, 
            how='left', 
            on=['uid', 'facility'], 
            suffixes=('', '_discharge')
            )
            if 'unique_key' in jn_adm_dis:
                jn_adm_dis['deduplicater'] =jn_adm_dis['unique_key'].map(lambda x: str(x)[:10] if len(str(x))>=10 else None) 
                jn_adm_dis = jn_adm_dis.drop_duplicates(
                    subset=["uid", "facility", "deduplicater"], 
                    keep='first'
                )
                # FURTHER DEDUPLICATION ON UID,FACILITY,OFC-DISCHARGE
                # THIS FIELD HELPS IN ISOLATING DIFFERENT ADMISSIONS MAPPED TO THE SAME DISCHARGE
                if "ofcdis" in jn_adm_dis:
                    jn_adm_dis = jn_adm_dis.drop_duplicates(
                    subset=["uid", "facility", "ofcdis"], 
                    keep='first'
                   )

                # FURTHER DEDUPLICATION ON UID,FACILITY,BIRTH-WEIGHT-DISCHARGE
                # THIS FIELD HELPS IN ISOLATING DIFFERENT ADMISSIONS MAPPED TO THE SAME DISCHARGE
                if "birthweight_discharge" in jn_adm_dis:
                      jn_adm_dis = jn_adm_dis.drop_duplicates(
                    subset=["uid", "facility", "birthweight_discharge"], 
                    keep='first'
                   )    

            #Add Non Existing Columns
            if table_exists('derived','clean_joined_adm_discharges'):
                    adm_cols = pd.DataFrame(get_table_column_names('clean_joined_adm_discharges', 'derived'))
                    new_adm_columns = set(jn_adm_dis.columns) - set(adm_cols.columns) 
                        
                    if new_adm_columns:
                        column_pairs =  [(col, str(jn_adm_dis[col].dtype)) for col in new_adm_columns]
                        if column_pairs:
                            create_new_columns('clean_joined_adm_discharges','derived',column_pairs)

        return jn_adm_dis