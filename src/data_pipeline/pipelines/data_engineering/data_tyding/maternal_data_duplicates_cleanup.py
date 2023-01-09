import logging
import pandas as pd
from conf.common.format_error import formatError
import random
from conf.base.catalog import catalog
from conf.common.sql_functions import inject_sql
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import update_maternal_uid_query_new,update_maternal_uid_query_old,update_misplaced_uid
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import update_maternal_outer_uid
from data_pipeline.pipelines.data_engineering.queries.check_row_exists_sql import row_exists
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists

def maternal_data_duplicates_cleanup():
    try:
        #####IDENTIFY THE DUPLICATES
        duplicates_df = pd.DataFrame()
        if table_exists('public','sessions'):
            duplicates_df = catalog.load('duplicate_maternal_data')
            data_to_fix_df = catalog.load('data_to_fix')
            if not data_to_fix_df.empty:
                for i, r, in data_to_fix_df.iterrows():
                    id = r['id']
                    sql_q= update_misplaced_uid(generateNeotreeId(),id)
                    logging.info("----SD--"+str(id))
                    inject_sql(sql_q,"CORRECTING DATA")
        if not duplicates_df.empty:
            processed = []
            for index,row in duplicates_df.iterrows():
                if row['uid'] not in processed:
                    fileterd_df = duplicates_df[duplicates_df['uid']==row['uid']]
                    if fileterd_df.size>1:
                        for fIndex,fRow in fileterd_df.iterrows(): 
                            if row['uid'] == fRow['uid']:
                                if row['DA'] ==fRow['DA']:
                                    pass;
                                else:
                                    neotree_id = generateNeotreeId()
                                    admission_date = fRow['DA']
                                    date_condition =f'''='{admission_date}' '''
                                    if admission_date is None:
                                        date_condition= 'is null'
                                    update_query =''
                                    if type(fRow['entries']) is list:
                                        update_query = update_maternal_uid_query_old(neotree_id,date_condition,fRow['uid'])
                                    else:
                                        update_query = update_maternal_uid_query_new(neotree_id,date_condition,fRow['uid'])
                                    outer_uid_update_query = update_maternal_outer_uid(neotree_id);
                                    inject_sql(update_query,'UPDATE DUPLICATE NEOTREE ID')
                                    inject_sql(outer_uid_update_query,'UPDATE OUTER NEOTREE ID')
                    processed.append(row['uid'])
        else:
            pass;

    except Exception as e:
        logging.error(formatError(e))

def generateNeotreeId():
    alphabet =['A','B','C','D','E','F','G','H','I','0','1','2','3','4','5','6','7','8','9']
    numeric = ['0','2','4','1','3','7','9','6','8','5']
    isDuplicate = True
    while(isDuplicate):
        prefix =''
        suffix=''
        for i in range(2):
            prefix = prefix + alphabet[random.randint(0, 18)]
        for i in range(4):
            suffix = suffix + numeric[random.randint(0,9)]
        neotree_id = prefix+'ZZ'+'-'+suffix
        isDuplicate = row_exists('public','sessions','uid',neotree_id)
    return str(neotree_id);
    
    
    