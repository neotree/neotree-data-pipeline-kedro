import pandas as pd
import json
from conf.common.sql_functions import inject_sql,get_table_columns, create_union_views
import logging

def union_views():
    try:
        adm_cols = pd.DataFrame(get_table_columns(
            'admissions', 'derived'), columns=["column_name", "data_type"])
        old_adm_cols = pd.DataFrame(get_table_columns(
            'old_smch_admissions', 'derived'), columns=["column_name", "data_type"])
        old_disc_cols = pd.DataFrame(get_table_columns(
            'old_smch_discharges', 'derived'), columns=["column_name", "data_type"])
        disc_cols = pd.DataFrame(get_table_columns(
            'discharges', 'derived'), columns=["column_name", "data_type"])
        old_matched_cols = pd.DataFrame(get_table_columns(
            'old_smch_matched_admissions_discharges', 'derived'), columns=["column_name", "data_type"])
        matched_cols = pd.DataFrame(get_table_columns(
            'joined_admissions_discharges', 'derived'), columns=["column_name", "data_type"])

        adm_view_columns = []
        dis_view_columns = []
        matched_view_columns = []

        for index, row in adm_cols.iterrows():
            col_name = row['column_name']
            data_type = row['data_type']
            using = ''
            for index2, row2 in old_adm_cols.iterrows():
                if col_name == row2['column_name']:
                    if(data_type == row2['data_type']):
                        adm_view_columns.append(col_name)
                    elif 'double' in data_type and 'timestamp' in row2['data_type']:
                        pass
                    else:
                        if 'timestamp' in data_type:
                            using = f'''USING "{col_name}"::{data_type}'''
                        query = f'''ALTER table derived.old_smch_admissions ALTER column "{col_name}" TYPE {data_type}  {using};'''
                        inject_sql(query)
                        adm_view_columns.append(col_name)
                else:
                    pass

        for index, row in disc_cols.iterrows():
            col_name = row['column_name']
            data_type = row['data_type']
            using = ''
            for index2, row2 in old_disc_cols.iterrows():
                if col_name == row2['column_name']:
                    if(data_type == row2['data_type']):
                        dis_view_columns.append(col_name)
                    elif 'double' in data_type and 'timestamp' in row2['data_type']:
                        pass
                    else:
                        if 'timestamp' in str(data_type):
                            using = f'''USING "{col_name}"::{data_type}'''
                        query = f''' ALTER table derived.old_smch_discharges ALTER column "{col_name}" TYPE {data_type} {using};'''
                        inject_sql(query)
                        dis_view_columns.append(col_name)
                else:
                    pass

        for index, row in matched_cols.iterrows():
            col_name = row['column_name']
            data_type = row['data_type']
            using = ''
            for index2, row2 in old_matched_cols .iterrows():
                if col_name == row2['column_name']:
                    if(data_type == row2['data_type']):
                        matched_view_columns.append(col_name)
                    elif 'double' in data_type and 'timestamp' in row2['data_type']:
                        pass
                    else:
                        if 'timestamp' in str(data_type):
                            using = f'''USING "{col_name}"::{data_type}'''
                        query = f''' ALTER table derived.old_smch_matched_admissions_discharges ALTER column "{col_name}" TYPE {data_type} {using};'''
                        inject_sql(query)
                        matched_view_columns.append(col_name)
                else:
                    pass


        adm_where = f'''where TO_DATE("DateTimeAdmission.value",'YYYY-MM-DD') >='2021-02-01' AND facility = 'SMCH' '''
        disc_where = f'''where TO_DATE("DateTimeDischarge.value",'YYYY-MM-DD') >='2021-02-01' or TO_DATE("DateTimeDeath.value",'YYYY-MM-DD')>'2021-02-01'  AND facility = 'SMCH' '''
        if len(adm_view_columns) > 0:
            logging.info("ADM-COLS--"+str(
                json.dumps(adm_view_columns))[1:-1]+" "+ adm_where);
            create_union_views('old_new_admissions_view', 'admissions', 'old_smch_admissions', str(
                json.dumps(adm_view_columns))[1:-1]+" "+adm_where)
        if len(dis_view_columns) > 0:
            logging.info('--DIS-COLS--'+str(
                json.dumps(dis_view_columns))[1:-1]+" "+ disc_where);
            create_union_views('old_new_discharges_view', 'discharges', 'old_smch_discharges', str(
                json.dumps(dis_view_columns))[1:-1], disc_where)
        if len(matched_view_columns) > 0:
            logging.info('--MATCHED-COLS--'+str(
                json.dumps(matched_view_columns))[1:-1]+" " +adm_where)

            create_union_views('old_new_matched_view', 'joined_admissions_discharges', 'old_smch_matched_admissions_discharges', str(
                json.dumps(matched_view_columns))[1:-1], adm_where)

    except Exception as ex:
        logging.error("!!! An error occured creating union views: ")
        logging.error(ex.with_traceback())
