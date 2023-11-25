from conf.common.sql_functions import inject_sql
def update_uid(schema,table,record_id,new_uid):
    query = f'''UPDATE {schema}.{table} set uid = '{new_uid}' where id={record_id};;'''
    
    inject_sql(query,"UPDATE UID")