from conf.common.sql_functions import inject_sql_with_return
# Query To check if a row exists on the specified schema 
# This is helpful in preventing duplicate records
def row_exists(schema, table_name,field,value):
    query = f''' SELECT EXISTS (
                SELECT FROM {schema}.{table_name}
                WHERE {field}='{value}'
                );;'''
    query_result = inject_sql_with_return(query)
    if len(query_result) >0:
        result = query_result[0]
        if result:
            return True
        else:
            return False