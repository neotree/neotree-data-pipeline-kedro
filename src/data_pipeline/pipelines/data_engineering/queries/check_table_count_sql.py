from conf.common.sql_functions import inject_sql_with_return
# Query To check the count for  a table  on the specified schema 
# This is helpful in preventing errors that unnecessary running of certain steps on the data pipeline
def table_data_count(schema, table_name):
    query = f''' SELECT count(*) 
                 from {schema}.{table_name}
            '''
    query_result = inject_sql_with_return(query);
    if len(query_result) >0:
        result = query_result[0];
        if 'count' in result.keys():
            return result['count']
        else:
            return 0