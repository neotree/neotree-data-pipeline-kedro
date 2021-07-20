from conf.common.sql_functions import inject_sql_with_return
# Query To check if a table exists on the specified schema
# This is helpful in preventing errors that comes with trying to query tables which do not exist


def table_exists(schema, table_name):
    query = ''' SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_schema = '{0}'
                AND    table_name   = '{1}'
                );'''.format(schema, table_name)
    query_result = inject_sql_with_return(query)
    if len(query_result) > 0:
        result = query_result[0]
        if 'exists' in result.keys():
            return result['exists']
        else:
            return False
