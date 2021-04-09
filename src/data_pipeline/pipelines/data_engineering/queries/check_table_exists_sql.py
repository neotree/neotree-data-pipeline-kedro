from conf.common.sql_functions import inject_sql_with_return

def table_exists(schema, table_name):
    query = ''' SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_schema = '{0}'
                AND    table_name   = '{1}'
                );'''.format(schema,table_name)
    query_result = inject_sql_with_return(query);
    print("@@@@@@@@@@@@@@---",query_result,table_name)