import logging
from conf.common.sql_functions import inject_sql_with_return
# Query To check if a table exists on the specified schema 
# This is helpful in preventing errors that comes with trying to query tables which do not exist
def table_exists(schema, table_name):
    query = f''' SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_schema = '{schema}'
                AND    table_name   = '{table_name}'
                );'''
    query_result = inject_sql_with_return(query);
    if len(query_result) >0:
        result = query_result[0];
        logging.info(f"###IN EXISTS {result}")
        if 'exists' in result:
            logging.info(f"###IN TRUTHFULNESS")
            return result['exists']
        else:
            return False