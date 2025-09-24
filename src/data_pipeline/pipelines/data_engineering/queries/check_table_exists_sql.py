import logging
import numpy as np
from conf.common.sql_functions import inject_sql_with_return
# Query To check if a table exists on the specified schema 
# This is helpful in preventing errors that comes with trying to query tables which do not exist

def table_exists(schema, table_name):
    query = f''' SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_schema = '{schema}'
                AND    table_name   = '{table_name}'
                );'''
    query_result = inject_sql_with_return(query)
    
    if query_result and len(query_result) > 0:
        result = query_result[0]
        
        # Convert result to boolean
        if isinstance(result, (tuple, list, np.ndarray)) and len(result) > 0:
            value = result[0]
        else:
            value = result
    
        logging.info(f"###----###--{value}")
        if isinstance(value, str):
            return value.lower() in ('true', 'yes')
        return bool(value)

    return False