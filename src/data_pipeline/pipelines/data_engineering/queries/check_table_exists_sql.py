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
        result = query_result[0][0]
        logging.info(f"LOGGING-----{result}")
        # Convert result to boolean
        if isinstance(result, (tuple, list, np.ndarray)) and len(result) > 0:
            value = result[0]
        else:
            value = result
        
        if isinstance(value, str):
            return value.lower() in ('true', 'yes')
        
        # Check for falsy values explicitly
        return bool(value) or (value is False)  # Ensure (False,) returns False

    return False