from conf.common.sql_functions import inject_sql


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _escape_sql_literal(value) -> str:
    return str(value).replace("'", "''")


def update_uid(schema,table,record_id,new_uid):
    safe_schema = _quote_identifier(schema)
    safe_table = _quote_identifier(table)
    safe_new_uid = _escape_sql_literal(new_uid)
    query = f'''UPDATE {safe_schema}.{safe_table} set uid = '{safe_new_uid}' where id={int(record_id)};;'''

    inject_sql(query,"UPDATE UID")