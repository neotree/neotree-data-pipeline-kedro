from conf.common.sql_functions import inject_sql_with_return
# Once Off Query To Find Different records with Same ID (CASE OF CHINHOI MATERNAL DATA)
def fix_duplicate_uid():
    query = f'''select id,uid,"data"->'entries'->>1 as "Date Admission" from public.clean_sessions where scriptid='-MYk0A3-Z_QjaXYU5MsS' order by uid,"data"->'entries'->>1;;'''
   
    return inject_sql_with_return(query)


   