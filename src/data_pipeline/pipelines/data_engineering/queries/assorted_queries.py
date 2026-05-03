import re
import logging
import json
from psycopg2 import sql
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from  conf.common.config import config
from datetime import datetime

params = config()
env = params['env']
PII_CLEANED_VERSION = 2

# TO BE USED AS IT IS AS IT CONTAINS SPECIAL REQUIREMENTS

def escape_special_characters(input_string): 
    return str(input_string).replace("\\","\\\\").replace("'","")

def deduplicate_neolab_query(neolab_where):
    return f'''
            drop table if exists scratch.deduplicated_neolab cascade;

        create table if not exists scratch.deduplicated_neolab as 
        (
        with earliest_neolab as (
            select
            scriptid,
            uid,
            extract(year from ingested_at) as year,
            extract(month from ingested_at) as month,
            CASE 
                WHEN "data"->'entries'->'DateBCT'->'values'->'value'::text->>0 is null 
                OR "data"->'entries'->'DateBCR'->'values'->'value'::text->>0 is null
                THEN NULL
                ELSE LEFT(
                COALESCE(
                    "data"->'entries'->'DateBCT'->'values'->'value'::text->>0,
                    "data"->'entries'::text->1->'values'->0->'value'::text->>0
                ), 10
                )
            END AS date_key,
            max(id) as id
            from public.clean_sessions
            where scriptid {neolab_where}
            group by 1,2,3,4,5
        )
        select
            earliest_neolab.scriptid,
            earliest_neolab.uid,
            earliest_neolab.id,
            sessions.ingested_at,
            earliest_neolab.year,
            earliest_neolab.month,
            earliest_neolab.date_key,
            sessions.unique_key,
            sessions.data
        from earliest_neolab 
        join clean_sessions sessions on earliest_neolab.id = sessions.id 
        where sessions.unique_key is not null 
        and sessions.scriptid {neolab_where}
        );; '''


def deduplicate_data_query(condition, destination_table):
    if (destination_table == 'public.clean_sessions'):
        return ""
    script_condition=condition
    if "maternity_completeness" in destination_table:
        # special case for malawi -> group on DateAdmission
        return f'''drop table if exists {destination_table} cascade;;
            create table if not exists {destination_table} as 
            (
            with earliest_record as (
            select
            scriptid,
            uid, 
            extract(year from (data->'entries'->'DateAdmission'->'values'->'value'->>0)::timestamp) as year,
            extract(month from (data->'entries'->'DateAdmission'->'values'->'value'->>0)::timestamp) as month,
            max(id) as id -- This takes the last upload 
                  -- of the session as the deduplicated record. 
                  -- We could replace with min(id) to take the 
                  -- first uploaded
            from public.clean_sessions
            where scriptid {condition} -- only pull out records for the specified script
            group by 1,2,3,4
            )
            select
            earliest_record.scriptid,
            earliest_record.uid,
            earliest_record.id,
            sessions.ingested_at,
            earliest_record.year,
            earliest_record.month,
            sessions.unique_key,
            data
            from earliest_record join clean_sessions sessions
            on earliest_record.id = sessions.id where sessions.scriptid {condition}
            );;
            '''
    elif 'daily_review' in destination_table or 'infections' in destination_table:
        schema, table = destination_table.split('.')
        exists = table_exists(schema, table)

        if exists:
            operation = f'''
            INSERT INTO {schema}."{table}" (
                scriptid,
                uid,
                id,
                ingested_at,
                completed_at,
                data,
                unique_key,
                review_number
            )'''
            condition = script_condition + f''' AND NOT EXISTS (
                SELECT 1 FROM {schema}."{table}" ds
                WHERE cs.uid = ds.uid
                 AND  CAST(cs.data->>'completed_at' AS date) = ds.completed_at          
            
            )'''

            return f"""{operation}
                (WITH filtered AS (
                    SELECT
                        cs.scriptid,
                        cs.uid,
                        cs.id,
                        cs.ingested_at,
                        CASE when cs.data->'entries'->'TodDate'->'values'->'value'::text->>0 is null
                        THEN  CAST(cs.data->>'completed_at' AS date)
                        ELSE CAST(cs.data->'entries'->'TodDate'->'values'->'value'::text->>0 as date)  
                        END AS completed_date,
                        cs.data,
                        cs.unique_key
                    FROM public.clean_sessions cs
                    WHERE cs.scriptid {condition}
                ),
                numbered_with_prior AS (
                    SELECT
                        f.scriptid,
                        f.uid,
                        f.id,
                        f.ingested_at,
                        f.completed_date AS completed_at,
                        f.data,
                        f.unique_key,
                        f.completed_date,
                        COALESCE((
                            SELECT MAX(di.review_number)
                            FROM {schema}."{table}" di
                            WHERE di.uid = f.uid
                        ), 0) AS max_existing_review_number
                    FROM filtered f
                ),
                final_numbering AS (
                    SELECT
                        scriptid,
                        uid,
                        id,
                        ingested_at,
                        completed_at,
                        data,
                        unique_key,
                        ROW_NUMBER() OVER (
                            PARTITION BY uid
                            ORDER BY completed_date, id
                        ) + max_existing_review_number AS review_number
                    FROM numbered_with_prior
                )
                SELECT
                    scriptid,
                    uid,
                    id,
                    ingested_at,
                    completed_at,
                    data,
                    unique_key,
                    review_number
                FROM final_numbering);;"""

        else:
            operation = f'''CREATE TABLE if not exists {schema}."{table}" AS'''
            condition = script_condition  # still safe
            
            return f"""{operation}
                (WITH filtered AS (
                    SELECT
                        cs.scriptid,
                        cs.uid,
                        cs.id,
                        cs.ingested_at,
                        CAST(cs.data->>'completed_at' AS date) AS completed_date,
                        cs.data,
                        cs.unique_key
                    FROM public.clean_sessions cs
                    WHERE cs.scriptid {condition}
                ),
                deduplicated AS (
                    SELECT DISTINCT ON (uid,completed_date)
                        scriptid,
                        uid,
                        id,
                        ingested_at,
                        completed_date,
                        data,
                        unique_key
                    FROM filtered
                    ORDER BY uid, completed_date DESC
                ),
                final_numbering AS (
                    SELECT
                        scriptid,
                        uid,
                        id,
                        ingested_at,
                        completed_date AS completed_at,
                        data,
                        unique_key,
                        ROW_NUMBER() OVER (
                            PARTITION BY uid
                            ORDER BY completed_date, id
                        ) AS review_number
                    FROM deduplicated
                )
                SELECT
                    scriptid,
                    uid,
                    id,
                    ingested_at,
                    completed_at,
                    data,
                    unique_key,
                    review_number
                FROM final_numbering);;"""
                      
    else:
        # all other cases -> group on ingested_at
        schema,table = destination_table.split('.')
        exists= table_exists(schema,table)
        operation = f''' create table if not exists {destination_table} as '''
        
        if(exists):
            operation= f''' INSERT INTO {schema}."{table}"  (
                        scriptid,
                        uid,
                        id,
                        ingested_at,
                        unique_key,
                        year,
                        month,
                        data
                        )'''
            condition = script_condition+ f''' and NOT EXISTS (SELECT 1 FROM {schema}."{table}"  ds where cs.unique_key is not null and cs.uid=ds.uid and cs.unique_key=ds.unique_key and cs.scriptid=ds.scriptid) '''
            
        return f'''{operation}
            (
            with earliest_record as (
            select
            cs.scriptid,
            cs.uid, 
            cs.unique_key,
            max(cs.id) as id -- This takes the last upload 
                  -- of the session as the deduplicated record. 
                  -- We could replace with min(id) to take the 
                  -- first uploaded
            from public.clean_sessions cs
            where cs.scriptid {condition} and cs.unique_key is not null-- only pull out records for the specified script
            group by 1,2,3
            )
            select
            earliest_record.scriptid,
            earliest_record.uid,
            earliest_record.id,
            sessions.ingested_at,
            earliest_record.unique_key,
        case
            when earliest_record.unique_key is not null and earliest_record.unique_key like '%-%-%'
            then extract(year from cast (earliest_record.unique_key as date))
        else null
        end as year,
        case
            when earliest_record.unique_key is not null and earliest_record.unique_key like '%-%-%'
            then extract(month from cast (earliest_record.unique_key as date))
        else null
        end as month,
            data
            from earliest_record join clean_sessions sessions
            on earliest_record.id = sessions.id where sessions.scriptid {script_condition}
            );;
            '''


def deduplicate_baseline_query(condition):
    return f'''drop table if exists scratch.deduplicated_baseline cascade;;
            create table scratch.deduplicated_baseline as 
            (
            with earliest_record as (
            select
            scriptid,
            uid, 
            unique_key,
            max(id) as id -- This takes the last upload 
                  -- of the session as the deduplicated record. 
                  -- We could replace with min(id) to take the 
                  -- first uploaded
            from public.clean_sessions
            where scriptid {condition} -- only pull out records for the specified script
            group by 1,2,3
            )
            select
            earliest_record.scriptid,
            earliest_record.uid,
            earliest_record.id,
            earliest_record.unique_key,
           case
            when earliest_record.unique_key is not null and earliest_record.unique_key like '%-%-%'
            then extract(year from cast (earliest_record.unique_key as date))
           else null
           end as year,
           case
            when earliest_record.unique_key is not null and earliest_record.unique_key like '%-%-%'
            then extract(month from cast (earliest_record.unique_key as date))
            else null
            end as month,
            sessions.ingested_at, 
            data
            from earliest_record join clean_sessions sessions
            on earliest_record.id = sessions.id where sessions.scriptid {condition} 
            );;
            '''


def read_deduplicated_data_query(case_condition, where_condition, source_table,destination_table):
    # logging.info(f'source_table={source_table}, where_condition={where_condition}, case_condition={case_condition}')
    condition =''
    sql=''
    exists = table_exists('derived',destination_table)
    if exists and env!='demo':
       condition= get_dynamic_condition(destination_table)
    
    if destination_table == 'daily_review' or destination_table == 'infections':
        sql = f'''
      
        SELECT
            cs.uid,
            cs.ingested_at,
            cs.scriptid,
            cs."data"->'appVersion' AS "appVersion",
            cs."data"->'scriptVersion' AS "scriptVersion",
            cs."data"->'started_at' AS "started_at",
            cs.completed_at,
            cs.review_number,
            cs."data"->'entries' AS "entries",
            cs."data"->'entries'->'repeatables' AS "repeatables",
            cs.unique_key,
            cs."data"->>'completed_at' as "completed_time"
            {case_condition}
        FROM {source_table} cs WHERE cs.scriptid {where_condition} AND cs."data"->>'completed_at' is NOT NULL AND cs.uid IS NOT NULL AND cs.uid != 'null' AND cs.uid != 'Unknown' AND cs.unique_key IS NOT NULL {condition};;
        '''
    elif 'neolab' in destination_table:
        sql=f'''
            SELECT
            cs.uid,
            cs.scriptid,
            cs.ingested_at,
            cs."data"->'appVersion' AS "appVersion",
            cs."data"->'scriptVersion' AS "scriptVersion",
            cs."data"->'started_at' AS "started_at",
             cs."data"->>'completed_at' as "completed_at",
            cs."data"->'entries' AS "entries",
            cs."data"->'entries'->'repeatables' AS "repeatables",
            cs.unique_key
            {case_condition}
            FROM {source_table} cs WHERE cs.scriptid {where_condition} AND cs."data"->>'completed_at' is NOT NULL AND cs.uid IS NOT NULL;;
            '''
    else:
        sql = f'''
            select 
            cs.uid,
            cs.scriptid,
            cs.ingested_at,
            cs."data"->'appVersion' as "appVersion",
            cs."data"->'scriptVersion' as "scriptVersion",
            cs."data"->'started_at' as "started_at",
            cs."data"->>'completed_at' as "completed_at",
            cs."data"->'entries' as "entries",
             cs."data"->'entries'->'repeatables' AS "repeatables",
            cs.unique_key
            {case_condition}
            from {source_table} cs where cs.scriptid {where_condition} AND cs."data"->>'completed_at' is NOT NUll and cs.uid!='Unkown' and cs.uid is not null and cs.unique_key is not null {condition};;
   
            '''
    return sql

def get_dynamic_condition(destination_table) :
    if('daily_review' in destination_table or 'infections' in destination_table):
        return f''' and NOT EXISTS (SELECT 1 FROM derived.{destination_table} ds where cs.unique_key=ds.unique_key and cs.review_number=ds.review_number and cs.uid=ds.uid and CAST(cs.completed_at AS DATE)=CAST(ds.completed_at AS DATE))'''
    
    return   f''' and NOT EXISTS (SELECT 1 FROM derived.{destination_table} ds where  LEFT(cs.unique_key,10)=LEFT(ds.unique_key,10) and  cs.uid=ds.uid and cs.uid is not null and ds.uid is not null and cs.unique_key is not null and ds.unique_key is not null)'''

def read_derived_data_query(source_table, destination_table=None):
    condition = ''
    if destination_table:
        exists = table_exists('derived', destination_table.strip())
        if exists:
            condition = get_dynamic_condition(destination_table.strip())

    # Clean the source_table to remove extra quotes/braces
    source_table_clean = str(source_table).strip().strip('"').strip("'").strip("{}")
    if table_exists('derived',source_table_clean):
        query = f'''select * from derived."{source_table_clean}" cs where cs.unique_key is not null and cs.uid is not null {condition};'''

        return query
    return None

def read_all_from_derived_table(table:str):
    if table_exists('derived', table.strip()):
        return f'select * from derived."{table}";;'
    return None

def read_label_cleanup_data(table:str):
    if table_exists('derived', table.strip()):
        return f'select * from derived."{table}" where transformed is FALSE or transformed is NULL;;'
    return None

def read_admissions_not_joined():   
        return f'''SELECT * 
FROM derived.admissions ad
WHERE NOT EXISTS (
    SELECT 1 
    FROM derived.joined_admissions_discharges j 
    WHERE ad.uid = j.uid 
      AND ad.unique_key = j.unique_key
);'''

def read_clean_admissions_not_joined():   
        return f'''SELECT * 
FROM derived.clean_admissions ad
WHERE NOT EXISTS (
    SELECT 1 
    FROM derived.clean_joined_adm_discharges j 
    WHERE ad.uid = j.uid 
      AND ad.unique_key = j.unique_key
);'''


def read_dicharges_not_joined():
        return f'''SELECT * 
FROM derived.discharges dis
WHERE NOT EXISTS (
    SELECT 1 
    FROM derived.joined_admissions_discharges j 
    WHERE dis.uid = j.uid
      AND dis.facility = j.facility
      AND dis.unique_key = j.unique_key_discharge
)'''

def read_clean_dicharges_not_joined():
        return f'''SELECT * 
FROM derived.clean_discharges dis
WHERE NOT EXISTS (
    SELECT 1 
    FROM derived.clean_joined_adm_discharges j 
    WHERE dis.uid = j.uid
      AND dis.facility = j.facility
      AND dis.unique_key = j.unique_key_discharge
)'''

def admissions_without_discharges():
     return f'''SELECT * 
FROM derived.admissions ad 
WHERE EXISTS (
    SELECT 1 
    FROM derived.joined_admissions_discharges j 
    WHERE ad.uid = j.uid 
      AND ad.unique_key = j.unique_key 
      AND (
          j."NeoTreeOutcome.value" IS NULL 
          OR (
              j."DateTimeDischarge.value" IS NULL 
              AND j."DateTimeDeath.value" IS NULL
          )
      )
)
'''
def read_clean_admissions_without_discharges():
     return f'''SELECT * 
FROM derived.clean_admissions ad 
WHERE EXISTS (
    SELECT 1 
    FROM derived.clean_joined_adm_discharges j 
    WHERE ad.uid = j.uid 
      AND ad.unique_key = j.unique_key 
      AND (
          j."neotreeoutcome" IS NULL 
          OR (
              j."datetimedischarge" IS NULL 
              AND j."datetimedeath" IS NULL
          )
      )
)
'''

def discharges_not_matched():
     return f'''SELECT * 
FROM derived.discharges 
WHERE uid IN (
    SELECT uid 
    FROM derived.admissions ad  
    WHERE EXISTS (
        SELECT 1 
        FROM derived.joined_admissions_discharges j 
        WHERE ad.uid = j.uid 
          AND ad.unique_key = j.unique_key 
          AND (
              j."NeoTreeOutcome.value" IS NULL 
              OR (
                  j."DateTimeDischarge.value" IS NULL 
                  AND j."DateTimeDeath.value" IS NULL
              )
          )
    )
)'''


def clean_discharges_not_matched():
     return f'''SELECT * 
FROM derived.clean_discharges 
WHERE uid IN (
    SELECT uid 
    FROM derived.clean_admissions ad  
    WHERE EXISTS (
        SELECT 1 
        FROM derived.clean_joined_adm_discharges j 
        WHERE ad.uid = j.uid 
          AND ad.unique_key = j.unique_key 
          AND (
              j."neotreeoutcome" IS NULL 
              OR (
                  j."datetimedischarge" IS NULL 
                  AND j."datetimedeath" IS NULL
              )
          )
    )
)'''




def read_data_with_no_unique_key():

    return f'''
           SELECT 
             id,
             "data"->'entries' AS "entries",
            "data"->>'appVersion' AS "appVersion"
        FROM 
            public.clean_sessions
        WHERE scriptid NOT IN('f715e123-3cd0-49ac-8e45-45ab5db72942','588477ee-9274-414c-baa9-dda5951fdf1d','afa5984e-c07d-4025-8150-de25bb37144a') AND

            (cleaned is false and ("unique_key" NOT LIKE '%-%-%' or unique_key is null))
        AND 
            (
                ("data"->'entries'->>'DateTimeAdmission' IS NOT NULL)
                OR ("data"->'entries'->>'DateAdmission' IS NOT NULL)
                OR ("data"->'entries'->>'DateTimeDischarge' IS NOT NULL)
                OR ("data"->'entries'->>'DateDischarge' IS NOT NULL)
                OR ("data"->'entries'->>'DateDeath' IS NOT NULL)
                OR ("data"->'entries'->>'DateBCT' IS NOT NULL)
            )
            '''

# SPECIAL CASE


def read_diagnoses_query(admissions_case, adm_where):
    return f'''
            select 
                cs.uid,
                cs.ingested_at,
                cs."data"->'appVersion' as "appVersion",
                cs."data"->'scriptVersion' as "scriptVersion",
                cs."data"->'started_at' as "started_at",
                cs."data"->'completed_at' as "completed_at",
                cs."data"->'diagnoses' as "diagnoses" {admissions_case},
                unique_key
            from scratch.deduplicated_admissions cs 
            where cs.scriptid {adm_where} and cs.uid!='null' and cs.uid!='Unknown';;
            '''


def read_drugs_query(admissions_case, adm_where):
    return f'''
            select 
                cs.uid,
                cs.ingested_at,
                cs."data"->'appVersion' as "appVersion",
                cs."data"->'scriptVersion' as "scriptVersion",
                cs."data"->'started_at' as "started_at",
                cs."data"->'completed_at' as "completed_at",
                cs."data"->'drugs' as "drugs" {admissions_case},
                unique_key
            from scratch.deduplicated_admissions cs 
            where cs.scriptid {adm_where} and cs.uid!='null' and cs.uid!='Unknown';;
            '''

def read_fluids_query(admissions_case, adm_where):
    return f'''
            select 
                cs.uid,
                cs.ingested_at,
                cs."data"->'appVersion' as "appVersion",
                cs."data"->'scriptVersion' as "scriptVersion",
                cs."data"->'started_at' as "started_at",
                cs."data"->'completed_at' as "completed_at",
                cs."data"->'fluids' as "fluids" {admissions_case},
                unique_key
            from scratch.deduplicated_admissions cs 
            where cs.scriptid {adm_where} and cs.uid!='null' and cs.uid!='Unknown';;
            '''



def read_new_smch_admissions_query():
    return f'''
            select 
                *,
                CASE WHEN ("DateTimeAdmission.value"::TEXT ='NaT'
                OR "DateTimeAdmission.value"::TEXT='NaN'
                OR "DateTimeAdmission.value"::TEXT='nan'
                )
                THEN NULL
                ELSE
                TO_DATE("DateTimeAdmission.value"::TEXT,'YYYY-MM-DD')
                END AS "DateTimeAdmission.value"
                from derived.admissions where
            "DateTimeAdmission.value">='2021-02-01' AND facility = 'SMCH';;'''

def read_raw_data_not_joined_in_all_table(table,condition):
    query = f'select a.* from derived.{table} a where {condition};'
    return query


def read_new_smch_discharges_query():
    return f'''
            SELECT 
    *,
    CASE 
        WHEN "DateTimeDischarge.value"::TEXT IN ('NaT', 'NaN', 'nan') OR 
             "DateTimeDischarge.value"::TEXT IS NULL OR
             "DateTimeDischarge.value"::TEXT = '' OR
             NOT ("DateTimeDischarge.value"::TEXT ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$') OR
             ("DateTimeDischarge.value"::TEXT ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' AND 
              ("DateTimeDischarge.value"::DATE BETWEEN '0001-01-01' AND '9999-12-31') = FALSE)
        THEN NULL
        ELSE TO_DATE("DateTimeDischarge.value"::TEXT, 'YYYY-MM-DD') 
    END AS "DateTimeDischarge.value",
    
    CASE 
        WHEN "DateTimeDeath.value"::TEXT IN ('NaT', 'NaN', 'nan') OR 
             "DateTimeDeath.value"::TEXT IS NULL OR
             "DateTimeDeath.value"::TEXT = '' OR
             NOT ("DateTimeDeath.value"::TEXT ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$') OR
             ("DateTimeDeath.value"::TEXT ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' AND 
              ("DateTimeDeath.value"::DATE BETWEEN '0001-01-01' AND '9999-12-31') = FALSE)
        THEN NULL
        ELSE TO_DATE("DateTimeDeath.value"::TEXT, 'YYYY-MM-DD')
    END AS "DateTimeDeath.value"
FROM derived.discharges 
WHERE facility = 'SMCH' 
AND (
    (
        "DateTimeDischarge.value"::TEXT ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' AND 
        "DateTimeDischarge.value"::DATE BETWEEN '0001-01-01' AND '9999-12-31' AND
        "DateTimeDischarge.value"::DATE >= '2021-02-01'
    )
    OR 
    (
        "DateTimeDeath.value"::TEXT ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' AND 
        "DateTimeDeath.value"::DATE BETWEEN '0001-01-01' AND '9999-12-31' AND
        "DateTimeDeath.value"::DATE >= '2021-02-01'
    )
);; '''


def read_old_smch_admissions_query():
    return '''
            select 
                *
            from derived.old_smch_admissions;;'''


def read_old_smch_discharges_query():
    return '''
            select 
                *
            from derived.old_smch_discharges;;'''


def read_old_smch_matched_view_query():
    return '''
            select 
                *
            from derived.old_smch_matched_admissions_discharges;;'''


def read_new_smch_matched_query():
    return '''
            select 
                *
            from derived.joined_admissions_discharges;;'''


def get_duplicate_maternal_query():
    return '''
            select uid, s."data"->'entries'->'DateAdmission'->'values'->'value'::text->>0 as "DA",s."data"->'entries' as "entries"
            from public.clean_sessions s where scriptid= '-MDPYzHcFVHt02D1Tz4Z' group by 
            s.uid,s."data"->'entries'->'DateAdmission'->'values'->'value'::text->>0,s."data"->'entries' order by
            s.uid,s."data"->'entries'->'DateAdmission'->'values'->'value'::text->>0 ;;
           '''


def update_maternal_uid_query_new(uid, date_condition, old_uid):
    return '''update public.clean_sessions set uid = '{0}',data = JSONB_SET(
             data,
             '{{entries,NeoTreeID}}',
               '{{
                "type": "string",
                "values": {{
                "label": [
                    "NeoTree ID number"
                ],
                "value": ["{0}"]
                
                }}
                }}'::TEXT::jsonb,
               true) where scriptid='-MDPYzHcFVHt02D1Tz4Z' and "uid" = '{2}' and "data"->'entries'->'DateAdmission'->'values'->'value'::text->>0 {1};;
            '''.format(uid, date_condition, old_uid)


def update_maternal_uid_query_old(uid, date_condition, old_uid):
    return '''update public.clean_sessions set uid = '{0}',data = JSONB_SET(
             data,
             '{{entries,0}}',
               '{{
                "key":"NeotreeID",
                "type": "string",
                "values": [
                    {{
                "label": "NeoTree ID number",
                "value": "{0}"
                }}
                ]
                }}'::TEXT::jsonb,
               true) where scriptid='-MDPYzHcFVHt02D1Tz4Z' and "uid" = '{2}' and "data"->'entries'->'DateAdmission'->'values'->'value'::text->>0 {1};;
            '''.format(uid, date_condition, old_uid)


def update_maternal_outer_uid(uid):
    return ''' update public.clean_sessions set data= JSONB_SET(
             data,
            '{{uid}}',
             to_json(uid)::TEXT::JSONB,
             true) where  uid='{0}' and scriptid= '-MDPYzHcFVHt02D1Tz4Z';;'''.format(uid)


def get_discharges_tofix_query():
    return '''select uid as "uid",scriptid as "scriptid",to_json("data"->'entries'::text) as "data" from public.clean_sessions where 
              ingested_at>='2024-01-01'
             and scriptid in ('-ZYDiO2BTM4kSGZDVXAO','-MJCntWHvPaIuxZp35ka','-KYDiO2BTM4kSGZDVXAO');;
             '''


def get_maternal_data_tofix_query():
    return '''select uid as "uid",scriptid as "scriptid",to_json("data"->'entries'::text) as "data" from public.clean_sessions where 
             ingested_at>='2024-01-01' and scriptid in ('-MDPYzHcFVHt02D1Tz4Z' 
             ,'-MYk0A3-Z_QjaXYU5MsS','-MOAjJ_In4TOoe0l_Gl5');;
             '''


def get_admissions_data_tofix_query():
    return '''select uid as "uid",scriptid as "scriptid",to_json("data"->'entries'::text) as "data" from public.clean_sessions where 
              ingested_at>='2024-01-01'
             and scriptid in ('-ZO1TK4zMvLhxTw6eKia','-MJBnoLY0YLDqLUhPgkK','-KO1TK4zMvLhxTw6eKia');;
             '''


def get_baseline_data_tofix_query():
    return '''select uid as "uid",scriptid as "scriptid",to_json("data"->'entries'::text) as "data" from public.clean_sessions where 
              ingested_at>='2024-01-01' and scriptid in ('-MX3bKFIUQxrUw9nmtfb'
             ,'-MX3mjB38q_DWo_XRXJE','-M4TVbN3FzhkDEV3wvWk');;
             '''


def get_script_ids_query():
    return "select scriptid, count(*) from public.sessions group by scriptid;;"


def update_eronous_label(uid, script_id, type, key, label, value):
    # Define the JSONB data to be inserted
    jsonb_data = json.dumps({
        "type": type,
        "values": {
            "label": [label],
            "value": [value]
        }
    })

    # logging.info(jsonb_data)

    # Construct the update query string
    query = f"""
        UPDATE public.clean_sessions
        SET data = JSONB_SET(
            data,
            '{{entries,{key}}}',
            '{jsonb_data}'::jsonb,
            true
        )
        WHERE uid = '{uid}' AND scriptid = '{script_id}';;
    """
    # logging.info(query)
    return query


def insert_sessions_data():

    sessions = 'public.sessions'

    clean_sessions = 'public.clean_sessions'

    # f'''drop table if exists {table} cascade;;
    # CREATE INDEX IF NOT EXISTS idx_clean_sessions_cleaned ON {clean_sessions} (cleaned);;
    return f'''ALTER TABLE {sessions}
                ADD COLUMN IF NOT EXISTS pii_cleaned BOOLEAN DEFAULT FALSE;;

                ALTER TABLE {sessions}
                ADD COLUMN IF NOT EXISTS pii_cleaned_version INTEGER DEFAULT 0;;

                UPDATE {sessions}
                SET pii_cleaned_version = {PII_CLEANED_VERSION}
                WHERE COALESCE(pii_cleaned, FALSE) = TRUE
                AND COALESCE(pii_cleaned_version, 0) = 0;;

                CREATE INDEX IF NOT EXISTS idx_sessions_pii_cleaned
                ON {sessions} (pii_cleaned);;

                CREATE INDEX IF NOT EXISTS idx_sessions_pii_cleaned_version
                ON {sessions} (pii_cleaned_version);;

                CREATE TABLE IF NOT EXISTS public.clean_sessions (
                id INTEGER PRIMARY KEY,
                uid TEXT,
                ingested_at TIMESTAMP WITHOUT TIME ZONE,
                data JSONB,
                scriptid TEXT,
                unique_key VARCHAR,
                cleaned BOOLEAN,
                pii_cleaned BOOLEAN DEFAULT FALSE,
                pii_cleaned_version INTEGER DEFAULT 0
            );;

                ALTER TABLE {clean_sessions}
                ADD COLUMN IF NOT EXISTS pii_cleaned BOOLEAN DEFAULT FALSE;;

                ALTER TABLE {clean_sessions}
                ADD COLUMN IF NOT EXISTS pii_cleaned_version INTEGER DEFAULT 0;;

                UPDATE {clean_sessions}
                SET pii_cleaned_version = {PII_CLEANED_VERSION}
                WHERE COALESCE(pii_cleaned, FALSE) = TRUE
                AND COALESCE(pii_cleaned_version, 0) = 0;;

                CREATE INDEX IF NOT EXISTS idx_clean_sessions_pii_cleaned
                ON {clean_sessions} (pii_cleaned);;

                CREATE INDEX IF NOT EXISTS idx_clean_sessions_pii_cleaned_version
                ON {clean_sessions} (pii_cleaned_version);;
        
        INSERT INTO {clean_sessions} 
        (id, uid, ingested_at, data, scriptid, unique_key, cleaned, pii_cleaned, pii_cleaned_version)
        SELECT s.id, s.uid, s.ingested_at, s.data, s.scriptid, s.unique_key, false, COALESCE(s.pii_cleaned, false),
        COALESCE(s.pii_cleaned_version, CASE WHEN COALESCE(s.pii_cleaned, false) THEN {PII_CLEANED_VERSION} ELSE 0 END)
        FROM {sessions} s
        WHERE NOT EXISTS (
        SELECT 1
        FROM {clean_sessions} cs
        WHERE cs.id = s.id)
        ;;'''


def regenerate_unique_key_query(id, unique_key):
    
    formatted= unique_key
    try: 
        formatted = datetime.strptime(unique_key, "%d %b, %Y %H:%M")
    except:
       formatted= unique_key 

    return f''' UPDATE public.clean_sessions SET cleaned=true, unique_key = '{formatted}' WHERE  id ={id} AND unique_key !~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}.*';;
              '''


def create_pii_redaction_functions():
    return rf"""
    CREATE SCHEMA IF NOT EXISTS scratch;;

    CREATE OR REPLACE FUNCTION scratch.is_protected_non_pii_text(input_text text)
    RETURNS boolean
    LANGUAGE sql
    IMMUTABLE
    AS $$
        SELECT COALESCE(
            input_text ~ '^\d{{4}}-\d{{2}}-\d{{2}}([T ][0-9]{{2}}:[0-9]{{2}}(:[0-9]{{2}}(\.[0-9]+)?)?(Z|[+-][0-9]{{2}}:[0-9]{{2}})?)?$'
            OR input_text ~ '^\d{{4}}/\d{{2}}/\d{{2}}([ T][0-9]{{2}}:[0-9]{{2}}(:[0-9]{{2}}(\.[0-9]+)?)?)?$'
            OR input_text ~ '^\d{{2}}-\d{{2}}-\d{{4}}([ T][0-9]{{2}}:[0-9]{{2}}(:[0-9]{{2}}(\.[0-9]+)?)?)?$'
            OR input_text ~ '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
            OR input_text ~ '^[A-Z0-9]{{3,5}}-[0-9]{{4,8}}$'
            OR input_text ~ '^[A-Za-z0-9]{{20,}}$'
            OR input_text ~ '^[0-9]+\.[0-9]+\.[0-9]+$',
            FALSE
        )
    $$;;

    CREATE OR REPLACE FUNCTION scratch.normalize_phone_candidate(input_text text)
    RETURNS text
    LANGUAGE sql
    IMMUTABLE
    AS $$
        SELECT regexp_replace(COALESCE(input_text, ''), '[\s\-\(\)]', '', 'g')
    $$;;

    CREATE OR REPLACE FUNCTION scratch.contains_identity_context(input_text text)
    RETURNS boolean
    LANGUAGE sql
    IMMUTABLE
    AS $$
        SELECT COALESCE(
            lower(input_text) ~ '(^|[^a-z])(id|nrn|national id|nationalid|identity|identity number|patient id|patientid|mother id|motherid|guardian id|guardianid)([^a-z]|$)',
            FALSE
        )
    $$;;

    CREATE OR REPLACE FUNCTION scratch.matches_malawi_nrn_text(input_text text)
    RETURNS boolean
    LANGUAGE plpgsql
    IMMUTABLE
    AS $$
    DECLARE
        trimmed text;
    BEGIN
        IF input_text IS NULL OR scratch.is_protected_non_pii_text(input_text) THEN
            RETURN FALSE;
        END IF;

        trimmed := btrim(input_text);

        IF trimmed ~ '^[A-Z][A-Z0-9]{7}$' THEN
            RETURN TRUE;
        END IF;

        IF scratch.contains_identity_context(input_text)
           AND input_text ~ '(^|[^[:alnum:]-])[A-Z][A-Z0-9]{7}(?=[^[:alnum:]-]|$)'
        THEN
            RETURN TRUE;
        END IF;

        RETURN FALSE;
    END;
    $$;;

    CREATE OR REPLACE FUNCTION scratch.matches_pii_text(input_text text)
    RETURNS boolean
    LANGUAGE plpgsql
    IMMUTABLE
    AS $$
    DECLARE
        normalized text;
    BEGIN
        IF input_text IS NULL OR scratch.is_protected_non_pii_text(input_text) THEN
            RETURN FALSE;
        END IF;

        normalized := scratch.normalize_phone_candidate(input_text);

        IF normalized ~ '^(\+?265)?[89][0-9]{{8}}$'
           OR normalized ~ '^0[89][0-9]{{8}}$'
           OR normalized ~ '^(\+?263)[0-9]{{5,10}}$'
           OR normalized ~ '^07[1378][0-9]{{7}}$'
           OR normalized ~ '^7[1378][0-9]{{7}}$'
           OR input_text ~ '(^|[^[:alnum:]])[0-9]{{2}}[ -]?[0-9]{{6,7}}[ -]?[A-Za-z][ -]?[0-9]{{2}}(?=[^[:alnum:]]|$)'
           OR scratch.matches_malawi_nrn_text(input_text)
        THEN
            RETURN TRUE;
        END IF;

        RETURN FALSE;
    END;
    $$;;

    CREATE OR REPLACE FUNCTION scratch.strip_pii_text(input_text text)
    RETURNS text
    LANGUAGE plpgsql
    IMMUTABLE
    AS $$
    DECLARE
        redacted text := input_text;
        previous text;
        pattern text;
        redaction_pass integer := 0;
        patterns text[] := ARRAY[
            -- Zimbabwe national ID: DD-NNNNNN-L-DD or DD-NNNNNNN-L-DD.
            '(^|[^[:alnum:]])[0-9]{2}[ -]?[0-9]{6,7}[ -]?[A-Za-z][ -]?[0-9]{2}(?=[^[:alnum:]]|$)',
            -- Malawi phone numbers stay mobile-focused to avoid matching date/time fragments.
            '(^|[^[:alnum:]])\+?265([ -]?[0-9]){9}(?=[^[:alnum:]]|$)',
            '(^|[^[:alnum:]])0[89]([ -]?[0-9]){8}(?=[^[:alnum:]]|$)',
            '(^|[^[:alnum:]])[89]([ -]?[0-9]){8}(?=[^[:alnum:]]|$)',
            -- Zimbabwe phone numbers stay mobile-focused for local formats to avoid matching timestamps.
            '(^|[^[:alnum:]])\+?263([ -]?[0-9]){5,10}(?=[^[:alnum:]]|$)',
            '(^|[^[:alnum:]])07[1378]([ -]?[0-9]){7}(?=[^[:alnum:]]|$)',
            '(^|[^[:alnum:]])7[1378]([ -]?[0-9]){7}(?=[^[:alnum:]]|$)'
        ];
    BEGIN
        IF redacted IS NULL THEN
            RETURN NULL;
        END IF;

        IF scratch.is_protected_non_pii_text(redacted) THEN
            RETURN redacted;
        END IF;

        IF scratch.normalize_phone_candidate(redacted) ~ '^(\+?265)?[89][0-9]{{8}}$'
           OR scratch.normalize_phone_candidate(redacted) ~ '^0[89][0-9]{{8}}$'
           OR scratch.normalize_phone_candidate(redacted) ~ '^(\+?263)[0-9]{{5,10}}$'
           OR scratch.normalize_phone_candidate(redacted) ~ '^07[1378][0-9]{{7}}$'
           OR scratch.normalize_phone_candidate(redacted) ~ '^7[1378][0-9]{{7}}$'
        THEN
            RETURN '[PII_REMOVED]';
        END IF;

        IF btrim(redacted) ~ '^[A-Z][A-Z0-9]{7}$' THEN
            RETURN '[PII_REMOVED]';
        END IF;

        LOOP
            previous := redacted;
            redaction_pass := redaction_pass + 1;

            FOREACH pattern IN ARRAY patterns LOOP
                redacted := regexp_replace(redacted, pattern, '\1[PII_REMOVED]', 'g');
            END LOOP;

            IF scratch.contains_identity_context(redacted) THEN
                redacted := regexp_replace(
                    redacted,
                    '(^|[^[:alnum:]-])[A-Z][A-Z0-9]{7}(?=[^[:alnum:]-]|$)',
                    '\1[PII_REMOVED]',
                    'g'
                );
            END IF;

            EXIT WHEN redacted = previous OR redaction_pass >= 10;
        END LOOP;

        RETURN redacted;
    END;
    $$;;

    CREATE OR REPLACE FUNCTION scratch.strip_pii_entries_jsonb(input_json jsonb)
    RETURNS jsonb
    LANGUAGE sql
    IMMUTABLE
    AS $$
        SELECT CASE jsonb_typeof(input_json)
            WHEN 'object' THEN COALESCE(
                (
                    SELECT jsonb_object_agg(key, scratch.strip_pii_entries_jsonb(value))
                    FROM jsonb_each(input_json)
                ),
                '{{}}'::jsonb
            )
            WHEN 'array' THEN COALESCE(
                (
                    SELECT jsonb_agg(scratch.strip_pii_entries_jsonb(value) ORDER BY ordinality)
                    FROM jsonb_array_elements(input_json) WITH ORDINALITY AS arr(value, ordinality)
                ),
                '[]'::jsonb
            )
            WHEN 'string' THEN to_jsonb(scratch.strip_pii_text(input_json #>> '{{}}'))
            WHEN 'number' THEN CASE
                WHEN scratch.strip_pii_text(input_json #>> '{{}}') <> input_json #>> '{{}}'
                THEN to_jsonb('[PII_REMOVED]'::text)
                ELSE input_json
            END
            ELSE input_json
            END
    $$;;

    CREATE OR REPLACE FUNCTION scratch.strip_pii_jsonb(input_json jsonb)
    RETURNS jsonb
    LANGUAGE sql
    IMMUTABLE
    AS $$
        SELECT CASE jsonb_typeof(input_json)
            WHEN 'object' THEN
                CASE
                    WHEN input_json ? 'entries' THEN jsonb_set(
                        input_json,
                        '{entries}',
                        scratch.strip_pii_entries_jsonb(input_json->'entries'),
                        true
                    )
                    ELSE input_json
                END
            ELSE input_json
        END
    $$;;
    """.strip()


def clean_pii_patterns(schema: str, table: str):
    def qident(s: str) -> str:
        return '"' + s.replace('"', '""') + '"'

    fq = f"{qident(schema)}.{qident(table)}"

    return rf"""
    {create_pii_redaction_functions()}

    CREATE TABLE IF NOT EXISTS scratch.pii_redaction_skips (
        table_schema text NOT NULL,
        table_name text NOT NULL,
        id bigint NOT NULL,
        uid text NULL,
        scriptid text NULL,
        ingested_at timestamp NULL,
        suspicious_pattern text NOT NULL,
        data_excerpt text NULL,
        logged_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
    );;

    DELETE FROM scratch.pii_redaction_skips
    WHERE table_schema = '{schema}'
    AND table_name = '{table}';;

    DROP TABLE IF EXISTS scratch.pii_redaction_candidates;;

    CREATE TABLE scratch.pii_redaction_candidates AS
    SELECT
        id,
        data AS original_data,
        scratch.strip_pii_jsonb(data) AS redacted_data,
        (
            scratch.strip_pii_jsonb(data)::text LIKE '%[PII_REMOVED]:%'
            OR scratch.strip_pii_jsonb(data)::text LIKE '%T[PII_REMOVED]%'
            OR scratch.strip_pii_jsonb(data)::text LIKE '%-[PII_REMOVED]-%'
        ) AS suspicious_output
    FROM {fq}
    WHERE COALESCE(pii_cleaned_version, CASE WHEN COALESCE(pii_cleaned, FALSE) THEN 1 ELSE 0 END, 0) < {PII_CLEANED_VERSION}
    AND data IS NOT NULL
    AND data ? 'entries';;

    INSERT INTO scratch.pii_redaction_skips (
        table_schema,
        table_name,
        id,
        uid,
        scriptid,
        ingested_at,
        suspicious_pattern,
        data_excerpt
    )
    SELECT
        '{schema}',
        '{table}',
        t.id,
        t.uid,
        t.scriptid,
        t.ingested_at,
        CASE
            WHEN candidates.redacted_data::text LIKE '%[PII_REMOVED]:%' THEN '[PII_REMOVED]:'
            WHEN candidates.redacted_data::text LIKE '%T[PII_REMOVED]%' THEN 'T[PII_REMOVED]'
            WHEN candidates.redacted_data::text LIKE '%-[PII_REMOVED]-%' THEN '-[PII_REMOVED]-'
            ELSE 'unknown'
        END,
        left(candidates.redacted_data::text, 300)
    FROM {fq} t
    JOIN scratch.pii_redaction_candidates candidates
      ON t.id = candidates.id
    WHERE candidates.suspicious_output = TRUE;;

    UPDATE {fq}
    SET data = candidates.redacted_data,
    pii_cleaned = TRUE,
    pii_cleaned_version = {PII_CLEANED_VERSION}
    FROM scratch.pii_redaction_candidates candidates
    WHERE {fq}.id = candidates.id
    AND candidates.suspicious_output = FALSE;;

    UPDATE {fq}
    SET data = candidates.original_data,
    pii_cleaned = FALSE,
    pii_cleaned_version = 0
    FROM scratch.pii_redaction_candidates candidates
    WHERE {fq}.id = candidates.id
    AND candidates.suspicious_output = TRUE;;

    UPDATE {fq}
    SET pii_cleaned = TRUE,
    pii_cleaned_version = {PII_CLEANED_VERSION}
    WHERE COALESCE(pii_cleaned_version, CASE WHEN COALESCE(pii_cleaned, FALSE) THEN 1 ELSE 0 END, 0) < {PII_CLEANED_VERSION};;

    UPDATE {fq}
    SET pii_cleaned = FALSE,
    pii_cleaned_version = 0
    WHERE id IN (
        SELECT id
        FROM scratch.pii_redaction_candidates
        WHERE suspicious_output = TRUE
    );;

    DROP TABLE IF EXISTS scratch.pii_redaction_candidates;;

    """.strip()


def pii_skipped_redaction_summary_query(schema: str, table: str, sample_limit: int = 10):
    escaped_schema = schema.replace("'", "''")
    escaped_table = table.replace("'", "''")
    return rf"""
    WITH skipped AS (
        SELECT *
        FROM scratch.pii_redaction_skips
        WHERE table_schema = '{escaped_schema}'
        AND table_name = '{escaped_table}'
        ORDER BY ingested_at DESC NULLS LAST, id DESC
    ),
    sample AS (
        SELECT string_agg(
            concat(id::text, ':', COALESCE(uid, 'NULL'), ':', suspicious_pattern),
            ', '
            ORDER BY ingested_at DESC NULLS LAST, id DESC
        ) AS sample_rows
        FROM (
            SELECT id, uid, suspicious_pattern, ingested_at
            FROM skipped
            LIMIT {sample_limit}
        ) limited
    )
    SELECT
        COUNT(*)::bigint AS skipped_count,
        COALESCE(string_agg(DISTINCT suspicious_pattern, ', '), '') AS patterns,
        COALESCE((SELECT sample_rows FROM sample), '') AS sample_rows
    FROM skipped;;
    """.strip()


def clean_known_confidential_columns(schema: str, table: str):
    def qident(s: str) -> str:
        return '"' + s.replace('"', '""') + '"'

    fq = f"{qident(schema)}.{qident(table)}"

    keys = [
        "KinCell",
        "KinAddress",
        "KinName",
        "MothersCell",
        "MotherFirstName",
        "MotherSurname",
        "BabyFirst",
        "BabySurname",
        "BabyLast",
        "MothCell",
        "BabyFirstName",
        "DOBTOB",
        "MotherAddressVillage",
        "MatPhysAddressDistrict",
        "BIDDOBTOB"
    ]

    arr = ", ".join("'" + k.replace("'", "''") + "'" for k in keys)

    sql = f"""
    ALTER TABLE {fq}
    ADD COLUMN IF NOT EXISTS pii_cleaned BOOLEAN DEFAULT FALSE;;

    ALTER TABLE {fq}
    ADD COLUMN IF NOT EXISTS pii_cleaned_version INTEGER DEFAULT 0;;

    UPDATE {fq}
    SET pii_cleaned_version = {PII_CLEANED_VERSION}
    WHERE COALESCE(pii_cleaned, FALSE) = TRUE
    AND COALESCE(pii_cleaned_version, 0) = 0;;

    CREATE INDEX IF NOT EXISTS idx_{schema}_{table}_pii_cleaned
    ON {fq} (pii_cleaned);;

    CREATE INDEX IF NOT EXISTS idx_{schema}_{table}_pii_cleaned_version
    ON {fq} (pii_cleaned_version);;

    UPDATE {fq}
    SET data = jsonb_set(
    data,
    '{{entries}}',
    (data->'entries') - ARRAY[{arr}]::text[],
    true
    )
    WHERE COALESCE(pii_cleaned_version, CASE WHEN COALESCE(pii_cleaned, FALSE) THEN 1 ELSE 0 END, 0) < {PII_CLEANED_VERSION}
    AND jsonb_typeof(data->'entries') = 'object'
    AND (data->'entries') ?| ARRAY[{arr}]::text[];;

    {clean_pii_patterns(schema, table)}
    """.strip()

    return sql
