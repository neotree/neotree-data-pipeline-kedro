import logging
import json
from psycopg2 import sql

# TO BE USED AS IT IS AS IT CONTAINS SPECIAL REQUIREMENTS


def deduplicate_neolab_query(neolab_where):
    return f'''
            drop table if exists scratch.deduplicated_neolab cascade;;
            create table scratch.deduplicated_neolab as 
            (
            with earliest_neolab as (
            select
            scriptid,
            uid,
            extract(year from ingested_at) as year,
            extract(month from ingested_at) as month,
            CASE WHEN "data"->'entries'->'DateBCT'->'values'->'value'::text->>0 is null 
            THEN "data"->'entries'::text->1->'values'->0->'value'::text->>0
            ELSE "data"->'entries'->'DateBCT'->'values'->'value'::text->>0  END AS "DateBCT",
            CASE WHEN "data"->'entries'->'DateBCR'->'values'->'value'::text->>0 is null 
            THEN "data"->'entries'::text->2->'values'->0->'value'::text->>0
            ELSE "data"->'entries'->'DateBCR'->'values'->'value'::text->>0  END AS "DateBCR",
            max(id) as id
            from public.clean_sessions
            where scriptid {neolab_where} -- only pull out neloab data
            group by 1,2,3,4,5,6
            )
            select
            earliest_neolab.scriptid,
            earliest_neolab.uid,
            earliest_neolab.id,
            sessions.ingested_at,
            earliest_neolab.year,
            earliest_neolab.month,
            earliest_neolab."DateBCT",
            earliest_neolab."DateBCR",
            sessions.unique_key,
            data
            from earliest_neolab join clean_sessions sessions
            on earliest_neolab.id = sessions.id where  sessions.scriptid {neolab_where}
            );; '''


def deduplicate_data_query(condition, destination_table):
    if (destination_table == 'public.clean_sessions'):
        return ""

    if "maternity_completeness" in destination_table:
        # special case for malawi -> group on DateAdmission
        return f'''drop table if exists {destination_table} cascade;;
            create table {destination_table} as 
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
    else:
        # all other cases -> group on ingested_at
        return f'''drop table if exists {destination_table} cascade;;
            create table {destination_table} as 
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
            on earliest_record.id = sessions.id where sessions.scriptid {condition}
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


def read_deduplicated_data_query(case_condition, where_condition, source_table):
    # logging.info(f'source_table={source_table}, where_condition={where_condition}, case_condition={case_condition}')
    sql = f'''
            select 
            uid,
            ingested_at,
            "data"->'appVersion' as "appVersion",
            "data"->'scriptVersion' as "scriptVersion",
            "data"->'started_at' as "started_at",
            "data"->'completed_at' as "completed_at",
            "data"->'entries' as "entries",
            unique_key
            {case_condition}
            from {source_table} where scriptid {where_condition} and uid!='null';;
   
            '''
    return sql


def read_derived_data_query(source_table):
    return f'''
                select 
                    *
                from derived.{source_table} where uid!='null';;
            '''


def read_data_with_no_unique_key():
    return f'''
                select 
                id,
                "data"->'entries' as "entries",
                "data"->'appVersion' as "appVersion"
                from public.clean_sessions where not cleaned;;'''

# SPECIAL CASE


def read_diagnoses_query(admissions_case, adm_where):
    return f'''
            select 
                uid,
                ingested_at,
                "data"->'appVersion' as "appVersion",
                "data"->'scriptVersion' as "scriptVersion",
                "data"->'started_at' as "started_at",
                "data"->'completed_at' as "completed_at",
                "data"->'diagnoses' as "diagnoses" {admissions_case},
                unique_key
            from scratch.deduplicated_admissions where scriptid {adm_where} and uid!='null';;
            '''


def read_new_smch_admissions_query():
    return '''
            select 
                *,
                CASE WHEN "DateTimeAdmission.value"::TEXT ='NaT'
                THEN NULL
                ELSE
                TO_DATE("DateTimeAdmission.value"::TEXT,'YYYY-MM-DD')
                END AS "DateTimeAdmission.value"
                from derived.admissions where
            "DateTimeAdmission.value">='2021-02-01' AND facility = 'SMCH';;'''


def read_new_smch_discharges_query():
    return '''
            select 
                *,
		  CASE WHEN "DateTimeDischarge.value"::TEXT ='NaT' 
		  THEN NULL
		  ELSE  TO_DATE("DateTimeDischarge.value"::TEXT,'YYYY-MM-DD') 
		  END AS "DateTimeDischarge.value",
		  CASE WHEN "DateTimeDeath.value"::TEXT = 'NaT' 
		  THEN NULL
		  ELSE TO_DATE("DateTimeDeath.value"::TEXT,'YYYY-MM-DD')
		  END AS "DateTimeDischarge.value"
            from derived.discharges where
			("DateTimeDischarge.value">='2021-02-01')
			or ("DateTimeDeath.value">='2021-02-01') AND facility = 'SMCH' ;;
            '''


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
    return f'''CREATE TABLE IF NOT EXISTS public.clean_sessions (
                id INTEGER PRIMARY KEY,
                uid TEXT,
                ingested_at TIMESTAMP WITHOUT TIME ZONE,
                data JSONB,
                scriptid TEXT,
                unique_key VARCHAR,
                cleaned BOOLEAN
            );;     
        
        INSERT INTO {clean_sessions} 
        SELECT *,false FROM {sessions} s
        WHERE NOT EXISTS (
        SELECT 1
        FROM {clean_sessions} cs
        WHERE cs.id = s.id);;'''


def regenerate_unique_key_query(id, unique_key):
    return f''' UPDATE public.clean_sessions set unique_key='{unique_key}' where id={id};;
 '''
