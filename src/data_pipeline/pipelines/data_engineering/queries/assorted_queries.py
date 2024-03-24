import re
import logging

def escape_special_characters(input_string): 
    return str(input_string).replace("\\","\\\\").replace("'","''")

#TO BE USED AS IT IS AS IT CONTAINS SPECIAL REQUIREMENTS
def deduplicate_neolab_query(neolab_where):
    return deduplicate_data_query(neolab_where, "scratch.deduplicated_neolab")


def deduplicate_data_query(condition,destination_table):
    if(destination_table!='public.sessions'):
        return f'''drop table if exists {destination_table} cascade;;
            create table {destination_table} as 
            (
                with earliest_record as (
                    select
                    scriptid,
                    uid, 
                    extract(year from ingested_at) as year,
                    extract(month from ingested_at) as month,
                    min(id) as id -- This takes the last upload 
                        -- of the session as the deduplicated record. 
                        -- We could replace with min(id) to take the 
                        -- first uploaded
                    from public.sessions
                    where scriptid {condition} -- only pull out records for the specified script
                    group by 1,2,3,4
                )
                select
                earliest_record.id,
                earliest_record.scriptid,
                earliest_record.uid,
                earliest_record.year,
                earliest_record.month,
                sessions.ingested_at,
                data
                from earliest_record join sessions
                on earliest_record.id = sessions.id where sessions.scriptid {condition}
            );;
            '''      

def deduplicate_baseline_query(condition):
    return deduplicate_data_query(condition, "scratch.deduplicated_baseline")
 
            
def read_deduplicated_data_query(case_condition,where_condition,source_table):
    return f'''
                select 
                uid,
                ingested_at,
                "data"->'appVersion' as "appVersion",
                "data"->'scriptVersion' as "scriptVersion",
                "data"->'started_at' as "started_at",
                "data"->'completed_at' as "completed_at",
                "data"->'entries' as "entries"
                {case_condition}
            from {source_table} where scriptid {where_condition} and uid!='null';;
   
  '''
  
def read_derived_data_query(source_table):
     return f'''
                select 
                    *
                from derived.{source_table} where uid!='null';;
            '''

##SPECIAL CASE
def read_diagnoses_query(admissions_case,adm_where):
    return f'''
            select 
                uid,
                ingested_at,
                "data"->'appVersion' as "appVersion",
                "data"->'scriptVersion' as "scriptVersion",
                "data"->'started_at' as "started_at",
                "data"->'completed_at' as "completed_at",
                "data"->'diagnoses' as "diagnoses" {admissions_case}
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
            from public.sessions s where scriptid= '-MDPYzHcFVHt02D1Tz4Z' group by 
            s.uid,s."data"->'entries'->'DateAdmission'->'values'->'value'::text->>0,s."data"->'entries' order by
            s.uid,s."data"->'entries'->'DateAdmission'->'values'->'value'::text->>0 ;;
           '''

def update_maternal_uid_query_new(uid,date_condition,old_uid):
    return '''update public.sessions set uid = '{0}',data = JSONB_SET(
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
            '''.format(uid,date_condition,old_uid) 

def update_maternal_uid_query_old(uid,date_condition,old_uid):
    return '''update public.sessions set uid = '{0}',data = JSONB_SET(
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
            '''.format(uid,date_condition,old_uid) 

def update_maternal_outer_uid(uid):
    return ''' update public.sessions set data= JSONB_SET(
             data,
            '{{uid}}',
             to_json(uid)::TEXT::JSONB,
             true) where  uid='{0}' and scriptid= '-MDPYzHcFVHt02D1Tz4Z';;'''.format(uid)

def get_discharges_tofix_query():
    return '''select uid as "uid",scriptid as "scriptid",to_json("data"->'entries'::text) as "data" from public.sessions where 
             ("data"->'entries'->'NeoTreeOutcome'->'values'->'label'::text->>0 like '%%Outcome%%'
             or "data"->'entries'->'ModeDelivery'->'values'->'label'::text->>0 like '%%Mode of Delivery%%') 
             and scriptid in ('-ZYDiO2BTM4kSGZDVXAO','-MJCntWHvPaIuxZp35ka','-KYDiO2BTM4kSGZDVXAO');;
             '''
def get_maternal_data_tofix_query():
    return '''select uid as "uid",scriptid as "scriptid",to_json("data"->'entries'::text) as "data" from public.sessions where 
             "data"->'entries'->'NeoTreeOutcome'->'values'->'label'::text->>0 like '%%Outcome%%' and scriptid in ('-MDPYzHcFVHt02D1Tz4Z' 
             ,'-MYk0A3-Z_QjaXYU5MsS','-MOAjJ_In4TOoe0l_Gl5');;
             '''
def get_admissions_data_tofix_query():
    return '''select uid as "uid",scriptid as "scriptid",to_json("data"->'entries'::text) as "data" from public.sessions where 
             ("data"->'entries'->'AdmReason'->'values'->'label'::text->>0 like '%%Presenting complaint%%'
             or "data"->'entries'->'ModeDelivery'->'values'->'label'::text->>0 like '%%Mode of Delivery%%'
             or "data"->'entries'->'HIVtestResult'->'values'->'label'::text->>0 like '%%What%%'
             ) and scriptid in ('-ZO1TK4zMvLhxTw6eKia','-MJBnoLY0YLDqLUhPgkK','-KO1TK4zMvLhxTw6eKia');;
             '''
def get_baseline_data_tofix_query():
    return '''select uid as "uid",scriptid as "scriptid",to_json("data"->'entries'::text) as "data" from public.sessions where 
             "data"->'entries'->'NeoTreeOutcome'->'values'->'label'::text->>0 like '%%Outcome%%' and scriptid in ('-MX3bKFIUQxrUw9nmtfb'
             ,'-MX3mjB38q_DWo_XRXJE','-M4TVbN3FzhkDEV3wvWk');;
             '''
                         
def update_eronous_label(uid,script_id,type,key,label,value):
    label = escape_special_characters(label)
    value = escape_special_characters(value)
    
    return '''update public.sessions set data = JSONB_SET(
             data,
             '{{entries,{3}}}',
               '{{
                "type": "{2}",
                "values": {{
                "label": [
                    "{4}"
                ],
                "value": ["{5}"]
                
                }}
                }}'::TEXT::jsonb,
               true) where "uid" = '{0}' and "scriptid"='{1}';;
            '''.format(uid,script_id,type,key,label,value)  