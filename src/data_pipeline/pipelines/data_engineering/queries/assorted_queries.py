import logging


def deduplicate_admissions_query(adm_where):
    return  f'''
            drop table if exists scratch.deduplicated_admissions cascade;
            create table scratch.deduplicated_admissions as 
            (
             with earliest_admissions as (
            select
            scriptid,
            uid, 
            min(id) as id -- This takes the first upload 
                    -- of the session as the deduplicated record. 
                    -- We could replace with max(id) to take the 
                    -- most recently uploaded
            from public.sessions
            where scriptid {adm_where}  -- only pull out admissions
            group by 1,2
            )
            select
            earliest_admissions.scriptid,
            earliest_admissions.uid,
            earliest_admissions.id,
            sessions.ingested_at,
            data
            from earliest_admissions join sessions
            on earliest_admissions.id = sessions.id where sessions.scriptid {adm_where}
            ); '''

def deduplicate_baseline_query(baseline_where):
    return f'''drop table if exists scratch.deduplicated_baseline cascade;
            create table scratch.deduplicated_baseline as 
            (
            with earliest_baseline as (
            select
            scriptid,
            uid, 
            min(id) as id -- This takes the first upload 
                    -- of the session as the deduplicated record. 
                    -- We could replace with max(id) to take the 
                    -- most recently uploaded
            from public.sessions
            where scriptid  {baseline_where} -- only pull out baseline
            group by 1,2
             )
            select
            earliest_baseline.scriptid,
            earliest_baseline.uid,
            earliest_baseline.id,
            sessions.ingested_at,
            data
            from earliest_baseline join sessions
            on earliest_baseline.id = sessions.id where sessions.scriptid {baseline_where}
            ); '''

def deduplicate_mat_completeness_query(mat_completeness_where):
    return  f'''drop table if exists scratch.deduplicated_maternity_completeness cascade;
            create table scratch.deduplicated_maternity_completeness as 
            (
            with earliest_mat_completeness as (
            select
            scriptid,
            uid, 
            min(id) as id -- This takes the first upload 
                    -- of the session as the deduplicated record. 
                    -- We could replace with max(id) to take the 
                    -- most recently uploaded
            from public.sessions
            where scriptid {mat_completeness_where} -- only pull out maternity completeness data
            group by 1,2
            )
            select
            earliest_mat_completeness.scriptid,
            earliest_mat_completeness.uid,
            earliest_mat_completeness.id,
            sessions.ingested_at,
            data
            from earliest_mat_completeness join sessions
            on earliest_mat_completeness.id = sessions.id where sessions.scriptid {mat_completeness_where}
            ); '''


def deduplicate_vitals_query(vitals_where):
    return f'''drop table if exists scratch.deduplicated_vitals cascade;
                create table scratch.deduplicated_vitals as 
                (
                with earliest_vitals as (
                select
                scriptid,
                uid, 
                min(id) as id -- This takes the first upload 
                    -- of the session as the deduplicated record. 
                    -- We could replace with max(id) to take the 
                    -- most recently uploaded
                from public.sessions
                where scriptid {vitals_where} -- only pull out vitals
                group by 1,2
                )
                select
                earliest_vitals.scriptid,
                earliest_vitals.uid,
                earliest_vitals.id,
                sessions.ingested_at,
                data
                from earliest_vitals join sessions
                on earliest_vitals.id = sessions.id where sessions.scriptid {vitals_where}
                ); '''

def deduplicate_neolab_query(neolab_where):
    return f'''
            drop table if exists scratch.deduplicated_neolabs cascade;
            create table scratch.deduplicated_neolabs as 
            (
            with earliest_neolab as (
            select
            scriptid,
            uid,
            CASE WHEN "data"->'entries'->'DateBCT'->'values'->'value'::text->>0 is null 
            THEN "data"->'entries'::text->1->'values'->0->'value'::text->>0
            ELSE "data"->'entries'->'DateBCT'->'values'->'value'::text->>0  END AS "DateBCT",
            CASE WHEN "data"->'entries'->'DateBCR'->'values'->'value'::text->>0 is null 
            THEN "data"->'entries'::text->2->'values'->0->'value'::text->>0
            ELSE "data"->'entries'->'DateBCR'->'values'->'value'::text->>0  END AS "DateBCR",
            max(id) as id -- This takes the last upload 
                    -- of the session as the deduplicated record. 
                    -- We could replace with min(id) to take the 
                    -- first uploaded
            from public.sessions
            where scriptid {neolab_where} -- only pull out neloab data
            group by 1,2,3,4
            )
            select
            earliest_neolab.scriptid,
            earliest_neolab.uid,
            earliest_neolab.id,
            sessions.ingested_at,
            earliest_neolab."DateBCT",
            earliest_neolab."DateBCR",
            data
            from earliest_neolab join sessions
            on earliest_neolab.id = sessions.id where  sessions.scriptid {neolab_where}
            ); '''

def deduplicate_maternal_query(mat_outcomes_where):
    return f'''
            drop table if exists scratch.deduplicated_maternals cascade;
            create table scratch.deduplicated_maternals as 
            (
            with earliest_maternal as (
            select
            scriptid,
            uid, 
            min(id) as id -- This takes the first upload 
                    -- of the session as the deduplicated record. 
                    -- We could replace with max(id) to take the 
                    -- most recently uploaded
            from public.sessions
            where scriptid {mat_outcomes_where} -- only pull out maternal  data
            group by 1,2
            )
            select
            earliest_maternal.scriptid,
            earliest_maternal.uid,
            earliest_maternal.id,
            sessions.ingested_at,
            data
            from earliest_maternal join sessions
            on earliest_maternal.id = sessions.id where sessions.scriptid {mat_outcomes_where}  
            ); '''

def deduplicate_discharges_query(disc_where):
    return f'''drop table if exists scratch.deduplicated_discharges cascade;
            create table scratch.deduplicated_discharges as 
            (
            with earliest_discharges as (
            select
            scriptid,
            uid, 
            min(id) as id -- This takes the first upload 
                    -- of the session as the deduplicated record. 
                    -- We could replace with max(id) to take the 
                    -- most recently uploaded
            from public.sessions
            where scriptid {disc_where} -- only pull out discharges
            group by 1,2
            )
            select
            earliest_discharges.scriptid,
            earliest_discharges.uid,
            earliest_discharges.id,
            sessions.ingested_at,
            data
            from earliest_discharges join sessions
            on earliest_discharges.id = sessions.id where sessions.scriptid {disc_where}
            ); '''

def read_admissions_query(admissions_case,adm_where):
    return f'''
            select 
                uid,
                ingested_at,
                "data"->'appVersion' as "appVersion",
                "data"->'started_at' as "started_at",
                "data"->'completed_at' as "completed_at",
                "data"->'entries' as "entries" {admissions_case} 
            from scratch.deduplicated_admissions where scriptid {adm_where} and uid!='null';
            '''
def read_discharges_query(dicharges_case,disc_where):
    return   f'''
             select 
                uid,
                ingested_at,
                "data"->'appVersion' as "appVersion",
                "data"->'started_at' as "started_at",
                "data"->'completed_at' as "completed_at",
                "data"->'entries' as "entries" {dicharges_case}
             from scratch.deduplicated_discharges where scriptid {disc_where} and uid!='null';
             '''

def read_maternal_outcome_query(maternal_case,mat_outcomes_from,mat_outcomes_where):
    return  f'''
            select 
                scriptid,
                uid,
                id,
                ingested_at,
                "data"->'appVersion' as "appVersion",
                "data"->'started_at' as "started_at",
                    "data"->'completed_at' as "completed_at",
                "data"->'entries' as "entries" {maternal_case}
            from {mat_outcomes_from} where scriptid {mat_outcomes_where} and uid!='null'; 
            '''
def read_vitalsigns_query(vitals_case,vital_signs_from,vitals_where):
    return f'''
            select 
                scriptid,
                uid,
                id,
                ingested_at,
                "data"->'appVersion' as "appVersion",
                "data"->'started_at' as "started_at",
                "data"->'completed_at' as "completed_at",
                "data"->'entries' as "entries" {vitals_case}
            from {vital_signs_from} where scriptid {vitals_where} and uid!='null';
            '''

def read_baselines_query(baseline_case,baseline_from,baseline_where):
    return  f'''
            select 
                scriptid,
                uid,
                id,
                ingested_at,
                "data"->'appVersion' as "appVersion",
                "data"->'started_at' as "started_at",
                "data"->'completed_at' as "completed_at",
                "data"->'entries' as "entries" {baseline_case}
            from {baseline_from} where scriptid {baseline_where} and uid!='null';
        '''

def read_mat_completeness_query(maternity_completeness_case,mat_completeness_from,mat_completeness_where):
    return f'''
            select 
                scriptid,
                uid,
                id,
                ingested_at,
                "data"->'appVersion' as "appVersion",
                "data"->'started_at' as "started_at",
                "data"->'completed_at' as "completed_at",
                "data"->'entries' as "entries" {maternity_completeness_case}
            from {mat_completeness_from} where scriptid{mat_completeness_where} and uid!='null';
            '''

def derived_admissions_query():
    return f'''
                select 
                    *
                from derived.admissions where uid!='null';
            '''

def derived_discharges_query():
    return f'''
            select 
                *
            from derived.discharges where uid!='null';
            '''


def read_noelab_query(neolabs_case,neolab_from,neolab_where):
   return f'''
            select 
                scriptid,
                uid,
                id,
                ingested_at,
                "data"->'appVersion' as "appVersion",
                "data"->'started_at' as "started_at",
                    "data"->'completed_at' as "completed_at",
                "data"->'entries' as "entries" {neolabs_case}
            from {neolab_from} where scriptid {neolab_where};
            '''

def read_diagnoses_query(admissions_case,adm_where):
    return f'''
            select 
                uid,
                ingested_at,
                "data"->'appVersion' as "appVersion",
                "data"->'started_at' as "started_at",
                "data"->'completed_at' as "completed_at",
                "data"->'diagnoses' as "diagnoses" {admissions_case}
            from scratch.deduplicated_admissions where scriptid {adm_where} and uid!='null';
            '''

def read_new_smch_admissions_query():
    return f'''
            select 
                *
            from derived.admissions where TO_DATE("DateTimeAdmission.value"::TEXT,'YYYY-MM-DD') >='2021-02-01' AND facility = 'SMCH';'''

def read_new_smch_discharges_query():
    return f'''
            select 
                *
            from derived.discharges where TO_DATE("DateTimeDischarge.value"::TEXT,'YYYY-MM-DD') >='2021-02-01' 
                or TO_DATE("DateTimeDeath.value" ::TEXT,'YYYY-MM-DD')>'2021-02-01'  AND facility = 'SMCH';'''

def read_old_smch_admissions_query():
    return f'''
            select 
                *
            from derived.old_smch_admissions;'''

def read_old_smch_discharges_query():
    return f'''
            select 
                *
            from derived.old_smch_discharges;'''

def read_old_smch_matched_view_query():
    return f'''
            select 
                *
            from derived.old_smch_matched_admissions_discharges;'''

def read_new_smch_matched_query():
    return f'''
            select 
                *
            from derived.joined_admissions_discharges;'''

def get_duplicate_maternal_query():
    return f'''
            select uid, s."data"->'entries'->'DateAdmission'->'values'->'value'::text->>0 as "DA",s."data"->'entries' as "entries"
            from public.sessions s where scriptid= '-MDPYzHcFVHt02D1Tz4Z' group by 
            s.uid,s."data"->'entries'->'DateAdmission'->'values'->'value'::text->>0,s."data"->'entries' having count(*)>1 order by
            s.uid,s."data"->'entries'->'DateAdmission'->'values'->'value'::text->>0 
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
               true) where scriptid='-MDPYzHcFVHt02D1Tz4Z' and "uid" = '{2}' and "data"->'entries'->'DateAdmission'->'values'->'value'::text->>0 {1};
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
               true) where scriptid='-MDPYzHcFVHt02D1Tz4Z' and "uid" = '{2}' and "data"->'entries'->'DateAdmission'->'values'->'value'::text->>0 {1};
            '''.format(uid,date_condition,old_uid)

def update_maternal_outer_uid(uid):
    return ''' update public.sessions set data= JSONB_SET(
             data,
            '{{uid}}',
             to_json(uid)::TEXT::JSONB,
             true) where  uid='{0}' and scriptid= '-MDPYzHcFVHt02D1Tz4Z';'''.format(uid)

def update_misplaced_uid(uid,id):
            return '''update public.sessions data = JSONB_SET(
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
               true) where scriptid='-MDPYzHcFVHt02D1Tz4Z' and id={1}';
            '''.format(uid,id)


def get_data_to_fix_query():
    return f'''
            select id from public.sessions s where uid like '%%ZZ-%%' '''