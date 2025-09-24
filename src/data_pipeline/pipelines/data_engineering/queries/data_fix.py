import logging
from conf.common.sql_functions import inject_sql_procedure,inject_sql_with_return
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists



def deduplicate_table(table:str):  
    if (table_exists('derived',table)):
        deduplicate_derived_tables(table) 
        logging.info(f'''HAS COMPLETED DEDUP {table}''')
        drop_confidential_columns(table)
        logging.info(f'''HAS CONFIDENTIAL DROP {table}''')
        if table=='clean_admissions':
            logging.info(f'''HAS STARTED MATAGE {table}''')
            update_mat_age('admissions','clean_admissions')
            logging.info(f'''HAS COMPLETE MATAGE {table}''')
        if table=='clean_joined_adm_discharges':
            logging.info(f'''HAS STARTED MATAGE {table}''')
            update_mat_age('joined_admissions_discharges','clean_joined_adm_discharges')
            logging.info(f'''HAS COMPLETE MATAGE {table}''')
        if table=='clean_maternal_outcomes':
            logging.info(f'''HAS STARTED MATAGE {table}''')
            update_mat_age('maternal_outcomes','clean_maternal_outcomes')
            logging.info(f'''HAS COMPLETE MATAGE {table}''')
             
    

def deduplicate_derived_tables(table: str):
    query = f'''DO $$
        DECLARE
            rows_deleted INTEGER := 1;
            total_deleted INTEGER := 0;
            batch_size INTEGER := 10000;
        BEGIN
            WHILE rows_deleted > 0 LOOP
                WITH ranked_duplicates AS (
                    SELECT ctid,
                        ROW_NUMBER() OVER (
                            PARTITION BY LEFT(unique_key,10), uid
                            ORDER BY ctid
                        ) AS rn
                    FROM derived."{table}"
                    WHERE unique_key IS NOT NULL
                ),
                to_delete AS (
                    SELECT ctid
                    FROM ranked_duplicates
                    WHERE rn > 1
                    LIMIT batch_size
                )
                DELETE FROM derived."{table}" 
                WHERE ctid IN (SELECT ctid FROM to_delete);

                GET DIAGNOSTICS rows_deleted = ROW_COUNT;
                total_deleted := total_deleted + rows_deleted;
                
                RAISE NOTICE 'Deleted % rows in this batch, % total', rows_deleted, total_deleted;
                
                PERFORM pg_sleep(0.1);
            END LOOP;
        END $$;'''
    inject_sql_procedure(query,f"DEDUPLICATE DERIVED {table}")

def drop_confidential_columns(table_name):
   
    query = f'''DO $$
            DECLARE
                cols text;
                sql  text;
            BEGIN
                -- Step 1: find matching columns and build DROP clause dynamically
                SELECT string_agg(format('DROP COLUMN IF EXISTS %I', column_name), ', ')
                INTO cols
                FROM information_schema.columns
                WHERE table_schema = 'derived'
                AND table_name   = '{table_name}'
                AND (
                    column_name ILIKE '%dobtob%'
                    OR column_name ILIKE '%firstname%'
                    OR column_name ILIKE '%lastname%'
                );

                -- Only run if we found matching columns
                IF cols IS NOT NULL THEN
                    sql := format('ALTER TABLE %I.%I %s;', 'derived', '{table_name}', cols);
                    EXECUTE sql;
                END IF;
            END $$;
            '''
    inject_sql_procedure(query,f"DROP CONFIDENTIAL COLS {table_name}")


def update_mat_age(source_table: str, dest_table: str) -> str:
    # Determine which column exists in source
    col_check_query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'derived' AND table_name = '{source_table}' 
        AND column_name IN ('matageyrs', 'MatAgeYrs.value')
        ORDER BY column_name='matageyrs' DESC  -- prefer matageyrs if present
        LIMIT 1;
    """

    source_col_result = inject_sql_with_return(col_check_query)
    
    if source_col_result and len(source_col_result) > 0:
        sc = source_col_result[0][0]
        sc_quoted = f'"{sc}"' if '.' in sc else sc

        query = f"""
        DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_schema = 'derived' 
            AND table_name = '{dest_table}' 
            AND column_name = 'matageyrs'
        )
        THEN
            UPDATE derived.{dest_table} d
            SET matageyrs = s_val.num_val
            FROM (
                SELECT 
                    uid,
                    unique_key,
                    COALESCE(
                        CAST(NULLIF(regexp_replace({sc_quoted}, '[^0-9]', '', 'g'), '') AS INT),
                        200
                    ) AS num_val
                FROM derived.{source_table}
            ) s_val
            WHERE d.uid = s_val.uid
            AND d.unique_key = s_val.unique_key
            AND d.matageyrs IS NULL
            AND s_val.num_val <= 85;
            END IF;
            END $$;
        """

        inject_sql_procedure(query, f"FIX MATERNAL AGE {dest_table}")

