import logging
from conf.common.sql_functions import inject_sql_procedure
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists



def deduplicate_table(table:str):
    
    if (table_exists('derived',table)):
        deduplicate_derived_tables(table) 
        drop_confidential_columns(table)
          
  


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