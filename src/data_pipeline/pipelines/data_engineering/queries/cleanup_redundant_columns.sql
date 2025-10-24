-- ============================================================================
-- CLEANUP REDUNDANT COLUMNS IN DERIVED SCHEMA
-- ============================================================================
-- Problem: When database has "BirthWeight.value" and "BirthWeight.label",
--          the system sometimes creates "BirthWeight" as a separate column.
--          This is redundant and wastes column space.
--
-- Solution: This script identifies and drops redundant columns where:
--           - Column "X" exists
--           - Column "X.value" OR "X.label" also exists
--           - Action: Drop "X" and keep "X.value" and "X.label"
--
-- Usage:
--   Run this SQL to analyze and drop redundant columns in all derived tables
-- ============================================================================

DO $$
DECLARE
    r RECORD;
    drop_sql TEXT;
    total_dropped INTEGER := 0;
    redundant_columns TEXT[];
BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'STARTING REDUNDANT COLUMN CLEANUP';
    RAISE NOTICE '============================================================';

    -- Loop through all tables in the derived schema
    FOR r IN
        SELECT DISTINCT table_name
        FROM information_schema.tables
        WHERE table_schema = 'derived'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    LOOP
        RAISE NOTICE '';
        RAISE NOTICE 'Checking table: %', r.table_name;

        -- Build a list of redundant columns for this table
        -- CAST to TEXT to ensure compatibility across PostgreSQL versions
        redundant_columns := ARRAY(
            SELECT base.column_name::TEXT
            FROM information_schema.columns base
            WHERE base.table_schema = 'derived'
            AND base.table_name = r.table_name
            AND NOT base.column_name LIKE '%.value'
            AND NOT base.column_name LIKE '%.label'
            AND base.column_name NOT IN (
                -- Core system columns
                'uid', 'unique_key', 'facility', 'script', 'scriptId',
                'hospital', 'country', 'version', 'started_at', 'completed_at',
                'time_spent', 'episode', 'transformed', 'DEDUPLICATER',
                -- Derived columns without .value/.label pairs (intentional)
                'AgeCategory', 'BCReturnTime', 'BCType', 'matageyrs',
                -- Legacy columns (kept for backwards compatibility)
                'LBWBinary', 'BirthWeightCategory'
            )
            AND EXISTS (
                SELECT 1
                FROM information_schema.columns paired
                WHERE paired.table_schema = 'derived'
                AND paired.table_name = r.table_name
                AND (
                    paired.column_name = base.column_name || '.value'
                    OR paired.column_name = base.column_name || '.label'
                )
            )
        );

        -- If redundant columns found, drop them
        IF array_length(redundant_columns, 1) > 0 THEN
            RAISE NOTICE '  Found % redundant column(s): %',
                array_length(redundant_columns, 1),
                array_to_string(redundant_columns, ', ');

            -- Build DROP COLUMN statement
            drop_sql := 'ALTER TABLE derived.' || quote_ident(r.table_name);

            FOR i IN 1..array_length(redundant_columns, 1) LOOP
                IF i = 1 THEN
                    drop_sql := drop_sql || ' DROP COLUMN IF EXISTS ' || quote_ident(redundant_columns[i]);
                ELSE
                    drop_sql := drop_sql || ', DROP COLUMN IF EXISTS ' || quote_ident(redundant_columns[i]);
                END IF;
            END LOOP;

            -- Execute the drop
            EXECUTE drop_sql;

            RAISE NOTICE '  ✓ Dropped % redundant column(s) from %',
                array_length(redundant_columns, 1),
                r.table_name;

            total_dropped := total_dropped + array_length(redundant_columns, 1);
        ELSE
            RAISE NOTICE '  ✓ No redundant columns found';
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'CLEANUP COMPLETE';
    RAISE NOTICE 'Total redundant columns dropped: %', total_dropped;
    RAISE NOTICE '============================================================';
END $$;


-- ============================================================================
-- DRY RUN VERSION (Reports without dropping)
-- ============================================================================
-- Uncomment and run this to see what would be dropped WITHOUT actually dropping:
/*
DO $$
DECLARE
    r RECORD;
    redundant_columns TEXT[];
    total_redundant INTEGER := 0;
BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'REDUNDANT COLUMN REPORT (DRY RUN)';
    RAISE NOTICE '============================================================';

    FOR r IN
        SELECT DISTINCT table_name
        FROM information_schema.tables
        WHERE table_schema = 'derived'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    LOOP
        redundant_columns := ARRAY(
            SELECT base.column_name::TEXT
            FROM information_schema.columns base
            WHERE base.table_schema = 'derived'
            AND base.table_name = r.table_name
            AND NOT base.column_name LIKE '%.value'
            AND NOT base.column_name LIKE '%.label'
            AND base.column_name NOT IN (
                -- Core system columns
                'uid', 'unique_key', 'facility', 'script', 'scriptId',
                'hospital', 'country', 'version', 'started_at', 'completed_at',
                'time_spent', 'episode', 'transformed', 'DEDUPLICATER',
                -- Derived columns without .value/.label pairs (intentional)
                'AgeCategory', 'BCReturnTime', 'BCType', 'matageyrs',
                -- Legacy columns (kept for backwards compatibility)
                'LBWBinary', 'BirthWeightCategory'
            )
            AND EXISTS (
                SELECT 1
                FROM information_schema.columns paired
                WHERE paired.table_schema = 'derived'
                AND paired.table_name = r.table_name
                AND (
                    paired.column_name = base.column_name || '.value'
                    OR paired.column_name = base.column_name || '.label'
                )
            )
        );

        IF array_length(redundant_columns, 1) > 0 THEN
            RAISE NOTICE '';
            RAISE NOTICE 'Table: %', r.table_name;
            RAISE NOTICE '  Redundant columns (%): %',
                array_length(redundant_columns, 1),
                array_to_string(redundant_columns, ', ');

            total_redundant := total_redundant + array_length(redundant_columns, 1);
        END IF;
    END LOOP;

    RAISE NOTICE '';
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Total redundant columns found: %', total_redundant;
    RAISE NOTICE '============================================================';
END $$;
*/
