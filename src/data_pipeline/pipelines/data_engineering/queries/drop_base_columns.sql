-- Script to drop base columns when .value columns exist
-- Drops columns without .value or .label extension if a .value column exists
-- Runs on all tables in the derived schema, excluding tables starting with "clean"

DO $$
DECLARE
    table_record RECORD;
    column_record RECORD;
    base_column_name TEXT;
    has_value_column BOOLEAN;
    drop_sql TEXT;
    columns_dropped INTEGER := 0;
BEGIN
    -- Loop through all tables in the derived schema that don't start with "clean"
    FOR table_record IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'derived'
        AND table_type = 'BASE TABLE'
        AND table_name NOT LIKE 'clean%'
        ORDER BY table_name
    LOOP
        RAISE NOTICE 'Processing table: derived.%', table_record.table_name;

        -- Loop through all columns in the current table
        FOR column_record IN
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'derived'
            AND table_name = table_record.table_name
            AND column_name NOT LIKE '%.value'
            AND column_name NOT LIKE '%.label'
            ORDER BY column_name
        LOOP
            -- Check if there's a corresponding .value column
            base_column_name := column_record.column_name;

            SELECT EXISTS(
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'derived'
                AND table_name = table_record.table_name
                AND column_name = base_column_name || '.value'
            ) INTO has_value_column;

            -- If .value column exists, drop the base column
            IF has_value_column THEN
                drop_sql := format(
                    'ALTER TABLE derived.%I DROP COLUMN IF EXISTS %I',
                    table_record.table_name,
                    base_column_name
                );

                RAISE NOTICE 'Dropping column: %.% (has %.value)',
                    table_record.table_name,
                    base_column_name,
                    base_column_name;

                EXECUTE drop_sql;
                columns_dropped := columns_dropped + 1;
            END IF;
        END LOOP;
    END LOOP;

    RAISE NOTICE 'Complete! Dropped % columns total', columns_dropped;
END $$;
