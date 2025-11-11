-- Script to normalize clean_* tables to lowercase columns only
-- For clean_* tables, we maintain only lowercase column names:
--   - For boolean, number, date, datetime, text: only "columnname"
--   - For dropdown/single_select_option/period: "columnname" and "columnname_label"
-- This script ONLY works with existing columns and drops unnecessary ones to reduce column count

DO $$
DECLARE
    table_record RECORD;
    column_record RECORD;
    base_column_name TEXT;
    lowercase_column_name TEXT;
    lowercase_label_column_name TEXT;
    has_lowercase_column BOOLEAN;
    update_sql TEXT;
    column_data_type TEXT;
    source_data_type TEXT;
    target_data_type TEXT;
    should_have_label BOOLEAN;
    columns_processed INTEGER := 0;
    columns_dropped INTEGER := 0;
BEGIN
    -- Loop through all tables in the derived schema that start with "clean"
    FOR table_record IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'derived'
        AND table_type = 'BASE TABLE'
        AND table_name LIKE 'clean%'
        ORDER BY table_name
    LOOP
        RAISE NOTICE '========================================';
        RAISE NOTICE 'Processing table: derived.%', table_record.table_name;
        RAISE NOTICE '========================================';

        -- First pass: Handle .value and .label columns
        -- Only copy if lowercase target exists, then drop source
        FOR column_record IN
            SELECT DISTINCT
                regexp_replace(column_name, '\.(value|label)$', '') as base_name
            FROM information_schema.columns
            WHERE table_schema = 'derived'
            AND table_name = table_record.table_name
            AND (column_name LIKE '%.value' OR column_name LIKE '%.label')
            ORDER BY base_name
        LOOP
            base_column_name := column_record.base_name;
            lowercase_column_name := lower(base_column_name);
            lowercase_label_column_name := lower(base_column_name) || '_label';

            -- Check if lowercase version exists
            SELECT EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'derived'
                AND table_name = table_record.table_name
                AND column_name = lowercase_column_name
            ) INTO has_lowercase_column;

            -- If .value column exists and lowercase target exists, copy data then drop
            IF EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'derived'
                AND table_name = table_record.table_name
                AND column_name = base_column_name || '.value'
            ) THEN
                IF has_lowercase_column THEN
                    -- Get target and source types
                    SELECT data_type INTO target_data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'derived'
                    AND table_name = table_record.table_name
                    AND column_name = lowercase_column_name
                    LIMIT 1;

                    SELECT data_type INTO source_data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'derived'
                    AND table_name = table_record.table_name
                    AND column_name = base_column_name || '.value'
                    LIMIT 1;

                    -- Try to copy data with casting
                    BEGIN
                        update_sql := format(
                            'UPDATE derived.%I SET %I = %I::%s WHERE %I IS NULL AND %I IS NOT NULL',
                            table_record.table_name,
                            lowercase_column_name,
                            base_column_name || '.value',
                            target_data_type,
                            lowercase_column_name,
                            base_column_name || '.value'
                        );
                        EXECUTE update_sql;
                        GET DIAGNOSTICS columns_processed = ROW_COUNT;

                        IF columns_processed > 0 THEN
                            RAISE NOTICE 'Updated % rows: % from %.value',
                                columns_processed, lowercase_column_name, base_column_name;
                        END IF;
                    EXCEPTION
                        WHEN OTHERS THEN
                            RAISE WARNING 'Failed to copy %.value to %: %',
                                base_column_name, lowercase_column_name, SQLERRM;
                    END;
                ELSE
                    RAISE NOTICE 'No lowercase column % exists, will drop %.value without copying',
                        lowercase_column_name, base_column_name;
                END IF;

                -- Always drop .value column
                EXECUTE format(
                    'ALTER TABLE derived.%I DROP COLUMN IF EXISTS %I',
                    table_record.table_name,
                    base_column_name || '.value'
                );
                RAISE NOTICE 'Dropped column: %.value', base_column_name;
                columns_dropped := columns_dropped + 1;
            END IF;

            -- If .label column exists, check if we should keep a _label column
            IF EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'derived'
                AND table_name = table_record.table_name
                AND column_name = base_column_name || '.label'
            ) THEN
                -- Determine if field should have label based on lowercase column type
                should_have_label := FALSE;

                IF has_lowercase_column THEN
                    SELECT data_type INTO column_data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'derived'
                    AND table_name = table_record.table_name
                    AND column_name = lowercase_column_name
                    LIMIT 1;

                    should_have_label := COALESCE(
                        column_data_type NOT IN ('boolean', 'integer', 'bigint', 'smallint',
                                                 'numeric', 'decimal', 'real', 'double precision',
                                                 'date', 'timestamp', 'timestamp without time zone',
                                                 'timestamp with time zone', 'time', 'time without time zone'),
                        FALSE
                    );
                END IF;

                -- If should have label and _label column exists, copy data
                IF should_have_label AND EXISTS(
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'derived'
                    AND table_name = table_record.table_name
                    AND column_name = lowercase_label_column_name
                ) THEN
                    update_sql := format(
                        'UPDATE derived.%I SET %I = %I WHERE %I IS NULL AND %I IS NOT NULL',
                        table_record.table_name,
                        lowercase_label_column_name,
                        base_column_name || '.label',
                        lowercase_label_column_name,
                        base_column_name || '.label'
                    );
                    EXECUTE update_sql;
                    GET DIAGNOSTICS columns_processed = ROW_COUNT;

                    IF columns_processed > 0 THEN
                        RAISE NOTICE 'Updated % rows: % from %.label',
                            columns_processed, lowercase_label_column_name, base_column_name;
                    END IF;
                END IF;

                -- Always drop .label column
                EXECUTE format(
                    'ALTER TABLE derived.%I DROP COLUMN IF EXISTS %I',
                    table_record.table_name,
                    base_column_name || '.label'
                );
                RAISE NOTICE 'Dropped column: %.label', base_column_name;
                columns_dropped := columns_dropped + 1;
            END IF;
        END LOOP;

        -- Second pass: Handle mixed-case columns (not .value/.label, not all lowercase)
        FOR column_record IN
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'derived'
            AND table_name = table_record.table_name
            AND column_name NOT LIKE '%.value'
            AND column_name NOT LIKE '%.label'
            AND column_name != lower(column_name)
            AND column_name NOT LIKE '%_label'
            ORDER BY column_name
        LOOP
            base_column_name := column_record.column_name;
            lowercase_column_name := lower(base_column_name);
            source_data_type := column_record.data_type;

            -- Check if lowercase version exists
            SELECT data_type INTO target_data_type
            FROM information_schema.columns
            WHERE table_schema = 'derived'
            AND table_name = table_record.table_name
            AND column_name = lowercase_column_name
            LIMIT 1;

            has_lowercase_column := (target_data_type IS NOT NULL);

            -- If lowercase exists, try to copy data then drop source
            IF has_lowercase_column THEN
                BEGIN
                    update_sql := format(
                        'UPDATE derived.%I SET %I = %I::%s WHERE %I IS NULL AND %I IS NOT NULL',
                        table_record.table_name,
                        lowercase_column_name,
                        base_column_name,
                        target_data_type,
                        lowercase_column_name,
                        base_column_name
                    );
                    EXECUTE update_sql;
                    GET DIAGNOSTICS columns_processed = ROW_COUNT;

                    IF columns_processed > 0 THEN
                        RAISE NOTICE 'Updated % rows: % from %',
                            columns_processed, lowercase_column_name, base_column_name;
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        RAISE WARNING 'Failed to copy % to %: %',
                            base_column_name, lowercase_column_name, SQLERRM;
                END;
            ELSE
                RAISE NOTICE 'No lowercase column % exists, will drop % without copying',
                    lowercase_column_name, base_column_name;
            END IF;

            -- Always drop mixed-case column
            EXECUTE format(
                'ALTER TABLE derived.%I DROP COLUMN IF EXISTS %I',
                table_record.table_name,
                base_column_name
            );
            RAISE NOTICE 'Dropped column: %', base_column_name;
            columns_dropped := columns_dropped + 1;
        END LOOP;

        -- Third pass: Handle mixed-case _label columns
        FOR column_record IN
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'derived'
            AND table_name = table_record.table_name
            AND column_name LIKE '%_label'
            AND column_name != lower(column_name)
            ORDER BY column_name
        LOOP
            base_column_name := column_record.column_name;
            lowercase_label_column_name := lower(base_column_name);
            lowercase_column_name := regexp_replace(lowercase_label_column_name, '_label$', '');

            -- Get the data type of the base column to determine if label should exist
            SELECT data_type INTO column_data_type
            FROM information_schema.columns
            WHERE table_schema = 'derived'
            AND table_name = table_record.table_name
            AND column_name = lowercase_column_name
            LIMIT 1;

            should_have_label := COALESCE(
                column_data_type NOT IN ('boolean', 'integer', 'bigint', 'smallint',
                                         'numeric', 'decimal', 'real', 'double precision',
                                         'date', 'timestamp', 'timestamp without time zone',
                                         'timestamp with time zone', 'time', 'time without time zone'),
                FALSE
            );

            -- If should have label and lowercase _label exists, copy data
            IF should_have_label AND EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'derived'
                AND table_name = table_record.table_name
                AND column_name = lowercase_label_column_name
            ) THEN
                update_sql := format(
                    'UPDATE derived.%I SET %I = %I WHERE %I IS NULL AND %I IS NOT NULL',
                    table_record.table_name,
                    lowercase_label_column_name,
                    base_column_name,
                    lowercase_label_column_name,
                    base_column_name
                );
                EXECUTE update_sql;
                GET DIAGNOSTICS columns_processed = ROW_COUNT;

                IF columns_processed > 0 THEN
                    RAISE NOTICE 'Updated % rows: % from %',
                        columns_processed, lowercase_label_column_name, base_column_name;
                END IF;
            ELSE
                RAISE NOTICE 'No lowercase label % exists or type does not require labels, will drop % without copying',
                    lowercase_label_column_name, base_column_name;
            END IF;

            -- Always drop mixed-case _label column
            EXECUTE format(
                'ALTER TABLE derived.%I DROP COLUMN IF EXISTS %I',
                table_record.table_name,
                base_column_name
            );
            RAISE NOTICE 'Dropped label column: %', base_column_name;
            columns_dropped := columns_dropped + 1;
        END LOOP;

        -- Fourth pass: Drop lowercase _label columns for fields that shouldn't have them
        FOR column_record IN
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'derived'
            AND table_name = table_record.table_name
            AND column_name LIKE '%_label'
            AND column_name = lower(column_name)
            ORDER BY column_name
        LOOP
            lowercase_label_column_name := column_record.column_name;
            lowercase_column_name := regexp_replace(lowercase_label_column_name, '_label$', '');

            -- Get the data type of the base column
            SELECT data_type INTO column_data_type
            FROM information_schema.columns
            WHERE table_schema = 'derived'
            AND table_name = table_record.table_name
            AND column_name = lowercase_column_name
            LIMIT 1;

            should_have_label := TRUE;

            IF column_data_type IS NOT NULL THEN
                should_have_label := column_data_type NOT IN (
                    'boolean', 'integer', 'bigint', 'smallint',
                    'numeric', 'decimal', 'real', 'double precision',
                    'date', 'timestamp', 'timestamp without time zone',
                    'timestamp with time zone', 'time', 'time without time zone'
                );
            END IF;

            -- Drop if field type doesn't support labels
            IF NOT should_have_label THEN
                EXECUTE format(
                    'ALTER TABLE derived.%I DROP COLUMN IF EXISTS %I',
                    table_record.table_name,
                    lowercase_label_column_name
                );
                RAISE NOTICE 'Dropped unnecessary label column: % (field type: % does not require labels)',
                    lowercase_label_column_name, column_data_type;
                columns_dropped := columns_dropped + 1;
            END IF;
        END LOOP;

        RAISE NOTICE 'Completed table: %', table_record.table_name;
        RAISE NOTICE '';
    END LOOP;

    RAISE NOTICE '========================================';
    RAISE NOTICE 'Complete! Dropped % columns total', columns_dropped;
    RAISE NOTICE '========================================';
END $$;
