from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[4]
ASSORTED_QUERIES = (
    PROJECT_ROOT
    / "src"
    / "data_pipeline"
    / "pipelines"
    / "data_engineering"
    / "queries"
    / "assorted_queries.py"
)
TIDY_DYNAMIC_TABLES = (
    PROJECT_ROOT
    / "src"
    / "data_pipeline"
    / "pipelines"
    / "data_engineering"
    / "data_tyding"
    / "tidy_dynamic_tables.py"
)
CREATE_JOINED = (
    PROJECT_ROOT
    / "src"
    / "data_pipeline"
    / "pipelines"
    / "data_engineering"
    / "derive_data"
    / "create_joined_table_and_derived_columns.py"
)
SQL_FUNCTIONS = PROJECT_ROOT / "conf" / "common" / "sql_functions.py"


def test_multi_review_cleanup_deduplicates_completion_times_to_the_minute():
    source = ASSORTED_QUERIES.read_text()

    assert 'FROM derived."{table}" AS review' in source
    assert "review.ctid AS row_ctid" in source
    assert "DATE_TRUNC('minute', {completed_at})" in source
    assert "PARTITION BY review.uid" in source


def test_multi_review_cleanup_does_not_use_unique_key_as_identity():
    source = ASSORTED_QUERIES.read_text()

    cleanup_start = source.index("def renumber_review_table_query")
    cleanup_end = source.index("def deduplicate_neolab_query")
    cleanup_source = source[cleanup_start:cleanup_end]
    deduplication_source = cleanup_source.split("WITH numbered", 1)[0]

    assert "review.scriptid" in deduplication_source
    assert "review.uid" in deduplication_source
    assert "DATE_TRUNC('minute', {completed_at})" in deduplication_source
    assert "review.unique_key" not in deduplication_source


def test_multi_review_staging_preserves_full_completed_at_timestamp():
    source = ASSORTED_QUERIES.read_text()

    assert "def review_completed_at_expr" in source
    assert "data->>'completed_at'" in source
    assert "::timestamp" in source
    assert "DATE_TRUNC('minute', completed_at)" in source
    assert "completed_at::date AS completed_at" not in source


def test_derived_review_timestamp_prefers_original_completed_time():
    source = ASSORTED_QUERIES.read_text()

    assert "def derived_review_completed_at_expr" in source
    assert "completed_time::text" in source
    assert "completed_at::timestamp" in source


def test_multi_review_queries_do_not_assume_derived_tables_have_an_id_column():
    source = ASSORTED_QUERIES.read_text()

    cleanup_start = source.index("def renumber_review_table_query")
    cleanup_end = source.index("def deduplicate_neolab_query")
    cleanup_source = source[cleanup_start:cleanup_end]

    read_start = source.index("def read_deduplicated_data_query")
    read_end = source.index("elif 'neolab' in destination_table", read_start)
    read_source = source[read_start:read_end]

    assert "review.id" not in cleanup_source
    assert "ds.id" not in read_source
    assert "review.ctid" in cleanup_source
    assert "ds.ctid::text AS source_row_id" in read_source


def test_multi_review_existing_row_check_uses_completed_minute():
    source = ASSORTED_QUERIES.read_text()

    condition_start = source.index("def get_raw_session_dynamic_condition")
    condition_end = source.index("def read_derived_data_query")
    condition_source = source[condition_start:condition_end]

    assert "DATE_TRUNC('minute', {source_completed_at})" in condition_source
    assert "DATE_TRUNC('minute', {destination_completed_at})" in condition_source
    assert "cs.unique_key = ds.unique_key" not in condition_source


def test_incremental_uid_checks_are_case_insensitive():
    source = ASSORTED_QUERIES.read_text()

    assert "UPPER(TRIM(cs.uid::text))=UPPER(TRIM(ds.uid::text))" in source
    assert "UPPER(TRIM(cs.uid::text)) = UPPER(TRIM(ds.uid::text))" in source
    assert "cs.uid=ds.uid" not in source
    assert "cs.uid = ds.uid" not in source


def test_joined_admission_discharge_uid_matching_is_case_insensitive():
    source = CREATE_JOINED.read_text()

    assert 'def join_predicate(' in source
    assert 'if column == "uid":' in source
    assert "UPPER(TRIM({left_col}::text)) = UPPER(TRIM({right_col}::text))" in source
    assert "adm_df[uid_join_col] = adm_df['uid'].astype(str).str.strip().str.upper()" in source
    assert "dis_df[uid_join_col] = dis_df['uid'].astype(str).str.strip().str.upper()" in source
    assert "jn_adm_dis['uid'] = jn_adm_dis['uid'].combine_first(" in source


def test_duplicate_match_resolution_coalesces_columns_before_concat():
    source = CREATE_JOINED.read_text()

    resolver_start = source.index("def resolve_duplicate_matches")
    resolver_end = source.index("def createJoinedDataSet")
    resolver_source = source[resolver_start:resolver_end]

    assert "duplicate admission-discharge match resolution" in resolver_source
    assert "non-duplicate admission-discharge matches" in resolver_source
    assert "resolved duplicate admission-discharge matches" in resolver_source
    assert "pd.concat([non_duplicates, resolved_duplicates]" in resolver_source


def test_joined_dataset_coalesces_merge_suffix_collisions_before_concat():
    source = CREATE_JOINED.read_text()

    create_start = source.index("def createJoinedDataSet")
    create_source = source[create_start:]

    coalesce_position = create_source.index("merged result before match split")
    split_position = create_source.index("right_only_rows =")

    assert coalesce_position < split_position


def test_joined_dataset_routes_overflow_columns_before_add_columns():
    source = CREATE_JOINED.read_text()

    assert "POSTGRES_COLUMN_LIMIT = 1600" in source
    assert (
        "JOINED_TABLE_MAX_COLUMNS = POSTGRES_COLUMN_LIMIT - "
        "JOINED_TABLE_COLUMN_HEADROOM"
    ) in source
    assert "def split_joined_dataframe_for_column_limit" in source
    assert "def write_overflow_columns" in source
    assert 'return f"{table_name}_extra_columns"' in source
    assert "fix_column_limit_error(table_name, schema, auto_rebuild=True)" in source
    assert "Routing %s new column(s) from %s.%s to overflow storage" in source

    create_start = source.index("def createJoinedDataSet")
    create_source = source[create_start:]
    split_position = create_source.index("split_joined_dataframe_for_column_limit(")
    add_position = create_source.index("add_missing_columns(main_jn_adm_dis")

    assert split_position < add_position


def test_joined_dataset_write_path_writes_overflow_columns():
    source = CREATE_JOINED.read_text()

    prepare_start = source.index("def prepare_joined_dataset_for_write")
    prepare_end = source.index("def write_joined_dataset")
    prepare_source = source[prepare_start:prepare_end]

    assert "full_joined_df = joined_df.copy()" in prepare_source
    assert "overflow_columns = split_joined_dataframe_for_column_limit(" in prepare_source
    assert "add_missing_columns(joined_df, table_name, schema)" in prepare_source

    write_start = source.index("def write_joined_dataset")
    write_end = source.index("def build_reconciled_one_sided_matches")
    write_source = source[write_start:write_end]

    assert "write_overflow_columns(full_joined_df, overflow_columns" in write_source


def test_all_null_columns_are_pruned_before_schema_expansion():
    sql_source = SQL_FUNCTIONS.read_text()
    joined_source = CREATE_JOINED.read_text()

    assert "def drop_all_null_dataframe_columns" in sql_source
    assert "def drop_all_null_table_columns" in sql_source
    assert "def compact_table_to_reclaim_dropped_columns" in sql_source
    assert "COUNT(*) AS total_count" in sql_source
    assert "AS null_count" in sql_source
    assert "if total_count == 0:" in sql_source
    assert "if null_count != total_count:" in sql_source
    assert "def normalize_column_name_for_safety" in sql_source
    assert "near_duplicate_columns" in sql_source
    assert "whitespace normalization" in sql_source
    assert "Whitespace-normalized sibling" in sql_source
    assert "Confirmed sibling" in sql_source
    assert "Reclaiming column slots" in sql_source
    assert "drop_all_null_table_columns(table_name, schema)" in sql_source
    assert "df, _ = drop_all_null_dataframe_columns(" in sql_source

    assert "drop_all_null_dataframe_columns" in joined_source
    assert "drop_all_null_table_columns" in joined_source


def test_join_table_attempts_peads_join_after_admissions_failure():
    source = CREATE_JOINED.read_text()

    join_start = source.index("def join_table")
    join_end = source.index("def calculate_match_score")
    join_source = source[join_start:join_end]

    assert "join_tasks = [" in join_source
    assert '"joined_table": "joined_admissions_discharges"' in join_source
    assert '"joined_table": "joined_peads_admissions_discharges"' in join_source
    assert "for join_task in join_tasks:" in join_source
    assert "join_errors.append((joined_table, exc))" in join_source
    assert "Failed creating joined table(s)" in join_source


def test_derived_multi_review_filter_does_not_reference_json_data_column():
    source = ASSORTED_QUERIES.read_text()

    condition_start = source.index("def get_raw_session_dynamic_condition")
    condition_end = source.index("def read_all_from_derived_table")
    condition_source = source[condition_start:condition_end]

    assert "def get_derived_dynamic_condition(destination_table)" in condition_source
    assert "derived_review_completed_at_expr('cs')" in condition_source
    derived_start = condition_source.index("def get_derived_dynamic_condition")
    derived_source = condition_source[derived_start:]
    assert "source_completed_at = review_completed_at_expr('cs')" not in derived_source


def test_multi_review_repeatables_use_canonical_parent_rows():
    source = TIDY_DYNAMIC_TABLES.read_text()

    assert "def load_multi_review_repeatables" in source
    assert 'FROM scratch."deduplicated_{script_name}" AS source' in source
    assert 'JOIN derived."{script_name}" AS parent' in source
    assert "parent.scriptid = source.scriptid" in source
    assert "parent.uid = source.uid" in source
    assert "DATE_TRUNC('minute', source.source_completed_at)" in source
    assert "parent.review_number" in source


def test_multi_review_repeatables_select_latest_source_in_each_minute():
    source = TIDY_DYNAMIC_TABLES.read_text()

    assert "WITH ranked_source AS" in source
    assert "ORDER BY source.id DESC, source.ingested_at DESC NULLS LAST" in source
    assert "WHERE source.source_rank = 1" in source
    assert "parent.id = source.id" not in source


def test_multi_review_repeatables_are_loaded_after_parent_repair():
    source = TIDY_DYNAMIC_TABLES.read_text()

    repair_position = source.index("repair_multi_review_table(script)")
    repeatables_position = source.index("process_multi_review_repeatables(script)", repair_position)

    assert repair_position < repeatables_position


def test_multi_review_repeatables_run_when_incremental_parent_query_is_empty():
    source = TIDY_DYNAMIC_TABLES.read_text()

    empty_branch_start = source.index("if script_raw.empty:")
    empty_branch_end = source.index("try:", empty_branch_start)
    empty_branch = source[empty_branch_start:empty_branch_end]

    assert "if is_multi_review_script(script):" in empty_branch
    assert "process_multi_review_repeatables(script)" in empty_branch
