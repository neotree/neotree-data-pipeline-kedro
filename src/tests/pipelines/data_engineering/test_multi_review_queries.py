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


def test_multi_review_existing_row_check_uses_completed_minute():
    source = ASSORTED_QUERIES.read_text()

    condition_start = source.index("def get_dynamic_condition")
    condition_end = source.index("def read_derived_data_query")
    condition_source = source[condition_start:condition_end]

    assert "DATE_TRUNC('minute', {source_completed_at})" in condition_source
    assert "DATE_TRUNC('minute', {destination_completed_at})" in condition_source
    assert "cs.unique_key = ds.unique_key" not in condition_source


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
