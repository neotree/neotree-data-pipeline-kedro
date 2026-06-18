from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[4]
DATA_TYDING = (
    PROJECT_ROOT
    / "src"
    / "data_pipeline"
    / "pipelines"
    / "data_engineering"
    / "data_tyding"
)
VALIDATE = (
    PROJECT_ROOT
    / "src"
    / "data_pipeline"
    / "pipelines"
    / "data_engineering"
    / "data_validation"
    / "validate.py"
)


def test_regenerate_unique_key_keeps_dataframe_interface():
    source = (DATA_TYDING / "regenerate_unique_key.py").read_text()

    assert 'pd.DataFrame.from_dict(raw_data, orient="index")' in source
    assert "raw_data = list(raw_data.items())" not in source


def test_feed_assessment_rename_requires_source_and_missing_destination():
    source = (DATA_TYDING / "tidy_dynamic_tables.py").read_text()

    assert '"FeedAsse.label" in df.columns' in source
    assert '"How is the baby being fed?.label" not in df.columns' in source
    assert '"FeedAsse.value" in df.columns' in source
    assert '"How is the baby being fed?.value" not in df.columns' in source


def test_dynamic_script_processing_reports_failure_status():
    source = (DATA_TYDING / "tidy_dynamic_tables.py").read_text()

    assert "def process_single_script(script: str) -> bool:" in source
    assert "if script_raw is None:" in source
    assert "dataset '%s' failed to load" in source
    assert "if process_single_script(script):" in source
    assert "error_count += 1" in source


def test_age_update_requires_target_column_and_avoids_chained_assignment():
    source = (
        DATA_TYDING / "tidy_admissions_discharges_and_create_mcl_tables.py"
    ).read_text()

    assert '"Age.value",' in source
    assert "birth_dates.loc[mask_fix]" in source
    assert "birth_dates[mask_fix] =" not in source


def test_validation_is_finalized_on_early_pipeline_returns():
    source = (
        DATA_TYDING / "tidy_admissions_discharges_and_create_mcl_tables.py"
    ).read_text()

    assert source.count("finalize_validation()") >= 3


def test_outlier_failures_are_logged_instead_of_silenced():
    source = VALIDATE.read_text()

    assert "Could not evaluate numeric outliers" in source


def test_duplicate_columns_are_coalesced_before_validation():
    source = (
        DATA_TYDING / "tidy_admissions_discharges_and_create_mcl_tables.py"
    ).read_text()

    assert "def coalesce_duplicate_columns" in source
    assert 'coalesce_duplicate_columns(dis_df, "discharges normalization")' in source
    assert 'coalesce_duplicate_columns(dis_df, "discharges derived columns")' in source
    assert "duplicate_values.bfill(axis=1).iloc[:, 0]" in source


def test_repeatable_backfill_cannot_block_parent_table_creation():
    source = (DATA_TYDING / "tidy_dynamic_tables.py").read_text()

    assert "if not table_exists('derived', script_name):" in source
    assert "Skipping %s repeatable backfill until derived.%s exists" in source
