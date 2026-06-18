from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[4]
DATA_FIX = (
    PROJECT_ROOT
    / "src"
    / "data_pipeline"
    / "pipelines"
    / "data_engineering"
    / "queries"
    / "data_fix.py"
)


def test_clean_session_date_fix_checks_for_label_column_before_updating():
    source = DATA_FIX.read_text()

    assert "destination_columns_query" in source
    assert "if label_col in destination_columns:" in source
    assert "label_assignment" in source
    assert 'SET "{dest_col}" = f.date_val' in source
