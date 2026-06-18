from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[4]
FIELD_INFO = (
    PROJECT_ROOT
    / "src"
    / "data_pipeline"
    / "pipelines"
    / "data_engineering"
    / "utils"
    / "field_info.py"
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


def test_field_metadata_preserves_screen_and_field_conditions():
    source = FIELD_INFO.read_text()

    assert 'screen_condition = screen.get("condition", "")' in source
    assert 'field_condition = field.get("condition", "")' in source
    assert '"screenCondition": screen_condition' in source
    assert '"fieldCondition": field_condition' in source
    assert '"visibilityConditions": [visibility_rule]' in source


def test_required_validation_excludes_confidential_fields():
    source = VALIDATE.read_text()

    assert "def _is_confidential(field: dict)" in source
    assert "if not is_optional and not is_confidential:" in source


def test_required_validation_combines_screen_and_field_conditions():
    source = VALIDATE.read_text()

    assert "if not condition or not isinstance(condition, str):" in source
    assert "if not expr:" in source
    assert "return pd.Series(True, index=df.index)" in source
    assert "def _field_visibility_mask(field: dict)" in source
    assert 'rule.get("screenCondition", "")' in source
    assert 'rule.get("fieldCondition", "")' in source
    assert "screen_mask & field_mask" in source
    assert "null_mask = visibility_mask & temp_series.isna()" in source


def test_high_null_analysis_is_condition_and_confidentiality_aware():
    source = VALIDATE.read_text()

    assert "if _is_confidential(field):" in source
    assert "eligible_mask = _field_visibility_mask(field)" in source
    assert "df.loc[eligible_mask, col].isna().sum()" in source
    assert '"eligible_records": eligible_count' in source


def test_range_validation_records_actual_values_and_record_identity():
    source = VALIDATE.read_text()

    assert '"recorded_value": val' in source
    assert '"unique_key": unique_key' in source
    assert '"row_index": idx' in source
    assert '"reason": error_msg' in source
    assert 'actual_value_sample=", ".join(' in source


def test_range_validation_ignores_empty_bounds_and_rejects_invalid_dates():
    source = VALIDATE.read_text()

    assert "str(min_val).strip() != '' else None" in source
    assert "str(max_val).strip() != '' else None" in source
    assert "if pd.isna(value_dt):" in source
    assert "is not a valid {data_type}" in source
    assert "(has_min or has_max) and not is_confidential" in source
    assert "non_null_mask = visibility_mask & df[value_col].notna()" in source
