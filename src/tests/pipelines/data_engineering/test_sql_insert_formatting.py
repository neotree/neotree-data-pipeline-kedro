import sys

if not any(arg.startswith("--env=") for arg in sys.argv):
    sys.argv.append("--env=dev")

from conf.common.sql_functions import format_insert_value_for_column


def test_date_like_value_for_double_precision_column_becomes_null():
    value = format_insert_value_for_column(
        "2026-05-09 11:00:00",
        "admissionweight",
        "double precision",
    )

    assert value == "NULL"


def test_numeric_value_for_double_precision_column_is_preserved():
    value = format_insert_value_for_column("36.5", "temperature", "double precision")

    assert value == "36.5"


def test_timestamp_value_for_timestamp_column_is_preserved():
    value = format_insert_value_for_column(
        "2026-05-09T11:00:00",
        "datetimeadmission",
        "timestamp without time zone",
    )

    assert value == "'2026-05-09 11:00:00'"
