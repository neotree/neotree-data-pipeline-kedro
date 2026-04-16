import re
from pathlib import Path


PATTERNS = [
    r"(^|[^A-Za-z0-9])[0-9]{2}[ -]?[0-9]{6,7}[ -]?[A-Za-z][ -]?[0-9]{2}(?=[^A-Za-z0-9]|$)",
    r"(^|[^A-Za-z0-9-])[A-Z][A-Z0-9]{7}(?=[^A-Za-z0-9-]|$)",
    r"(^|[^A-Za-z0-9])\+?265([ -]?[0-9]){9}(?=[^A-Za-z0-9]|$)",
    r"(^|[^A-Za-z0-9])0([ -]?[0-9]){9}(?=[^A-Za-z0-9]|$)",
    r"(^|[^A-Za-z0-9])[89]([ -]?[0-9]){8}(?=[^A-Za-z0-9]|$)",
    r"(^|[^A-Za-z0-9])\+?263([ -]?[0-9]){5,10}(?=[^A-Za-z0-9]|$)",
    r"(^|[^A-Za-z0-9])0([ -]?[0-9]){5,10}(?=[^A-Za-z0-9]|$)",
    r"(^|[^A-Za-z0-9])7[1378]([ -]?[0-9]){7}(?=[^A-Za-z0-9]|$)",
]


def redact(value: str) -> str:
    redacted = value
    for _ in range(10):
        previous = redacted
        for pattern in PATTERNS:
            redacted = re.sub(pattern, r"\1[PII_REMOVED]", redacted)
        if redacted == previous:
            break
    return redacted


def test_redacts_country_specific_phone_number_formats():
    examples = [
        "+265 9 1234 5678",
        "+265991234567",
        "265 991 234 567",
        "265-991-234-567",
        "0991 23 45 67",
        "0991234567",
        "991234567",
        "891 234 567",
        "+263 242 123456",
        "+263242123456",
        "+263 77 123 4567",
        "263-77-123-4567",
        "0771234567",
        "077 123 4567",
        "771234567",
    ]

    for example in examples:
        assert redact(f"phone {example} done") == "phone [PII_REMOVED] done"


def test_redacts_national_id_formats():
    examples = [
        "63-123456-A-12",
        "63123456A12",
        "63 1234567 A 12",
        "631234567A12",
        "631234567a12",
        "63-1234567-A-12",
        "A1B2C3D4",
        "Z9999999",
    ]

    for example in examples:
        assert redact(f"id {example} done") == "id [PII_REMOVED] done"


def test_preserves_hyphenated_neotree_uids():
    examples = [
        "ABDC-1234567",
        "A128-1234567",
        "1234-1234567",
        "0EE2-6244",
    ]

    for example in examples:
        assert redact(example) == example


def test_redacts_multiple_pii_values_in_one_text_value():
    value = "mother 0991234567 father +263 77 123 4567 nrn A1B2C3D4"

    assert redact(value) == "mother [PII_REMOVED] father [PII_REMOVED] nrn [PII_REMOVED]"


def test_preserves_common_non_pii_codes_and_dates():
    examples = [
        "ABCD123",
        "A1B2C3D4-EXTRA",
        "2024-01-31",
        "facility SMCH ward KMC",
        "weight 1234 grams",
        "temperature 36.5",
    ]

    for example in examples:
        assert redact(example) == example


def test_numeric_phone_like_values_are_redactable_by_patterns():
    examples = [
        "265991234567",
        "991234567",
        "263771234567",
        "0771234567",
    ]

    for example in examples:
        assert redact(example) == "[PII_REMOVED]"


def test_sql_scrubber_handles_json_numbers():
    project_root = Path(__file__).parents[4]
    assorted_queries = project_root / "src" / "data_pipeline" / "pipelines" / "data_engineering" / "queries" / "assorted_queries.py"

    source = assorted_queries.read_text()

    assert "WHEN 'number' THEN CASE" in source
    assert "THEN to_jsonb('[PII_REMOVED]'::text)" in source
