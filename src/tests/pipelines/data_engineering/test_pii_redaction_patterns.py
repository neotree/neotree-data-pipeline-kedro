import re
import json
from pathlib import Path


PATTERNS = [
    r"(^|[^A-Za-z0-9])[0-9]{2}[ -]?[0-9]{6,7}[ -]?[A-Za-z][ -]?[0-9]{2}(?=[^A-Za-z0-9]|$)",
    r"(^|[^A-Za-z0-9-])[A-Z][A-Z0-9]{7}(?=[^A-Za-z0-9-]|$)",
    r"(^|[^A-Za-z0-9])\+?265([ -]?[0-9]){9}(?=[^A-Za-z0-9]|$)",
    r"(^|[^A-Za-z0-9])0[89]([ -]?[0-9]){8}(?=[^A-Za-z0-9]|$)",
    r"(^|[^A-Za-z0-9])[89]([ -]?[0-9]){8}(?=[^A-Za-z0-9]|$)",
    r"(^|[^A-Za-z0-9])\+?263([ -]?[0-9]){5,10}(?=[^A-Za-z0-9]|$)",
    r"(^|[^A-Za-z0-9])07[1378]([ -]?[0-9]){7}(?=[^A-Za-z0-9]|$)",
    r"(^|[^A-Za-z0-9])7[1378]([ -]?[0-9]){7}(?=[^A-Za-z0-9]|$)",
]


SNAPSHOT_PAYLOAD = {
    "id": 102,
    "uid": "CDB0-4190159",
    "appEnv": "PROD",
    "script": {
        "id": "88e3dfc6-218c-4d72-8d24-190acc31a77f",
        "type": "admission",
        "title": "Generic PHC Admission",
    },
    "country": "mwi",
    "entries": {
        "PHC": {
            "type": "dropdown",
            "values": {"label": ["Chamwabvi Health Centre"], "value": ["CHV"]},
            "comments": [],
            "prePopulate": ["admissionSearches", "twinSearches"],
        },
        "NUID": {
            "type": "text",
            "values": {"label": [None], "value": ["CDB0-4190159"]},
            "comments": [],
            "prePopulate": ["admissionSearches"],
        },
        "Cadre": {
            "type": "dropdown",
            "values": {
                "label": ["NMT - Nurse Midwife Technician"],
                "value": ["NMT"],
            },
            "comments": [],
            "prePopulate": [],
        },
        "Gender": {
            "type": "dropdown",
            "values": {"label": ["Female"], "value": ["F"]},
            "comments": [],
            "prePopulate": [],
        },
        "HCWSig": {
            "type": "dropdown",
            "values": {"label": ["Yasceen Saidi"], "value": ["YasSai"]},
            "comments": [],
            "prePopulate": [],
        },
        "WBOrigin": {
            "type": "dropdown",
            "values": {"label": ["Primary Health Centre"], "value": ["PHC"]},
            "comments": [],
            "prePopulate": [],
        },
        "Gestation": {
            "type": "number",
            "values": {"label": ["38"], "value": ["38"]},
            "comments": [],
            "prePopulate": [],
        },
        "WellvsSick": {
            "type": "set<id>",
            "values": {"label": ["None (well baby)"], "value": ["Norm"]},
            "comments": [],
            "prePopulate": [],
        },
        "BirthWeight": {
            "type": "number",
            "values": {"label": ["3000"], "value": ["3000"]},
            "comments": [],
            "prePopulate": [],
        },
        "Temperature": {
            "type": "number",
            "values": {"label": ["37.4"], "value": ["37.4"]},
            "comments": [],
            "prePopulate": [],
        },
        "repeatables": {},
        "ModeDelivery": {
            "type": "dropdown",
            "values": {"label": ["Spontaneous Vaginal Delivery (SVD)"], "value": ["1"]},
            "comments": [],
            "prePopulate": [],
        },
        "NeoTreeOutcome": {
            "type": "single_select",
            "values": {"label": ["Admit to Post Natal Ward"], "value": ["AdmPNW"]},
            "comments": [],
            "prePopulate": [],
        },
        "EndScriptDatetime": {
            "type": "date",
            "values": {"label": ["2026-04-28T23:15"], "value": ["2026-04-28T23:15"]},
            "comments": [],
            "prePopulate": [],
        },
    },
    "app_mode": "production",
    "exported": 0,
    "diagnoses": [],
    "appVersion": "2.5.22",
    "started_at": "2026-04-28T23:13:47.379Z",
    "unique_key": "ljpbu9nfs0ro8lcph5wcp1mbgp9yc1v5",
    "canceled_at": None,
    "hospital_id": "d9e0c601-97de-4a7a-a944-8ae382d30a8d",
    "scriptTitle": "88e3dfc6-218c-4d72-8d24-190acc31a77f",
    "completed_at": "2026-04-28T23:15:14.501Z",
    "local_export": 0,
    "scriptVersion": 625,
}


def redact(value: str) -> str:
    redacted = value
    for _ in range(10):
        previous = redacted
        for pattern in PATTERNS:
            redacted = re.sub(pattern, r"\1[PII_REMOVED]", redacted)
        if redacted == previous:
            break
    return redacted


def walk_scalar_strings(value):
    if isinstance(value, dict):
        for nested_value in value.values():
            yield from walk_scalar_strings(nested_value)
    elif isinstance(value, list):
        for nested_value in value:
            yield from walk_scalar_strings(nested_value)
    elif isinstance(value, str):
        yield value


def scrub_entries_only(value):
    if isinstance(value, dict):
        return {key: scrub_entries_only(nested_value) for key, nested_value in value.items()}
    if isinstance(value, list):
        return [scrub_entries_only(nested_value) for nested_value in value]
    if isinstance(value, str):
        return redact(value)
    return value


def scrub_payload_entries_only(payload):
    scrubbed = json.loads(json.dumps(payload))
    if isinstance(scrubbed, dict) and "entries" in scrubbed:
        scrubbed["entries"] = scrub_entries_only(scrubbed["entries"])
    return scrubbed


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


def test_redacts_phone_numbers_with_punctuation_and_wrappers():
    examples = [
        "(0991234567)",
        "[+265 991 234 567]",
        "tel:+263771234567",
        "call 077-123-4567 now",
        "mother=0991-234-567",
        "contact:+265-991-234-567;",
    ]

    expected = [
        "([PII_REMOVED])",
        "[[PII_REMOVED]]",
        "tel:[PII_REMOVED]",
        "call [PII_REMOVED] now",
        "mother=[PII_REMOVED]",
        "contact:[PII_REMOVED];",
    ]

    for example, redacted_value in zip(examples, expected):
        assert redact(example) == redacted_value


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


def test_redacts_ids_with_surrounding_punctuation():
    examples = [
        "(63-123456-A-12)",
        "[A1B2C3D4]",
        "nrn=A1B2C3D4;",
        "zim_id:63-1234567-A-12",
    ]

    expected = [
        "([PII_REMOVED])",
        "[[PII_REMOVED]]",
        "nrn=[PII_REMOVED];",
        "zim_id:[PII_REMOVED]",
    ]

    for example, redacted_value in zip(examples, expected):
        assert redact(example) == redacted_value


def test_preserves_hyphenated_neotree_uids():
    examples = [
        "ABDC-1234567",
        "A128-1234567",
        "1234-1234567",
        "0EE2-6244",
    ]

    for example in examples:
        assert redact(example) == example


def test_preserves_common_uuid_and_key_shapes():
    examples = [
        "88e3dfc6-218c-4d72-8d24-190acc31a77f",
        "d9e0c601-97de-4a7a-a944-8ae382d30a8d",
        "ljpbu9nfs0ro8lcph5wcp1mbgp9yc1v5",
        "f715e123-3cd0-49ac-8e45-45ab5db72942",
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
        "2026-04-27 15:19",
        "2026-04-27 15:58",
        "2026-04-27 15:05",
        "2026-04-27T15:19:58",
        "2026-04-27T15:19:58Z",
        "2026-04-27 15:19:58+02:00",
        "2026-04-27 15:19:58-03:00",
        "2026/04/27 15:19",
        "27-04-2026 15:19",
        "2026-04-27 15:19:58.123",
        "2026-04-27T23:15",
        "2026-04-28T23:15",
        "2026-04-29T00:05",
        "facility SMCH ward KMC",
        "weight 1234 grams",
        "temperature 36.5",
        "2.5.22",
        "production",
        "PROD",
        "CHV",
        "NMT",
        "AdmPNW",
        "YasSai",
        "Female",
        "Primary Health Centre",
    ]

    for example in examples:
        assert redact(example) == example


def test_preserves_full_timestamp_minute_range():
    for minute in range(60):
        value = f"2026-04-27 15:{minute:02d}"
        assert redact(value) == value


def test_preserves_mixed_timestamp_text_when_no_pii_is_present():
    examples = [
        "DateTimeAdmission 2026-04-27 15:19",
        "completed_at=2026-04-27T15:19:58Z",
        "reviewed on 2026-04-27 15:05 by nurse",
        "dob 2024-01-31 weight 1234 grams temp 36.5",
        "started_at 2026-04-28T23:13:47.379Z completed_at 2026-04-28T23:15:14.501Z",
        "EndScriptDatetime 2026-04-28T23:15 appVersion 2.5.22",
    ]

    for example in examples:
        assert redact(example) == example


def test_redacts_phone_but_preserves_timestamp_in_same_text():
    value = "completed_at 2026-04-27 15:19 mother phone 0991234567"
    assert redact(value) == "completed_at 2026-04-27 15:19 mother phone [PII_REMOVED]"


def test_redacts_multiple_phone_numbers_but_preserves_other_codes():
    value = "call 0991234567 or +263 77 123 4567 from CHV at 2026-04-28T23:15"
    assert redact(value) == "call [PII_REMOVED] or [PII_REMOVED] from CHV at 2026-04-28T23:15"


def test_snapshot_scalar_values_are_preserved_when_not_pii():
    for value in walk_scalar_strings(SNAPSHOT_PAYLOAD):
        assert redact(value) == value


def test_snapshot_json_serialization_is_preserved_when_not_pii():
    serialized = json.dumps(SNAPSHOT_PAYLOAD, sort_keys=True)
    assert redact(serialized) == serialized


def test_snapshot_timestamp_fields_are_preserved():
    safe_values = [
        SNAPSHOT_PAYLOAD["started_at"],
        SNAPSHOT_PAYLOAD["completed_at"],
        SNAPSHOT_PAYLOAD["entries"]["EndScriptDatetime"]["values"]["label"][0],
        SNAPSHOT_PAYLOAD["entries"]["EndScriptDatetime"]["values"]["value"][0],
    ]

    for value in safe_values:
        assert redact(value) == value


def test_snapshot_identifiers_and_metadata_are_preserved():
    safe_values = [
        SNAPSHOT_PAYLOAD["uid"],
        SNAPSHOT_PAYLOAD["script"]["id"],
        SNAPSHOT_PAYLOAD["hospital_id"],
        SNAPSHOT_PAYLOAD["scriptTitle"],
        SNAPSHOT_PAYLOAD["unique_key"],
        SNAPSHOT_PAYLOAD["appVersion"],
        SNAPSHOT_PAYLOAD["entries"]["PHC"]["values"]["value"][0],
        SNAPSHOT_PAYLOAD["entries"]["HCWSig"]["values"]["value"][0],
        SNAPSHOT_PAYLOAD["entries"]["NeoTreeOutcome"]["values"]["value"][0],
    ]

    for value in safe_values:
        assert redact(value) == value


def test_snapshot_numeric_and_measurement_values_are_preserved():
    safe_values = [
        SNAPSHOT_PAYLOAD["entries"]["Gestation"]["values"]["label"][0],
        SNAPSHOT_PAYLOAD["entries"]["Gestation"]["values"]["value"][0],
        SNAPSHOT_PAYLOAD["entries"]["BirthWeight"]["values"]["label"][0],
        SNAPSHOT_PAYLOAD["entries"]["BirthWeight"]["values"]["value"][0],
        SNAPSHOT_PAYLOAD["entries"]["Temperature"]["values"]["label"][0],
        SNAPSHOT_PAYLOAD["entries"]["Temperature"]["values"]["value"][0],
        str(SNAPSHOT_PAYLOAD["scriptVersion"]),
        str(SNAPSHOT_PAYLOAD["exported"]),
        str(SNAPSHOT_PAYLOAD["local_export"]),
    ]

    for value in safe_values:
        assert redact(value) == value


def test_timestamp_neighborhood_samples_are_preserved():
    examples = [
        "2026-04-28T23:13",
        "2026-04-28T23:14",
        "2026-04-28T23:15",
        "2026-04-28T23:16",
        "2026-04-28T23:17",
        "2026-04-28T23:15:14.501Z",
        "2026-04-28T23:13:47.379Z",
        "2026-04-28 23:15",
        "2026-04-28 23:13",
    ]

    for example in examples:
        assert redact(example) == example


def test_preserves_full_hour_minute_sweep_for_snapshot_date():
    for hour in range(24):
        for minute in range(60):
            compact = f"2026-04-28T{hour:02d}:{minute:02d}"
            spaced = f"2026-04-28 {hour:02d}:{minute:02d}"
            assert redact(compact) == compact
            assert redact(spaced) == spaced


def test_preserves_common_numeric_and_code_sequences():
    examples = [
        "3000",
        "37.4",
        "38",
        "625",
        "0",
        "1",
        "CHV",
        "NMT",
        "Norm",
        "PHC",
        "F",
        "mwi",
    ]

    for example in examples:
        assert redact(example) == example


def test_preserves_snapshot_like_json_fragments():
    examples = [
        '{"started_at":"2026-04-28T23:13:47.379Z","completed_at":"2026-04-28T23:15:14.501Z"}',
        '{"unique_key":"ljpbu9nfs0ro8lcph5wcp1mbgp9yc1v5","appVersion":"2.5.22"}',
        '{"value":["2026-04-28T23:15"],"label":["2026-04-28T23:15"]}',
        '{"value":["CHV"],"label":["Chamwabvi Health Centre"]}',
    ]

    for example in examples:
        assert redact(example) == example


def test_redacts_snapshot_like_json_fragments_when_they_contain_pii():
    examples = [
        '{"value":["0991234567"],"label":["0991234567"]}',
        '{"value":["A1B2C3D4"],"label":["Malawi ID"]}',
        '{"contact":"+263 77 123 4567","timestamp":"2026-04-28T23:15:14.501Z"}',
    ]

    expected = [
        '{"value":["[PII_REMOVED]"],"label":["[PII_REMOVED]"]}',
        '{"value":["[PII_REMOVED]"],"label":["Malawi ID"]}',
        '{"contact":"[PII_REMOVED]","timestamp":"2026-04-28T23:15:14.501Z"}',
    ]

    for example, redacted_value in zip(examples, expected):
        assert redact(example) == redacted_value


def test_preserves_words_that_share_partial_shapes_with_pii():
    examples = [
        "admission",
        "production",
        "AdmissionSearches",
        "twinSearches",
        "Spontaneous Vaginal Delivery (SVD)",
        "Nurse Midwife Technician",
        "Chamwabvi Health Centre",
    ]

    for example in examples:
        assert redact(example) == example


def test_payload_scoped_scrubber_only_redacts_entries_content():
    payload = json.loads(json.dumps(SNAPSHOT_PAYLOAD))
    payload["entries"]["GuardianPhone"] = {
        "type": "text",
        "values": {"label": ["0991234567"], "value": ["0991234567"]},
        "comments": [],
        "prePopulate": [],
    }
    payload["completed_at"] = "2026-04-28T23:15:14.501Z"
    payload["started_at"] = "2026-04-28T23:13:47.379Z"

    scrubbed = scrub_payload_entries_only(payload)

    assert scrubbed["entries"]["GuardianPhone"]["values"]["label"][0] == "[PII_REMOVED]"
    assert scrubbed["entries"]["GuardianPhone"]["values"]["value"][0] == "[PII_REMOVED]"
    assert scrubbed["completed_at"] == "2026-04-28T23:15:14.501Z"
    assert scrubbed["started_at"] == "2026-04-28T23:13:47.379Z"


def test_payload_scoped_scrubber_preserves_top_level_phone_like_metadata():
    payload = json.loads(json.dumps(SNAPSHOT_PAYLOAD))
    payload["support_contact"] = "0991234567"
    payload["scriptTitle"] = "88e3dfc6-218c-4d72-8d24-190acc31a77f"

    scrubbed = scrub_payload_entries_only(payload)

    assert scrubbed["support_contact"] == "0991234567"
    assert scrubbed["scriptTitle"] == "88e3dfc6-218c-4d72-8d24-190acc31a77f"


def test_payload_scoped_scrubber_redacts_nested_repeatables_inside_entries():
    payload = json.loads(json.dumps(SNAPSHOT_PAYLOAD))
    payload["entries"]["repeatables"] = {
        "contacts": [
            {"phone": "0991234567", "timestamp": "2026-04-28T23:15"},
            {"id_number": "A1B2C3D4", "note": "reviewed 2026-04-28T23:16"},
        ]
    }

    scrubbed = scrub_payload_entries_only(payload)

    assert scrubbed["entries"]["repeatables"]["contacts"][0]["phone"] == "[PII_REMOVED]"
    assert scrubbed["entries"]["repeatables"]["contacts"][0]["timestamp"] == "2026-04-28T23:15"
    assert scrubbed["entries"]["repeatables"]["contacts"][1]["id_number"] == "[PII_REMOVED]"
    assert scrubbed["entries"]["repeatables"]["contacts"][1]["note"] == "reviewed 2026-04-28T23:16"


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


def test_sql_scrubber_is_incremental_with_pii_flag():
    project_root = Path(__file__).parents[4]
    assorted_queries = project_root / "src" / "data_pipeline" / "pipelines" / "data_engineering" / "queries" / "assorted_queries.py"

    source = assorted_queries.read_text()

    assert "ADD COLUMN IF NOT EXISTS pii_cleaned BOOLEAN DEFAULT FALSE" in source
    assert "ADD COLUMN IF NOT EXISTS pii_cleaned_version INTEGER DEFAULT 0" in source
    assert "PII_CLEANED_VERSION = 2" in source
    assert "pii_cleaned_version = {PII_CLEANED_VERSION}" in source
    assert "SET pii_cleaned = TRUE," in source
    assert "COALESCE(s.pii_cleaned_version" in source


def test_sql_scrubber_has_protected_shape_and_normalization_helpers():
    project_root = Path(__file__).parents[4]
    assorted_queries = project_root / "src" / "data_pipeline" / "pipelines" / "data_engineering" / "queries" / "assorted_queries.py"

    source = assorted_queries.read_text()

    assert "CREATE OR REPLACE FUNCTION scratch.is_protected_non_pii_text" in source
    assert "CREATE OR REPLACE FUNCTION scratch.normalize_phone_candidate" in source
    assert "CREATE OR REPLACE FUNCTION scratch.matches_pii_text" in source
    assert "WHEN input_json ? 'entries' THEN jsonb_set(" in source


def test_sql_scrubber_has_canary_and_telemetry_helpers():
    project_root = Path(__file__).parents[4]
    assorted_queries = project_root / "src" / "data_pipeline" / "pipelines" / "data_engineering" / "queries" / "assorted_queries.py"

    source = assorted_queries.read_text()

    assert "def pii_redaction_preview_query(" in source
    assert "def pii_redaction_key_telemetry_query(" in source
    assert "def pii_suspicious_fragments_query(" in source
    assert "def pii_canary_summary_query(" in source
    assert "LIKE '%[PII_REMOVED]:%'" in source
