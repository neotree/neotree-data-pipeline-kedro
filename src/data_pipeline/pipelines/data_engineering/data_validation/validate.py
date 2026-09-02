import os
import ast
import operator
import json
import pandas as pd
import numpy as np
import great_expectations as gx
import traceback
from difflib import SequenceMatcher
import smtplib
from email.message import EmailMessage
import logging
from .templates import get_html_validation_template
from conf.common.scripts import get_script, merge_script_data
from conf.common.logger import setup_logger
from typing import Any, Dict, Optional, cast
from datetime import datetime, timedelta
from conf.base.catalog import params, hospital_conf
import re
import pdfkit
import uuid
from data_pipeline.pipelines.data_engineering.utils.field_info import (
    get_script_field_schemas,
    get_script_metadata_details,
    load_json_for_comparison,
)

STATUS_FILE = "logs/validation_status.json"
VALIDATION_LOG_FILES = {
    "tech": "logs/validation_tech.log",
    "implementation": "logs/validation_implementation.log",
    "compliance": "logs/validation_compliance.log",
}
VALIDATION_SUMMARY_LOG_FILES = {
    "tech": "logs/validation_tech_summary.log",
    "implementation": "logs/validation_implementation_summary.log",
    "compliance": "logs/validation_compliance_summary.log",
}
VALIDATION_MAIL_RECEIVERS = {
    "tech": "tech_mail_receivers",
    "implementation": "impl_mail_receivers",
    "compliance": "comp_mail_receivers",
}
DEFAULT_VALIDATION_EMAIL_INTERVAL_DAYS = 2


def _is_field_schema(schema) -> bool:
    """Return True for a field metadata dict keyed by field key."""
    if not isinstance(schema, dict) or not schema:
        return False

    first_value = next(iter(schema.values()))
    return isinstance(first_value, dict) and "key" in first_value


def _normalize_script_id(script_id) -> str:
    return str(script_id).strip()


def _is_confidential(field: dict) -> bool:
    value = field.get("confidential", False)
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return bool(value)


def _drop_confidential_columns(
    df: pd.DataFrame,
    schemas,
    logger: logging.Logger,
) -> pd.DataFrame:
    """Return a dataframe without schema-defined confidential columns."""
    columns_to_drop = set()

    for schema in schemas:
        fields = schema.values() if isinstance(schema, dict) else schema
        for field in fields or []:
            if not isinstance(field, dict) or not _is_confidential(field):
                continue

            field_key = field.get("key")
            if not field_key:
                continue

            for suffix in (".value", ".label"):
                column = f"{field_key}{suffix}"
                if column in df.columns:
                    columns_to_drop.add(column)

    if not columns_to_drop:
        return df

    ordered_columns = sorted(columns_to_drop)
    logger.info(
        "Dropping %s confidential column(s) before downstream processing: %s",
        len(ordered_columns),
        ordered_columns,
    )
    return df.drop(columns=ordered_columns)


def _format_script_details(details: dict) -> str:
    if not details:
        return ""

    parts = []
    if details.get("title"):
        parts.append(f"Title: {details['title']}")
    if details.get("hospitalName"):
        parts.append(f"Hospital: {details['hospitalName']}")
    api_script_id = details.get("scriptId")
    metadata_key = details.get("metadataKey")
    if api_script_id and api_script_id != metadata_key:
        parts.append(f"API scriptId: {api_script_id}")

    return f" | {' | '.join(parts)}" if parts else ""


def _get_validation_loggers(log_file_path="logs/validation.log") -> Dict[str, logging.Logger]:
    return {
        "tech": setup_logger(VALIDATION_LOG_FILES["tech"], "validation_tech_logger"),
        "implementation": setup_logger(VALIDATION_LOG_FILES["implementation"], "validation_implementation_logger"),
        "compliance": setup_logger(VALIDATION_LOG_FILES["compliance"], "validation_compliance_logger"),
        "legacy": setup_logger(log_file_path, "validation_logger"),
    }


def _log_to_all(loggers: Dict[str, logging.Logger], level: str, message: str) -> None:
    for category in ("tech", "implementation", "compliance"):
        getattr(loggers[category], level)(message)


def _get_validation_email_interval_days() -> int:
    try:
        return max(1, int(params.get("validation_email_interval_days", DEFAULT_VALIDATION_EMAIL_INTERVAL_DAYS)))
    except (TypeError, ValueError):
        return DEFAULT_VALIDATION_EMAIL_INTERVAL_DAYS


def _json_default(value):
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if pd.isna(value):
        return None
    return str(value)


def _safe_json(value) -> str:
    return json.dumps(value if value is not None else {}, default=_json_default)


def _clean_text(value):
    if value is None or pd.isna(value):
        return None
    return str(value)


def _unique_text_values(values, max_items: int = 5) -> list:
    if values is None:
        return []
    if isinstance(values, (str, int, float, bool)):
        values = [values]
    elif not isinstance(values, (list, tuple, set, np.ndarray, pd.Series)):
        try:
            if pd.isna(values):
                return []
        except (TypeError, ValueError):
            pass
        values = [values]

    result = []
    for value in values:
        if value is None or pd.isna(value):
            continue
        text_value = str(value)
        if text_value not in result:
            result.append(text_value)
        if len(result) >= max_items:
            break
    return result


def _all_unique_text_values(values) -> list:
    """Return all unique non-empty scalar values as strings."""
    if values is None:
        return []
    if isinstance(values, (str, int, float, bool)):
        values = [values]
    elif not isinstance(values, (list, tuple, set, np.ndarray, pd.Series)):
        try:
            if pd.isna(values):
                return []
        except (TypeError, ValueError):
            pass
        values = [values]

    result = []
    for value in values:
        if value is None or pd.isna(value):
            continue
        text_value = str(value).strip()
        if text_value and text_value not in result:
            result.append(text_value)
    return result


def _normalise_email_receivers(email_receivers) -> list:
    if isinstance(email_receivers, list):
        raw_receivers = email_receivers
    else:
        raw_receivers = str(email_receivers).split(",")

    return [
        receiver.strip()
        for receiver in raw_receivers
        if receiver and receiver.strip()
    ]


def ensure_validation_tracking_tables():
    create_sql = """
        CREATE SCHEMA IF NOT EXISTS derived;;

        CREATE TABLE IF NOT EXISTS derived.validation_runs (
            run_id TEXT PRIMARY KEY,
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            status TEXT NOT NULL,
            email_sent BOOLEAN NOT NULL DEFAULT FALSE,
            email_sent_at TIMESTAMP,
            email_interval_days INTEGER
        );;

        CREATE TABLE IF NOT EXISTS derived.validation_issues (
            id BIGSERIAL PRIMARY KEY,
            run_id TEXT NOT NULL REFERENCES derived.validation_runs(run_id),
            identified_at TIMESTAMP NOT NULL DEFAULT now(),
            identified_date DATE NOT NULL DEFAULT current_date,
            category TEXT NOT NULL,
            script_name TEXT NOT NULL,
            scriptid TEXT,
            script_title TEXT,
            hospital_name TEXT,
            issue_type TEXT NOT NULL,
            field_key TEXT,
            field_label TEXT,
            severity TEXT NOT NULL DEFAULT 'error',
            issue_message TEXT NOT NULL,
            affected_records INTEGER NOT NULL DEFAULT 0,
            affected_neotree_ids TEXT[],
            sample_values JSONB,
            min_value TEXT,
            max_value TEXT,
            expected_value TEXT,
            actual_value_sample TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT now()
        );;

        CREATE INDEX IF NOT EXISTS idx_validation_issues_date
        ON derived.validation_issues (identified_date);;

        CREATE INDEX IF NOT EXISTS idx_validation_issues_category_script
        ON derived.validation_issues (category, script_name, scriptid);;

        CREATE INDEX IF NOT EXISTS idx_validation_issues_type_field
        ON derived.validation_issues (issue_type, field_key);;

        CREATE INDEX IF NOT EXISTS idx_validation_issues_run
        ON derived.validation_issues (run_id);;

        CREATE TABLE IF NOT EXISTS derived.validation_logged_records (
            id BIGSERIAL PRIMARY KEY,
            uid TEXT NOT NULL,
            scriptid TEXT NOT NULL DEFAULT '',
            facility TEXT NOT NULL DEFAULT '',
            first_logged_at TIMESTAMP NOT NULL DEFAULT now(),
            UNIQUE (uid, scriptid, facility)
        );;

        UPDATE derived.validation_logged_records
        SET scriptid = COALESCE(scriptid, ''),
            facility = COALESCE(facility, '');;

        ALTER TABLE derived.validation_logged_records
        ALTER COLUMN scriptid SET DEFAULT '',
        ALTER COLUMN facility SET DEFAULT '';;

        CREATE INDEX IF NOT EXISTS idx_validation_logged_records_lookup
        ON derived.validation_logged_records (uid, scriptid, facility);;

        CREATE TABLE IF NOT EXISTS derived.validation_maintenance_state (
            state_key TEXT PRIMARY KEY,
            completed_at TIMESTAMP NOT NULL DEFAULT now()
        );;

        DO $$
        BEGIN
            IF to_regclass('derived.validation_issues') IS NOT NULL
               AND NOT EXISTS (
                SELECT 1
                FROM derived.validation_maintenance_state
                WHERE state_key = 'validation_logged_records_backfill_v1'
            ) THEN
                INSERT INTO derived.validation_logged_records (
                    uid,
                    scriptid,
                    facility,
                    first_logged_at
                )
                SELECT DISTINCT
                    NULLIF(TRIM(record_uid), '') AS uid,
                    COALESCE(issue.scriptid, '') AS scriptid,
                    COALESCE(issue.hospital_name, '') AS facility,
                    MIN(COALESCE(issue.identified_at, issue.created_at, now())) AS first_logged_at
                FROM derived.validation_issues issue
                CROSS JOIN LATERAL unnest(
                    COALESCE(issue.affected_neotree_ids, ARRAY[]::text[])
                ) AS record_uid
                WHERE NULLIF(TRIM(record_uid), '') IS NOT NULL
                GROUP BY
                    NULLIF(TRIM(record_uid), ''),
                    COALESCE(issue.scriptid, ''),
                    COALESCE(issue.hospital_name, '')
                ON CONFLICT (uid, scriptid, facility) DO NOTHING;

                INSERT INTO derived.validation_maintenance_state (state_key)
                VALUES ('validation_logged_records_backfill_v1')
                ON CONFLICT (state_key) DO NOTHING;
            END IF;
        END $$;;
    """
    try:
        from conf.common.sql_functions import inject_sql
        inject_sql(create_sql, "CREATE validation tracking tables")
    except Exception as exc:
        logging.error(f"Failed to create validation tracking tables: {exc}")


def _start_validation_run(run_id: str):
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    interval_days = _get_validation_email_interval_days()
    sql = f"""
        INSERT INTO derived.validation_runs (run_id, started_at, status, email_interval_days)
        VALUES ('{run_id}', '{started_at}', 'running', {interval_days})
        ON CONFLICT (run_id) DO UPDATE SET
            started_at = EXCLUDED.started_at,
            status = EXCLUDED.status,
            email_interval_days = EXCLUDED.email_interval_days;;
    """
    try:
        from conf.common.sql_functions import inject_sql
        inject_sql(sql, "START validation run")
    except Exception as exc:
        logging.error(f"Failed to start validation run tracking: {exc}")


def _complete_validation_run(run_id: str, email_sent: bool = False):
    completed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    email_sql = f", email_sent = TRUE, email_sent_at = '{completed_at}'" if email_sent else ""
    sql = f"""
        UPDATE derived.validation_runs
        SET completed_at = '{completed_at}',
            status = 'done'
            {email_sql}
        WHERE run_id = '{run_id}';;
    """
    try:
        from conf.common.sql_functions import inject_sql
        inject_sql(sql, "COMPLETE validation run")
    except Exception as exc:
        logging.error(f"Failed to complete validation run tracking: {exc}")


def _last_validation_email_sent_at():
    try:
        from conf.common.sql_functions import run_query_and_return_df
        result = run_query_and_return_df("""
            SELECT max(email_sent_at) AS last_email_sent_at
            FROM derived.validation_runs
            WHERE email_sent IS TRUE AND email_sent_at IS NOT NULL;;
        """)
        if result is not None and not result.empty:
            value = result.iloc[0]["last_email_sent_at"]
            if pd.notna(value):
                return pd.to_datetime(value).to_pydatetime()
    except Exception as exc:
        logging.error(f"Failed to read last validation email timestamp: {exc}")
    return None


def _is_validation_email_due() -> bool:
    last_sent = _last_validation_email_sent_at()
    if last_sent is None:
        return True
    return datetime.now() - last_sent >= timedelta(days=_get_validation_email_interval_days())


def _load_validation_issues_for_email(run_id: str):
    last_sent = _last_validation_email_sent_at()
    if last_sent is None:
        where_clause = "1 = 1"
    else:
        where_clause = f"identified_at > '{last_sent.strftime('%Y-%m-%d %H:%M:%S')}'"

    query = f"""
        SELECT
            category,
            script_name,
            scriptid,
            script_title,
            hospital_name,
            issue_type,
            field_key,
            field_label,
            severity,
            issue_message,
            affected_records,
            affected_neotree_ids,
            sample_values,
            min_value,
            max_value,
            expected_value,
            actual_value_sample,
            identified_date,
            run_id
        FROM derived.validation_issues
        WHERE {where_clause}
        ORDER BY category, script_name, scriptid, issue_type, field_key, issue_message;;
    """
    try:
        from conf.common.sql_functions import run_query_and_return_df
        result = run_query_and_return_df(query)
        return result if result is not None else pd.DataFrame()
    except Exception as exc:
        logging.error(f"Failed to load validation issues for summary email: {exc}")
        return pd.DataFrame()


def _normalise_sample_ids(value) -> list:
    if value is None or (not isinstance(value, (list, tuple, np.ndarray)) and pd.isna(value)):
        return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                value = parsed
            else:
                value = [value]
        except json.JSONDecodeError:
            value = [value]
    return _unique_text_values(list(value), 5)


def _write_validation_summary_logs(run_id: str) -> Dict[str, str]:
    issues = _load_validation_issues_for_email(run_id)
    period_days = _get_validation_email_interval_days()
    country = params.get("country", "")

    for category, path in VALIDATION_SUMMARY_LOG_FILES.items():
        os.makedirs(os.path.dirname(path), exist_ok=True)
        category_issues = issues[issues["category"] == category] if not issues.empty else pd.DataFrame()

        lines = [
            f"Data Validation {category.title()} Summary - {country}",
            f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Configured email interval: {period_days} day(s)",
            f"Current run_id: {run_id}",
            "",
        ]

        if category_issues.empty:
            lines.append("No validation issues found for this category in the reporting period.")
        else:
            category_issues = category_issues.copy()
            category_issues["affected_records"] = pd.to_numeric(category_issues["affected_records"], errors="coerce").fillna(0).astype(int)

            script_cols = [
                "script_name", "scriptid", "script_title", "hospital_name",
            ]
            issue_cols = [
                "issue_type", "field_key", "field_label", "severity", "issue_message",
                "min_value", "max_value", "expected_value"
            ]

            for script_key, script_df in category_issues.groupby(script_cols, dropna=False):
                script_group = dict(zip(script_cols, script_key))
                header_parts = [
                    f"Script: {script_group['script_name']}",
                    f"Script ID: {script_group['scriptid']}",
                ]
                if pd.notna(script_group.get("script_title")) and script_group.get("script_title"):
                    header_parts.append(f"Title: {script_group['script_title']}")
                if pd.notna(script_group.get("hospital_name")) and script_group.get("hospital_name"):
                    header_parts.append(f"Hospital: {script_group['hospital_name']}")

                lines.extend([
                    "SCRIPT_HEADER: " + " | ".join(header_parts),
                    "",
                ])

                for issue_key, group_df in script_df.groupby(issue_cols, dropna=False):
                    group = dict(zip(issue_cols, issue_key))
                    sample_ids = []
                    for ids in group_df["affected_neotree_ids"].tolist():
                        sample_ids.extend(_normalise_sample_ids(ids))
                    sample_ids = _unique_text_values(sample_ids, 5)
                    first_seen = group_df["identified_date"].min()
                    last_seen = group_df["identified_date"].max()
                    run_count = group_df["run_id"].nunique()
                    total_affected = int(group_df["affected_records"].sum())

                    lines.append(f"{str(group['severity']).upper()}: {group['issue_message']}")
                    if pd.notna(group.get("field_key")) and group.get("field_key"):
                        field_text = f"Field: {group['field_key']}"
                        if pd.notna(group.get("field_label")) and group.get("field_label"):
                            field_text += f" ({group['field_label']})"
                        lines.append(field_text)
                    if pd.notna(group.get("min_value")) or pd.notna(group.get("max_value")):
                        lines.append(f"Configured range: [{group.get('min_value')}, {group.get('max_value')}]")
                    if pd.notna(group.get("expected_value")) and group.get("expected_value"):
                        lines.append(f"Expected: {group['expected_value']}")

                    actual_samples = _unique_text_values(
                        group_df["actual_value_sample"].dropna().tolist(),
                        5,
                    )
                    if actual_samples:
                        lines.append(f"Recorded value samples: {actual_samples}")

                    lines.extend([
                        f"Affected: {total_affected} | Runs: {run_count} | First: {first_seen} | Latest: {last_seen}",
                        (
                            f"Sample NeoTree IDs: {sample_ids}"
                            if sample_ids
                            else "Sample NeoTree IDs: unavailable"
                        ),
                        "",
                    ])

                lines.extend([
                    f"SCRIPT_END: End validation for {script_group['script_name']} ({script_group['scriptid']})",
                    "",
                ])

        with open(path, "w") as f:
            f.write("\n".join(lines))

    return VALIDATION_SUMMARY_LOG_FILES


def _insert_validation_issues(issues: list):
    issues = [
        issue for issue in issues
        if int(issue.get("affected_records") or 0) > 0
    ]
    if not issues:
        return

    run_id = get_run_id()
    if not run_id:
        logging.warning("Validation issue tracking skipped because run_id is missing")
        return

    def _tracker_value(value) -> str:
        if value is None or pd.isna(value):
            return ""
        return str(value).strip()

    def _record_keys_for_issue(issue: dict) -> list:
        identifiers = _all_unique_text_values(issue.get("affected_record_ids"))
        if not identifiers:
            identifiers = _all_unique_text_values(issue.get("affected_neotree_ids"))
        if not identifiers:
            return []

        scriptid = _tracker_value(issue.get("scriptid"))
        facility = _tracker_value(issue.get("hospital_name"))
        keys = []
        for identifier in identifiers:
            uid = _tracker_value(identifier)
            if uid:
                keys.append((uid, scriptid, facility))
        return list(dict.fromkeys(keys))

    def _fetch_existing_record_keys(cur, record_keys: list) -> set:
        if not record_keys:
            return set()

        values_sql = ",".join(["(%s, %s, %s)"] * len(record_keys))
        params = [
            value
            for record_key in record_keys
            for value in record_key
        ]
        cur.execute(
            f"""
                SELECT logged.uid, logged.scriptid, logged.facility
                FROM derived.validation_logged_records logged
                JOIN (VALUES {values_sql}) AS incoming(uid, scriptid, facility)
                  ON logged.uid = incoming.uid
                 AND logged.scriptid = incoming.scriptid
                 AND logged.facility = incoming.facility
            """,
            params,
        )
        return set(cur.fetchall())

    def _build_issue_row(issue: dict, now, record_keys: list = None):
        affected_record_ids = [record_key[0] for record_key in record_keys or []]
        affected_records = (
            len(affected_record_ids)
            if affected_record_ids
            else int(issue.get("affected_records") or 0)
        )
        affected_sample_ids = (
            _unique_text_values(affected_record_ids, 5)
            if affected_record_ids
            else _unique_text_values(issue.get("affected_neotree_ids"), 5)
        )
        return (
            run_id,
            now,
            now.date(),
            issue.get("category"),
            issue.get("script_name"),
            issue.get("scriptid"),
            issue.get("script_title"),
            issue.get("hospital_name"),
            issue.get("issue_type"),
            issue.get("field_key"),
            issue.get("field_label"),
            issue.get("severity", "error"),
            issue.get("issue_message"),
            affected_records,
            affected_sample_ids,
            _safe_json(issue.get("sample_values")),
            _clean_text(issue.get("min_value")),
            _clean_text(issue.get("max_value")),
            _clean_text(issue.get("expected_value")),
            _clean_text(issue.get("actual_value_sample")),
        )

    insert_sql = """
        INSERT INTO derived.validation_issues (
            run_id, identified_at, identified_date, category, script_name, scriptid,
            script_title, hospital_name, issue_type, field_key, field_label, severity,
            issue_message, affected_records, affected_neotree_ids, sample_values,
            min_value, max_value, expected_value, actual_value_sample
        )
        VALUES %s
    """
    insert_record_sql = """
        INSERT INTO derived.validation_logged_records (
            uid,
            scriptid,
            facility
        )
        VALUES %s
        ON CONFLICT (uid, scriptid, facility) DO NOTHING
    """

    try:
        from conf.common.sql_functions import engine, execute_values
        if not engine or not execute_values:
            raise RuntimeError("Database engine or execute_values is not initialized")
        raw_conn = engine.raw_connection()
        try:
            cur = raw_conn.cursor()
            try:
                rows = []
                record_rows = []
                now = datetime.now()

                for issue in issues:
                    record_keys = _record_keys_for_issue(issue)
                    if record_keys:
                        existing_keys = _fetch_existing_record_keys(cur, record_keys)
                        new_keys = [
                            record_key
                            for record_key in record_keys
                            if record_key not in existing_keys
                        ]

                        if not new_keys:
                            continue

                        rows.append(_build_issue_row(issue, now, new_keys))
                        record_rows.extend(new_keys)
                    else:
                        rows.append(_build_issue_row(issue, now))

                if not rows:
                    raw_conn.commit()
                    return

                execute_values(
                    cur,
                    insert_sql,
                    rows,
                    template="""(
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s
                    )"""
                )
                if record_rows:
                    execute_values(
                        cur,
                        insert_record_sql,
                        record_rows,
                    )
                raw_conn.commit()
            except Exception:
                raw_conn.rollback()
                raise
            finally:
                cur.close()
        finally:
            raw_conn.close()
    except Exception as exc:
        logging.error(f"Failed to persist validation issues: {exc}")


def set_status(status: str, run_id: str = None):
    payload = {"status": status, "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    if run_id:
        payload["run_id"] = run_id
    with open(STATUS_FILE, "w") as f:
        json.dump(payload, f)


def get_status():
    if not os.path.exists(STATUS_FILE):
        return "unknown"
    with open(STATUS_FILE, "r") as f:
        return json.load(f).get("status")


def get_run_id():
    if not os.path.exists(STATUS_FILE):
        return None
    with open(STATUS_FILE, "r") as f:
        return json.load(f).get("run_id")


def reset_log(log_file_path="logs/validation.log"):
    for path in [log_file_path, *VALIDATION_LOG_FILES.values(), *VALIDATION_SUMMARY_LOG_FILES.values()]:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write("")


def begin_validation_run(log_file_path="logs/validation.log"):
    run_id = datetime.now().strftime("%Y%m%d%H%M%S") + "-" + str(uuid.uuid4())
    ensure_validation_tracking_tables()
    set_status("running", run_id=run_id)
    reset_log(log_file_path)
    _start_validation_run(run_id)


def finalize_validation():
    if get_status() == "running":
        run_id = get_run_id()
        email_sent = False

        if run_id and _is_validation_email_due():
            summary_logs = _write_validation_summary_logs(run_id)
            for category, log_file_path in summary_logs.items():
                receiver_key = VALIDATION_MAIL_RECEIVERS[category]
                email_recipients = params.get(receiver_key)
                if email_recipients:
                    sent = send_log_via_email(
                        log_file_path,
                        email_receivers=email_recipients,
                        category=category,
                    )
                    email_sent = sent or email_sent

        if run_id:
            _complete_validation_run(run_id, email_sent=email_sent)
        set_status("done", run_id=run_id)


def get_safe_sample_uids(df: pd.DataFrame, mask: pd.Series, max_samples: int = 5) -> list:
    """
    Get sample UIDs from rows matching the mask, but only return non-NULL UIDs.
    This prevents the contradiction of showing NULL UIDs when reporting NULL values.
    """
    if 'uid' not in df.columns:
        return []

    # Get UIDs from matching rows, but filter out NULL UIDs
    sample_uids = df.loc[mask, 'uid'].dropna().head(max_samples).tolist()
    return sample_uids


def get_record_identifiers(df: pd.DataFrame, mask: pd.Series) -> list:
    """
    Return stable record identifiers for every row matching a validation mask.

    UID is preferred. If UID is unavailable, fall back to unique_key and then to
    the dataframe index so repeated DB-backed issue logging can still be deduped.
    """
    if mask is None:
        return []

    identifiers = []
    matching = df.loc[mask]
    for idx, row in matching.iterrows():
        identifier = None
        if 'uid' in matching.columns:
            value = row.get('uid')
            if pd.notna(value) and str(value).strip():
                identifier = value
        if identifier is None and 'unique_key' in matching.columns:
            value = row.get('unique_key')
            if pd.notna(value) and str(value).strip():
                identifier = value
        if identifier is None:
            identifier = idx
        identifiers.append(identifier)

    return _all_unique_text_values(identifiers)


def convert_value_to_type(value, data_type, min_val, max_val):
    """
    Convert value to the appropriate type based on data_type and min/max values.

    Returns: (converted_value, conversion_successful)
    """
    if pd.isna(value) or value == '' or str(value).strip() == '':
        return None, True

    try:
        # Determine target type from min/max values or data_type
        if data_type in ['number', 'integer', 'float', 'timer']:
            # Try to convert to numeric
            converted = pd.to_numeric(value, errors='raise')
            return converted, True

        elif data_type in ['datetime', 'timestamp', 'date']:
            # Try to convert to datetime
            converted = pd.to_datetime(value, errors='raise')
            return converted, True

        else:
            # Keep as string
            return str(value), True

    except (ValueError, TypeError):
        return value, False


def check_value_range(value, min_val, max_val, data_type):
    """
    Check if value is within the specified range.

    Returns: (is_valid, error_message)
    """
    if pd.isna(value) or value == '' or str(value).strip() == '':
        return True, None

    min_val = min_val if min_val is not None and str(min_val).strip() != '' else None
    max_val = max_val if max_val is not None and str(max_val).strip() != '' else None

    # Convert min/max to appropriate type
    try:
        if data_type in ['number', 'integer', 'float', 'timer']:
            if min_val is not None:
                min_val = float(min_val)
            if max_val is not None:
                max_val = float(max_val)
            value_num = float(value)

            if min_val is not None and value_num < min_val:
                return False, f"Value {value_num} is below minimum {min_val}"
            if max_val is not None and value_num > max_val:
                return False, f"Value {value_num} is above maximum {max_val}"

        elif data_type in ['datetime', 'timestamp', 'date']:
            if min_val is not None:
                min_val = pd.to_datetime(min_val, errors='coerce')
            if max_val is not None:
                max_val = pd.to_datetime(max_val, errors='coerce')
            value_dt = pd.to_datetime(value, errors='coerce')

            if pd.isna(value_dt):
                return False, f"Recorded value '{value}' is not a valid {data_type}"
            if pd.notna(min_val) and value_dt < min_val:
                return False, f"Date {value_dt} is before minimum {min_val}"
            if pd.notna(max_val) and value_dt > max_val:
                return False, f"Date {value_dt} is after maximum {max_val}"

        return True, None

    except (ValueError, TypeError) as e:
        return False, f"Cannot validate range: {str(e)}"


def validate_dataframe_with_ge(
    df: pd.DataFrame,
    script: str,
    log_file_path="logs/validation.log",
) -> pd.DataFrame:
    """
    Comprehensive validation using Great Expectations with schema-based rules.
    Optimized to minimize redundant dataframe iterations.

    NEW: Supports scriptId-based validation - splits dataframe by scriptId
    and validates each subset against its corresponding metadata.

    Validates:
    - Required fields (optional=false)
    - Value ranges (minValue, maxValue)
    - Data types
    - Data quality metrics

    Returns:
        The validated dataframe with schema-defined confidential columns removed.
    """
    context = gx.get_context()
    loggers = _get_validation_loggers(log_file_path)
    logger = loggers["tech"]

    # Load metadata (could be new format {scriptId: [fields]} or legacy [fields])
    metadata = load_json_for_comparison(script)

    if not metadata:
        logger.warning(f"##### SCHEMA FOR SCRIPT {script} NOT FOUND - SKIPPING VALIDATION")
        _insert_validation_issues([{
            "category": "tech",
            "script_name": script,
            "scriptid": None,
            "issue_type": "missing_validation_schema",
            "severity": "warning",
            "issue_message": f"Schema for script '{script}' not found; validation skipped",
            "affected_records": len(df),
        }])
        return df

    _log_to_all(loggers, "info", f"\n{'='*60}")
    _log_to_all(loggers, "info", f"VALIDATING: {script.upper()} | Rows: {len(df)} | Cols: {len(df.columns)}")
    _log_to_all(loggers, "info", f"{'='*60}")

    script_field_schemas = get_script_field_schemas(metadata)
    is_scriptid_metadata = isinstance(script_field_schemas, dict) and not _is_field_schema(script_field_schemas)

    if isinstance(metadata, dict):
        logger.info(
            "Metadata shape: %s | top-level keys: %s",
            "scriptid-based" if is_scriptid_metadata else "field-based",
            list(script_field_schemas.keys())[:10] if isinstance(script_field_schemas, dict) else [],
        )

    # Check if we have new scriptId-based structure and scriptId column
    if is_scriptid_metadata and 'scriptid' in df.columns:
        # NEW FORMAT: Split by scriptId and validate each subset
        logger.info(f"\nUsing scriptId-based validation")

        metadata_by_script_id = {
            _normalize_script_id(script_id): schema
            for script_id, schema in script_field_schemas.items()
        }
        script_id_keys = df['scriptid'].map(
            lambda value: pd.NA if pd.isna(value) else _normalize_script_id(value)
        )
        unique_script_ids = script_id_keys.dropna().unique()
        logger.info(f"Found {len(unique_script_ids)} unique dataframe scriptid(s): {unique_script_ids.tolist()}")
        logger.info(f"Metadata scriptid(s) available: {list(metadata_by_script_id.keys())}")

        missing_metadata_ids = sorted(set(unique_script_ids) - set(metadata_by_script_id.keys()))
        unused_metadata_ids = sorted(set(metadata_by_script_id.keys()) - set(unique_script_ids))
        if missing_metadata_ids:
            logger.warning(f"Dataframe scriptid(s) without metadata: {missing_metadata_ids}")
        if unused_metadata_ids:
            logger.info(f"Metadata scriptid(s) not present in dataframe: {unused_metadata_ids}")

        # Validate each scriptId subset
        for script_id_str in unique_script_ids:
            subset_df = df[script_id_keys == script_id_str].copy()
            schema = metadata_by_script_id.get(script_id_str)

            if not schema:
                logger.warning(f"\n⚠ No metadata found for scriptid: {script_id_str} ({len(subset_df)} rows) - SKIPPING")
                _insert_validation_issues([{
                    "category": "tech",
                    "script_name": script,
                    "scriptid": script_id_str,
                    "issue_type": "missing_scriptid_metadata",
                    "severity": "warning",
                    "issue_message": f"No metadata found for scriptid '{script_id_str}'; validation skipped",
                    "affected_records": len(subset_df),
                    "affected_neotree_ids": get_safe_sample_uids(subset_df, pd.Series(True, index=subset_df.index), 5),
                }])
                continue

            script_details = get_script_metadata_details(metadata, script_id_str)
            details_suffix = _format_script_details(script_details)
            _log_to_all(loggers, "info", f"\n{'─'*60}")
            _log_to_all(loggers, "info", f"Validating scriptid: {script_id_str} | {len(subset_df)} rows{details_suffix}")
            logger.info(f"Schema field count: {len(schema)} | sample fields: {list(schema.keys())[:10]}")
            _log_to_all(loggers, "info", f"{'─'*60}")

            # Call the validation logic for this subset
            _validate_subset(
                subset_df,
                schema,
                script_or_id=script_id_str,
                loggers=loggers,
                context=context,
                script_name=script,
                script_id=script_id_str,
                script_details=script_details,
            )

        # Handle rows with NULL scriptId
        null_script_id_df = df[df['scriptid'].isna()]
        if not null_script_id_df.empty:
            logger.warning(f"\n⚠ {len(null_script_id_df)} rows have NULL scriptid - SKIPPING VALIDATION")
            if 'uid' in null_script_id_df.columns:
                sample_uids = null_script_id_df['uid'].dropna().head(3).tolist()
                logger.warning(f"   Sample UIDs: {sample_uids}")
            _insert_validation_issues([{
                "category": "tech",
                "script_name": script,
                "scriptid": None,
                "issue_type": "null_scriptid",
                "severity": "warning",
                "issue_message": "Rows have NULL scriptid; validation skipped for those rows",
                "affected_records": len(null_script_id_df),
                "affected_neotree_ids": null_script_id_df['uid'].dropna().head(5).tolist() if 'uid' in null_script_id_df.columns else [],
            }])

        logger.info(f"\n{'='*60}")
        logger.info(f"COMPLETED: {script.upper()} | All scriptIds validated")
        logger.info(f"{'='*60}\n")
        return _drop_confidential_columns(
            df,
            metadata_by_script_id.values(),
            logger,
        )

    # LEGACY FORMAT or no scriptId column: Use existing validation
    if isinstance(metadata, dict):
        if _is_field_schema(metadata):
            # This is a legacy format converted to dict {fieldKey: field}
            logger.info(f"Using legacy validation format (converted from array)")
            schema = metadata
        else:
            # This is scriptId-based format but no scriptId column
            logger.warning(f"No scriptId column in dataframe - using first available schema")
            if len(script_field_schemas) == 1:
                first_script_id = next(iter(script_field_schemas.keys()))
                schema = script_field_schemas[first_script_id]
                logger.info(f"Using single available schema: {first_script_id}")
            else:
                logger.warning(f"Multiple schemas available but no scriptId column - using first schema")
                schema = next(iter(script_field_schemas.values()))
    else:
        logger.error(f"Unexpected metadata type: {type(metadata)}")
        return df

    # Call validation logic for entire dataframe (legacy)
    _validate_subset(
        df,
        schema,
        script_or_id=script,
        loggers=loggers,
        context=context,
        script_name=script,
        script_id=script if 'scriptid' not in df.columns else None,
        script_details={},
    )
    return _drop_confidential_columns(df, [schema], logger)


# Screen/field visibility "condition" expressions (e.g. "__age > 5 and __gender == 'Male'")
# are sourced from externally fetched script metadata, so they are interpreted with a small
# hand-rolled evaluator below instead of eval()/pd.eval() — attribute access, calls, imports,
# and any name outside of `local_dict` are simply never reachable, rather than merely
# blocklisted, so condition strings can never execute arbitrary code.
_CONDITION_COMPARE_OPS = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
}


def _condition_logical_and(left, right):
    if isinstance(left, (pd.Series, np.ndarray)) or isinstance(right, (pd.Series, np.ndarray)):
        return left & right
    return bool(left) and bool(right)


def _condition_logical_or(left, right):
    if isinstance(left, (pd.Series, np.ndarray)) or isinstance(right, (pd.Series, np.ndarray)):
        return left | right
    return bool(left) or bool(right)


def _condition_logical_not(value):
    if isinstance(value, (pd.Series, np.ndarray)):
        return ~value
    return not value


def _eval_condition_node(node, local_dict: dict):
    if isinstance(node, ast.Expression):
        return _eval_condition_node(node.body, local_dict)
    if isinstance(node, ast.BoolOp):
        values = [_eval_condition_node(value, local_dict) for value in node.values]
        if isinstance(node.op, ast.And):
            combine = _condition_logical_and
        elif isinstance(node.op, ast.Or):
            combine = _condition_logical_or
        else:
            raise ValueError(f"boolean operator '{type(node.op).__name__}' is not allowed in condition")
        result = values[0]
        for value in values[1:]:
            result = combine(result, value)
        return result
    if isinstance(node, ast.UnaryOp):
        operand = _eval_condition_node(node.operand, local_dict)
        if isinstance(node.op, ast.Not):
            return _condition_logical_not(operand)
        if isinstance(node.op, ast.USub):
            return -operand
        if isinstance(node.op, ast.UAdd):
            return operand
        raise ValueError(f"unary operator '{type(node.op).__name__}' is not allowed in condition")
    if isinstance(node, ast.Compare):
        if len(node.ops) != 1 or type(node.ops[0]) not in _CONDITION_COMPARE_OPS:
            raise ValueError("only a single ==, !=, <, <=, >, >= comparison is allowed in condition")
        left = _eval_condition_node(node.left, local_dict)
        right = _eval_condition_node(node.comparators[0], local_dict)
        return _CONDITION_COMPARE_OPS[type(node.ops[0])](left, right)
    if isinstance(node, ast.Name):
        if node.id not in local_dict:
            raise ValueError(f"name '{node.id}' is not allowed in condition")
        return local_dict[node.id]
    if isinstance(node, ast.Constant):
        if node.value is not None and not isinstance(node.value, (str, int, float, bool)):
            raise ValueError("unsupported literal in condition")
        return node.value
    raise ValueError(f"expression element '{type(node).__name__}' is not allowed in condition")


def _normalize_condition_operators(expr: str) -> str:
    """
    Normalize boolean/comparison syntax ("and"/"or" words, "&"/"|" symbols,
    "true"/"false", and a bare "=") to Python's native spelling -- but only
    outside of single-quoted string literals. A naive whole-string replace
    would silently corrupt a compared value that happens to contain one of
    these tokens (e.g. "M&B", "A&E", "salt and pepper", "a=b").
    """
    parts = re.split(r"('(?:[^']|'')*')", expr)
    for i in range(0, len(parts), 2):  # even indices are the non-literal spans
        part = parts[i]
        part = part.replace("&", " and ").replace("|", " or ")
        part = re.sub(r"\band\b", "and", part, flags=re.I)
        part = re.sub(r"\bor\b", "or", part, flags=re.I)
        part = re.sub(r"\btrue\b", "True", part, flags=re.I)
        part = re.sub(r"\bfalse\b", "False", part, flags=re.I)
        part = re.sub(r"(?<![<>=!])=(?!=)", "==", part)
        parts[i] = part
    return "".join(parts)


def _safe_eval_condition(expr: str, local_dict: dict):
    """
    Evaluate a screen/field visibility condition without ever calling
    eval()/pd.eval() on it — condition strings originate from externally
    fetched script metadata and must never reach a real Python evaluator.
    Only boolean combination (and/or/not), a single comparison, field
    lookups from `local_dict`, and literal constants are supported; nothing
    else in the parsed expression is ever executed.
    """
    tree = ast.parse(expr, mode="eval")
    return _eval_condition_node(tree, local_dict)


def _validate_subset(
    df: pd.DataFrame,
    schema,
    script_or_id: str,
    loggers: Dict[str, logging.Logger],
    context,
    script_name: str,
    script_id: Optional[str] = None,
    script_details: Optional[Dict[str, Any]] = None,
):
    """
    Validate a single dataframe subset against its schema.

    This function contains the core validation logic extracted from validate_dataframe_with_ge.
    It can be called for the entire dataframe (legacy) or for scriptId subsets (new).

    Args:
        df: DataFrame to validate
        schema: Dict of {fieldKey: field} or list of field definitions (legacy)
        script_or_id: Script name or scriptId for logging
        loggers: Category-specific logger instances
        context: Great Expectations context
    """
    tech_logger = loggers["tech"]
    impl_logger = loggers["implementation"]
    comp_logger = loggers["compliance"]
    errors = []
    warnings = []
    issues = []
    script_details = script_details or {}
    script_title = script_details.get("title")
    hospital_name = script_details.get("hospitalName")

    def _field_label(field_key):
        field = field_info.get(field_key, {}) if isinstance(field_info, dict) else {}
        return field.get("label")

    def _add_issue(
        category: str,
        issue_type: str,
        issue_message: str,
        affected_records: int,
        field_key: str = None,
        severity: str = "error",
        affected_neotree_ids=None,
        sample_values=None,
        min_value=None,
        max_value=None,
        expected_value=None,
        actual_value_sample=None,
        affected_record_ids=None,
    ):
        if int(affected_records or 0) <= 0:
            return

        issues.append({
            "category": category,
            "script_name": script_name,
            "scriptid": script_id or script_or_id,
            "script_title": script_title,
            "hospital_name": hospital_name,
            "issue_type": issue_type,
            "field_key": field_key,
            "field_label": _field_label(field_key) if field_key else None,
            "severity": severity,
            "issue_message": issue_message,
            "affected_records": affected_records,
            "affected_neotree_ids": affected_neotree_ids or [],
            "sample_values": sample_values or {},
            "min_value": min_value,
            "max_value": max_value,
            "expected_value": expected_value,
            "actual_value_sample": actual_value_sample,
            "affected_record_ids": affected_record_ids or affected_neotree_ids or [],
        })

    # Create validator
    validator = context.sources.pandas_default.read_dataframe(df)

    # Create field lookup dictionary
    # Handle both dict (new format) and list (legacy format)
    if isinstance(schema, dict):
        field_info = schema
    else:
        field_info = {f['key']: f for f in schema}

    bool_map = {
        "y": True, "yes": True, "true": True, "1": True, True: True,
        "n": False, "no": False, "false": False, "0": False, False: False,
    }

    def _coerce_boolean_series(series: pd.Series) -> pd.Series:
        return (
            series.astype(str)
            .str.strip()
            .str.lower()
            .map(bool_map)
        )

    def _evaluate_condition_mask(condition: str) -> pd.Series:
        if not condition or not isinstance(condition, str):
            return pd.Series(True, index=df.index)

        expr = condition.strip()
        if not expr:
            return pd.Series(True, index=df.index)
        keys = re.findall(r"\$([A-Za-z0-9_]+)", expr)

        for key in keys:
            expr = expr.replace(f"${key}", f"__{key}")

        # Normalize both spellings ("and"/"or" words and "&"/"|" symbols) to
        # Python's native `and`/`or` keywords so ast.parse groups them around
        # the comparisons on either side with correct (low) precedence --
        # unlike bitwise &/|, which Python would otherwise bind tighter than
        # ==, <, >, etc., splitting a compound condition apart incorrectly.
        # Done outside of quoted literals so a compared value like 'M&B' or
        # 'salt and pepper' is never mangled by the substitution.
        expr = _normalize_condition_operators(expr)

        local_dict = {}
        for key in keys:
            col = f"{key}.value"
            if col in df.columns:
                series = df[col]
            else:
                series = pd.Series([pd.NA] * len(df), index=df.index)
            meta = field_info.get(key, {})
            if (meta.get("dataType") or "").lower() == "boolean":
                series = _coerce_boolean_series(series)
            local_dict[f"__{key}"] = series

        try:
            result = _safe_eval_condition(expr, local_dict)
            if isinstance(result, (bool, np.bool_)):
                return pd.Series(bool(result), index=df.index)

            if isinstance(result, pd.Series):
                if not result.index.equals(df.index):
                    result = result.reindex(df.index)
                return result.fillna(False).astype(bool)

            if isinstance(result, (np.ndarray, list, tuple)):
                if len(result) != len(df.index):
                    raise ValueError(
                        "Condition result length "
                        f"{len(result)} does not match dataframe length {len(df.index)}"
                    )
                return pd.Series(result, index=df.index).fillna(False).astype(bool)

            raise TypeError(
                f"Unsupported condition result type: {type(result).__name__}"
            )
        except Exception as exc:
            impl_logger.warning(f"⚠ Failed to evaluate condition '{condition}': {exc}")
            return pd.Series(False, index=df.index)

    def _field_visibility_mask(field: dict) -> pd.Series:
        """
        Return rows where a field should be displayed.

        A field placement is eligible only when both its screen and field
        conditions are met. When a field appears in multiple places, any
        eligible placement makes the field visible.
        """
        visibility_rules = field.get("visibilityConditions")
        if not isinstance(visibility_rules, list) or not visibility_rules:
            visibility_rules = [{
                "screenCondition": field.get("screenCondition", ""),
                "fieldCondition": field.get("condition", ""),
            }]

        visible_mask = pd.Series(False, index=df.index)
        for rule in visibility_rules:
            if not isinstance(rule, dict):
                continue
            screen_mask = _evaluate_condition_mask(rule.get("screenCondition", ""))
            field_mask = _evaluate_condition_mask(rule.get("fieldCondition", ""))
            visible_mask = visible_mask | (screen_mask & field_mask)

        return visible_mask.fillna(False)

    tech_logger.info("\n[TECH] UID SCHEMA & STRUCTURE")

    try:
        if 'uid' in df.columns:
            uid_missing_mask = (
                df["uid"].isna()
                | df["uid"].astype(str).str.strip().eq("")
            )
            missing_uid_count = int(uid_missing_mask.sum())
            if missing_uid_count:
                if "unique_key" in df.columns:
                    sample_identifiers = (
                        df.loc[uid_missing_mask, "unique_key"]
                        .dropna()
                        .head(5)
                        .tolist()
                    )
                else:
                    sample_identifiers = df.index[uid_missing_mask].tolist()[:5]
                tech_logger.error(
                    "❌ %s rows have a missing UID | Record identifiers: %s",
                    missing_uid_count,
                    sample_identifiers,
                )
                _add_issue(
                    category="tech",
                    issue_type="null_uid",
                    issue_message="Rows have a missing UID",
                    affected_records=missing_uid_count,
                    sample_values={"record_identifiers": sample_identifiers},
                    actual_value_sample=", ".join(
                        str(identifier)
                        for identifier in sample_identifiers
                    ) or None,
                    affected_record_ids=get_record_identifiers(df, uid_missing_mask),
                )
            else:
                tech_logger.info("✓ All rows have a UID")
        else:
            tech_logger.error("❌ UID column missing from dataset")
            errors.append("UID column missing")
            _add_issue(
                category="tech",
                issue_type="missing_uid_column",
                issue_message="UID column missing from dataset",
                affected_records=len(df),
            )
    except Exception as e:
        err_msg = f"Error validating 'uid' column: {str(e)}\n{traceback.format_exc()}"
        tech_logger.error(err_msg)
        errors.append(err_msg)
        _add_issue(
            category="tech",
            issue_type="uid_validation_error",
            issue_message=f"Error validating uid column: {str(e)}",
            affected_records=len(df),
            sample_values={"traceback": traceback.format_exc()},
        )

    impl_logger.info("\n[IMPLEMENTATION] FIELD VALIDATION")

    # Storage for results to be reported in sections
    required_results = []
    range_results = []
    type_results = []
    label_results = []
    visible_schema_cells = 0
    populated_schema_cells = 0
    required_visible_cells = 0
    populated_required_cells = 0

    for base_key, field in field_info.items():
        if not isinstance(field, dict) or _is_confidential(field):
            continue

        visibility_mask = _field_visibility_mask(field)
        eligible_count = int(visibility_mask.sum())
        value_col = f"{base_key}.value"
        populated_count = 0
        if value_col in df.columns:
            normalized_values = (
                df[value_col]
                .astype(str)
                .replace(['nan', '<NA>', 'None', 'null', 'NAT', 'NaT'], '')
                .str.strip()
                .replace('', np.nan)
            )
            populated_count = int(
                (visibility_mask & normalized_values.notna()).sum()
            )

        visible_schema_cells += eligible_count
        populated_schema_cells += populated_count
        if not field.get('optional', True):
            required_visible_cells += eligible_count
            populated_required_cells += populated_count

    # Single loop through all .value columns
    for value_col in [col for col in df.columns if col.endswith('.value')]:
        base_key = value_col[:-6]
        label_col = f"{base_key}.label"

        if base_key not in field_info:
            continue

        field = field_info[base_key]
        field_type = field.get('type', '')
        data_type = field.get('dataType', '')
        is_optional = field.get('optional', True)
        is_confidential = _is_confidential(field)
        visibility_mask = _field_visibility_mask(field)
        min_val = field.get('minValue')
        max_val = field.get('maxValue')
        field_options = field.get('options', [])
        normalized_values = (
            df[value_col]
            .astype(str)
            .replace(['nan', '<NA>', 'None', 'null', 'NAT', 'NaT'], '')
            .str.strip()
            .replace('', np.nan)
        )

        # --- REQUIRED FIELDS VALIDATION ---
        if not is_optional and not is_confidential:
            eligible_count = int(visibility_mask.sum())
            if eligible_count == 0:
                continue

            # Check for NULL/empty values
            null_mask = visibility_mask & normalized_values.isna()
            null_count = int(null_mask.sum())
            if null_count > 0:
                null_pct = (null_count / eligible_count) * 100

                # Special handling: if the field being checked is 'uid', we can't show UIDs as samples
                # Instead, show unique_key or row indices
                if base_key.lower() == 'uid':
                    if 'unique_key' in df.columns:
                        sample_identifiers = df.loc[null_mask, 'unique_key'].head(5).tolist()
                    else:
                        # Fallback to row indices
                        sample_identifiers = df[null_mask].head(5).index.tolist()
                else:
                    sample_identifiers = get_safe_sample_uids(df, null_mask, 5)

                required_results.append({
                    'base_key': base_key,
                    'null_count': null_count,
                    'total_count': eligible_count,
                    'null_pct': null_pct,
                    'sample_identifiers': sample_identifiers,
                    'is_uid_field': base_key.lower() == 'uid',
                    'record_identifiers': get_record_identifiers(df, null_mask),
                })

        # --- VALUE RANGE VALIDATION ---
        # Skip validation if both min and max are None or empty
        has_min = min_val is not None and str(min_val).strip() != ''
        has_max = max_val is not None and str(max_val).strip() != ''

        if (has_min or has_max) and not is_confidential:
            non_null_mask = visibility_mask & df[value_col].notna()
            non_null_values = df.loc[non_null_mask, value_col]

            if len(non_null_values) > 0:
                out_of_range_values = []
                for idx, val in non_null_values.items():
                    is_valid, error_msg = check_value_range(val, min_val, max_val, data_type)
                    if not is_valid:
                        uid = df.loc[idx, 'uid'] if 'uid' in df.columns else None
                        unique_key = df.loc[idx, 'unique_key'] if 'unique_key' in df.columns else None
                        identifier = uid if pd.notna(uid) else unique_key
                        if identifier is None or pd.isna(identifier):
                            identifier = idx
                        out_of_range_values.append({
                            "row_index": idx,
                            "uid": uid,
                            "unique_key": unique_key,
                            "identifier": identifier,
                            "recorded_value": val,
                            "reason": error_msg,
                        })

                if out_of_range_values:
                    range_results.append({
                        'base_key': base_key,
                        'violations': out_of_range_values,
                        'total': len(non_null_values),
                        'min_val': min_val,
                        'max_val': max_val
                    })

        # --- DATA TYPE VALIDATION ---
        # All-NULL fields are handled by required-field validation when the
        # field is required and visible. Do not emit a duplicate warning.
        visible_values = normalized_values.loc[visibility_mask]
        if not visible_values.empty and visible_values.isna().all():
            continue

        # Validate based on data type
        try:
            if data_type in ['number', 'integer', 'float', 'timer']:
                numeric_regex = r"^\s*$|^-?\d+(\.\d+)?([eE][+-]?\d+)?$"
                result = validator.expect_column_values_to_match_regex(
                    column=value_col,
                    regex=numeric_regex,
                    mostly=1.0
                )

                if not result['success']:
                    invalid_count = result['result'].get('unexpected_count', 0)
                    try:
                        non_empty = df[value_col].astype(str).str.strip().replace('', np.nan).notna()
                        invalid_mask = non_empty & ~df[value_col].astype(str).str.match(numeric_regex, na=False)
                        invalid_samples = df.loc[invalid_mask, [value_col] + (['uid'] if 'uid' in df.columns else [])]

                        samples_list = []
                        if not invalid_samples.empty:
                            for idx, row in invalid_samples.head(5).iterrows():
                                uid_val = f"{row['uid']}={row[value_col]}" if 'uid' in invalid_samples.columns else row[value_col]
                                samples_list.append(uid_val)
                        invalid_record_ids = get_record_identifiers(df, invalid_mask)
                    except (ValueError, TypeError) as mask_error:
                        # Handle array comparison errors
                        samples_list = []
                        invalid_record_ids = []
                        logging.warning(f"Could not extract samples for {base_key}: {str(mask_error)}")

                    type_results.append({
                        'base_key': base_key,
                        'invalid_count': invalid_count,
                        'samples': samples_list,
                        'error_type': 'non-numeric',
                        'record_identifiers': invalid_record_ids,
                    })

            elif data_type in ['datetime', 'timestamp', 'date']:
                # Allow dates with or without seconds: YYYY-MM-DD HH:MM or YYYY-MM-DD HH:MM:SS
                datetime_regex = r"^\s*$|^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:\d{2})?)?$"
                result = validator.expect_column_values_to_match_regex(
                    column=value_col,
                    regex=datetime_regex,
                    mostly=1.0
                )

                if not result['success']:
                    invalid_count = result['result'].get('unexpected_count', 0)
                    try:
                        non_empty = df[value_col].astype(str).str.strip().replace('', np.nan).notna()
                        invalid_mask = non_empty & ~df[value_col].astype(str).str.match(datetime_regex, na=False)
                        invalid_samples = df.loc[invalid_mask, [value_col] + (['uid'] if 'uid' in df.columns else [])]

                        samples_list = []
                        if not invalid_samples.empty:
                            for idx, row in invalid_samples.head(5).iterrows():
                                uid_val = f"{row['uid']}={row[value_col]}" if 'uid' in invalid_samples.columns else row[value_col]
                                samples_list.append(uid_val)
                        invalid_record_ids = get_record_identifiers(df, invalid_mask)
                    except (ValueError, TypeError) as mask_error:
                        # Handle array comparison errors
                        samples_list = []
                        invalid_record_ids = []
                        logging.warning(f"Could not extract samples for {base_key}: {str(mask_error)}")

                    type_results.append({
                        'base_key': base_key,
                        'invalid_count': invalid_count,
                        'samples': samples_list,
                        'error_type': 'invalid datetime',
                        'record_identifiers': invalid_record_ids,
                    })

            elif data_type in ['boolean', 'yesno']:
                pattern = r"(?i)^\s*$|^(true|false|1|0|y|n|yes|no)$"
                result = validator.expect_column_values_to_match_regex(
                    column=value_col,
                    regex=pattern,
                    mostly=1.0
                )

                if not result['success']:
                    invalid_count = result['result'].get('unexpected_count', 0)
                    try:
                        non_empty = df[value_col].astype(str).str.strip().replace('', np.nan).notna()
                        invalid_mask = non_empty & ~df[value_col].astype(str).str.match(pattern, na=False)
                        invalid_samples = df.loc[invalid_mask, [value_col] + (['uid'] if 'uid' in df.columns else [])]

                        samples_list = []
                        if not invalid_samples.empty:
                            for idx, row in invalid_samples.head(5).iterrows():
                                uid_val = f"{row['uid']}={row[value_col]}" if 'uid' in invalid_samples.columns else row[value_col]
                                samples_list.append(uid_val)
                        invalid_record_ids = get_record_identifiers(df, invalid_mask)
                    except (ValueError, TypeError) as mask_error:
                        # Handle array comparison errors
                        samples_list = []
                        invalid_record_ids = []
                        logging.warning(f"Could not extract samples for {base_key}: {str(mask_error)}")

                    type_results.append({
                        'base_key': base_key,
                        'invalid_count': invalid_count,
                        'samples': samples_list,
                        'error_type': 'invalid boolean',
                        'record_identifiers': invalid_record_ids,
                    })

            # --- LABEL VALIDATION ---
            if (
                label_col in df.columns
                and field_options
                and field_type in (
                    'single_select_option',
                    'dropdown',
                    'multi_select_option',
                )
            ):
                value_to_label = {
                    str(opt.get('value', '')).strip():
                    str(opt.get('valueLabel', '')).strip()
                    for opt in field_options
                    if opt.get('value') is not None
                }

                mismatched_rows = []
                for idx in df.index[visibility_mask]:
                    row_value = df.loc[idx, value_col]
                    row_label = df.loc[idx, label_col]

                    if (pd.isna(row_value) or str(row_value).strip() == '') and \
                       (pd.isna(row_label) or str(row_label).strip() == ''):
                        continue

                    if pd.isna(row_value) or str(row_value).strip() == '':
                        continue

                    row_value_str = str(row_value).strip()
                    expected_label_for_value = value_to_label.get(row_value_str)

                    if expected_label_for_value is not None:
                        row_label_str = (
                            str(row_label).strip()
                            if pd.notna(row_label)
                            else ''
                        )
                        if row_label_str.lower() != expected_label_for_value.lower():
                            uid = df.loc[idx, 'uid'] if 'uid' in df.columns else idx
                            unique_key = df.loc[idx, 'unique_key'] if 'unique_key' in df.columns else None
                            record_identifier = uid if pd.notna(uid) else unique_key
                            if record_identifier is None or pd.isna(record_identifier):
                                record_identifier = idx
                            mismatched_rows.append({
                                'uid': uid,
                                'unique_key': unique_key,
                                'record_identifier': record_identifier,
                                'value': row_value_str,
                                'actual_label': row_label_str,
                                'expected_label': expected_label_for_value
                            })

                if mismatched_rows:
                    label_results.append({
                        'base_key': base_key,
                        'mismatched_rows': mismatched_rows
                    })

        except Exception as e:
            err_msg = f"Type validation failed for {base_key}: {str(e)}"
            errors.append(err_msg)
            type_results.append({
                'base_key': base_key,
                'error': err_msg
            })

    tech_logger.info("\n[TECH] DATA TYPES")
    type_errors_count = len(type_results) + len(label_results)

    for result in type_results:
        if 'error' in result:
            tech_logger.error(f"❌ ERROR: {result['error']}")
            _add_issue(
                category="tech",
                issue_type="type_validation_error",
                issue_message=result["error"],
                affected_records=0,
                field_key=result.get("base_key"),
            )
        else:
            samples_str = f" | Samples: {result['samples']}" if result['samples'] else ""
            tech_logger.error(f"❌ '{result['base_key']}': {result['invalid_count']} {result['error_type']} values{samples_str}")
            sample_ids = []
            sample_values = []
            for sample in result.get("samples", []):
                sample_text = str(sample)
                if "=" in sample_text:
                    uid, value = sample_text.split("=", 1)
                    sample_ids.append(uid)
                    sample_values.append(value)
                else:
                    sample_values.append(sample_text)
            _add_issue(
                category="tech",
                issue_type=result["error_type"].replace(" ", "_"),
                issue_message=f"Field '{result['base_key']}' has {result['invalid_count']} {result['error_type']} values",
                affected_records=result["invalid_count"],
                field_key=result["base_key"],
                affected_neotree_ids=sample_ids,
                affected_record_ids=result.get("record_identifiers"),
                sample_values={"samples": sample_values},
                actual_value_sample=", ".join(sample_values[:5]) if sample_values else None,
            )

    for result in label_results:
        mismatch_count = len(result['mismatched_rows'])
        samples = [f"{m['uid']}:val={m['value']}/lbl={m['actual_label']}" for m in result['mismatched_rows'][:5]]
        tech_logger.error(f"❌ '{result['base_key']}': {mismatch_count} label mismatches | {samples}")
        errors.append(f"Field '{result['base_key']}': {mismatch_count} label mismatches")
        _add_issue(
            category="tech",
            issue_type="label_mismatch",
            issue_message=f"Field '{result['base_key']}' has label mismatches",
            affected_records=mismatch_count,
            field_key=result["base_key"],
            affected_neotree_ids=[m["uid"] for m in result["mismatched_rows"][:5]],
            affected_record_ids=[
                m["record_identifier"]
                for m in result["mismatched_rows"]
            ],
            sample_values={"samples": result["mismatched_rows"][:5]},
        )

    if type_errors_count == 0:
        tech_logger.info(f"✓ All data types valid")
    else:
        tech_logger.info(f"Summary: {type_errors_count} fields with errors")

    tech_logger.info("\n[TECH] DATA QUALITY")

    # TECH-3.1 Schema-aware completeness metrics
    schema_completeness_pct = (
        (populated_schema_cells / visible_schema_cells) * 100
        if visible_schema_cells
        else 100.0
    )
    required_completeness_pct = (
        (populated_required_cells / required_visible_cells) * 100
        if required_visible_cells
        else 100.0
    )
    tech_logger.info(
        "   Visible schema values populated: %.2f%% (%s/%s)",
        schema_completeness_pct,
        populated_schema_cells,
        visible_schema_cells,
    )
    tech_logger.info(
        "   Required visible values populated: %.2f%% (%s/%s)",
        required_completeness_pct,
        populated_required_cells,
        required_visible_cells,
    )

    # TECH-3.2 Consistency Checks (value-label pairs)
    inconsistencies = 0
    for value_col in [col for col in df.columns if col.endswith('.value')]:
        base_key = value_col[:-6]
        label_col = f"{base_key}.label"

        if label_col in df.columns and base_key in field_info:
            field = field_info[base_key]
            is_optional = field.get('optional', True)
            is_confidential = _is_confidential(field)

            # Only check consistency for required fields
            if not is_optional and not is_confidential:
                visibility_mask = _field_visibility_mask(field)
                inconsistent_mask = cast(
                    pd.Series,
                    visibility_mask & df[value_col].isna() & df[label_col].notna()
                )
                if inconsistent_mask.sum() > 0:
                    inconsistencies += 1
                    inconsistent_count = inconsistent_mask.sum()

                    # Special handling for UID field
                    if base_key.lower() == 'uid':
                        if 'unique_key' in df.columns:
                            sample_identifiers = df["unique_key"][inconsistent_mask].head(5).tolist()
                            identifier_label = "unique_keys"
                        else:
                            sample_identifiers = df[inconsistent_mask].head(5).index.tolist()
                            identifier_label = "Row indices"
                    else:
                        sample_identifiers = get_safe_sample_uids(df, inconsistent_mask, 5)
                        identifier_label = "UIDs"

                    tech_logger.error(f"❌ '{base_key}': {inconsistent_count} NULL value but non-NULL label | {identifier_label}: {sample_identifiers}")
                    errors.append(f"Required field '{base_key}' has {inconsistent_count} NULL values with non-NULL labels")
                    _add_issue(
                        category="tech",
                        issue_type="value_label_inconsistency",
                        issue_message=f"Field '{base_key}' has NULL value but non-NULL label",
                        affected_records=int(inconsistent_count),
                        field_key=base_key,
                        affected_neotree_ids=sample_identifiers,
                        affected_record_ids=get_record_identifiers(df, inconsistent_mask),
                    )

    if inconsistencies == 0:
        tech_logger.info("   ✓ No value-label inconsistencies in required fields")
    else:
        tech_logger.error(f"   ❌ {inconsistencies} required fields with inconsistencies")

    # TECH-3.3 Referential Integrity & Record Distribution
    if 'uid' in df.columns:
        unique_uids = df['uid'].nunique()
        total_rows = len(df)
        avg_records_per_uid = total_rows / unique_uids if unique_uids > 0 else 0
        tech_logger.info(f"   UIDs: {unique_uids} unique | {total_rows} total rows | Avg: {avg_records_per_uid:.2f} records/UID")

    impl_logger.info("\n[IMPLEMENTATION] REQUIRED FIELDS")
    if required_results:
        for result in required_results:
            # Determine the label for sample identifiers
            if result.get('is_uid_field', False):
                identifier_label = "unique_keys" if 'unique_key' in df.columns else "Row indices"
            else:
                identifier_label = "UIDs"

            impl_logger.error(f"❌ '{result['base_key']}': {result['null_count']}/{result['total_count']} ({result['null_pct']:.1f}%) NULL | {identifier_label}: {result['sample_identifiers']}")
            errors.append(f"Required field '{result['base_key']}' has {result['null_count']} NULL values")
            _add_issue(
                category="implementation",
                issue_type="required_field_null",
                issue_message=f"Required field '{result['base_key']}' has NULL values",
                affected_records=result["null_count"],
                field_key=result["base_key"],
                affected_neotree_ids=result["sample_identifiers"],
                affected_record_ids=result["record_identifiers"],
                sample_values={"null_pct": result["null_pct"], "eligible_records": result["total_count"]},
            )
        impl_logger.info(f"Summary: {len([r for r in required_results])} fields checked, {len(required_results)} with errors")
    else:
        # Count how many required fields were checked
        required_count = sum(
            1
            for f in field_info.values()
            if not f.get('optional', True) and not _is_confidential(f)
        )
        if required_count > 0:
            impl_logger.info(f"✓ All {required_count} required fields populated")

    impl_logger.info("\n[IMPLEMENTATION] VALUE RANGES")
    if range_results:
        for result in range_results:
            violation_count = len(result['violations'])
            violation_pct = (violation_count / result['total']) * 100
            samples = result["violations"][:5]
            samples_str = ", ".join(
                f"{sample['identifier']}={sample['recorded_value']} ({sample['reason']})"
                for sample in samples
            )
            impl_logger.error(f"❌ '{result['base_key']}': {violation_count}/{result['total']} ({violation_pct:.1f}%) out of [{result['min_val']}, {result['max_val']}] | {samples_str}")
            errors.append(f"Field '{result['base_key']}': {violation_count} out-of-range values")
            _add_issue(
                category="implementation",
                issue_type="range_violation",
                issue_message=f"Field '{result['base_key']}' has out-of-range values",
                affected_records=violation_count,
                field_key=result["base_key"],
                affected_neotree_ids=[
                    sample["identifier"]
                    for sample in samples
                ],
                affected_record_ids=[
                    sample["identifier"]
                    for sample in result["violations"]
                ],
                sample_values={
                    "samples": [
                        {
                            "uid": sample["uid"],
                            "unique_key": sample["unique_key"],
                            "row_index": sample["row_index"],
                            "recorded_value": sample["recorded_value"],
                            "reason": sample["reason"],
                        }
                        for sample in samples
                    ],
                    "violation_pct": violation_pct,
                },
                min_value=result["min_val"],
                max_value=result["max_val"],
                actual_value_sample=", ".join(
                    str(sample["recorded_value"])
                    for sample in samples
                ),
            )
        impl_logger.info(f"Summary: {len(range_results)} fields checked, {len(range_results)} with violations")
    else:
        # Count fields with actual (non-empty) min or max values
        range_count = sum(1 for f in field_info.values()
                         if (f.get('minValue') is not None and str(f.get('minValue')).strip() != '') or
                            (f.get('maxValue') is not None and str(f.get('maxValue')).strip() != ''))
        if range_count > 0:
            impl_logger.info(f"✓ All {range_count} range-validated fields valid")

    comp_logger.info("\n[COMPLIANCE] SENSITIVE/CONFIDENTIAL DATA CHECK")

    # Known sensitive keywords (static list)
    drop_keywords = ['surname', 'firstname', 'dobtob', 'column_name', 'mothcell',
                     'dob.value', 'dob.label', 'kinaddress', 'kincell', 'kinname']

    found_sensitive_columns = []
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in drop_keywords:
            found_sensitive_columns.append(col)

    # Check for fields marked as confidential in schema (dynamic)
    confidential_fields_found = []
    # Handle both dict and list formats
    schema_fields = field_info.values() if isinstance(field_info, dict) else schema
    for field in schema_fields:
        field_key = field.get('key')
        is_confidential = _is_confidential(field)

        if is_confidential:
            # Check if this field exists in the dataset
            value_col = f"{field_key}.value"
            label_col = f"{field_key}.label"

            if value_col in df.columns or label_col in df.columns:
                field_label = field.get('label', field_key)
                confidential_fields_found.append({
                    'key': field_key,
                    'label': field_label,
                    'has_value': value_col in df.columns,
                    'has_label': label_col in df.columns
                })

    sensitive_columns_with_data = []
    if found_sensitive_columns:
        sensitive_mask = df[found_sensitive_columns].notna().any(axis=1)
        affected_records = int(sensitive_mask.sum())
        if affected_records > 0:
            sensitive_columns_with_data = found_sensitive_columns
            sample_uids = get_safe_sample_uids(df, sensitive_mask, 5)
            comp_logger.error(
                f"❌ {len(sensitive_columns_with_data)} known sensitive column(s) with data: "
                f"{', '.join(sensitive_columns_with_data)} | Records: {affected_records} | UIDs: {sample_uids}"
            )
            warnings.append(
                f"Found {len(sensitive_columns_with_data)} sensitive/unwanted columns with data: "
                f"{', '.join(sensitive_columns_with_data)}"
            )
            _add_issue(
                category="compliance",
                issue_type="known_sensitive_columns",
                issue_message="Known sensitive columns found in dataset",
                affected_records=affected_records,
                severity="warning",
                affected_neotree_ids=sample_uids,
                affected_record_ids=get_record_identifiers(df, sensitive_mask),
                sample_values={"columns": sensitive_columns_with_data},
            )

    confidential_fields_with_data = []
    if confidential_fields_found:
        for field in confidential_fields_found:
            columns = []
            affected_mask = pd.Series(False, index=df.index)
            if field['has_value']:
                value_col = f"{field['key']}.value"
                columns.append(value_col)
                affected_mask |= df[value_col].notna()
            if field['has_label']:
                label_col = f"{field['key']}.label"
                columns.append(label_col)
                affected_mask |= df[label_col].notna()

            affected_records = int(affected_mask.sum())

            if affected_records <= 0:
                continue

            sample_uids = get_safe_sample_uids(df, affected_mask, 5)
            confidential_fields_with_data.append({
                "field": field,
                "columns": columns,
                "affected_records": affected_records,
                "sample_uids": sample_uids,
                "record_identifiers": get_record_identifiers(df, affected_mask),
            })

            _add_issue(
                category="compliance",
                issue_type="confidential_field_present",
                issue_message=f"Confidential field '{field['key']}' found in dataset",
                affected_records=affected_records,
                field_key=field["key"],
                affected_neotree_ids=sample_uids,
                affected_record_ids=get_record_identifiers(df, affected_mask),
                sample_values={"columns": columns},
            )

        if confidential_fields_with_data:
            comp_logger.error(f"❌ {len(confidential_fields_with_data)} schema-based confidential field(s) with data")
            for issue in confidential_fields_with_data[:3]:
                field = issue["field"]
                comp_logger.error(
                    f"   {field['key']} ({field['label']}): {', '.join(issue['columns'])} "
                    f"| Records: {issue['affected_records']} | UIDs: {issue['sample_uids']}"
                )
            if len(confidential_fields_with_data) > 3:
                comp_logger.error(f"   ... and {len(confidential_fields_with_data) - 3} more")
            errors.append(f"Found {len(confidential_fields_with_data)} confidential fields with data in dataset")

    if not sensitive_columns_with_data and not confidential_fields_with_data:
        comp_logger.info("✓ No sensitive/confidential data detected")

    _insert_validation_issues(issues)


def not_90_percent_similar_to_label(x, reference_value):
    """Check if value is less than 90% similar to reference."""
    if x is None:
        return True
    ratio = SequenceMatcher(None, str(x).lower(), str(reference_value).lower()).ratio()
    return ratio < 0.9


def send_log_via_email(log_file_path: str, email_receivers, category: str = "validation"):
    """Send validation log via email with PDF attachment."""
    with open(log_file_path, 'r') as f:
        log_content = f.read()

    if 'ERROR' in log_content or "WARN" in log_content:
        MAIL_HOST = str('smtp.' + params['mail_host'])
        MAIL_USERNAME = params['MAIL_USERNAME'.lower()]
        MAIL_PASSWORD = params['MAIL_PASSWORD'.lower()]
        MAIL_FROM_ADDRESS = params['MAIL_FROM_ADDRESS'.lower()]
        country = params['country']

        pdf_options = {
            'page-size': 'A4',
            'margin-top': '10mm',
            'margin-right': '15mm',
            'margin-bottom': '10mm',
            'margin-left': '15mm',
            'encoding': "UTF-8",
            'no-outline': None,
            'enable-local-file-access': None,
        }

        msg = EmailMessage()
        category_label = category.replace("_", " ").title()
        msg['Subject'] = f'Data Validation {category_label} Log - {country}'
        msg['From'] = MAIL_FROM_ADDRESS

        recipients = _normalise_email_receivers(email_receivers)
        if not recipients:
            logging.warning(f"No valid email recipients configured for {category} validation log")
            return False

        msg['To'] = ', '.join(recipients)

        html_body = get_html_validation_template(country, log_content)

        try:
            # output_path=None returns the rendered PDF as bytes instead of
            # writing it to a (predictable, world-readable) file under /tmp --
            # the report can contain patient identifiers/field values.
            pdf_bytes = pdfkit.from_string(html_body, None, options=pdf_options)
        except Exception as e:
            logging.error(f"Failed to create PDF: {str(e)}")
            return False

        msg.set_content(f"Your {category_label} validation log is attached as PDF.")
        msg.add_alternative(html_body, subtype='html')

        try:
            msg.add_attachment(
                pdf_bytes,
                maintype='application',
                subtype='pdf',
                filename=f'validation_{category}.pdf'
            )
        except Exception as e:
            logging.error(f"Failed to attach PDF: {str(e)}")
            return False

        try:
            with smtplib.SMTP(MAIL_HOST, 587) as server:
                server.starttls()
                server.login(MAIL_USERNAME, MAIL_PASSWORD)
                server.send_message(msg)
            logging.info("Error log emailed successfully.")
            return True
        except Exception as e:
            logging.error(f"Failed to send email: {str(e)}")
            return False

    return False
