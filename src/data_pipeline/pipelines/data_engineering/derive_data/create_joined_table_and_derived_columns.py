import logging
from datetime import datetime as dt
from typing import Callable, List, Optional, Tuple

import pandas as pd  # type: ignore

from conf.base.catalog import catalog, params
from data_pipeline.pipelines.data_engineering.utils.date_validator import is_date, is_date_formatable
from data_pipeline.pipelines.data_engineering.utils.custom_date_formatter import format_date_without_timezone
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import (
    read_dicharges_not_joined,
    read_admissions_not_joined,
    admissions_without_discharges,
    discharges_not_matched,
    read_all_from_derived_table
)
from conf.common.sql_functions import (
    create_new_columns,
    get_table_column_names,
    generateAndRunUpdateQuery,
    generate_create_insert_sql,
    get_date_column_names,
    run_query_and_return_df,
    inject_sql,
    inject_sql_with_return,
    column_exists
)
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from data_pipeline.pipelines.data_engineering.data_validation.validate import reset_log
from data_pipeline.pipelines.data_engineering.queries.data_fix import deduplicate_table, count_table_columns, fix_column_limit_error


def coalesce_duplicate_columns(df: pd.DataFrame, context: str) -> pd.DataFrame:
    duplicate_names = list(
        dict.fromkeys(df.columns[df.columns.duplicated(keep=False)].tolist())
    )
    if not duplicate_names:
        return df

    logging.warning(
        "Coalescing duplicate columns in %s: %s",
        context,
        duplicate_names,
    )
    coalesced_columns = []
    seen = set()
    for position, column_name in enumerate(df.columns):
        if column_name in seen:
            continue
        seen.add(column_name)
        matching_positions = [
            index
            for index, name in enumerate(df.columns)
            if name == column_name
        ]
        if len(matching_positions) == 1:
            series = df.iloc[:, position]
        else:
            duplicate_values = df.iloc[:, matching_positions]
            series = duplicate_values.bfill(axis=1).iloc[:, 0]
        coalesced_columns.append(series.rename(column_name))

    return pd.concat(coalesced_columns, axis=1)


def first_column_values(rows) -> List[str]:
    if rows is None:
        return []
    if isinstance(rows, pd.DataFrame):
        if rows.empty:
            return []
        values = rows["column_name"] if "column_name" in rows.columns else rows.iloc[:, 0]
        return [str(value) for value in values.dropna().tolist()]

    values = []
    for row in rows:
        if isinstance(row, dict):
            value = row.get("column_name")
        elif isinstance(row, (list, tuple)):
            value = row[0] if row else None
        elif hasattr(row, "_mapping"):
            mapping = row._mapping
            value = mapping.get("column_name") if "column_name" in mapping else row[0]
        else:
            value = row
        if value is not None:
            values.append(str(value))
    return values


def column_as_series(df: pd.DataFrame, column: str) -> pd.Series:
    values = df[column]
    if isinstance(values, pd.DataFrame):
        return values.bfill(axis=1).iloc[:, 0]
    return values


def add_missing_columns(df: pd.DataFrame, table_name: str, schema: str = 'derived') -> None:
    """
    Add any new columns from dataframe to existing table.

    Proactively checks column limit and rebuilds table if approaching PostgreSQL's 1600 limit.
    This prevents column limit errors by reclaiming dropped columns before adding new ones.
    """
    if not table_exists(schema, table_name):
        return

    # PROACTIVE COLUMN LIMIT CHECK
    # Check current column usage and rebuild if > 1200 to prevent hitting the 1600 limit
    col_info = count_table_columns(table_name, schema)

    if col_info['total'] > 1200:
        logging.warning(f"Table {schema}.{table_name} has {col_info['total']} columns (> 1200 threshold)")
        logging.warning(f"  Active: {col_info['active']}, Dropped: {col_info['dropped']}")

        if col_info['dropped'] > 0:
            logging.info(f"Proactively rebuilding {schema}.{table_name} to reclaim {col_info['dropped']} dropped columns")
            rebuild_success = fix_column_limit_error(table_name, schema, auto_rebuild=True)

            if rebuild_success:
                logging.info(f"✓ Successfully reclaimed {col_info['dropped']} column slots in {schema}.{table_name}")
            else:
                logging.warning(f"⚠ Rebuild of {schema}.{table_name} did not complete successfully")
        else:
            logging.warning(f"⚠ No dropped columns to reclaim. Table genuinely has {col_info['active']} active columns")

    # Now proceed with adding new columns
    df = coalesce_duplicate_columns(df, f"{schema}.{table_name} column check")
    existing_col_names = set(first_column_values(get_table_column_names(table_name, schema)))

    new_columns = set(df.columns) - existing_col_names

    if new_columns:
        logging.info(f"Adding {len(new_columns)} new column(s) to {schema}.{table_name}")
        column_pairs = [(col, str(df[col].dtype)) for col in new_columns]
        if column_pairs:
            create_new_columns(table_name, schema, column_pairs)


def get_query_for_table(table_name: str, joined_table_name: str, not_joined_query_fn, all_query_fn) -> str:
    """Determine which query to use based on whether joined table exists."""
    if table_exists('derived', joined_table_name):
        return not_joined_query_fn()
    else:
        return all_query_fn(table_name)


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def qualified_table(schema: str, table_name: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table_name)}"


def sql_literal(value) -> str:
    if pd.isna(value):
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def join_predicate(left_alias: str, right_alias: str, column: str) -> str:
    left_col = f"{left_alias}.{quote_identifier(column)}"
    right_col = f"{right_alias}.{quote_identifier(column)}"
    if column == "uid":
        return f"UPPER(TRIM({left_col}::text)) = UPPER(TRIM({right_col}::text))"
    return f"{left_col} = {right_col}"


def read_records_not_already_joined(
    source_table: str,
    joined_table: str,
    join_columns: List[str],
    source_unique_key_col: str = "unique_key",
    joined_unique_key_col: str = "unique_key",
    schema: str = "derived",
) -> Optional[str]:
    if not table_exists(schema, source_table):
        logging.warning(f"Skipping join input {schema}.{source_table}: table does not exist")
        return None

    if not table_exists(schema, joined_table):
        return read_all_from_derived_table(source_table)

    if not column_exists(schema, joined_table, joined_unique_key_col):
        logging.info(
            f"Column '{joined_unique_key_col}' missing on {schema}.{joined_table}; "
            f"reading all rows from {schema}.{source_table}"
        )
        return read_all_from_derived_table(source_table)

    source_alias = "src"
    joined_alias = "j"
    predicates = [
        join_predicate(source_alias, joined_alias, column)
        for column in join_columns
    ]
    predicates.append(
        f"{source_alias}.{quote_identifier(source_unique_key_col)} = "
        f"{joined_alias}.{quote_identifier(joined_unique_key_col)}"
    )
    predicate_sql = "\n      AND ".join(predicates)

    return f"""SELECT *
FROM {qualified_table(schema, source_table)} {source_alias}
WHERE NOT EXISTS (
    SELECT 1
    FROM {qualified_table(schema, joined_table)} {joined_alias}
    WHERE {predicate_sql}
);"""


def read_peads_admissions_not_joined() -> Optional[str]:
    return read_records_not_already_joined(
        source_table="peads_admissions",
        joined_table="joined_peads_admissions_discharges",
        join_columns=["uid"],
        joined_unique_key_col="unique_key",
    )


def read_peads_discharges_not_joined() -> Optional[str]:
    return read_records_not_already_joined(
        source_table="peads_discharges",
        joined_table="joined_peads_admissions_discharges",
        join_columns=["uid"],
        joined_unique_key_col="unique_key_discharge",
    )


def read_one_sided_joined_records(
    joined_table: str,
    present_key_col: str,
    missing_key_col: str,
    schema: str = "derived",
) -> Optional[str]:
    if not table_exists(schema, joined_table):
        return None

    for column in [present_key_col, missing_key_col]:
        if not column_exists(schema, joined_table, column):
            logging.info(
                f"Skipping one-sided reconciliation for {schema}.{joined_table}: "
                f"column '{column}' does not exist"
            )
            return None

    return f"""SELECT *
FROM {qualified_table(schema, joined_table)}
WHERE {quote_identifier(present_key_col)} IS NOT NULL
  AND {quote_identifier(present_key_col)}::TEXT <> ''
  AND (
      {quote_identifier(missing_key_col)} IS NULL
      OR {quote_identifier(missing_key_col)}::TEXT = ''
  );"""


def present_values(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(False, index=df.index)

    values = column_as_series(df, column)
    value_text = values.astype(str).str.strip()
    return (
        values.notna()
        & (value_text != "")
        & (~value_text.str.lower().isin({"nan", "none", "nat", "<na>"}))
    )


def key_set(df: pd.DataFrame, column: str) -> set:
    if df.empty or column not in df.columns:
        return set()

    values = column_as_series(df, column).loc[present_values(df, column)]
    return set(values.astype(str))


def drop_rows_by_keys(df: pd.DataFrame, column: str, keys: set) -> pd.DataFrame:
    if df.empty or not keys or column not in df.columns:
        return df

    values = column_as_series(df, column).astype(str)
    if column == "uid":
        values = values.str.strip().str.upper()
    return df[~values.isin(keys)].copy()


def drop_empty_placeholder_columns(
    df: pd.DataFrame,
    columns_to_keep: List[str],
) -> pd.DataFrame:
    if df.empty:
        return df

    keep = set(columns_to_keep)
    columns_to_drop = [
        column for column in df.columns
        if column not in keep and not present_values(df, column).any()
    ]
    return df.drop(columns=columns_to_drop, errors="ignore")


def delete_one_sided_joined_rows(
    joined_table: str,
    key_column: str,
    keys: set,
    missing_key_column: str,
    schema: str = "derived",
    batch_size: int = 1000,
) -> None:
    if not keys:
        return

    key_list = list(keys)
    for index in range(0, len(key_list), batch_size):
        batch = key_list[index:index + batch_size]
        key_values = ", ".join(sql_literal(value) for value in batch)
        delete_query = f"""DELETE FROM {qualified_table(schema, joined_table)}
WHERE {quote_identifier(key_column)} IN ({key_values})
  AND (
      {quote_identifier(missing_key_column)} IS NULL
      OR {quote_identifier(missing_key_column)}::TEXT = ''
  );;"""
        inject_sql(delete_query, f"DELETE STALE ONE-SIDED {schema}.{joined_table}")


def delete_placeholders_with_joined_counterparts(
    joined_table: str,
    schema: str = "derived",
) -> None:
    if not table_exists(schema, joined_table):
        return

    for column in ["unique_key", "unique_key_discharge"]:
        if not column_exists(schema, joined_table, column):
            return

    table_sql = qualified_table(schema, joined_table)
    cleanup_query = f"""
DELETE FROM {table_sql} stale
WHERE stale."unique_key" IS NOT NULL
  AND stale."unique_key"::TEXT <> ''
  AND (stale."unique_key_discharge" IS NULL OR stale."unique_key_discharge"::TEXT = '')
  AND EXISTS (
      SELECT 1
      FROM {table_sql} joined
      WHERE joined."unique_key" = stale."unique_key"
        AND joined."unique_key_discharge" IS NOT NULL
        AND joined."unique_key_discharge"::TEXT <> ''
  );;

DELETE FROM {table_sql} stale
WHERE stale."unique_key_discharge" IS NOT NULL
  AND stale."unique_key_discharge"::TEXT <> ''
  AND (stale."unique_key" IS NULL OR stale."unique_key"::TEXT = '')
  AND EXISTS (
      SELECT 1
      FROM {table_sql} joined
      WHERE joined."unique_key_discharge" = stale."unique_key_discharge"
        AND joined."unique_key" IS NOT NULL
        AND joined."unique_key"::TEXT <> ''
  );;
"""
    inject_sql(cleanup_query, f"DELETE JOINED PLACEHOLDERS {schema}.{joined_table}")


def prepare_joined_dataset_for_write(
    joined_df: pd.DataFrame,
    table_name: str,
    schema: str = "derived",
) -> pd.DataFrame:
    joined_df = coalesce_duplicate_columns(joined_df, f"{schema}.{table_name} write")
    add_missing_columns(joined_df, table_name, schema)

    date_column_types = first_column_values(get_date_column_names(table_name, schema))
    if date_column_types:
        joined_df = format_date_without_timezone(joined_df, date_column_types)

    joined_df.columns = joined_df.columns.astype(str)
    return joined_df.loc[:, ~joined_df.columns.str.match(r'^\d+$|^[a-zA-Z]$', na=False)]


def write_joined_dataset(joined_df: pd.DataFrame, table_name: str, schema: str = "derived") -> None:
    if joined_df is None or joined_df.empty:
        logging.info(f"No rows to save for {schema}.{table_name}")
        return

    joined_df = prepare_joined_dataset_for_write(joined_df, table_name, schema)
    logging.info(f"Saving joined dataset to {schema}.{table_name}: {len(joined_df)} rows")
    generate_create_insert_sql(joined_df, schema, table_name)


def build_reconciled_one_sided_matches(
    existing_one_sided_df: pd.DataFrame,
    counterpart_df: pd.DataFrame,
    joined_table: str,
    join_columns: List[str],
    existing_key_col: str,
    counterpart_key_col: str,
    existing_is_left: bool,
) -> Tuple[pd.DataFrame, set, set]:
    if existing_one_sided_df.empty or counterpart_df.empty:
        return pd.DataFrame(), set(), set()

    existing_one_sided_df = coalesce_duplicate_columns(
        existing_one_sided_df,
        f"{joined_table} existing one-sided records",
    )
    counterpart_df = coalesce_duplicate_columns(
        counterpart_df,
        f"{joined_table} counterpart records",
    )
    existing_one_sided_df = drop_empty_placeholder_columns(
        existing_one_sided_df,
        columns_to_keep=join_columns + [existing_key_col],
    )
    left_df = existing_one_sided_df if existing_is_left else counterpart_df
    right_df = counterpart_df if existing_is_left else existing_one_sided_df
    reconciled_df = createJoinedDataSet(
        left_df,
        right_df,
        joined_table_name=joined_table,
        join_columns=join_columns,
    )
    if reconciled_df.empty:
        return pd.DataFrame(), set(), set()

    matched_mask = present_values(reconciled_df, existing_key_col) & present_values(
        reconciled_df,
        counterpart_key_col,
    )
    matched_df = reconciled_df[matched_mask].copy()
    if matched_df.empty:
        return pd.DataFrame(), set(), set()

    existing_keys = key_set(matched_df, existing_key_col)
    counterpart_keys = key_set(matched_df, counterpart_key_col)
    logging.info(
        f"Reconciled {len(matched_df)} existing one-sided rows in {joined_table}"
    )

    return matched_df, existing_keys, counterpart_keys


def calculate_date_differences_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Length of Stay and Length of Life using vectorized operations.

    OPTIMIZATION: Replaces iterrows() loop with pandas vectorized operations.
    Performance: ~100-1000x faster for large datasets.
    """
    df = coalesce_duplicate_columns(df, "joined date calculations")
    # Initialize columns
    df['LengthOfStay.label'] = 'Length of Stay'
    df['LengthOfLife.label'] = 'Length of Life'
    df['LengthOfStay.value'] = None
    df['LengthOfLife.value'] = None

    # Ensure date columns are datetime
    date_cols = ['DateTimeAdmission.value', 'DateTimeDischarge.value']
    if 'DateTimeDeath.value' in df.columns:
        date_cols.append('DateTimeDeath.value')

    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(column_as_series(df, col), errors='coerce')

    # Calculate Length of Stay (vectorized)
    if 'DateTimeDischarge.value' in df.columns and 'DateTimeAdmission.value' in df.columns:
        # Create mask for valid dates
        valid_dates = (
            column_as_series(df, 'DateTimeDischarge.value').notna() &
            column_as_series(df, 'DateTimeAdmission.value').notna()
        )
        # Calculate days difference
        df.loc[valid_dates, 'LengthOfStay.value'] = (
            df.loc[valid_dates, 'DateTimeDischarge.value'] -
            df.loc[valid_dates, 'DateTimeAdmission.value']
        ).dt.days

    # Calculate Length of Life (vectorized)
    if 'DateTimeDeath.value' in df.columns and 'DateTimeAdmission.value' in df.columns:
        valid_death_dates = (
            column_as_series(df, 'DateTimeDeath.value').notna() &
            column_as_series(df, 'DateTimeAdmission.value').notna()
        )
        df.loc[valid_death_dates, 'LengthOfLife.value'] = (
            df.loc[valid_death_dates, 'DateTimeDeath.value'] -
            df.loc[valid_death_dates, 'DateTimeAdmission.value']
        ).dt.days

    return df


def index_exists(schema: str, index_name: str) -> bool:
    query = (
        "SELECT 1 FROM pg_indexes "
        f"WHERE schemaname = '{schema}' AND indexname = '{index_name}' LIMIT 1;"
    )
    result = inject_sql_with_return(query)
    return bool(result)


def ensure_index(schema: str, table: str, columns: List[str], index_name: str) -> None:
    if not table_exists(schema, table):
        return

    for col in columns:
        if not column_exists(schema, table, col):
            logging.warning(
                f"Skipping index {schema}.{index_name}: column '{col}' missing on {schema}.{table}"
            )
            return

    if index_exists(schema, index_name):
        return

    cols_sql = ", ".join([f'"{c}"' for c in columns])
    create_index = f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{schema}"."{table}" ({cols_sql});;'
    inject_sql(create_index, f"CREATE INDEX {schema}.{index_name}")


def create_and_write_joined_table(
    left_table: str,
    right_table: str,
    joined_table: str,
    join_columns: List[str],
    left_not_joined_query_fn: Optional[Callable[[], str]] = None,
    right_not_joined_query_fn: Optional[Callable[[], str]] = None,
    calculate_derived_columns: bool = True,
) -> None:
    logging.info(f"... Creating {joined_table} from {left_table} and {right_table}")

    if not table_exists("derived", left_table) or not table_exists("derived", right_table):
        logging.warning(
            f"Skipping {joined_table}: source tables {left_table} and/or {right_table} do not exist"
        )
        return

    ensure_index("derived", joined_table, join_columns + ["unique_key"], f"idx_{joined_table}_left_join")
    ensure_index("derived", joined_table, join_columns + ["unique_key_discharge"], f"idx_{joined_table}_right_join")
    ensure_index("derived", left_table, join_columns + ["unique_key"], f"idx_{left_table}_join")
    ensure_index("derived", right_table, join_columns + ["unique_key"], f"idx_{right_table}_join")

    if table_exists("derived", joined_table) and left_not_joined_query_fn is not None:
        left_query = left_not_joined_query_fn()
    else:
        left_query = read_records_not_already_joined(
            left_table,
            joined_table,
            join_columns,
            joined_unique_key_col="unique_key",
        )

    if table_exists("derived", joined_table) and right_not_joined_query_fn is not None:
        right_query = right_not_joined_query_fn()
    else:
        right_query = read_records_not_already_joined(
            right_table,
            joined_table,
            join_columns,
            joined_unique_key_col="unique_key_discharge",
        )

    left_df = run_query_and_return_df(left_query) if left_query else pd.DataFrame()
    logging.info(f"{left_table} loaded: {len(left_df)} rows")

    right_df = run_query_and_return_df(right_query) if right_query else pd.DataFrame()
    logging.info(f"{right_table} loaded: {len(right_df)} rows")

    output_frames = []
    stale_left_keys = set()
    stale_right_keys = set()

    existing_left_query = read_one_sided_joined_records(
        joined_table=joined_table,
        present_key_col="unique_key",
        missing_key_col="unique_key_discharge",
    )
    existing_left_df = run_query_and_return_df(existing_left_query) if existing_left_query else pd.DataFrame()
    reconciled_left_df, matched_left_keys, consumed_right_keys = build_reconciled_one_sided_matches(
        existing_one_sided_df=existing_left_df,
        counterpart_df=right_df,
        joined_table=joined_table,
        join_columns=join_columns,
        existing_key_col="unique_key",
        counterpart_key_col="unique_key_discharge",
        existing_is_left=True,
    )
    if not reconciled_left_df.empty:
        output_frames.append(reconciled_left_df)
        stale_left_keys.update(matched_left_keys)
        right_df = drop_rows_by_keys(right_df, "unique_key", consumed_right_keys)

    existing_right_query = read_one_sided_joined_records(
        joined_table=joined_table,
        present_key_col="unique_key_discharge",
        missing_key_col="unique_key",
    )
    existing_right_df = run_query_and_return_df(existing_right_query) if existing_right_query else pd.DataFrame()
    reconciled_right_df, matched_right_keys, consumed_left_keys = build_reconciled_one_sided_matches(
        existing_one_sided_df=existing_right_df,
        counterpart_df=left_df,
        joined_table=joined_table,
        join_columns=join_columns,
        existing_key_col="unique_key_discharge",
        counterpart_key_col="unique_key",
        existing_is_left=False,
    )
    if not reconciled_right_df.empty:
        output_frames.append(reconciled_right_df)
        stale_right_keys.update(matched_right_keys)
        left_df = drop_rows_by_keys(left_df, "unique_key", consumed_left_keys)

    new_joined_df = createJoinedDataSet(
        left_df,
        right_df,
        joined_table_name=joined_table,
        join_columns=join_columns,
        calculate_derived_columns=calculate_derived_columns,
    )
    if not new_joined_df.empty:
        output_frames.append(new_joined_df)

    joined_df = pd.concat(output_frames, ignore_index=True, sort=False) if output_frames else pd.DataFrame()
    logging.info(f"{joined_table} dataset created: {len(joined_df)} rows")

    write_joined_dataset(joined_df, joined_table)
    delete_one_sided_joined_rows(
        joined_table=joined_table,
        key_column="unique_key",
        keys=stale_left_keys,
        missing_key_column="unique_key_discharge",
    )
    delete_one_sided_joined_rows(
        joined_table=joined_table,
        key_column="unique_key_discharge",
        keys=stale_right_keys,
        missing_key_column="unique_key",
    )
    delete_placeholders_with_joined_counterparts(joined_table)


def join_table():
    logging.info("... Starting script to create joined table")

    # Read the raw admissions and discharge data into dataframes
    logging.info("... Fetching admissions and discharges data")
    reset_log('logs/queries.log')

    try:
        create_and_write_joined_table(
            left_table="admissions",
            right_table="discharges",
            joined_table="joined_admissions_discharges",
            join_columns=["uid", "facility"],
            left_not_joined_query_fn=read_admissions_not_joined,
            right_not_joined_query_fn=read_dicharges_not_joined,
        )

        create_and_write_joined_table(
            left_table="peads_admissions",
            right_table="peads_discharges",
            joined_table="joined_peads_admissions_discharges",
            join_columns=["uid"],
            left_not_joined_query_fn=read_peads_admissions_not_joined,
            right_not_joined_query_fn=read_peads_discharges_not_joined,
        )

    except Exception as e:
        logging.exception("!!! An error occurred creating joined dataframe")
        raise e

    # Now write the table back to the database
    logging.info("... Writing the output back to the database")
    try:
        # MERGE DISCHARGES CURRENTLY ADDED TO THE NEW DATA SET
        discharge_exists = table_exists('derived', 'discharges')
        joined_exists = table_exists('derived', 'joined_admissions_discharges')

        if discharge_exists and joined_exists:
            read_admissions_query_2 = admissions_without_discharges()
            adm_df_2 = run_query_and_return_df(read_admissions_query_2)

            read_discharges_query_2 = discharges_not_matched()
            dis_df_2 = run_query_and_return_df(read_discharges_query_2)

            if adm_df_2 is not None and dis_df_2 is not None and not adm_df_2.empty:
                jn_adm_dis_2 = createJoinedDataSet(adm_df_2, dis_df_2)

                # Clean column names
                jn_adm_dis_2.columns = jn_adm_dis_2.columns.astype(str)
                jn_adm_dis_2 = jn_adm_dis_2.loc[:, ~jn_adm_dis_2.columns.str.match(r'^\d+$|^[a-zA-Z]$', na=False)]

                if not jn_adm_dis_2.empty:
                    # Filter for rows with NeoTreeOutcome
                    if 'NeoTreeOutcome.value' not in jn_adm_dis_2.columns:
                        filtered_df = pd.DataFrame()
                    else:
                        outcome_values = column_as_series(jn_adm_dis_2, 'NeoTreeOutcome.value')
                        filtered_df = jn_adm_dis_2[
                            outcome_values.notna() &
                            (outcome_values != '')
                        ]
                    if isinstance(filtered_df, pd.Series):
                        filtered_df = filtered_df.to_frame().T
                    generateAndRunUpdateQuery('derived.joined_admissions_discharges', filtered_df)
                    deduplicate_table('joined_admissions_discharges')

    except Exception as e:
        logging.error("!!! An error occurred writing join output back to the database")
        raise e

    logging.info("... Join script completed!")

def calculate_match_score(row: pd.Series) -> float:
    """
    Calculate similarity score between admission and discharge records.

    Compares clinical measurements to determine how well a discharge matches an admission:
    - OFC (Occipital-Frontal Circumference)
    - Gestation (weeks)
    - BirthWeight (grams)

    Returns a score where higher values indicate better matches.
    """
    score = 0.0
    comparisons_made = 0

    # Compare OFC: admission OFC.value vs discharge OFCDis.value
    if pd.notna(row.get('OFC.value')) and pd.notna(row.get('OFCDis.value')):
        try:
            ofc_adm = float(row['OFC.value'])
            ofc_dis = float(row['OFCDis.value'])
            # Score based on how close they are (max 10 points if identical)
            # Penalize 1 point per cm difference
            diff = abs(ofc_adm - ofc_dis)
            score += max(0, 10 - diff)
            comparisons_made += 1
        except (ValueError, TypeError):
            pass

    # Compare Gestation: admission Gestation.value vs discharge Gestation.value_discharge
    if pd.notna(row.get('Gestation.value')) and pd.notna(row.get('Gestation.value_discharge')):
        try:
            gest_adm = float(row['Gestation.value'])
            gest_dis = float(row['Gestation.value_discharge'])
            # Score based on how close they are (max 10 points if identical)
            # Penalize 1 point per week difference
            diff = abs(gest_adm - gest_dis)
            score += max(0, 10 - diff)
            comparisons_made += 1
        except (ValueError, TypeError):
            pass

    # Compare BirthWeight: admission BirthWeight.value vs discharge BirthWeight.value_discharge
    # Note: BirthWeight On Discharge is "not to be trusted", so weight it less
    if pd.notna(row.get('BirthWeight.value')) and pd.notna(row.get('BirthWeight.value_discharge')):
        try:
            bw_adm = float(row['BirthWeight.value'])
            bw_dis = float(row['BirthWeight.value_discharge'])
            # Score based on how close they are (max 5 points if identical - weighted less)
            # Penalize 1 point per 500g difference
            diff = abs(bw_adm - bw_dis) / 500
            score += max(0, 5 - diff)
            comparisons_made += 1
        except (ValueError, TypeError):
            pass

    # Normalize score if we made comparisons
    # If no comparisons possible, return -1 to indicate no data
    if comparisons_made == 0:
        return -1.0

    return score


def calculate_match_scores_vectorized(df: pd.DataFrame) -> pd.Series:
    scores = pd.Series(0.0, index=df.index)
    comparisons = pd.Series(0, index=df.index)

    score_specs = [
        ('OFC.value', 'OFCDis.value', 10.0, 1.0),
        ('Gestation.value', 'Gestation.value_discharge', 10.0, 1.0),
        ('BirthWeight.value', 'BirthWeight.value_discharge', 5.0, 500.0),
    ]

    for left_col, right_col, max_score, divisor in score_specs:
        if left_col not in df.columns or right_col not in df.columns:
            continue

        left_values = pd.to_numeric(column_as_series(df, left_col), errors='coerce')
        right_values = pd.to_numeric(column_as_series(df, right_col), errors='coerce')
        valid = left_values.notna() & right_values.notna()
        if not valid.any():
            continue

        diffs = (left_values[valid] - right_values[valid]).abs() / divisor
        scores.loc[valid] += (max_score - diffs).clip(lower=0)
        comparisons.loc[valid] += 1

    scores.loc[comparisons == 0] = -1.0
    return scores


def resolve_duplicate_matches(merged_df: pd.DataFrame, adm_unique_col: str = '_adm_idx') -> pd.DataFrame:
    """
    Resolve duplicate matches by selecting best discharge match for EACH individual admission.

    For each admission with multiple discharge matches, calculates similarity scores
    based on clinical measurements (OFC, Gestation, BirthWeight) and keeps only
    the discharge with the highest score for that specific admission.

    Args:
        merged_df: DataFrame after merge, potentially with duplicate matches per admission
        adm_unique_col: Column name that uniquely identifies each admission row

    Returns:
        DataFrame with duplicates resolved by keeping best discharge match per admission
    """
    logging.info("Resolving duplicate admission-discharge matches using clinical measurement comparison")

    # Count how many discharge matches each admission has
    merged_df['_match_count'] = merged_df.groupby(adm_unique_col)[adm_unique_col].transform('count')

    # Separate admissions with multiple matches from those with single matches
    duplicates = merged_df[merged_df['_match_count'] > 1].copy()
    non_duplicates = merged_df[merged_df['_match_count'] == 1].copy()

    logging.info(f"Found {len(non_duplicates)} admissions with single discharge match")
    logging.info(f"Found {duplicates[adm_unique_col].nunique()} admissions with multiple discharge matches ({len(duplicates)} total rows)")

    if duplicates.empty:
        # No duplicates to resolve
        result = non_duplicates.drop(columns=['_match_count'])
        return result

    # Calculate match scores for all duplicate matches.
    duplicates['_match_score'] = calculate_match_scores_vectorized(duplicates)

    # For each individual admission, keep the discharge with the highest match score
    def select_best_discharge_for_admission(group):
        """Select the best matching discharge for this specific admission."""
        # If all scores are -1 (no data to compare), keep the first discharge
        if (group['_match_score'] == -1).all():
            logging.debug(f"No clinical data to compare for admission {group.iloc[0][adm_unique_col]} - keeping first discharge")
            return group.iloc[0:1]

        # Otherwise, take the discharge with highest score
        valid_scores = group[group['_match_score'] >= 0]
        if not valid_scores.empty:
            best_idx = valid_scores['_match_score'].idxmax()
            best_match = valid_scores.loc[best_idx]
            logging.debug(f"Best match score {best_match['_match_score']:.2f} for admission {best_match[adm_unique_col]}")
            return valid_scores.loc[[best_idx]]
        else:
            return group.iloc[0:1]

    # Group by each individual admission and select best discharge
    resolved_duplicates = duplicates.groupby(adm_unique_col, group_keys=False).apply(select_best_discharge_for_admission)

    logging.info(f"Resolved {len(duplicates)} duplicate matches down to {len(resolved_duplicates)} best matches")

    # Combine resolved duplicates with non-duplicates
    # Reset indices to ensure no duplicate index values during concatenation
    non_duplicates = non_duplicates.reset_index(drop=True)
    resolved_duplicates = resolved_duplicates.reset_index(drop=True)
    result = pd.concat([non_duplicates, resolved_duplicates], ignore_index=True)

    # Clean up temporary columns
    result = result.drop(columns=['_match_count', '_match_score'], errors='ignore')

    return result


def createJoinedDataSet(
    adm_df: pd.DataFrame,
    dis_df: pd.DataFrame,
    joined_table_name: str = "joined_admissions_discharges",
    join_columns: Optional[List[str]] = None,
    calculate_derived_columns: bool = True,
) -> pd.DataFrame:
    """
    Create joined admissions-discharges dataset with intelligent duplicate resolution.

    Uses clinical measurements (OFC, Gestation, BirthWeight) to match each admission
    with its most appropriate discharge record when duplicates exist.
    """
    join_columns = join_columns or ['uid', 'facility']
    logging.info(f"Creating joined dataset {joined_table_name} on {join_columns}")

    if adm_df.empty and dis_df.empty:
        logging.warning("Empty input dataframes - returning empty result")
        return pd.DataFrame()

    adm_df = adm_df.copy()
    dis_df = dis_df.copy()
    adm_df = coalesce_duplicate_columns(adm_df, f"{joined_table_name} left input")
    dis_df = coalesce_duplicate_columns(dis_df, f"{joined_table_name} right input")
    available_join_columns = [
        column for column in join_columns
        if column in adm_df.columns and column in dis_df.columns
    ]

    if not available_join_columns:
        logging.warning(
            f"None of the requested join columns {join_columns} are present in both inputs"
        )
        return pd.concat([adm_df, dis_df], ignore_index=True, sort=False)

    if not adm_df.empty:
        # Preserve each admission identity so duplicate discharge matches can be resolved safely.
        adm_df['_adm_idx'] = range(len(adm_df))

    merge_left_on = available_join_columns
    merge_right_on = available_join_columns
    temp_join_columns = []
    if 'uid' in available_join_columns:
        uid_join_col = '_join_uid'
        adm_df[uid_join_col] = adm_df['uid'].astype(str).str.strip().str.upper()
        dis_df[uid_join_col] = dis_df['uid'].astype(str).str.strip().str.upper()
        merge_left_on = [
            uid_join_col if column == 'uid' else column
            for column in available_join_columns
        ]
        merge_right_on = merge_left_on
        temp_join_columns.append(uid_join_col)

    # Merge admissions and discharges on the configured join columns.
    # Use a full outer join so unmatched admissions and unmatched discharges are both retained.
    jn_adm_dis = adm_df.merge(
        dis_df,
        how='outer',
        left_on=merge_left_on,
        right_on=merge_right_on,
        suffixes=('', '_discharge'),
        indicator=True
    )
    if 'uid_discharge' in jn_adm_dis.columns:
        if 'uid' in jn_adm_dis.columns:
            jn_adm_dis['uid'] = jn_adm_dis['uid'].combine_first(
                jn_adm_dis['uid_discharge']
            )
            jn_adm_dis = jn_adm_dis.drop(columns=['uid_discharge'])
        else:
            jn_adm_dis = jn_adm_dis.rename(columns={'uid_discharge': 'uid'})

    logging.info(
        f"Initial merge created {len(jn_adm_dis)} rows from "
        f"{len(adm_df)} admissions and {len(dis_df)} discharges"
    )

    # Pandas creates `_merge` because `indicator=True` is set above.
    # We use it only as a temporary internal marker to identify `right_only`
    # discharge rows before dropping the column again.
    right_only_rows = jn_adm_dis[jn_adm_dis['_merge'] == 'right_only'].copy()
    left_and_matched_rows = jn_adm_dis[jn_adm_dis['_merge'] != 'right_only'].copy()

    if not left_and_matched_rows.empty and '_adm_idx' in left_and_matched_rows.columns:
        left_and_matched_rows = resolve_duplicate_matches(left_and_matched_rows, adm_unique_col='_adm_idx')

    jn_adm_dis = pd.concat([left_and_matched_rows, right_only_rows], ignore_index=True, sort=False)
    jn_adm_dis = coalesce_duplicate_columns(jn_adm_dis, f"{joined_table_name} merged result")

    # `_merge` is not part of the business schema; remove merge bookkeeping now.
    jn_adm_dis = jn_adm_dis.drop(
        columns=['_adm_idx', '_merge', *temp_join_columns],
        errors='ignore',
    )

    dedup_candidates = available_join_columns + ['unique_key', 'unique_key_discharge']
    dedup_subset = [col for col in dedup_candidates if col in jn_adm_dis.columns]
    if dedup_subset:
        jn_adm_dis = jn_adm_dis.drop_duplicates(
            subset=dedup_subset,
            keep='first'
        )

    logging.info(f"After all deduplication: {len(jn_adm_dis)} rows")

    # Add missing columns to database table
    add_missing_columns(jn_adm_dis, joined_table_name)

    # Convert Gestation to numeric
    if 'Gestation.value' in jn_adm_dis.columns:
        jn_adm_dis['Gestation.value'] = pd.to_numeric(
            column_as_series(jn_adm_dis, 'Gestation.value'),
            errors='coerce'
        )

    if calculate_derived_columns:
        # Format dates
        jn_adm_dis = format_date_without_timezone(
            jn_adm_dis,
            ['DateTimeAdmission.value', 'DateTimeDischarge.value']
        )
        jn_adm_dis = coalesce_duplicate_columns(
            jn_adm_dis,
            f"{joined_table_name} formatted dates",
        )

        # OPTIMIZATION: Use vectorized date calculations instead of iterrows()
        jn_adm_dis = calculate_date_differences_vectorized(jn_adm_dis)

    logging.info(f"Finished creating joined dataset: {len(jn_adm_dis)} rows")
    return jn_adm_dis
