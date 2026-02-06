import pandas as pd
import logging
from typing import List, Optional, cast
from conf.base.catalog import params
from conf.common.sql_functions import inject_sql, run_query_and_return_df,generate_create_insert_sql,generateAndRunUpdateQuery, escape_special_characters
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from conf.common.sql_functions import get_table_column_names
from conf.common.scripts import process_dataframe_with_types_raw_data
from conf.common.hospital_config import hospital_conf
from conf.common.scripts import get_script, merge_script_data
from conf.base.catalog import cron_log_file, cron_time, env
from conf.common.format_error import formatError
from data_pipeline.pipelines.data_engineering.queries.assorted_queries import read_raw_data_not_joined_in_all_table
cron_log = open(cron_log_file, "a+")


def is_empty_df(df: Optional[object]) -> bool:
    if df is None:
        return True
    if isinstance(df, (pd.DataFrame, pd.Series)):
        return df.empty
    return False


def get_metadata_for_script(script_type: str):
    """
    Retrieve and merge metadata for a given script type from all hospitals.

    Args:
        script_type: The script type (e.g., 'admissions', 'discharges')

    Returns:
        Dictionary of merged metadata or empty dict if unavailable
    """
    try:
        hospital_scripts = hospital_conf()
        if not hospital_scripts:
            logging.warning(f"No hospital configuration found for {script_type}")
            return {}

        merged_script_data = None
        for hospital in hospital_scripts:
            ids = hospital_scripts[hospital]
            script_id_entry = ids.get(script_type, '')

            if not script_id_entry:
                continue

            script_ids = str(script_id_entry).split(',')
            for script_id in script_ids:
                script_id = script_id.strip()
                if not script_id:
                    continue

                script_json = get_script(script_id)
                if script_json is not None:
                    merged_script_data = merge_script_data(merged_script_data, script_json)

        if merged_script_data is None:
            logging.warning(f"No metadata found for script type: {script_type}")
            return {}

        return merged_script_data
    except Exception as e:
        logging.error(f"Error retrieving metadata for {script_type}: {str(e)}")
        return {}


def validate_and_process_admissions(adm_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and process admissions dataframe against admissions metadata.
    Applies type coercion and maintains case sensitivity of keys.

    Args:
        adm_df: Raw admissions DataFrame

    Returns:
        Processed admissions DataFrame with type coercion applied
    """
    if is_empty_df(adm_df):
        return adm_df

    try:
        # Get metadata for admissions script
        adm_metadata = get_metadata_for_script('admissions')

        if not adm_metadata:
            logging.warning("No metadata found for admissions - processing without type coercion")
            return adm_df

        # Process using metadata while preserving case sensitivity
        processed_adm = process_dataframe_with_types_raw_data(adm_df, adm_metadata)
        logging.info(f"Processed admissions dataframe: {len(adm_df.columns)} → {len(processed_adm.columns)} columns")

        return processed_adm
    except Exception as e:
        logging.error(f"Error processing admissions dataframe: {str(e)}")
        logging.info("Returning unprocessed admissions dataframe")
        return adm_df


def validate_and_process_discharges(dis_df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and process discharges dataframe against discharges metadata.
    Applies type coercion and maintains case sensitivity of keys.

    Args:
        dis_df: Raw discharges DataFrame

    Returns:
        Processed discharges DataFrame with type coercion applied
    """
    if is_empty_df(dis_df):
        return dis_df

    try:
        # Get metadata for discharges script
        dis_metadata = get_metadata_for_script('discharges')

        if not dis_metadata:
            logging.warning("No metadata found for discharges - processing without type coercion")
            return dis_df

        # Process using metadata while preserving case sensitivity
        processed_dis = process_dataframe_with_types_raw_data(dis_df, dis_metadata)
        logging.info(f"Processed discharges dataframe: {len(dis_df.columns)} → {len(processed_dis.columns)} columns")

        return processed_dis
    except Exception as e:
        logging.error(f"Error processing discharges dataframe: {str(e)}")
        logging.info("Returning unprocessed discharges dataframe")
        return dis_df


def create_all_merged_admissions_discharges(
    new_adm: pd.DataFrame,
    new_dis: pd.DataFrame,
    existing_merged: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Build/extend a merged admissions+discharges dataframe with incremental updates.

    - Admissions are de-duplicated on (uid, facility, DateTimeAdmission.value).
    - Matching is done on uid+facility with hierarchical discriminators.
    - Unmatched admissions/discharges are retained as standalone rows.
    """

    # ---------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------
    def normalize_key_columns(df: pd.DataFrame) -> pd.DataFrame:
        if is_empty_df(df):
            return df

        rename_map = {}
        if "uid" not in df.columns:
            for alt in ("UID", "NeoTreeID", "NeotreeID", "Neotree_ID", "neotree_id", "neotreeid", "uid_dis"):
                if alt in df.columns:
                    rename_map[alt] = "uid"
                    break
            if "uid" not in rename_map:
                uid_like = [
                    col for col in df.columns
                    if "uid" in str(col).lower()
                ]
                if uid_like:
                    rename_map[uid_like[0]] = "uid"

        if "facility" not in df.columns:
            for alt in ("Facility", "facility_id", "facilityId","facility_dis"):
                if alt in df.columns:
                    rename_map[alt] = "facility"
                    break

        if rename_map:
            df = df.rename(columns=rename_map)

        return df

    def ensure_discharge_keys(df: pd.DataFrame) -> pd.DataFrame:
        if is_empty_df(df):
            return df
        if "uid" not in df.columns and "uid_dis" in df.columns:
            df["uid"] = df["uid_dis"]
        if "facility" not in df.columns and "facility_dis" in df.columns:
            df["facility"] = df["facility_dis"]
        return df

    def ensure_required_columns(df: pd.DataFrame, label: str) -> pd.DataFrame:
        if is_empty_df(df):
            return df

        missing = [col for col in ("uid", "facility") if col not in df.columns]
        if missing:
            logging.error(f"{label} dataframe missing required columns {missing}; skipping {label} processing.")
            # Ensure we always return a DataFrame (avoid returning a Series in edge cases)
            cols = list(df.columns) if hasattr(df, "columns") else []
            return pd.DataFrame(columns=cols)

        return df

    def drop_unwanted_base_columns(df: pd.DataFrame) -> pd.DataFrame:
        if is_empty_df(df):
            return df

        allowed_base = {
            "uid",
            "facility",
            "unique_key",
            "unique_key_dis",
            "has_admission",
            "has_discharge",
            "is_closed",
            "match_status",
        }
        cols = set(df.columns)
        drop_cols = []
        for col in cols:
            if col in allowed_base or "." in col:
                continue
            if f"{col}.value" in cols or f"{col}.label" in cols:
                drop_cols.append(col)

        if drop_cols:
            df = df.drop(columns=drop_cols)

        return df

    # ---------------------------------------------------------
    # VALIDATE INPUT AND NORMALISE TYPES
    # ---------------------------------------------------------

    new_adm = validate_and_process_admissions(new_adm)
    new_dis = validate_and_process_discharges(new_dis)

    new_adm = normalize_key_columns(new_adm)
    new_dis = normalize_key_columns(new_dis)
    new_dis = ensure_discharge_keys(new_dis)
    new_adm = drop_unwanted_base_columns(new_adm)
    new_dis = drop_unwanted_base_columns(new_dis)

    new_adm = ensure_required_columns(new_adm, "Admissions")
    new_dis = ensure_required_columns(new_dis, "Discharges")

    if 'OFC.value' in new_adm.columns:
        new_adm['OFC.value'] = pd.to_numeric(new_adm['OFC.value'], errors='coerce')

    if 'OFCDis.value' in new_dis.columns:
        new_dis['OFCDis.value'] = pd.to_numeric(new_dis['OFCDis.value'], errors='coerce')

    def ensure_match_columns(df: pd.DataFrame, required: List[str]) -> pd.DataFrame:
        if is_empty_df(df):
            return df
        missing = [col for col in required if col not in df.columns]
        if missing:
            for col in missing:
                df[col] = pd.NA
        return df

    def effective_discharge_datetime(df: pd.DataFrame) -> pd.Series:
        if is_empty_df(df):
            return pd.Series(dtype="datetime64[ns]")
        discharge = df["DateTimeDischarge.value"] if "DateTimeDischarge.value" in df.columns else pd.Series(pd.NA, index=df.index)
        death = df["DateTimeDeath.value"] if "DateTimeDeath.value" in df.columns else pd.Series(pd.NA, index=df.index)
        return discharge.combine_first(death)

    def deduplicate_admissions(df: pd.DataFrame) -> pd.DataFrame:
        if is_empty_df(df):
            return df
        if "DateTimeAdmission.value" not in df.columns:
            return df.drop_duplicates(subset=["uid", "facility"], keep="first")
        df = df.sort_values(["uid", "facility", "DateTimeAdmission.value"])
        return df.drop_duplicates(subset=["uid", "facility", "DateTimeAdmission.value"], keep="first")

    # ---------------------------------------------------------
    # NORMALIZE / DEDUPE
    # ---------------------------------------------------------

    new_adm = deduplicate_admissions(new_adm)

    if existing_merged is None:
        existing_merged = pd.DataFrame()
    else:
        existing_merged = existing_merged.copy()
    existing_merged = cast(pd.DataFrame, existing_merged)
    existing_merged = normalize_key_columns(existing_merged)
    existing_merged = ensure_discharge_keys(existing_merged)
    existing_merged = drop_unwanted_base_columns(existing_merged)
    if not is_empty_df(existing_merged):
        existing_merged["_source"] = "existing"

    # ---------------------------------------------------------
    # PREP POOLS FOR MATCHING
    # ---------------------------------------------------------

    admission_cols = list(new_adm.columns)
    discharge_cols = list(new_dis.columns)

    def is_discharge_column(col: str) -> bool:
        col_lower = col.lower()
        return (
            col_lower.endswith("_dis")
            or "discharge" in col_lower
            or "death" in col_lower
            or col_lower in {
                "ofcdis.value",
                "birthweight.value_dis",
                "temperature.value_dis",
                "unique_key_dis",
            }
        )

    meta_columns = {"has_admission", "has_discharge", "is_closed", "match_status"}
    if not is_empty_df(existing_merged):
        for col in existing_merged.columns:
            if col in meta_columns:
                continue
            if col in admission_cols or col in discharge_cols:
                continue
            if is_discharge_column(col):
                discharge_cols.append(col)
            else:
                admission_cols.append(col)

    if not is_empty_df(existing_merged):
        existing_merged = ensure_match_columns(
            existing_merged,
            ["has_admission", "has_discharge", "is_closed", "match_status"],
        )

    def extract_existing_unmatched_admissions() -> pd.DataFrame:
        if is_empty_df(existing_merged):
            return pd.DataFrame(columns=admission_cols)

        existing_merged_df = cast(pd.DataFrame, existing_merged)
        mask = pd.Series(False, index=existing_merged_df.index)
        if "has_admission" in existing_merged_df.columns and "has_discharge" in existing_merged_df.columns:
            mask = (existing_merged_df["has_admission"] == True) & (existing_merged_df["has_discharge"] == False)
        else:
            discharge_null = existing_merged_df[discharge_cols].isna().all(axis=1) if discharge_cols else True
            admission_present = existing_merged_df[admission_cols].notna().any(axis=1)
            mask = discharge_null & admission_present

        df = existing_merged_df.loc[mask, admission_cols].copy()
        df = cast(pd.DataFrame, df)
        df["_merged_index"] = existing_merged_df.loc[mask].index
        df["_source"] = "existing"
        return df

    def extract_existing_unmatched_discharges() -> pd.DataFrame:
        if is_empty_df(existing_merged):
            return pd.DataFrame(columns=discharge_cols)

        existing_merged_df = cast(pd.DataFrame, existing_merged)
        mask = pd.Series(False, index=existing_merged_df.index)
        if "has_admission" in existing_merged_df.columns and "has_discharge" in existing_merged_df.columns:
            mask = (existing_merged_df["has_admission"] == False) & (existing_merged_df["has_discharge"] == True)
        else:
            admission_null = existing_merged_df[admission_cols].isna().all(axis=1) if admission_cols else True
            discharge_present = existing_merged_df[discharge_cols].notna().any(axis=1)
            mask = admission_null & discharge_present

        df = existing_merged_df.loc[mask, discharge_cols].copy()
        df = cast(pd.DataFrame, df)
        df["_merged_index"] = existing_merged_df.loc[mask].index
        df["_source"] = "existing"
        return df

    admissions_pool = pd.concat(
        [
            extract_existing_unmatched_admissions(),
            new_adm.assign(_merged_index=pd.NA, _source="new"),
        ],
        ignore_index=True,
    )
    discharges_pool = pd.concat(
        [
            extract_existing_unmatched_discharges(),
            new_dis.assign(_merged_index=pd.NA, _source="new"),
        ],
        ignore_index=True,
    )

    admissions_pool = ensure_required_columns(admissions_pool, "Admissions")
    discharges_pool = ensure_required_columns(discharges_pool, "Discharges")

    if "DateTimeAdmission.value" in admissions_pool.columns:
        admissions_pool["DateTimeAdmission.value"] = pd.to_datetime(
            admissions_pool["DateTimeAdmission.value"], errors="coerce"
        )
    if "DateTimeDischarge.value" in discharges_pool.columns:
        discharges_pool["DateTimeDischarge.value"] = pd.to_datetime(
            discharges_pool["DateTimeDischarge.value"], errors="coerce"
        )
    if "DateTimeDeath.value" in discharges_pool.columns:
        discharges_pool["DateTimeDeath.value"] = pd.to_datetime(
            discharges_pool["DateTimeDeath.value"], errors="coerce"
        )

    discharges_pool["_effective_discharge_dt"] = effective_discharge_datetime(discharges_pool)

    # ---------------------------------------------------------
    # MATCHING
    # ---------------------------------------------------------

    matched_admission_indices: set[int] = set()
    updates: list[dict] = []
    new_rows: list[dict] = []
    drops: set[int] = set()

    def fallback_by_datetime(candidates: pd.DataFrame, discharge_row: pd.Series) -> pd.Series:
        admission_dt_col = None
        if "DateTimeAdmission.value" in candidates.columns:
            admission_dt_col = "DateTimeAdmission.value"
        elif "DateTimeAdmission.value_dis" in candidates.columns:
            admission_dt_col = "DateTimeAdmission.value_dis"

        if admission_dt_col:
            discharge_dt = discharge_row.get("_effective_discharge_dt")
            if pd.notna(discharge_dt):
                candidates = candidates.copy()
                candidates["_delta"] = discharge_dt - candidates[admission_dt_col]
                candidates["_delta_seconds"] = candidates["_delta"].apply(lambda x: x.total_seconds() if pd.notna(x) else None)
                valid = candidates[candidates["_delta_seconds"] >= 0]
                if not valid.empty:
                    return valid.nsmallest(1, "_delta_seconds").iloc[0]
            try:
                return candidates.sort_values(admission_dt_col).iloc[0]
            except Exception:
                return candidates.iloc[0]
        return candidates.iloc[0]

    for _, dis_row in discharges_pool.iterrows():
        uid_val = dis_row.get("uid")
        facility_val = dis_row.get("facility")

        candidates = admissions_pool[
            (admissions_pool["uid"] == uid_val)
            & (admissions_pool["facility"] == facility_val)
            & (~admissions_pool.index.isin(matched_admission_indices))
        ]

        if candidates.empty:
            if dis_row.get("_source") == "new":
                rec = dis_row.drop(labels=["_merged_index", "_source", "_effective_discharge_dt"], errors="ignore").to_dict()
                rec.update(dict(has_admission=False, has_discharge=True, is_closed=False))
                rec["match_status"] = "unmatched_discharge"
                rec["_source"] = "new"
                new_rows.append(rec)
            continue

        selected = None
        ambiguous = False

        ofc_dis_val = dis_row.get("OFCDis.value")
        ofc_adm_col = "OFC.value" if "OFC.value" in candidates.columns else (
            "OFC.value_dis" if "OFC.value_dis" in candidates.columns else None
        )
        if pd.notna(ofc_dis_val) and ofc_adm_col:
            ofc_matches = candidates[candidates[ofc_adm_col] == ofc_dis_val]
            if len(ofc_matches) == 1:
                selected = ofc_matches.iloc[0]
            elif len(ofc_matches) > 1:
                ambiguous = True

        if selected is None:
            bw_dis_val = dis_row.get("BirthWeight.value_dis")
            bw_adm_col = "BirthWeight.value" if "BirthWeight.value" in candidates.columns else (
                "BirthWeight.value_dis" if "BirthWeight.value_dis" in candidates.columns else None
            )
            if pd.notna(bw_dis_val) and bw_adm_col:
                bw_matches = candidates[candidates[bw_adm_col] == bw_dis_val]
                if len(bw_matches) == 1:
                    selected = bw_matches.iloc[0]
                elif len(bw_matches) > 1:
                    ambiguous = True

        if selected is None:
            selected = fallback_by_datetime(candidates, dis_row)
            ambiguous = True

        if selected is None:
            if dis_row.get("_source") == "new":
                rec = dis_row.drop(labels=["_merged_index", "_source", "_effective_discharge_dt"], errors="ignore").to_dict()
                rec.update(dict(has_admission=False, has_discharge=True, is_closed=False))
                rec["match_status"] = "unmatched_discharge"
                rec["_source"] = "new"
                new_rows.append(rec)
            continue

        idx = cast(int, selected.name)
        matched_admission_indices.add(idx if idx is not None else 0)

        adm_source = selected.get("_source")
        dis_source = dis_row.get("_source")

        adm_data = selected.drop(labels=["_merged_index", "_source"], errors="ignore").to_dict()
        dis_data = dis_row.drop(labels=["_merged_index", "_source", "_effective_discharge_dt"], errors="ignore").to_dict()

        if adm_source == "existing":
            merged_index = int(selected["_merged_index"])
            updates.append(
                dict(
                    _merged_index=merged_index,
                    **dis_data,
                    has_discharge=True,
                    has_admission=True,
                    is_closed=True,
                    match_status="ambiguous" if ambiguous else "matched",
                )
            )
            if dis_source == "existing":
                drops.add(int(dis_row["_merged_index"]))
        elif dis_source == "existing":
            merged_index = int(dis_row["_merged_index"])
            updates.append(
                dict(
                    _merged_index=merged_index,
                    **adm_data,
                    has_discharge=True,
                    has_admission=True,
                    is_closed=True,
                    match_status="ambiguous" if ambiguous else "matched",
                )
            )
        else:
            rec = {**adm_data, **dis_data}
            rec.update(dict(has_admission=True, has_discharge=True, is_closed=True))
            rec["match_status"] = "ambiguous" if ambiguous else "matched"
            rec["_source"] = "new"
            new_rows.append(rec)

        if ambiguous:
            logging.warning(
                "Ambiguous match resolved via fallback for uid=%s facility=%s",
                uid_val,
                facility_val,
            )

    # ---------------------------------------------------------
    # UNMATCHED NEW ADMISSIONS
    # ---------------------------------------------------------

    unmatched_adm = admissions_pool[
        (admissions_pool["_source"] == "new")
        & (~admissions_pool.index.isin(matched_admission_indices))
    ]
    for _, adm_row in unmatched_adm.iterrows():
        rec = adm_row.drop(labels=["_merged_index", "_source"], errors="ignore").to_dict()
        rec.update(dict(has_admission=True, has_discharge=False, is_closed=False))
        rec["match_status"] = "unmatched_admission"
        rec["_source"] = "new"
        new_rows.append(rec)

    # ---------------------------------------------------------
    # APPLY UPDATES TO EXISTING MERGED
    # ---------------------------------------------------------

    if not is_empty_df(existing_merged) and updates:
        for update in updates:
            merged_index = update.pop("_merged_index")
            for col, val in update.items():
                existing_merged.loc[merged_index, col] = val

    if drops and not is_empty_df(existing_merged):
        existing_merged = existing_merged.drop(index=list(drops))

    # Ensure columns consistency and append new rows
    merged_rows_df = pd.DataFrame(new_rows)
    combined = pd.concat([existing_merged, merged_rows_df], ignore_index=True, sort=False)

    combined = ensure_match_columns(
        combined,
        ["has_admission", "has_discharge", "is_closed", "match_status"],
    )

    merged_df = combined[
        (combined["has_admission"] == True) & (combined["has_discharge"] == True)
    ].copy()
    merged_df["is_closed"] = True

    admissions_only = combined[
        (combined["has_admission"] == True) & (combined["has_discharge"] == False)
    ].copy()
    admissions_only["has_admission"] = True
    admissions_only["has_discharge"] = False
    admissions_only["is_closed"] = False
    admissions_only = admissions_only.drop(columns=["uid_dis", "facility_dis"], errors="ignore")

    discharges_only = combined[
        (combined["has_admission"] == False) & (combined["has_discharge"] == True)
    ].copy()
    discharges_only["has_admission"] = False
    discharges_only["has_discharge"] = True
    discharges_only["is_closed"] = False

    

    return dict(
        admissions_only=admissions_only,
        discharges_only=discharges_only,
        merged=merged_df,
    )


def seed_all_table(table_name, schema):
    # CREATE TABLE IF NOT EXIST WITH MINIMAL FIELDS
    create_table_query = f'''CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" (
    "uid" TEXT,
    "facility" TEXT,
    "unique_key" TEXT,
    "unique_key_dis" TEXT,
    "OFC.value" NUMERIC(10,2),
    "OFCDis.value" NUMERIC(10,2),
    "Temperature.value_dis" NUMERIC(10,2),
    "Temperature.value" NUMERIC(10,2),
    "BirthWeight.value_dis" NUMERIC(10,2),
    "BirthWeight.value" NUMERIC(10,2),
    "DateTimeDischarge.value" TIMESTAMP, 
    "DateTimeDeath.value" TIMESTAMP,
    "DateTimeAdmission.value" TIMESTAMP, 
    "has_admission" BOOLEAN DEFAULT FALSE,
    "has_discharge" BOOLEAN DEFAULT FALSE,
    "is_closed" BOOLEAN DEFAULT FALSE
);;'''
    inject_sql(create_table_query, "CREATE ALL TABLE")
    


def merge_raw_admissions_and_discharges(clean_derived_data_output):
    """Main function to clean and join research data."""
    try:
        # Test if previous node has completed successfully
        if not clean_derived_data_output:
            logging.error("Creating Clean Tables Did Not Complete Well.")
            return None

        logging.info("...........Creating Raw Joined Admission Discharges.............")
        country = params['country']
        country_abrev = 'ZIM' if country.lower() == 'zimbabwe' else ('MWI' if country.lower() == 'malawi' else None)
        table_name = f'ALL_{country_abrev}'
        schema = 'derived'
        ###SEED TABLES IF NOT EXISTS
        seed_all_table(table_name, schema)

        existing_merged = pd.DataFrame()
        total_merged_rows = 0
        if table_exists(schema, table_name):
            total_rows_df = run_query_and_return_df(
                f'SELECT COUNT(*) AS cnt FROM {schema}."{table_name}"'
            )
            if total_rows_df is not None and not total_rows_df.empty:
                total_merged_rows = int(total_rows_df.iloc[0]["cnt"])
            existing_merged = run_query_and_return_df(
                f'SELECT * FROM {schema}."{table_name}" WHERE is_closed = FALSE'
            )
            if existing_merged is None:
                existing_merged = pd.DataFrame()

        if total_merged_rows == 0:
            admissions_condition = "1=1"
            discharges_condition = "1=1"
        else:
            admissions_condition = (
                f'NOT EXISTS ('
                f'SELECT 1 FROM {schema}."{table_name}" b '
                f'WHERE a.uid=b.uid AND a.facility=b.facility '
                f'AND a."DateTimeAdmission.value"=b."DateTimeAdmission.value" '
                f'AND a."DateTimeAdmission.value" is not NULL'
                f')'
            )
            discharges_condition = (
                f'NOT EXISTS ('
                f'SELECT 1 FROM {schema}."{table_name}" b '
                f'WHERE a.uid=b.uid AND a.facility=b.facility '
                f'AND ('
                f'('
                f'a."DateTimeDischarge.value" IS NOT NULL '
                f'AND b."DateTimeDischarge.value" IS NOT NULL '
                f'AND a."DateTimeDischarge.value" = b."DateTimeDischarge.value"'
                f') '
                f'OR ('
                f'a."DateTimeDischarge.value" IS NULL '
                f'AND b."DateTimeDischarge.value" IS NULL '
                f'AND a."DateTimeDeath.value" IS NOT NULL '
                f'AND b."DateTimeDeath.value" IS NOT NULL '
                f'AND a."DateTimeDeath.value" = b."DateTimeDeath.value"'
                f')'
                f')'
                f') '
            )

        adm_df = pd.DataFrame()
        dis_df = pd.DataFrame()
        if table_exists(schema, "admissions"):
            admission_query = read_raw_data_not_joined_in_all_table("admissions", admissions_condition)
            adm_df = run_query_and_return_df(admission_query)
            if adm_df is None:
                adm_df = pd.DataFrame()
        else:
            logging.warning('Table derived."admissions" does not exist; skipping admissions fetch.')

        if table_exists(schema, "discharges"):
            discharges_query = read_raw_data_not_joined_in_all_table("discharges", discharges_condition)
            dis_df = run_query_and_return_df(discharges_query)
            if dis_df is None:
                dis_df = pd.DataFrame()
        else:
            logging.warning('Table derived."discharges" does not exist; skipping discharges fetch.')
        admissions_columns = None
        if table_exists(schema, "admissions") and not is_empty_df(dis_df):
            admissions_columns = pd.DataFrame(
                get_table_column_names("admissions", "derived"),
                columns=["column_name"],
            )
            # Rename discharge columns that collide with admissions columns
            if not is_empty_df(admissions_columns) and "column_name" in admissions_columns.columns:
                adm_cols = set(admissions_columns["column_name"].tolist())
                rename_map = {
                    c: f"{c}_dis"
                    for c in dis_df.columns
                    if c in adm_cols and c != "unique_key"
                }
                if rename_map:
                    dis_df = dis_df.rename(columns=rename_map)


        if not is_empty_df(adm_df) or not is_empty_df(dis_df):
            if (not is_empty_df(dis_df) and 'unique_key' in dis_df.columns):
                dis_df.rename(columns={'unique_key': 'unique_key_dis'}, inplace=True)
                  
            merged_outputs = create_all_merged_admissions_discharges(
                adm_df,
                dis_df,
                existing_merged=existing_merged,
            )
        else:
            merged_outputs = dict(
                admissions_only=existing_merged[
                    (existing_merged.get("has_admission") == True)
                    & (existing_merged.get("has_discharge") == False)
                ].copy(),
                discharges_only=existing_merged[
                    (existing_merged.get("has_admission") == False)
                    & (existing_merged.get("has_discharge") == True)
                ].copy(),
                merged=pd.DataFrame(),
            )



        admissions_only = merged_outputs.get("admissions_only", pd.DataFrame())
        discharges_only = merged_outputs.get("discharges_only", pd.DataFrame())
        merged_df = merged_outputs.get("merged", pd.DataFrame())
        if isinstance(merged_df, pd.Series):
            merged_df = merged_df.to_frame().T

        admissions_only_new = admissions_only
        if not is_empty_df(admissions_only) and "_source" in admissions_only.columns:
            admissions_only_new = admissions_only[admissions_only["_source"] == "new"]
        if not is_empty_df(admissions_only_new):
            admissions_only_new = admissions_only_new.drop(
                columns=["_source", "_merged_index"], errors="ignore"
            )
            generate_create_insert_sql(admissions_only_new, schema, table_name)

        discharges_only_new = discharges_only
        if not is_empty_df(discharges_only) and "_source" in discharges_only.columns:
            discharges_only_new = discharges_only[discharges_only["_source"] == "new"]
        if not is_empty_df(discharges_only_new):
            discharges_only_new = discharges_only_new.drop(
                columns=["_source", "_merged_index"], errors="ignore"
            )
            generate_create_insert_sql(discharges_only_new, schema, table_name)

        if isinstance(merged_df, pd.DataFrame) and not is_empty_df(merged_df):
            merged_new = merged_df
            merged_existing = pd.DataFrame()
            if "_source" in merged_df.columns:
                merged_new = merged_df[merged_df["_source"] == "new"]
                merged_existing = merged_df[merged_df["_source"] == "existing"]

            if not is_empty_df(merged_new):
                merged_new = merged_new.drop(columns=["_source", "_merged_index"], errors="ignore")
                generate_create_insert_sql(merged_new, schema, table_name)

            if not is_empty_df(merged_existing):
                merged_existing = merged_existing.drop(columns=["_source", "_merged_index"], errors="ignore")
                generateAndRunUpdateQuery(f'{schema}."{table_name}"', merged_existing)

        return dict(status="Success", message="Raw Data Merging Complete")
    except Exception as e:
        logging.error("!!! An error occurred Cleaning Derived Data: ")
        cron_log.write(
            f"StartTime: {cron_time}   Instance: {env}   Status: Failed Stage: Data Cleaning for Research"
        )
        cron_log.close()
        logging.error(formatError(e))
      
