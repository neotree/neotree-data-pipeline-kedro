import pandas as pd
import logging
from typing import List, Optional, cast
from conf.base.catalog import params
from conf.common.sql_functions import (
    inject_sql,
    run_query_and_return_df,
    generate_create_insert_sql,
    generateAndRunUpdateQuery,
    escape_special_characters,
    inject_sql_with_return,
    column_exists,
)
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

        if "uid" not in df.columns:
            logging.error(f"{label} dataframe missing required column 'uid'; skipping {label} processing.")
            cols = list(df.columns) if hasattr(df, "columns") else []
            return pd.DataFrame(columns=cols)

        if "facility" not in df.columns:
            # Allow processing to continue; unmatched rows will remain unmatched.
            logging.warning(
                f"{label} dataframe missing 'facility'; inserting unmatched rows with NULL facility."
            )
            df = df.copy()
            df["facility"] = pd.NA

        return df

    def ensure_no_admission_flag(df: pd.DataFrame) -> pd.DataFrame:
        if is_empty_df(df):
            return df
        if "_no_admission_in_base" not in df.columns:
            df = df.copy()
            df["_no_admission_in_base"] = False
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
            if str(col).startswith("_"):
                continue
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
    new_dis = ensure_no_admission_flag(new_dis)

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

    admissions_pool = new_adm.assign(_source="new")
    discharges_pool = new_dis.assign(_source="new")

    admissions_pool = ensure_required_columns(admissions_pool, "Admissions")
    discharges_pool = ensure_required_columns(discharges_pool, "Discharges")
    discharges_pool = ensure_no_admission_flag(discharges_pool)

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
    new_rows: list[dict] = []

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

        if dis_row.get("_no_admission_in_base") is True:
            if dis_row.get("_source") == "new":
                rec = dis_row.drop(
                    labels=["_merged_index", "_source", "_effective_discharge_dt", "_no_admission_in_base"],
                    errors="ignore",
                ).to_dict()
                rec.update(dict(has_admission=False, has_discharge=True, is_closed=False))
                rec["match_status"] = "unmatched_discharge"
                rec["_source"] = "new"
                new_rows.append(rec)
            continue

        candidates = admissions_pool[
            (admissions_pool["uid"] == uid_val)
            & (admissions_pool["facility"] == facility_val)
            & (~admissions_pool.index.isin(matched_admission_indices))
        ]

        if candidates.empty:
            rec = dis_row.drop(
                labels=["_merged_index", "_source", "_effective_discharge_dt", "_no_admission_in_base"],
                errors="ignore",
            ).to_dict()
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
            rec = dis_row.drop(
                labels=["_merged_index", "_source", "_effective_discharge_dt", "_no_admission_in_base"],
                errors="ignore",
            ).to_dict()
            rec.update(dict(has_admission=False, has_discharge=True, is_closed=False))
            rec["match_status"] = "unmatched_discharge"
            rec["_source"] = "new"
            new_rows.append(rec)
            continue

        idx = cast(int, selected.name)
        matched_admission_indices.add(idx if idx is not None else 0)

        adm_data = selected.drop(labels=["_merged_index", "_source"], errors="ignore").to_dict()
        dis_data = dis_row.drop(
            labels=["_merged_index", "_source", "_effective_discharge_dt", "_no_admission_in_base"],
            errors="ignore",
        ).to_dict()
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

    merged_rows_df = pd.DataFrame(new_rows)
    combined = merged_rows_df.copy()

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

        # Ensure indexes for incremental merge performance
        ensure_index(schema, table_name, ["uid", "facility", "DateTimeAdmission.value"], f"idx_{table_name.lower()}_uid_fac_adm")
        ensure_index(schema, table_name, ["uid", "facility", "DateTimeDischarge.value"], f"idx_{table_name.lower()}_uid_fac_dis")
        ensure_index(schema, table_name, ["uid", "facility", "DateTimeDeath.value"], f"idx_{table_name.lower()}_uid_fac_death")
        ensure_index(schema, "admissions", ["uid", "facility", "DateTimeAdmission.value"], "idx_admissions_uid_fac_adm")
        ensure_index(schema, "discharges", ["uid", "facility", "DateTimeDischarge.value"], "idx_discharges_uid_fac_dis")
        ensure_index(schema, "discharges", ["uid", "facility", "DateTimeDeath.value"], "idx_discharges_uid_fac_death")

        total_merged_rows = 0
        if table_exists(schema, table_name):
            total_rows_df = run_query_and_return_df(
                f'SELECT COUNT(*) AS cnt FROM {schema}."{table_name}"'
            )
            if total_rows_df is not None and not total_rows_df.empty:
                total_merged_rows = int(total_rows_df.iloc[0]["cnt"])

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
            # Ensure uid-only unmatched admissions are included
            uid_only_adm = run_query_and_return_df(
                f'''
                SELECT a.* FROM {schema}."admissions" a
                WHERE NOT EXISTS (
                    SELECT 1 FROM {schema}."discharges" d
                    WHERE d.uid = a.uid
                )
                  AND ({admissions_condition})
                '''
            )
            if uid_only_adm is not None and not uid_only_adm.empty:
                adm_df = pd.concat([adm_df, uid_only_adm], ignore_index=True).drop_duplicates()
        else:
            logging.warning('Table derived."admissions" does not exist; skipping admissions fetch.')

        if table_exists(schema, "discharges"):
            discharges_query = read_raw_data_not_joined_in_all_table("discharges", discharges_condition)
            dis_df = run_query_and_return_df(discharges_query)
            if dis_df is None:
                dis_df = pd.DataFrame()
            # Ensure uid-only unmatched discharges are included
            uid_only_dis = run_query_and_return_df(
                f'''
                SELECT a.* FROM {schema}."discharges" a
                WHERE NOT EXISTS (
                    SELECT 1 FROM {schema}."admissions" ad
                    WHERE ad.uid = a.uid
                )
                  AND ({discharges_condition})
                '''
            )
            if not is_empty_df(dis_df) and "_no_admission_in_base" not in dis_df.columns:
                dis_df = dis_df.copy()
                dis_df["_no_admission_in_base"] = False
            if uid_only_dis is not None and not uid_only_dis.empty:
                uid_only_dis = uid_only_dis.copy()
                uid_only_dis["_no_admission_in_base"] = True
                # Keep the uid-only marker when duplicates exist by placing uid_only_dis first
                dis_df = pd.concat([uid_only_dis, dis_df], ignore_index=True).drop_duplicates()
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
            )
        else:
            merged_outputs = dict(
                admissions_only=pd.DataFrame(),
                discharges_only=pd.DataFrame(),
                merged=pd.DataFrame(),
            )



        admissions_only = merged_outputs.get("admissions_only", pd.DataFrame())
        discharges_only = merged_outputs.get("discharges_only", pd.DataFrame())
        merged_df = merged_outputs.get("merged", pd.DataFrame())
        if isinstance(merged_df, pd.Series):
            merged_df = merged_df.to_frame().T

        def _fetch_existing_keys(df: pd.DataFrame, key_cols: List[str]) -> set[tuple]:
            if is_empty_df(df) or not all(col in df.columns for col in key_cols):
                return set()
            keys_df = df.loc[:, key_cols].dropna().astype(str).drop_duplicates()
            if keys_df.empty:
                return set()
            values_rows = ", ".join(
                f"('{escape_special_characters(u)}','{escape_special_characters(f)}','{escape_special_characters(k)}')"
                for u, f, k in keys_df.to_numpy()
            )
            existing = run_query_and_return_df(
                f"""
                SELECT t.uid, t.facility, t."{key_cols[2]}"
                FROM {schema}."{table_name}" AS t
                JOIN (VALUES {values_rows}) AS v(uid, facility, keyval)
                  ON t.uid = v.uid AND t.facility = v.facility AND t."{key_cols[2]}" = v.keyval;
                """
            )
            if existing is None or existing.empty:
                return set()
            return set(
                tuple(row)
                for row in existing.loc[:, ["uid", "facility", key_cols[2]]].astype(str).to_numpy()
            )

        admissions_only_new = admissions_only.copy()
        if not is_empty_df(admissions_only_new):
            admissions_only_new = admissions_only_new.drop(
                columns=["_source", "_merged_index"], errors="ignore"
            )
            if {"uid", "facility", "unique_key"}.issubset(admissions_only_new.columns):
                existing_adm_keys = _fetch_existing_keys(admissions_only_new, ["uid", "facility", "unique_key"])
                if existing_adm_keys:
                    mask = admissions_only_new.apply(
                        lambda r: (str(r.get("uid")), str(r.get("facility")), str(r.get("unique_key"))) not in existing_adm_keys,
                        axis=1,
                    )
                    admissions_only_new = admissions_only_new[mask]
            if not is_empty_df(admissions_only_new):
                generate_create_insert_sql(admissions_only_new, schema, table_name)

        discharges_only_new = discharges_only.copy()
        if not is_empty_df(discharges_only_new):
            discharges_only_new = discharges_only_new.drop(
                columns=["_source", "_merged_index"], errors="ignore"
            )
            if {"uid", "facility", "unique_key_dis"}.issubset(discharges_only_new.columns):
                existing_dis_keys = _fetch_existing_keys(discharges_only_new, ["uid", "facility", "unique_key_dis"])
                if existing_dis_keys:
                    mask = discharges_only_new.apply(
                        lambda r: (str(r.get("uid")), str(r.get("facility")), str(r.get("unique_key_dis"))) not in existing_dis_keys,
                        axis=1,
                    )
                    discharges_only_new = discharges_only_new[mask]
            if not is_empty_df(discharges_only_new):
                generate_create_insert_sql(discharges_only_new, schema, table_name)

        if isinstance(merged_df, pd.DataFrame) and not is_empty_df(merged_df):
            merged_df = merged_df.drop(columns=["_source", "_merged_index"], errors="ignore")
            # Split merged into insert vs update based on unique_key or unique_key_dis
            existing_keys_df = pd.DataFrame()
            existing_adm_keys_df = pd.DataFrame()
            existing_dis_keys_df = pd.DataFrame()
            if {"uid", "facility", "unique_key"}.issubset(merged_df.columns):
                adm_keys = (
                    merged_df.loc[:, ["uid", "facility", "unique_key"]]
                    .dropna()
                    .astype(str)
                    .drop_duplicates()
                )
                if not adm_keys.empty:
                    values_rows = ", ".join(
                        f"('{escape_special_characters(u)}','{escape_special_characters(f)}','{escape_special_characters(k)}')"
                        for u, f, k in adm_keys.to_numpy()
                    )
                    existing_adm_keys_df = run_query_and_return_df(
                        f"""
                        SELECT t.uid, t.facility, t.unique_key, t.unique_key_dis
                        FROM {schema}."{table_name}" AS t
                        JOIN (VALUES {values_rows}) AS v(uid, facility, unique_key)
                          ON t.uid = v.uid AND t.facility = v.facility AND t.unique_key = v.unique_key;
                        """
                    ) or pd.DataFrame()

            if {"uid", "facility", "unique_key_dis"}.issubset(merged_df.columns):
                dis_keys = (
                    merged_df.loc[:, ["uid", "facility", "unique_key_dis"]]
                    .dropna()
                    .astype(str)
                    .drop_duplicates()
                )
                if not dis_keys.empty:
                    values_rows = ", ".join(
                        f"('{escape_special_characters(u)}','{escape_special_characters(f)}','{escape_special_characters(k)}')"
                        for u, f, k in dis_keys.to_numpy()
                    )
                    existing_dis_keys_df = run_query_and_return_df(
                        f"""
                        SELECT t.uid, t.facility, t.unique_key, t.unique_key_dis
                        FROM {schema}."{table_name}" AS t
                        JOIN (VALUES {values_rows}) AS v(uid, facility, unique_key_dis)
                          ON t.uid = v.uid AND t.facility = v.facility AND t.unique_key_dis = v.unique_key_dis;
                        """
                    ) or pd.DataFrame()

            if not existing_adm_keys_df.empty or not existing_dis_keys_df.empty:
                existing_keys_df = pd.concat(
                    [existing_adm_keys_df, existing_dis_keys_df],
                    ignore_index=True,
                )

            merged_df = merged_df.copy()
            for col in ["uid", "facility", "unique_key", "unique_key_dis"]:
                if col in merged_df.columns:
                    merged_df[col] = merged_df[col].astype(str)

            if not existing_keys_df.empty:
                existing_keys_df = existing_keys_df.copy()
                for col in ["uid", "facility", "unique_key", "unique_key_dis"]:
                    if col in existing_keys_df.columns:
                        existing_keys_df[col] = existing_keys_df[col].astype(str)

                key_set = set()
                if {"uid", "facility", "unique_key"}.issubset(existing_keys_df.columns):
                    key_set.update(
                        (u, f, k)
                        for u, f, k in zip(
                            existing_keys_df["uid"],
                            existing_keys_df["facility"],
                            existing_keys_df["unique_key"],
                        )
                        if k not in [None, "None", "nan", "NaT", "<NA>"]
                    )
                dis_key_set = set()
                if {"uid", "facility", "unique_key_dis"}.issubset(existing_keys_df.columns):
                    dis_key_set.update(
                        (u, f, k)
                        for u, f, k in zip(
                            existing_keys_df["uid"],
                            existing_keys_df["facility"],
                            existing_keys_df["unique_key_dis"],
                        )
                        if k not in [None, "None", "nan", "NaT", "<NA>"]
                    )

                def _is_existing(row: pd.Series) -> bool:
                    uid = row.get("uid")
                    facility = row.get("facility")
                    ukey = row.get("unique_key")
                    ukey_dis = row.get("unique_key_dis")
                    if uid is None or facility is None:
                        return False
                    if (uid, facility, ukey) in key_set:
                        return True
                    if ukey_dis is not None and (uid, facility, ukey_dis) in dis_key_set:
                        return True
                    return False

                existing_mask = merged_df.apply(_is_existing, axis=1)
                to_insert = merged_df[~existing_mask]
                to_update = merged_df[existing_mask]
            else:
                to_insert = merged_df
                to_update = pd.DataFrame()

            if isinstance(to_insert, pd.DataFrame) and not is_empty_df(to_insert):
                generate_create_insert_sql(to_insert, schema, table_name)

            if isinstance(to_update, pd.DataFrame) and not is_empty_df(to_update):
                generateAndRunUpdateQuery(f'{schema}."{table_name}"', to_update)

        return dict(status="Success", message="Raw Data Merging Complete")
    except Exception as e:
        logging.error("!!! An error occurred Cleaning Derived Data: ")
        cron_log.write(
            f"StartTime: {cron_time}   Instance: {env}   Status: Failed Stage: Data Cleaning for Research"
        )
        cron_log.close()
        logging.error(formatError(e))
      
