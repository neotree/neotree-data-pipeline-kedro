import logging
import pandas as pd

from data_pipeline.pipelines.data_engineering.utils.field_info import script_field_exists

# Raw field keys renamed upstream (survey/script definition changes), keyed by the
# script/derived-table they land in. Each entry is coalesced old->new via
# coalesce_renamed_keys(), which only fires once that script's live metadata has
# actually adopted the new key -- add future renames here as (old_key, new_key) pairs.
# Shared with data_fix.backfill_all_legacy_key_renames(), which applies the same
# renames to rows already sitting in the database.
LEGACY_KEY_RENAMES = {
    'discharges': [('NeoTreeOutcome', 'NeotreeOutcome')],
    'baseline': [('NeoTreeOutcome', 'NeotreeOutcome')],
    'maternal_outcomes': [('NeoTreeOutcome', 'NeotreeOutcome')],
    'joined_admissions_discharges': [('NeoTreeOutcome', 'NeotreeOutcome')],
}


def key_change(df: pd.DataFrame,row,position,old_key,new_key):
    try:
        if old_key in row and (str(df.at[position,old_key])!= 'nan' or df.at[position,old_key] is not None or str(df.at[position,old_key])!='None'):
            df.at[position,new_key] = df.at[position,old_key]
        return df
    except Exception as ex:
        #logging.info(f'''---CANT CONVERT--{old_key} to {new_key}''')
        return df;


def coalesce_renamed_keys(df: pd.DataFrame, script: str, renames: list) -> pd.DataFrame:
    """
    Copy values from a renamed raw key's exploded ``.value``/``.label`` columns
    into the new key's columns, wherever the new key is empty for that row.

    Each rename is gated on ``script_field_exists(script, new_key)`` so a script
    whose live metadata hasn't been updated to the new key yet is left untouched --
    this is what stops the rename from being applied blindly to scripts that only
    some, not all, of the survey definitions have adopted.

    Idempotent by construction: once a row's new-key column is populated (from a
    prior run or from raw data submitted under the new key), the mask excludes it,
    so rerunning the pipeline is a no-op for that row regardless of which key name
    the underlying raw (immutable) record carries.

    Args:
        df: dataframe holding exploded ``Key.value``/``Key.label`` columns for one script
        script: script name used to look up live metadata, e.g. 'discharges'
        renames: list of (old_key, new_key) base-key pairs (without .value/.label)
    """
    for old_key, new_key in renames:
        if not script_field_exists(script, new_key):
            logging.info(
                "Skipping key rename %s -> %s for script '%s': '%s' not found in current script metadata",
                old_key, new_key, script, new_key,
            )
            continue
        for suffix in ('value', 'label'):
            old_col, new_col = f'{old_key}.{suffix}', f'{new_key}.{suffix}'
            if old_col not in df.columns:
                continue
            if new_col not in df.columns:
                df[new_col] = None
            missing_mask = df[new_col].isna() | df[new_col].astype(str).isin(['nan', 'None'])
            df.loc[missing_mask, new_col] = df.loc[missing_mask, old_col]
    return df
