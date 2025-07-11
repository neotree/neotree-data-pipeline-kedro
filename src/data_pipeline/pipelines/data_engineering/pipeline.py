
from kedro.pipeline import Pipeline

from .nodes import (deduplicate_data_node,
tidy_data_node,
union_views_node,
manually_fix_admissions_node,
manually_fix_discharges_node,
create_convenience_views_node,
create_summary_counts_node,
join_tables_node,grant_privileges_node,
create_summary_tables_node,
create_summary_baseline_node,
import_raw_json_files_node,
create_summary_discharge_diagnosis_node,
clean_derived_data_node
)

# The Pipeline Connecting All The Nodes For The Data Pipeline
def create_pipeline(**kwargs):
    return Pipeline(
        [
        import_raw_json_files_node,
        deduplicate_data_node,
        tidy_data_node,
        manually_fix_admissions_node,
        manually_fix_discharges_node,
        create_summary_tables_node,
        join_tables_node,
        union_views_node,
        create_convenience_views_node,
        create_summary_discharge_diagnosis_node,
        create_summary_baseline_node,
        create_summary_counts_node,
        grant_privileges_node,
        #clean_derived_data_node
        ]
    )
