
from kedro.pipeline import Pipeline

from .nodes import deduplicate_admissions_node,deduplicate_discharges_node,tidy_data_node,manually_fix_admissions_node,manually_fix_discharges_node,create_convenience_views_node,create_summary_counts_node,join_tables_node,grant_privileges_node,create_summary_maternal_outcomes_node


def create_pipeline(**kwargs):
    return Pipeline(
        [
        deduplicate_admissions_node,
        deduplicate_discharges_node,
        tidy_data_node,
        create_summary_maternal_outcomes_node,
        manually_fix_admissions_node,
        manually_fix_discharges_node,
        join_tables_node,
        create_convenience_views_node,
        create_summary_counts_node,
        grant_privileges_node
        ]
    )
