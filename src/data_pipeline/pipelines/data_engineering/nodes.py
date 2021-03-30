from kedro.pipeline import node
from .nodes_grouped.data_impotation_nodes.import_from_raw_json import import_json_files
from .nodes_grouped.step_1_nodes.deduplicate_admissions import deduplicate_admissions
from .nodes_grouped.step_1_nodes.deduplicate_discharges import deduplicate_discharges
from .nodes_grouped.step_2_nodes.tidy_data import tidy_data
from .nodes_grouped.step_2_nodes.summary_maternal_outcomes import create_summary_maternal_outcomes
from .nodes_grouped.step_3_nodes.manually_fix_admissions_records import manually_fix_admissions
from .nodes_grouped.step_3_nodes.manually_fix_discharge_records import manually_fix_discharges
from .nodes_grouped.step_4_nodes.convenience_views import create_convenience_views
from .nodes_grouped.step_4_nodes.join_tables import join_tables
from .nodes_grouped.step_5_nodes.grant_privileges import grant_privileges
from .nodes_grouped.step_4_nodes.summary_counts import create_summary_counts
from kedro.io import CachedDataSet


#A File That is used to create all the nodes that make up the data pipeline

import_raw_json_files_node = node(
    import_json_files,inputs=None,outputs ="data_import_output"
)
#Create A Deduplicating Admissions Node
deduplicate_admissions_node = node(
    deduplicate_admissions, inputs="data_import_output", outputs="deduplicate_admissions_output"
)

#Create A Deduplicating Discharges Node
deduplicate_discharges_node = node(
    deduplicate_discharges, inputs="data_import_output", outputs="deduplicate_discharges_output"
)

# Create A Data Tyding Node And Pass OutPut From Deduplication
tidy_data_node = node(
    tidy_data,  inputs=["deduplicate_discharges_output","deduplicate_admissions_output"], outputs ="tidy_data_output"
)

#Create Summary Maternal Outcomes  
create_summary_maternal_outcomes_node = node(
    create_summary_maternal_outcomes, inputs= "tidy_data_output", outputs = "create_summary_maternal_outcomes_output"
)

# Create Manually Fixing Admisiions Node And Pass Data Tyding Output as input
manually_fix_admissions_node = node(
    manually_fix_admissions,  inputs="tidy_data_output", outputs ="manually_Fix_admissions_output"
)

# Create Manually Fix Discharges Node And Pass Same Input as In Fixing Admissions So That They Can Run Parallell To Each Other
manually_fix_discharges_node = node(
   manually_fix_discharges,  inputs="tidy_data_output", outputs ="manually_fix_discharges_output" 
)

# Create Join Tables Node And Pass Manually Fix Admissions OutPut as we currently have nothing in Fix Discharges
join_tables_node = node(
    join_tables, inputs="manually_Fix_admissions_output",outputs="join_tables_output"
)

#Create Convinience Views and Pass Joining Tables Output 
create_convenience_views_node = node(
    create_convenience_views, inputs= "join_tables_output", outputs = "create_convinience_views_output"
)

#Create Convinience Views and Pass Joining Tables Output 
create_summary_counts_node = node(
    create_summary_counts, inputs= "create_convinience_views_output", outputs = "create_summary_counts_output"
)



# Create Grant Privileges Node and Pass Create Convinience Views Output
grant_privileges_node = node(
    grant_privileges,inputs = "create_summary_counts_output", outputs = "grant_privileges_output"
)
 




