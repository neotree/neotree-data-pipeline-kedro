# Function To Drop VIEWS IF THEY EXIST
def drop_views_query():
    return f''' DROP VIEW IF EXISTS derived.old_new_admissions_view;
                DROP VIEW IF EXISTS derived.old_new_discharges_view;
                DROP VIEW IF EXISTS derived.old_new_matched_view;
             '''
