def drop_views_query():
    return f''' DROP TABLE IF EXISTS derived.old_new_admissions_view;
                DROP TABLE IF EXISTS derived.old_new_discharges_view;
                DROP TABLE IF EXISTS derived.old_new_matched_view;
             '''
