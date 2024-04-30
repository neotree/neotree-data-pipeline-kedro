import logging
from conf.common.sql_functions import inject_sql

def patch_derived_admissions():
    logging.info("patch_derived_admissions")
    sql_script = '''
    
    '''
    inject_sql(sql_script, "manually-fix-derived_admissions")

def patch_derived_discharges():
    logging.info("patch_derived_discharges")

def patch_maternal_outcomes():
    logging.info("patch_maternal_outcomes")

def patch_derived_vital_signs():
    logging.info("patch_derived_vital_signs")

def patch_derived_neolab():
    logging.info("patch_derived_neolab")

def patch_derived_baseline():
    logging.info("patch_derived_baseline")

def patch_derived_diagnoses():
    logging.info("patch_derived_diagnoses")

def patch_derived_maternity_completeness():
    logging.info("patch_derived_maternity_completeness")
 