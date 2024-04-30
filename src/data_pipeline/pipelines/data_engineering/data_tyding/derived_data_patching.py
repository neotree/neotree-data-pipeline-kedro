import logging
from conf.common.sql_functions import inject_sql

def patch_derived_admissions():
    logging.info("patch_derived_admissions")
    # sql_script = '''
    
    # '''
    # inject_sql(sql_script, "manually-fix-derived_admissions")

def patch_derived_discharges():
    logging.info("patch_derived_discharges")
    # sql_script = f'''
    #     {update_meds_given('Ampicillin','AMP')}
    #     {update_meds_given('X-penicillin and gentamicin','ABX')}
    #     {update_meds_given('Amoxicillin','AMOX')}
    #     {update_meds_given('AZT (Zidovudine)','AZT')}
    #     {update_meds_given('X penicillin','BP')}
    #     {update_meds_given('Caffeine','CAF')}
    #     {update_meds_given('Ceftriaxone','CEF')}
    #     {update_meds_given('Gentamicin','GENT')}
    #     {update_meds_given('Nevirapine','NVP')}
    #     {update_meds_given('Other','OTH')}
    #     {update_meds_given('Paracetamol','PCM')}
    #     {update_meds_given('Phenobarbitone','PHEN')}
    # '''
    # inject_sql(sql_script, "manually-fix-derived_discharges")

def patch_derived_maternal_outcomes():
    logging.info("patch_derived_maternal_outcomes")

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
    
 