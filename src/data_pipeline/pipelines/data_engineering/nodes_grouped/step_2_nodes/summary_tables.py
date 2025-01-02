import logging
import sys
from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from conf.base.catalog import cron_log_file,cron_time,env
from .summary_vitalsigns import create_summary_vitalsigns
from .maternal_completeness_summary import create_maternal_completeness_summary
from .summary_joined_vitalsigns import create_summary_joined_vitalsigns
from .summary_maternal_outcomes import create_summary_maternal_outcomes
from .summary_admissions import create_summary_admissions
from .summary_discharges import create_summary_discharges
from .summary_neolab import create_summary_neolabs
from .combined_diagnoses import create_combined_diagnoses
from conf.base.catalog import params, env



def create_summary_tables(manually_Fix_admissions_output,manually_fix_discharges_output):
    try:
        if env!='demo' and manually_Fix_admissions_output is not None and manually_fix_discharges_output is not None:
            create_summary_vitalsigns()
            create_summary_joined_vitalsigns() 
            create_summary_maternal_outcomes()
            create_summary_neolabs()
            if('country' in params and str(params['country']).lower() =='malawi' and env=='prod'):
                create_maternal_completeness_summary() 
                create_summary_admissions()
                create_summary_discharges()

            exploded_Diagnoses_exists = table_exists("derived","exploded_Diagnoses.label")
            diagnoses_exists = table_exists("derived","diagnoses")
            if exploded_Diagnoses_exists and diagnoses_exists:
                create_combined_diagnoses()

            return dict(
                status='Success',
                message="Summary Tables Complete"
            )
        else:
            logging.error(
                "Data Tidying Did Not Execute To Completion")
            return None
    except Exception as e:
        logging.error(
            "!!! An error occured in creating summary tables: ")
        logging.error(e)
        # Only Open This File When Need Be To Write To It
        cron_log = open(cron_log_file, "a+")
        cron_log.write(
            "StartTime: {0}   Instance: {1}   Status: Failed Stage: Summary Tables ".format(cron_time, env))
        cron_log.close()
        sys.exit(1)
