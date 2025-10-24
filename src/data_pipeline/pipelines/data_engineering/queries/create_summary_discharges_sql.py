from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
from conf.common.sql_functions import get_table_column_names

def get_column_or_null(existing_columns, column_name, alias, date_case=None):
    """
    Helper function to generate SQL for a column that may or may not exist.

    Args:
        existing_columns: Set of columns that exist in the table
        column_name: The column name to check for (e.g., "FeedsAdm.label")
        alias: The alias to use for the output (e.g., "Feeds during admission")
        date_case: Optional CASE statement for date formatting

    Returns:
        SQL fragment that either selects the column or returns NULL
    """
    if column_name in existing_columns:
        if date_case:
            return date_case
        else:
            return f'"{column_name}" AS "{alias}"'
    else:
        return f'NULL AS "{alias}"'

#Query to create summary_maternala_completeness table
def summary_discharges_query():
  # Get existing columns from the discharges table
  existing_cols_result = get_table_column_names('discharges', 'derived')
  existing_columns = {row[0] for row in existing_cols_result} if existing_cols_result else set()

  prefix = f''' DROP TABLE IF EXISTS derived.summary_discharges;;
                CREATE TABLE derived.summary_discharges AS   '''
  where = ''
  if(table_exists("derived","summary_discharges")):
    prefix=  f''' INSERT INTO derived.summary_discharges (
    "Facility Name","Neotree_ID","Started_at","Completed_at","Time Spent","DateAdmissionDC","DateTime of Discharge","Outcome",
    "Apgar score at 1min DC","Apgar score at 5mins DC","Apgar score at 10mins DC","HIV test Result DC","NVP given?",
    "Mode of Delivery DC","Date Discharge Vitals taken","Birth Weight (g) DC","Gestation DC","Discharge Heart Rate",
    "Discharge Oxygen Saturations","Discharge Temperature","Discharge Respiratory Rate","Discharge Weight (g)","Date of Discharge Weight",
    "Discharge Primary Diagnosis","Other discharge diagnosis","Thermoregulation during admission","Feeds during admission",
    "Respiratory Support","Date Weaned off the support","Phototherapy given during admission?","Medications Given",
    "Other medications given","Baby review clinic organized?","Baby review clinic","Other baby review clinic","Date of clinic review",
    "Health Education given?","Other Problems","Other Problems (additional)","DateTime of Death","Cause of Death",
    "Other Cause of Death_","Other Cause of death","Contributory Cause of Death","Other Contributory cause of death","Modifable Factor1",
    "Modifable Factor2","Modifable Factor3","Covid Risk?","Discharge Surgical Conditions diagnosis","Covid Repeat Results","Covid Confirmation"
  )  '''
    where=f''' WHERE NOT EXISTS ( SELECT 1  FROM derived.summary_discharges  WHERE "Neotree_ID" IN (select uid from derived.discharges)) '''
      
  
  # Build SELECT clause columns
  columns = [
    get_column_or_null(existing_columns, "facility", "Facility Name"),
    get_column_or_null(existing_columns, "uid", "Neotree_ID"),
    get_column_or_null(existing_columns, "started_at", "Started_at",
      """CASE
                        WHEN pg_typeof("started_at")::text LIKE '%timestamp%' THEN "started_at"
                        WHEN "started_at"::text ~ '^[0-9]{1,2} [A-Za-z]{3},[0-9]{4}$' THEN
                        to_timestamp("started_at"::text || ' 00:00:00', 'DD Mon,YYYY HH24:MI:SS')
                        WHEN "started_at"::text ~ '^[0-9]{4} [A-Za-z]{3},[0-9]{1,2}$' THEN
                        to_timestamp("started_at"::text || ' 00:00:00', 'YYYY Mon,DD HH24:MI:SS')
                        ELSE CAST("started_at" AS timestamp without time zone)
                  END AS "Started_at" """ if "started_at" in existing_columns else None),
    get_column_or_null(existing_columns, "completed_at", "Completed_at",
      """CASE
                        WHEN pg_typeof("completed_at")::text LIKE '%timestamp%' THEN "completed_at"
                        WHEN "completed_at"::text ~ '^[0-9]{1,2} [A-Za-z]{3},[0-9]{4}$' THEN
                        to_timestamp("completed_at"::text || ' 00:00:00', 'DD Mon,YYYY HH24:MI:SS')
                        WHEN "completed_at"::text ~ '^[0-9]{4} [A-Za-z]{3},[0-9]{1,2}$' THEN
                        to_timestamp("completed_at"::text || ' 00:00:00', 'YYYY Mon,DD HH24:MI:SS')
                        ELSE CAST("completed_at" AS timestamp without time zone)
                  END AS "Completed_at" """ if "completed_at" in existing_columns else None),
    get_column_or_null(existing_columns, "time_spent", "Time Spent"),
    get_column_or_null(existing_columns, "DateAdmissionDC.value", "DateAdmissionDC",
      """CASE
                        WHEN pg_typeof("DateAdmissionDC.value")::text LIKE '%timestamp%' THEN "DateAdmissionDC.value"
                        WHEN "DateAdmissionDC.value"::text ~ '^[0-9]{1,2} [A-Za-z]{3},[0-9]{4}$' THEN
                        to_timestamp("DateAdmissionDC.value"::text || ' 00:00:00', 'DD Mon,YYYY HH24:MI:SS')
                        WHEN "DateAdmissionDC.value"::text ~ '^[0-9]{4} [A-Za-z]{3},[0-9]{1,2}$' THEN
                        to_timestamp("DateAdmissionDC.value"::text || ' 00:00:00', 'YYYY Mon,DD HH24:MI:SS')
                        ELSE CAST("DateAdmissionDC.value" AS timestamp without time zone)
                  END AS "DateAdmissionDC" """ if "DateAdmissionDC.value" in existing_columns else None),
    get_column_or_null(existing_columns, "DateTimeDischarge.value", "DateTime of Discharge",
      """CASE
                        WHEN pg_typeof("DateTimeDischarge.value")::text LIKE '%timestamp%' THEN "DateTimeDischarge.value"
                        WHEN "DateTimeDischarge.value"::text ~ '^[0-9]{1,2} [A-Za-z]{3},[0-9]{4}$' THEN
                        to_timestamp("DateTimeDischarge.value"::text || ' 00:00:00', 'DD Mon,YYYY HH24:MI:SS')
                        WHEN "DateTimeDischarge.value"::text ~ '^[0-9]{4} [A-Za-z]{3},[0-9]{1,2}$' THEN
                        to_timestamp("DateTimeDischarge.value"::text || ' 00:00:00', 'YYYY Mon,DD HH24:MI:SS')
                        ELSE CAST("DateTimeDischarge.value" AS timestamp without time zone)
                  END AS "DateTime of Discharge" """ if "DateTimeDischarge.value" in existing_columns else None),
    get_column_or_null(existing_columns, "NeoTreeOutcome.label", "Outcome"),
    get_column_or_null(existing_columns, "Apgar1DC.value", "Apgar score at 1min DC"),
    get_column_or_null(existing_columns, "Apgar5DC.value", "Apgar score at 5mins DC"),
    get_column_or_null(existing_columns, "Apgar10DC.value", "Apgar score at 10mins DC"),
    get_column_or_null(existing_columns, "HIVtestResultDC.label", "HIV test Result DC"),
    get_column_or_null(existing_columns, "NVPgiven.value", "NVP given?"),
    get_column_or_null(existing_columns, "ModeDeliveryDC.label", "Mode of Delivery DC"),
    get_column_or_null(existing_columns, "DateDischVitals.value", "Date Discharge Vitals taken",
      """CASE
                        WHEN pg_typeof("DateDischVitals.value")::text LIKE '%timestamp%' THEN "DateDischVitals.value"
                        WHEN "DateDischVitals.value"::text ~ '^[0-9]{1,2} [A-Za-z]{3},[0-9]{4}$' THEN
                        to_timestamp("DateDischVitals.value"::text || ' 00:00:00', 'DD Mon,YYYY HH24:MI:SS')
                        WHEN "DateDischVitals.value"::text ~ '^[0-9]{4} [A-Za-z]{3},[0-9]{1,2}$' THEN
                        to_timestamp("DateDischVitals.value"::text || ' 00:00:00', 'YYYY Mon,DD HH24:MI:SS')
                        ELSE CAST("DateDischVitals.value" AS timestamp without time zone)
                  END AS "Date Discharge Vitals taken" """ if "DateDischVitals.value" in existing_columns else None),
    get_column_or_null(existing_columns, "BWDC.value", "Birth Weight (g) DC"),
    get_column_or_null(existing_columns, "GestationDC.value", "Gestation DC"),
    get_column_or_null(existing_columns, "DischHR.value", "Discharge Heart Rate"),
    get_column_or_null(existing_columns, "DischSats.value", "Discharge Oxygen Saturations"),
    get_column_or_null(existing_columns, "DischTemp.value", "Discharge Temperature"),
    get_column_or_null(existing_columns, "DischRR.value", "Discharge Respiratory Rate"),
    get_column_or_null(existing_columns, "DischWeight.value", "Discharge Weight (g)"),
    get_column_or_null(existing_columns, "DateDischWeight.value", "Date of Discharge Weight",
      """CASE
                        WHEN pg_typeof("DateDischWeight.value")::text LIKE '%timestamp%' THEN "DateDischWeight.value"
                        WHEN "DateDischWeight.value"::text ~ '^[0-9]{1,2} [A-Za-z]{3},[0-9]{4}$' THEN
                        to_timestamp("DateDischWeight.value"::text || ' 00:00:00', 'DD Mon,YYYY HH24:MI:SS')
                        WHEN "DateDischWeight.value"::text ~ '^[0-9]{4} [A-Za-z]{3},[0-9]{1,2}$' THEN
                        to_timestamp("DateDischWeight.value"::text || ' 00:00:00', 'YYYY Mon,DD HH24:MI:SS')
                        ELSE CAST("DateDischWeight.value" AS timestamp without time zone)
                  END AS "Date of Discharge Weight" """ if "DateDischWeight.value" in existing_columns else None),
    get_column_or_null(existing_columns, "DIAGDIS1.label", "Discharge Primary Diagnosis"),
    get_column_or_null(existing_columns, "DIAGDIS1OTH.value", "Other discharge diagnosis"),
    get_column_or_null(existing_columns, "ThermCare.label", "Thermoregulation during admission"),
    get_column_or_null(existing_columns, "FeedsAdm.label", "Feeds during admission"),
    get_column_or_null(existing_columns, "RESPSUP.label", "Respiratory Support"),
    get_column_or_null(existing_columns, "DateWeaned.value", "Date Weaned off the support",
      """CASE
                        WHEN pg_typeof("DateWeaned.value")::text LIKE '%timestamp%' THEN "DateWeaned.value"
                        WHEN "DateWeaned.value"::text ~ '^[0-9]{1,2} [A-Za-z]{3},[0-9]{4}$' THEN
                        to_timestamp("DateWeaned.value"::text || ' 00:00:00', 'DD Mon,YYYY HH24:MI:SS')
                        WHEN "DateWeaned.value"::text ~ '^[0-9]{4} [A-Za-z]{3},[0-9]{1,2}$' THEN
                        to_timestamp("DateWeaned.value"::text || ' 00:00:00', 'YYYY Mon,DD HH24:MI:SS')
                        ELSE CAST("DateWeaned.value" AS timestamp without time zone)
                  END AS "Date Weaned off the support" """ if "DateWeaned.value" in existing_columns else None),
    get_column_or_null(existing_columns, "PHOTOTHERAPY.label", "Phototherapy given during admission?"),
    get_column_or_null(existing_columns, "MedsGiven.label", "Medications Given"),
    get_column_or_null(existing_columns, "MEDOTH.label", "Other medications given"),
    get_column_or_null(existing_columns, "REVCLIN.label", "Baby review clinic organized?"),
    get_column_or_null(existing_columns, "REVCLINTYP.label", "Baby review clinic"),
    get_column_or_null(existing_columns, "REVCLINOTH.value", "Other baby review clinic"),
    get_column_or_null(existing_columns, "CLINREVDAT.value", "Date of clinic review"),
    get_column_or_null(existing_columns, "HealthEd.label", "Health Education given?"),
    get_column_or_null(existing_columns, "OtherProbs.label", "Other Problems"),
    get_column_or_null(existing_columns, "OtherProbsOth.label", "Other Problems (additional)"),
    get_column_or_null(existing_columns, "DateTimeDeath.value", "DateTime of Death",
      """CASE
                        WHEN pg_typeof("DateTimeDeath.value")::text LIKE '%timestamp%' THEN "DateTimeDeath.value"
                        WHEN "DateTimeDeath.value"::text ~ '^[0-9]{1,2} [A-Za-z]{3},[0-9]{4}$' THEN
                        to_timestamp("DateTimeDeath.value"::text || ' 00:00:00', 'DD Mon,YYYY HH24:MI:SS')
                        WHEN "DateTimeDeath.value"::text ~ '^[0-9]{4} [A-Za-z]{3},[0-9]{1,2}$' THEN
                        to_timestamp("DateTimeDeath.value"::text || ' 00:00:00', 'YYYY Mon,DD HH24:MI:SS')
                        ELSE CAST("DateTimeDeath.value" AS timestamp without time zone)
                  END AS "DateTime of Death" """ if "DateTimeDeath.value" in existing_columns else None),
    get_column_or_null(existing_columns, "CauseDeath.label", "Cause of Death"),
    get_column_or_null(existing_columns, "CauseDeathOther.value", "Other Cause of Death_"),
    get_column_or_null(existing_columns, "CauseDeathOth.value", "Other Cause of death"),
    get_column_or_null(existing_columns, "ContCauseDeath.label", "Contributory Cause of Death"),
    get_column_or_null(existing_columns, "ContribOth.label", "Other Contributory cause of death"),
    get_column_or_null(existing_columns, "ModFactor1.value", "Modifable Factor1"),
    get_column_or_null(existing_columns, "ModFactor2.value", "Modifable Factor2"),
    get_column_or_null(existing_columns, "ModFactor3.value", "Modifable Factor3"),
    get_column_or_null(existing_columns, "DiscCovidRisk.label", "Covid Risk?"),
    get_column_or_null(existing_columns, "DiscDiagSurgicalCond.label", "Discharge Surgical Conditions diagnosis"),
    get_column_or_null(existing_columns, "CovidRepResults.label", "Covid Repeat Results"),
    get_column_or_null(existing_columns, "CovidConfirmation.label", "Covid Confirmation")
  ]

  # Join columns with commas
  columns_sql = ",\n                  ".join(columns)

  return prefix + f''' SELECT
                  {columns_sql}
                FROM "derived"."discharges" {where};; '''