from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
#Query to create summary_maternala_completeness table
def summary_discharges_query():
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
      
  
  return   prefix+f''' SELECT
                  "facility" AS "Facility Name",
                  "uid" AS "Neotree_ID",
                   CASE
                        -- If already timestamp, use as is
                        WHEN pg_typeof("started_at")::text LIKE '%timestamp%' THEN "started_at"
                        -- Try text format: DD Mon,YYYY
                        WHEN "started_at"::text ~ '^[0-9]{{1,2}} [A-Za-z]{{3}},[0-9]{{4}}$' THEN
                        to_timestamp("started_at"::text || ' 00:00:00', 'DD Mon,YYYY HH24:MI:SS')
                        -- Try text format: YYYY Mon,DD
                        WHEN "started_at"::text ~ '^[0-9]{{4}} [A-Za-z]{{3}},[0-9]{{1,2}}$' THEN
                        to_timestamp("started_at"::text || ' 00:00:00', 'YYYY Mon,DD HH24:MI:SS')
                        -- Try to cast text to timestamp (handles ISO format)
                        ELSE CAST("started_at" AS timestamp without time zone)
                  END AS "Started_at",
                   CASE
                        -- If already timestamp, use as is
                        WHEN pg_typeof("completed_at")::text LIKE '%timestamp%' THEN "completed_at"
                        -- Try text format: DD Mon,YYYY
                        WHEN "completed_at"::text ~ '^[0-9]{{1,2}} [A-Za-z]{{3}},[0-9]{{4}}$' THEN
                        to_timestamp("completed_at"::text || ' 00:00:00', 'DD Mon,YYYY HH24:MI:SS')
                        -- Try text format: YYYY Mon,DD
                        WHEN "completed_at"::text ~ '^[0-9]{{4}} [A-Za-z]{{3}},[0-9]{{1,2}}$' THEN
                        to_timestamp("completed_at"::text || ' 00:00:00', 'YYYY Mon,DD HH24:MI:SS')
                        -- Try to cast text to timestamp (handles ISO format)
                        ELSE CAST("completed_at" AS timestamp without time zone)
                  END AS "Completed_at",
                  "time_spent" AS "Time Spent",
                  CASE
                        WHEN pg_typeof("DateAdmissionDC.value")::text LIKE '%timestamp%' THEN "DateAdmissionDC.value"
                        WHEN "DateAdmissionDC.value"::text ~ '^[0-9]{{1,2}} [A-Za-z]{{3}},[0-9]{{4}}$' THEN
                        to_timestamp("DateAdmissionDC.value"::text || ' 00:00:00', 'DD Mon,YYYY HH24:MI:SS')
                        WHEN "DateAdmissionDC.value"::text ~ '^[0-9]{{4}} [A-Za-z]{{3}},[0-9]{{1,2}}$' THEN
                        to_timestamp("DateAdmissionDC.value"::text || ' 00:00:00', 'YYYY Mon,DD HH24:MI:SS')
                        ELSE CAST("DateAdmissionDC.value" AS timestamp without time zone)
                  END AS "DateAdmissionDC",
                   CASE
                        WHEN pg_typeof("DateTimeDischarge.value")::text LIKE '%timestamp%' THEN "DateTimeDischarge.value"
                        WHEN "DateTimeDischarge.value"::text ~ '^[0-9]{{1,2}} [A-Za-z]{{3}},[0-9]{{4}}$' THEN
                        to_timestamp("DateTimeDischarge.value"::text || ' 00:00:00', 'DD Mon,YYYY HH24:MI:SS')
                        WHEN "DateTimeDischarge.value"::text ~ '^[0-9]{{4}} [A-Za-z]{{3}},[0-9]{{1,2}}$' THEN
                        to_timestamp("DateTimeDischarge.value"::text || ' 00:00:00', 'YYYY Mon,DD HH24:MI:SS')
                        ELSE CAST("DateTimeDischarge.value" AS timestamp without time zone)
                  END AS "DateTime of Discharge",
                  "NeoTreeOutcome.label" AS "Outcome",
                  "Apgar1DC.value" AS "Apgar score at 1min DC",
                  "Apgar5DC.value" AS "Apgar score at 5mins DC",
                  "Apgar10DC.value" AS "Apgar score at 10mins DC",
                  "HIVtestResultDC.label" AS "HIV test Result DC",
                  "NVPgiven.value" AS "NVP given?",
                  "ModeDeliveryDC.label" AS "Mode of Delivery DC",
                   CASE
                        WHEN pg_typeof("DateDischVitals.value")::text LIKE '%timestamp%' THEN "DateDischVitals.value"
                        WHEN "DateDischVitals.value"::text ~ '^[0-9]{{1,2}} [A-Za-z]{{3}},[0-9]{{4}}$' THEN
                        to_timestamp("DateDischVitals.value"::text || ' 00:00:00', 'DD Mon,YYYY HH24:MI:SS')
                        WHEN "DateDischVitals.value"::text ~ '^[0-9]{{4}} [A-Za-z]{{3}},[0-9]{{1,2}}$' THEN
                        to_timestamp("DateDischVitals.value"::text || ' 00:00:00', 'YYYY Mon,DD HH24:MI:SS')
                        ELSE CAST("DateDischVitals.value" AS timestamp without time zone)
                  END AS "Date Discharge Vitals taken",
                  "BWDC.value" AS "Birth Weight (g) DC",
                  "GestationDC.value" AS "Gestation DC",
                  "DischHR.value" AS "Discharge Heart Rate",
                  "DischSats.value" AS "Discharge Oxygen Saturations",
                  "DischTemp.value" AS "Discharge Temperature",
                  "DischRR.value" AS "Discharge Respiratory Rate",
                  "DischWeight.value" AS "Discharge Weight (g)",
                     CASE
                        WHEN pg_typeof("DateDischWeight.value")::text LIKE '%timestamp%' THEN "DateDischWeight.value"
                        WHEN "DateDischWeight.value"::text ~ '^[0-9]{{1,2}} [A-Za-z]{{3}},[0-9]{{4}}$' THEN
                        to_timestamp("DateDischWeight.value"::text || ' 00:00:00', 'DD Mon,YYYY HH24:MI:SS')
                        WHEN "DateDischWeight.value"::text ~ '^[0-9]{{4}} [A-Za-z]{{3}},[0-9]{{1,2}}$' THEN
                        to_timestamp("DateDischWeight.value"::text || ' 00:00:00', 'YYYY Mon,DD HH24:MI:SS')
                        ELSE CAST("DateDischWeight.value" AS timestamp without time zone)
                  END AS "Date of Discharge Weight",
                  "DIAGDIS1.label" AS "Discharge Primary Diagnosis",
                  "DIAGDIS1OTH.value" AS "Other discharge diagnosis",
                  "ThermCare.label" AS "Thermoregulation during admission",
                  "FeedsAdm.label" AS "Feeds during admission",
                  "RESPSUP.label" AS "Respiratory Support",
                  CASE
                        WHEN pg_typeof("DateWeaned.value")::text LIKE '%timestamp%' THEN "DateWeaned.value"
                        WHEN "DateWeaned.value"::text ~ '^[0-9]{{1,2}} [A-Za-z]{{3}},[0-9]{{4}}$' THEN
                        to_timestamp("DateWeaned.value"::text || ' 00:00:00', 'DD Mon,YYYY HH24:MI:SS')
                        WHEN "DateWeaned.value"::text ~ '^[0-9]{{4}} [A-Za-z]{{3}},[0-9]{{1,2}}$' THEN
                        to_timestamp("DateWeaned.value"::text || ' 00:00:00', 'YYYY Mon,DD HH24:MI:SS')
                        ELSE CAST("DateWeaned.value" AS timestamp without time zone)
                  END AS "Date Weaned off the support",
                  "PHOTOTHERAPY.label" AS "Phototherapy given during admission?",
                  "MedsGiven.label" AS "Medications Given",
                  "MEDOTH.label" AS "Other medications given",
                  "REVCLIN.label" AS "Baby review clinic organized?",
                  "REVCLINTYP.label" AS "Baby review clinic",
                  "REVCLINOTH.value" AS "Other baby review clinic",
                  "CLINREVDAT.value" AS "Date of clinic review",
                  "HealthEd.label" AS "Health Education given?",
                  "OtherProbs.label" AS "Other Problems",
                  "OtherProbsOth.label" AS "Other Problems (additional)",
                  CASE
                        WHEN pg_typeof("DateTimeDeath.value")::text LIKE '%timestamp%' THEN "DateTimeDeath.value"
                        WHEN "DateTimeDeath.value"::text ~ '^[0-9]{{1,2}} [A-Za-z]{{3}},[0-9]{{4}}$' THEN
                        to_timestamp("DateTimeDeath.value"::text || ' 00:00:00', 'DD Mon,YYYY HH24:MI:SS')
                        WHEN "DateTimeDeath.value"::text ~ '^[0-9]{{4}} [A-Za-z]{{3}},[0-9]{{1,2}}$' THEN
                        to_timestamp("DateTimeDeath.value"::text || ' 00:00:00', 'YYYY Mon,DD HH24:MI:SS')
                        ELSE CAST("DateTimeDeath.value" AS timestamp without time zone)
                  END AS "DateTime of Death",
                  "CauseDeath.label" AS "Cause of Death",
                  "CauseDeathOther.value" AS "Other Cause of Death_",
                  "CauseDeathOth.value" AS "Other Cause of death",
                  "ContCauseDeath.label" AS "Contributory Cause of Death",
                  "ContribOth.label" AS "Other Contributory cause of death",
                  "ModFactor1.value" AS "Modifable Factor1",
                  "ModFactor2.value" AS "Modifable Factor2",
                  "ModFactor3.value" AS "Modifable Factor3",
                  "DiscCovidRisk.label" AS "Covid Risk?",
                  "DiscDiagSurgicalCond.label" AS "Discharge Surgical Conditions diagnosis",
                  "CovidRepResults.label" AS "Covid Repeat Results",
                  "CovidConfirmation.label" AS "Covid Confirmation"
                FROM "derived"."discharges" {where};; '''