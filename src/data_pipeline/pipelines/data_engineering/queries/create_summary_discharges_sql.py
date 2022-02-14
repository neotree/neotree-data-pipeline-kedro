
#Query to create summary_maternala_completeness table
def summary_discharges_query():
  return   f''' DROP TABLE IF EXISTS derived.summary_discharges;
                CREATE TABLE derived.summary_discharges AS 
                SELECT
                  "facility" AS "Facility Name",
                  "uid" AS "Neotree_ID",
                  "started_at" AS "Started_at",
                  "completed_at" AS "Completed_at",
                  "time_spent" AS "Time Spent",
                  "DateAdmissionDC.value" AS "DateAdmissionDC",
                  "DateTimeDischarge.value" AS "DateTime of Discharge",
                  "NeoTreeOutcome.label" AS "Outcome",
                  "Apgar1DC.value" AS "Apgar score at 1min DC",
                  "Apgar5DC.value" AS "Apgar score at 5mins DC",
                  "Apgar10DC.value" AS "Apgar score at 10mins DC",
                  "HIVtestResultDC.label" AS "HIV test Result DC",
                  "NVPgiven.value" AS "NVP given?",
                  "ModeDeliveryDC.label" AS "Mode of Delivery DC",
                  "DateDischVitals.value" AS "Date Discharge Vitals taken",
                  "BWDC.value" AS "Birth Weight (g) DC",
                  "GestationDC.value" AS "Gestation DC",
                  "DischHR.value" AS "Discharge Heart Rate",
                  "DischSats.value" AS "Discharge Oxygen Saturations",
                  "DischTemp.value" AS "Discharge Temperature",
                  "DischRR.value" AS "Discharge Respiratory Rate",
                  "DischWeight.value" AS "Discharge Weight (g)",
                  "DateDischWeight.value" AS "Date of Discharge Weight",
                  "DIAGDIS1.label" AS "Discharge Primary Diagnosis",
                  "DIAGDIS1OTH.value" AS "Other discharge diagnosis",
                  "ThermCare.label" AS "Thermoregulation during admission",
                  "FeedsAdm.label" AS "Feeds during admission",
                  "RESPSUP.label" AS "Respiratory Support",
                  "DateWeaned.value" AS "Date Weaned off the support",
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
                  "DateTimeDeath.value" AS "DateTime of Death",
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
                FROM "derived"."discharges"; '''