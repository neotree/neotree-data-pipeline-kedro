
#Query to create summary_maternala_completeness table
def summary_maternal_completeness_query():
  return   f''' DROP TABLE IF EXISTS derived.summary_maternal_completeness;
                CREATE TABLE derived.summary_maternal_completeness AS 
                SELECT "facility" AS "facility",
                    "uid" AS "Neotree_ID",
                    "started_at" AS "Started_at",
                    "completed_at" AS "Completed_at",
                    "time_spent" AS "Time_spent",
                    date("DateAdmission.value") AS "Date of Admission",
                    to_char(date("DateAdmission.value"), 'Mon-YYYY') AS "Admission Month-Year",
                    to_char(date("DateAdmission.value"), 'YYYYMM') AS "Admission Month-Year-sort",
                    "SexDis.label" AS "Gender",
                    "NeoTreeOutcome.label" AS "Baby Outcome",
                    "Gestation.value" AS "Gestation",
                    "BWTDis.value" AS "Birth Weight(g)",
                    "Presentation.label" AS "Presentation",
                    "ModeDelivery.label" AS "Mode of Delivery",
                    "Reason.label" AS "Reason for Caesarean section",
                    "ReasonOther.label" AS "Other reason for Caesarean section",
                    "TypeBirth.label" AS "Type of Birth",
                    "CryBirth.label" AS "Did baby cry at birth?",
                    "Apgar1.value" AS "Apgar Score at 1min",
                    "Apgar5.value" AS "Apgar Score at 5mins",
                    "Apgar10.value" AS "Apgar Score at 10mins",
                    "BirthDateDis.value" AS "Date of birth",
                    "BabyNursery.label" AS "Baby in nursery?",
                    "MatOutcome.label" AS "Maternal Outcome",
                    "MaternalCauseOfDeath.label" AS "Maternal cause of death",
                    "MaternalAdmissionSource.label" AS "Maternal Admission Source",
                    "OutFacility.label" AS "Outside Facility",
                    "ReferredFrom.label" AS "Referred From",
                    "PregConditions.label" AS "Conditions in pregnancy",
                    "MaternalDiagnosis.label" AS "Maternal Diagnosis",
                    "OtherMatDiag.label" AS "Other Maternal Diagnosis",
                    "ReasonForReferral.label" AS "Reason for Referral",
                    "PeritonitisTreatment.label" AS "Peritonitis Treatment",
                    "DurationInWard.value" AS "Duration in the Ward",
                    "NoOfTimesInTheater.value" AS "Number of times in theatre"
                FROM "derived"."maternity_completeness" '''