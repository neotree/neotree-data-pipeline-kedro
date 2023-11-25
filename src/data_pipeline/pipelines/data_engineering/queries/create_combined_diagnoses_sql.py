def combined_diagnoses_query():
 return f'''
             DROP TABLE IF EXISTS derived.combined_diagnoses;;
             CREATE TABLE derived.combined_diagnoses AS
             SELECT "source"."uid" AS "uid", "source"."Diagnoses.label" AS "Diagnoses"
             FROM (SELECT "derived"."exploded_Diagnoses.label"."uid" AS "uid", "derived"."exploded_Diagnoses.label"."Diagnoses.label" AS "Diagnoses.label", "derived"."exploded_Diagnoses.label"."facility" AS "facility" FROM "derived"."exploded_Diagnoses.label"
             LEFT JOIN "derived"."admissions" "Admissions" ON "derived"."exploded_Diagnoses.label"."uid" = "Admissions"."uid") "source"
             UNION ALL
             SELECT "derived"."diagnoses"."uid" AS "uid", "derived"."diagnoses"."diagnosis" AS "diagnosis"
             FROM "derived"."diagnoses";;
             '''