def combined_diagnoses_query():
 return f'''
             DROP TABLE IF EXISTS derived.combined_diagnoses;
             CREAT TABLE derived.combined_diagnoses AS
             SELECT "uid" AS "uid", "Diagnoses.label" AS "Diagnoses", "Admissions__ingested_at" AS "ingested_at"
             FROM (SELECT "derived"."exploded_Diagnoses.label"."uid" AS "uid", "derived"."exploded_Diagnoses.label"."Diagnoses.label" AS "Diagnoses.label", "derived"."exploded_Diagnoses.label"."facility" AS "facility", "Admissions"."ingested_at" AS "Admissions__ingested_at" FROM "derived"."exploded_Diagnoses.label"
             LEFT JOIN "derived"."admissions" "Admissions" ON "derived"."exploded_Diagnoses.label"."uid" = "Admissions"."uid")
             UNION ALL
             SELECT "derived"."diagnoses"."uid" AS "uid", "derived"."diagnoses"."diagnosis" AS "diagnosis", "derived"."diagnoses"."ingested_at" AS "ingested_at"
             FROM "derived"."diagnoses";
             '''