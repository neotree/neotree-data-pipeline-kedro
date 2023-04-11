#Query to create summary_discharge_diagnosis table
def summary_discharge_diagnosis_query():
    return f'''DROP TABLE IF EXISTS derived.summary_discharge_diagnosis;
                CREATE TABLE derived.summary_discharge_diagnosis AS 
                SELECT derived.joined_admissions_discharges."uid" AS "uid",
                derived.joined_admissions_discharges."facility" AS "facility",
                unnest( 
                CASE WHEN derived.joined_admissions_discharges."DIAGDIS1.value" ='OTH' THEN 
                string_to_array(derived.joined_admissions_discharges."DIAGDIS1OTH.value",',')
                ELSE
                string_to_array(derived.joined_admissions_discharges."DIAGDIS1.label",',')
                END) AS "Diagnosis"
            FROM derived.joined_admissions_discharges; '''