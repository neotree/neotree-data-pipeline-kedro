def summary_discharge_diagnosis_query():
    return '''DROP TABLE IF EXISTS derived.summary_discharge_diagnosis;
        CREATE TABLE derived.summary_discharge_diagnosis AS 
        SELECT derived.joined_admissions_discharges."uid" AS "uid",
        CASE WHEN derived.joined_admissions_discharges."DIAGDIS1.value" ='OTH' THEN 
        derived.joined_admissions_discharges."DIAGDIS1OTH.value" 
        ELSE
        derived.joined_admissions_discharges."DIAGDIS1.label"
        END AS "Diagnosis"
        FROM derived.joined_admissions_discharges; '''