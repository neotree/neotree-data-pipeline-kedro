import logging

from conf.base.catalog import params
#Query to create summary_maternala_outcomes table
def summary_maternal_outcomes_query():
    #Defaulting to Malawi Case 
     
    gestation_case = f''' CASE
        WHEN derived.maternal_outcomes."Gestation.value" IS NULL THEN 'Unkown'
        WHEN derived.maternal_outcomes."Gestation.value" < 28 THEN '<28wks'
        WHEN derived.maternal_outcomes."Gestation.value" >= 28 AND derived.maternal_outcomes."Gestation.value" < 32 THEN '28-32wks'
        WHEN derived.maternal_outcomes."Gestation.value" >= 32 AND derived.maternal_outcomes."Gestation.value" < 34 THEN '34-34wks'
        WHEN derived.maternal_outcomes."Gestation.value" >= 34 AND derived.maternal_outcomes."Gestation.value" < 37 THEN '34-36wks'
        WHEN derived.maternal_outcomes."Gestation.value" >= 37 AND derived.maternal_outcomes."Gestation.value" < 42 THEN 'Term'
        WHEN derived.maternal_outcomes."Gestation.value" >= 42 THEN 'Post Term'
        END AS "GestationGroup" '''
    if('country' in params and str(params['country']).lower()) =='zimbabwe':
        gestation_case= f''' CASE
        WHEN derived.maternal_outcomes."Gestation.value" IS NULL THEN 'Unkown'
        WHEN derived.maternal_outcomes."Gestation.value" < 28 THEN '<28 weeks'
        WHEN derived.maternal_outcomes."Gestation.value" >= 28 AND derived.maternal_outcomes."Gestation.value" < 32 THEN '28-31 weeks'
        WHEN derived.maternal_outcomes."Gestation.value" >= 32 AND derived.maternal_outcomes."Gestation.value" < 34 THEN '32-33 weeks'
        WHEN derived.maternal_outcomes."Gestation.value" >= 34 AND derived.maternal_outcomes."Gestation.value" < 37 THEN '34-36 weeks'
        WHEN derived.maternal_outcomes."Gestation.value" >= 37 AND derived.maternal_outcomes."Gestation.value" < 40 THEN '37-39 weeks'
        WHEN derived.maternal_outcomes."Gestation.value" >=40 AND derived.maternal_outcomes."Gestation.value" <= 44 THEN '40-44 weeks'
        END AS "GestationGroup"
        '''
        
        
    sql= f'''DROP TABLE IF EXISTS derived.summary_maternal_outcomes;;
        CREATE TABLE derived.summary_maternal_outcomes AS 
        SELECT derived.maternal_outcomes."uid" AS "NeoTreeID",
        derived.maternal_outcomes."facility" AS "facility",
        CASE
        WHEN derived.maternal_outcomes."DateAdmission.value" IS NULL THEN NULL
        WHEN derived.maternal_outcomes."DateAdmission.value"::TEXT ='NaT' THEN NULL
        ELSE
        DATE(derived.maternal_outcomes."DateAdmission.value") 
        END AS "Date of Admission",
        CASE 
        WHEN derived.maternal_outcomes."BirthDateDis.value" IS NULL THEN NULL
        WHEN derived.maternal_outcomes."BirthDateDis.value" = '' THEN NULL
        WHEN derived.maternal_outcomes."BirthDateDis.value"::TEXT ='NaT' THEN NULL
        ELSE
        DATE(derived.maternal_outcomes."BirthDateDis.value") 
        END AS "Birth Date",
        derived.maternal_outcomes."SexDis.label" AS "Gender",
        derived.maternal_outcomes."TypeBirth.label" AS "Type of Birth",
        derived.maternal_outcomes."Gestation.value" AS "Gestation",
        derived.maternal_outcomes."NeoTreeOutcome.label" AS "Neonate Outcome",
        derived.maternal_outcomes."BWTDis.value" AS "Birth Weight(g)",
        derived.maternal_outcomes."MatOutcome.label" AS "Maternal Outcome",
        derived.maternal_outcomes."Presentation.label" AS "Presentation",
        derived.maternal_outcomes."ModeDelivery.label" AS "Mode of Delivery",
        derived.maternal_outcomes."BabyNursery.label" AS "Is baby in Nursery?",
        derived.maternal_outcomes."Reason.label" AS "Reason for CS",
        derived.maternal_outcomes."ReasonOther.label" AS "Other Reason for CS",
        derived.maternal_outcomes."CryBirth.label" AS "Did by Cry after birth?",
        derived.maternal_outcomes."Apgar1.value" AS "Apgar at 1min",
        derived.maternal_outcomes."Apgar5.value" AS "Apgar at 5min",
        derived.maternal_outcomes."Apgar10.value" AS "Apgar at 10min",
        derived.maternal_outcomes."PregConditions.label" AS "Conditions in Pregnancy",
        case when derived.maternal_outcomes."DateAdmission.value" IS NOT NULL THEN 1 End AS "BirthCount",
        {gestation_case},
        CASE
         WHEN derived.maternal_outcomes."BWTDis.value" IS NULL THEN 'Unknown'
        WHEN derived.maternal_outcomes."BWTDis.value" < 1000 THEN '<1000g'
        WHEN derived.maternal_outcomes."BWTDis.value" >= 1000 AND derived.maternal_outcomes."BWTDis.value" < 1500 THEN '1000-1500g'
        WHEN derived.maternal_outcomes."BWTDis.value" >= 1500 AND derived.maternal_outcomes."BWTDis.value" < 2500 THEN '1500-2500g'
        WHEN derived.maternal_outcomes."BWTDis.value" >= 2500 AND derived.maternal_outcomes."BWTDis.value" < 3500 THEN '2500-3500g'
        WHEN derived.maternal_outcomes."BWTDis.value" >= 3500 AND derived.maternal_outcomes."BWTDis.value" < 4000 THEN '3500-4000g'
        WHEN derived.maternal_outcomes."BWTDis.value" >= 4000 THEN '>4000g'
        END AS "BirthWeightGroup",
        CASE
         WHEN derived.maternal_outcomes."Gestation.value" IS NULL THEN 7
        WHEN derived.maternal_outcomes."Gestation.value" < 28 THEN 1
        WHEN derived.maternal_outcomes."Gestation.value" >= 28 AND derived.maternal_outcomes."Gestation.value" < 32 THEN 2
        WHEN derived.maternal_outcomes."Gestation.value" >= 32 AND derived.maternal_outcomes."Gestation.value" < 34 THEN 3
        WHEN derived.maternal_outcomes."Gestation.value" >= 34 AND derived.maternal_outcomes."Gestation.value" < 37 THEN 4
        WHEN derived.maternal_outcomes."Gestation.value" >= 37 AND derived.maternal_outcomes."Gestation.value" < 42 THEN 5
        WHEN derived.maternal_outcomes."Gestation.value" >= 42 THEN 6
        END AS "GestationGroupSort",
        CASE
         WHEN derived.maternal_outcomes."BWTDis.value" IS NULL THEN 7
        WHEN derived.maternal_outcomes."BWTDis.value" < 1000 THEN 1
        WHEN derived.maternal_outcomes."BWTDis.value" >= 1000 AND derived.maternal_outcomes."BWTDis.value" < 1500 THEN 2
        WHEN derived.maternal_outcomes."BWTDis.value" >= 1500 AND derived.maternal_outcomes."BWTDis.value" < 2500 THEN 3
        WHEN derived.maternal_outcomes."BWTDis.value" >= 2500 AND derived.maternal_outcomes."BWTDis.value" < 3500 THEN 4
        WHEN derived.maternal_outcomes."BWTDis.value" >= 3500 AND derived.maternal_outcomes."BWTDis.value" < 4000 THEN 5
        WHEN derived.maternal_outcomes."BWTDis.value" >= 4000 THEN 6
        END AS "BirthWeightGroupSort"
        FROM derived.maternal_outcomes;; '''
           
    return sql