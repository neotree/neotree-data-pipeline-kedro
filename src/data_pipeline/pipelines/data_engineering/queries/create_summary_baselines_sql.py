from conf.common.sql_functions import column_exists
def summary_baseline_query():

    ANSteroids = ''
    if column_exists('derived','baseline','ANSteroids.label'):
          ANSteroids = 'derived.baseline."ANSteroids.label" As "AntenatalSteroids", '
          
    return f''' DROP TABLE IF EXISTS derived.summary_baseline;;
            CREATE TABLE derived.summary_baseline AS 
            SELECT derived.baseline."uid" AS "uid", 
            derived.baseline."facility" AS "facility", 
            derived.baseline."time_spent" AS "TimeSpent",
            derived.baseline."DateTimeAdmission.value" AS "AdmissionDateTime",
            derived.baseline."AdmittedFrom.label" AS "admission_source",
            derived.baseline."ReferredFrom2.label" AS "referredFrom", 
            derived.baseline."Gender.label" AS "Gender", 
            derived.baseline."AW.value" AS "AdmissionWeight", 
            derived.baseline."AWGroup.value" AS "AdmissionWeightGroup", 
            derived.baseline."BW.value" AS "BirthWeight", 
            derived.baseline."BWGroup.value" AS "BirthWeightGroup",
            derived.baseline."Gestation.value" AS "Gestation",
            derived.baseline."MethodEstGest.label" AS "ModeOfEsttimating", 
            derived.baseline."AgeCat.label" AS "AgeCategory",
            derived.baseline."MatHIVtest.label" AS "MotherHIVTest", 
            derived.baseline."HIVtestResult.label" AS "HIVTestResult", 
            derived.baseline."HAART.label" AS "OnHAART",
            derived.baseline."LengthHAART.label" AS "LengthOfHAART",
            derived.baseline."TempThermia.value" AS "TempThermia",
            derived.baseline."TempGroup.value" AS "TempGroup",
            derived.baseline."Temperature.value" AS "Temperature", 
            derived.baseline."GestGroup.value" As "GestationGroup",
            derived.baseline."InOrOut.label" AS "InOrOut",
            derived.baseline."ReferredFrom.label" AS "FacilityReferredFrom",
            derived.baseline."DateTimeDischarge.value" AS "DischargeDateTime",
            derived.baseline."NeoTreeOutcome.label" AS "NeonateOutcome", 
            derived.baseline."LengthOfLife.value" AS "LengthOfLife", 
            derived.baseline."LengthOfStay.value" AS "LengthOfStay",
            derived.baseline."OFC.value" AS "HeadCircumf",
            CAST(TO_CHAR(DATE(derived.baseline."DateTimeAdmission.value") :: DATE, 'Mon-YYYY') AS text) AS "AdmissionMonthYear", 
            CAST(TO_CHAR(DATE(derived.baseline."DateTimeAdmission.value") :: DATE, 'YYYYmm') AS decimal) AS "AdmissionMonthYearSort",
            CASE WHEN derived.baseline."DateTimeDischarge.value"  IS NOT NULL THEN 
            CAST(TO_CHAR(DATE(derived.baseline."DateTimeDischarge.value") :: DATE, 'Mon-YYYY') AS text)
            WHEN derived.baseline."DateTimeDeath.value" IS NOT NULL THEN
            CAST(TO_CHAR(DATE(derived.baseline."DateTimeDeath.value") :: DATE, 'Mon-YYYY') AS text)
            WHEN derived.baseline."NeoTreeOutcome.label" = 'Absconded' THEN
            CAST(TO_CHAR(DATE(derived.baseline."DateTimeAdmission.value") :: DATE, 'Mon-YYYY') AS text)
            WHEN  (derived.baseline."NeoTreeOutcome.label" = 'Discharged' AND 
            derived.baseline."DateTimeDischarge.value" IS NULL) OR
            ((derived.baseline."NeoTreeOutcome.label" like '%%Death%%' OR 
            derived.baseline."NeoTreeOutcome.label" like '%%Died%%' OR
            derived.baseline."NeoTreeOutcome.label" like '%%NND%%' OR
            derived.baseline."NeoTreeOutcome.label" like '%%BID%%') AND
            derived.baseline."DateTimeDeath.value" IS NULL
            ) THEN NULL 
            END AS "OutcomeMonthYear",
            {ANSteroids}
            CASE WHEN derived.baseline."Gestation.value" is not null
            and 
            derived.baseline."Gestation.value" < 28 
            AND derived.baseline."BW.value" < 1000 then 1 End AS "Less28wks/1kgCount",
            CASE WHEN derived.baseline."GestGroup.value" <> 'Term' THEN 1 END AS "PretermCount",
            CASE 
            WHEN derived.baseline."NeoTreeOutcome.label" like '%%Death%%' THEN 1 
            WHEN derived.baseline."NeoTreeOutcome.label" like '%%Died%%' THEN 1
            WHEN derived.baseline."NeoTreeOutcome.label" like '%%NND%%' THEN 1 
            WHEN derived.baseline."NeoTreeOutcome.label" like '%%BID%%' THEN 1
            end AS "DeathCount",
            CASE WHEN derived.baseline."NeoTreeOutcome.label" = 'Absconded' Then 1 end AS "AbscondedCount",
            CASE WHEN derived.baseline."NeoTreeOutcome.label" = 'Transferred to other hospital' Then 1 end AS "TransferredOutCount",
            CASE WHEN derived.baseline."NeoTreeOutcome.label" = 'Discharged on Request' Then 1 end AS "DischargeOnRequestCount",
            CASE WHEN derived.baseline."NeoTreeOutcome.label" IN ('Death (at LESS than 24 hrs of age)', 'NND less than 24 hrs old' ) Then 1 end AS "Death<24hrsCount",
            CASE WHEN derived.baseline."NeoTreeOutcome.label" IN ('Death (at MORE than 24 hrs of age)', 'NND more than 24 hrs old' ) Then 1 end AS "Death>24hrsCount",
            CASE WHEN derived.baseline."NeoTreeOutcome.label" = 'NND' Then 1 end AS "NNDCount",
            CASE WHEN derived.baseline."NeoTreeOutcome.label" = 'Stillbirth' Then 1 end AS "StillBirthCount",
            CASE WHEN derived.baseline."NeoTreeOutcome.label" = 'BID - Brought in dead' Then 1 end AS "BIDCount",
            CASE WHEN derived.baseline."NeoTreeOutcome.label" = 'Discharged' Then 1 end AS "DischargeCount",
            CASE WHEN derived.baseline."NeoTreeOutcome.label" IS NOT NULL THEN 1 end AS "NeoTreeOutcomeCount",
            CASE WHEN derived.baseline."BWGroup.value" IS NOT NULL THEN 1 end AS "BirthWeightCount",
            CASE WHEN derived.baseline."AWGroup.value" IS NOT NULL THEN 1 end AS "AdmissionWeightCount",
            CASE WHEN derived.baseline."GestGroup.value" IS NOT NULL THEN 1 end AS "GestationCount",
            CASE WHEN derived.baseline."InOrOut.label" like '%%Outside%%' THEN 1 end AS "OutsideFacilityCount",
            CASE WHEN derived.baseline."InOrOut.label" like '%%Within%%' THEN 1  end AS "WithinFacilityCount",
            CASE WHEN derived.baseline."DateTimeAdmission.value" IS NOT NULL Then 1  end AS "AdmissionCount",
            CASE WHEN derived.baseline."BW.value" < 2500 THEN 1 end AS "PrematureCount",
            CASE WHEN derived.baseline."TempThermia.value" = 'Hypothermia' Then 1 end AS "HypothermiaCount",
            CASE WHEN derived.baseline."TempThermia.value" = 'Hypothermia' Then 1
            WHEN derived.baseline."TempThermia.value" = 'Normothermia' Then 2
            WHEN derived.baseline."TempThermia.value" = 'Hyperthermia' Then 3
            End AS "TempThermiaSort",
            CASE 
            WHEN derived.baseline."AWGroup.value" = '<1000g' THEN 1
            WHEN derived.baseline."AWGroup.value" = '1000-1500g' THEN 2
            WHEN derived.baseline."AWGroup.value" = '1500-2500g' THEN 3
            WHEN derived.baseline."AWGroup.value" = '2500-4000g' THEN 4
              WHEN derived.baseline."AWGroup.value" = '>4000g' THEN 5
            END AS "AdmissionWeightSort",
            CASE 
            WHEN derived.baseline."BWGroup.value" = 'Unknown' THEN 6
            WHEN derived.baseline."BWGroup.value" = 'ELBW' THEN 1
            WHEN derived.baseline."BWGroup.value" = 'VLBW' THEN 2
            WHEN derived.baseline."BWGroup.value" = 'LBW' THEN 3
              WHEN derived.baseline."BWGroup.value" = 'NBW' THEN 4
              WHEN derived.baseline."BWGroup.value" = 'HBW' THEN 5
            END AS "BirthWeightSort",
            CASE
                WHEN derived.baseline."GestGroup.value" = '<28' THEN 1
                WHEN derived.baseline."GestGroup.value" = '28-32 wks' THEN 2
                WHEN derived.baseline."GestGroup.value" = '32-34 wks' THEN 3
                WHEN derived.baseline."GestGroup.value" = '34-36+6 wks' THEN 4
                WHEN derived.baseline."GestGroup.value" = 'Term' THEN 5
            END AS "GestSort",
            CASE
                WHEN derived.baseline."AgeCat.label" = 'Fresh Newborn (< 2 hours old)' THEN 1
                WHEN derived.baseline."AgeCat.label" = 'Newborn (2 - 23 hrs old)' THEN 2
                WHEN derived.baseline."AgeCat.label" = 'Newborn (1 day - 1 day 23 hrs old)' THEN 3
                WHEN derived.baseline."AgeCat.label" = 'Infant (2 days - 2 days 23 hrs old)' THEN 4
                WHEN derived.baseline."AgeCat.label" = 'Infant (> 3 days old)' THEN 5
            END AS "AgeCatSort"
            FROM derived.baseline
            ORDER BY derived.baseline."uid" ASC;; '''