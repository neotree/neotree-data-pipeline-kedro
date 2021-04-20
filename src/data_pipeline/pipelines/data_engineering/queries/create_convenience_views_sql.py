#Query to create the Summary_Joined_Admissions_Discharges table.
def create_convinience_views_query():
  return ''' DROP TABLE IF EXISTS derived.summary_joined_admissions_discharges;
    CREATE TABLE derived.summary_joined_admissions_discharges AS
    SELECT derived.joined_admissions_discharges."uid" AS "uid", 
        derived.joined_admissions_discharges."facility" AS "facility", 
        derived.joined_admissions_discharges."DateTimeAdmission.value" AS "AdmissionDateTime",
        derived.joined_admissions_discharges."Readmission.label" AS "Readmitted", 
        derived.joined_admissions_discharges."AdmittedFrom.label" AS "admission_source",
            derived.joined_admissions_discharges."ReferredFrom2.label" AS "referredFrom", 
            derived.joined_admissions_discharges."Gender.label" AS "Gender", 
            derived.joined_admissions_discharges."AW.value" AS "AdmissionWeight", 
            derived.joined_admissions_discharges."AWGroup.value" AS "AdmissionWeightGroup", 
            derived.joined_admissions_discharges."BW.value" AS "BirthWeight", 
            derived.joined_admissions_discharges."BWGroup.value" AS "BirthWeightGroup",
            derived.joined_admissions_discharges."Genitalia.value" AS "Genitalia",
            derived.joined_admissions_discharges."Gestation.value" AS "Gestation",
            derived.joined_admissions_discharges."MethodEstGest.label" AS "ModeOfEsttimating", 
            derived.joined_admissions_discharges."AgeCat.label" AS "AgeCategory",
            derived.joined_admissions_discharges."MatHIVtest.label" AS "MotherHIVTest", 
            derived.joined_admissions_discharges."HIVtestResult.label" AS "HIVTestResult", 
            derived.joined_admissions_discharges."HAART.label" AS "OnHAART",
            derived.joined_admissions_discharges."LengthHAART.label" AS "LengthOfHAART",
            derived.joined_admissions_discharges."TempThermia.value" AS "TempThermia",
            derived.joined_admissions_discharges."NVPgiven.label" AS "NVPgiven", 
            derived.joined_admissions_discharges."TempGroup.value" AS "TempGroup",
            derived.joined_admissions_discharges."Temperature.value" AS "Temperature", 
            derived.joined_admissions_discharges."GestGroup.value" As "GestationGroup",
            derived.joined_admissions_discharges."InOrOut.label" AS "InOrOut",
            derived.joined_admissions_discharges."ReferredFrom.label" AS "FacilityReferredFrom",
            derived.joined_admissions_discharges."DateTimeDischarge.value" AS "DischargeDateTime",
            derived.joined_admissions_discharges."NeoTreeOutcome.label" AS "NeonateOutcome", 
            derived.joined_admissions_discharges."LengthOfLife.value" AS "LengthOfLife", 
            derived.joined_admissions_discharges."LengthOfStay.value" AS "LengthOfStay",
            derived.joined_admissions_discharges."RR.value" AS "RespiratoryRate", 
            derived.joined_admissions_discharges."SatsAir.value" AS "OxygenSatsInAir", 
            derived.joined_admissions_discharges."HR.value" AS "HeartRate",
            derived.joined_admissions_discharges."BSmmol.value" AS "BloodSugar_mmo_L",
            derived.joined_admissions_discharges."BSmg.value" AS "BloodSugar_mg_dL",
            derived.joined_admissions_discharges."OFC.value" AS "HeadCircumf",
            derived.joined_admissions_discharges."SatsO2.value" AS "OxygenSats",
            CAST(TO_CHAR(DATE(derived.joined_admissions_discharges."DateTimeAdmission.value") :: DATE, 'Mon-YYYY') AS text) AS "AdmissionMonthYear", 
            CAST(TO_CHAR(DATE(derived.joined_admissions_discharges."DateTimeAdmission.value") :: DATE, 'YYYYmm') AS decimal) AS "AdmissionMonthYearSort",
            CASE WHEN derived.joined_admissions_discharges."DateTimeDischarge.value"  IS NOT NULL THEN 
            CAST(TO_CHAR(DATE(derived.joined_admissions_discharges."DateTimeDischarge.value") :: DATE, 'Mon-YYYY') AS text)
            WHEN derived.joined_admissions_discharges."DateTimeDeath.value" IS NOT NULL THEN
            CAST(TO_CHAR(DATE(derived.joined_admissions_discharges."DateTimeDeath.value") :: DATE, 'Mon-YYYY') AS text)
            WHEN derived.joined_admissions_discharges."NeoTreeOutcome.label" = 'Absconded' THEN
            CAST(TO_CHAR(DATE(derived.joined_admissions_discharges."DateTimeAdmission.value") :: DATE, 'Mon-YYYY') AS text)
            WHEN  (derived.joined_admissions_discharges."NeoTreeOutcome.label" = 'Discharged' AND 
            derived.joined_admissions_discharges."DateTimeDischarge.value" IS NULL) OR
            ((derived.joined_admissions_discharges."NeoTreeOutcome.label" like '%%Death%%' OR 
            derived.joined_admissions_discharges."NeoTreeOutcome.label" like '%%Died%%' OR
            derived.joined_admissions_discharges."NeoTreeOutcome.label" like '%%NND%%' OR
            derived.joined_admissions_discharges."NeoTreeOutcome.label" like '%%BID%%') AND
            derived.joined_admissions_discharges."DateTimeDeath.value" IS NULL
            ) THEN NULL 
            END AS "OutcomeMonthYear",
            derived.joined_admissions_discharges."ANSteroids.label" As "AntenatalSteroids",
            CASE WHEN derived.joined_admissions_discharges."Gestation.value" < 28 AND derived.joined_admissions_discharges."BW.value" < 1000 then 1 End AS "Less28wks/1kgCount",
            CASE WHEN derived.joined_admissions_discharges."GestGroup.value" <> 'Term' THEN 1 END AS "PretermCount",
            CASE 
            WHEN derived.joined_admissions_discharges."NeoTreeOutcome.label" like '%%Death%%' THEN 1 
            WHEN derived.joined_admissions_discharges."NeoTreeOutcome.label" like '%%Died%%' THEN 1
            WHEN derived.joined_admissions_discharges."NeoTreeOutcome.label" like '%%NND%%' THEN 1 
            WHEN derived.joined_admissions_discharges."NeoTreeOutcome.label" like '%%BID%%' THEN 1
            end AS "DeathCount",
            CASE WHEN derived.joined_admissions_discharges."NeoTreeOutcome.label" = 'Absconded' Then 1 end AS "AbscondedCount",
            CASE WHEN derived.joined_admissions_discharges."NeoTreeOutcome.label" = 'Transferred to other hospital' Then 1 end AS "TransferredOutCount",
            CASE WHEN derived.joined_admissions_discharges."NeoTreeOutcome.label" = 'Discharged on Request' Then 1 end AS "DischargeOnRequestCount",
            CASE WHEN derived.joined_admissions_discharges."NeoTreeOutcome.label" IN ('Death (at LESS than 24 hrs of age)', 'NND less than 24 hrs old' ) Then 1 end AS "Death<24hrsCount",
            CASE WHEN derived.joined_admissions_discharges."NeoTreeOutcome.label" IN ('Death (at MORE than 24 hrs of age)', 'NND more than 24 hrs old' ) Then 1 end AS "Death>24hrsCount",
            CASE WHEN derived.joined_admissions_discharges."NeoTreeOutcome.label" = 'NND' Then 1 end AS "NNDCount",
            CASE WHEN derived.joined_admissions_discharges."NeoTreeOutcome.label" = 'Stillbirth' Then 1 end AS "StillBirthCount",
            CASE WHEN derived.joined_admissions_discharges."NeoTreeOutcome.label" = 'BID - Brought in dead' Then 1 end AS "BIDCount",
            CASE WHEN derived.joined_admissions_discharges."NeoTreeOutcome.label" = 'Discharged' Then 1 end AS "DischargeCount",
            CASE WHEN derived.joined_admissions_discharges."NeoTreeOutcome.label" IS NOT NULL THEN 1 end AS "NeoTreeOutcomeCount",
            CASE WHEN derived.joined_admissions_discharges."BWGroup.value" IS NOT NULL THEN 1 end AS "BirthWeightCount",
            CASE WHEN derived.joined_admissions_discharges."AWGroup.value" IS NOT NULL THEN 1 end AS "AdmissionWeightCount",
            CASE WHEN derived.joined_admissions_discharges."GestGroup.value" IS NOT NULL THEN 1 end AS "GestationCount",
            CASE WHEN derived.joined_admissions_discharges."InOrOut.label" like '%%Outside%%' THEN 1 end AS "OutsideFacilityCount",
            CASE WHEN derived.joined_admissions_discharges."InOrOut.label" like '%%Within%%' THEN 1  end AS "WithinFacilityCount",
            CASE WHEN derived.joined_admissions_discharges."DateTimeAdmission.value" IS NOT NULL Then 1  end AS "AdmissionCount",
            CASE WHEN derived.joined_admissions_discharges."BW.value" < 2500 THEN 1 end AS "PrematureCount",
            CASE WHEN derived.joined_admissions_discharges."TempThermia.value" = 'Hypothermia' Then 1 end AS "HypothermiaCount",
            CASE WHEN derived.joined_admissions_discharges."TempThermia.value" = 'Hypothermia' Then 1
            WHEN derived.joined_admissions_discharges."TempThermia.value" = 'Normothermia' Then 2
            WHEN derived.joined_admissions_discharges."TempThermia.value" = 'Hyperthermia' Then 3
        End AS "TempThermiaSort",
        CASE 
            WHEN derived.joined_admissions_discharges."AWGroup.value" = '<1000g' THEN 1
            WHEN derived.joined_admissions_discharges."AWGroup.value" = '1000-1500g' THEN 2
            WHEN derived.joined_admissions_discharges."AWGroup.value" = '1500-2500g' THEN 3
            WHEN derived.joined_admissions_discharges."AWGroup.value" = '2500-4000g' THEN 4
              WHEN derived.joined_admissions_discharges."AWGroup.value" = '>4000g' THEN 5
            END AS "AdmissionWeightSort",
            CASE 
            WHEN derived.joined_admissions_discharges."BWGroup.value" = 'Unknown' THEN 6
            WHEN derived.joined_admissions_discharges."BWGroup.value" = 'ELBW' THEN 1
            WHEN derived.joined_admissions_discharges."BWGroup.value" = 'VLBW' THEN 2
            WHEN derived.joined_admissions_discharges."BWGroup.value" = 'LBW' THEN 3
              WHEN derived.joined_admissions_discharges."BWGroup.value" = 'NBW' THEN 4
              WHEN derived.joined_admissions_discharges."BWGroup.value" = 'HBW' THEN 5
            END AS "BirthWeightSort",
            CASE
                WHEN derived.joined_admissions_discharges."GestGroup.value" = '<28' THEN 1
                WHEN derived.joined_admissions_discharges."GestGroup.value" = '28-32 wks' THEN 2
                WHEN derived.joined_admissions_discharges."GestGroup.value" = '32-34 wks' THEN 3
                WHEN derived.joined_admissions_discharges."GestGroup.value" = '34-36+6 wks' THEN 4
                WHEN derived.joined_admissions_discharges."GestGroup.value" = 'Term' THEN 5
            END AS "GestSort",
            CASE
                WHEN derived.joined_admissions_discharges."AgeCat.label" = 'Fresh Newborn (< 2 hours old)' THEN 1
                WHEN derived.joined_admissions_discharges."AgeCat.label" = 'Newborn (2 - 23 hrs old)' THEN 2
                WHEN derived.joined_admissions_discharges."AgeCat.label" = 'Newborn (1 day - 1 day 23 hrs old)' THEN 3
                WHEN derived.joined_admissions_discharges."AgeCat.label" = 'Infant (2 days - 2 days 23 hrs old)' THEN 4
                WHEN derived.joined_admissions_discharges."AgeCat.label" = 'Infant (> 3 days old)' THEN 5
            END AS "AgeCatSort"
    FROM derived.joined_admissions_discharges
    ORDER BY derived.joined_admissions_discharges."uid" ASC; '''
