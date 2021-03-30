DROP TABLE IF EXISTS derived.summary_counts;
CREATE TABLE derived.summary_counts AS
SELECT  
derived.summary_joined_admissions_discharges."AdmissionMonthYear" AS "AdmissionMonthYear",
        derived.summary_joined_admissions_discharges."AdmissionMonthYearSort" AS "AdmissionMonthYearSort", 
        sum(derived.summary_joined_admissions_discharges."AdmissionCount") AS "TotalAdmissions", 
        sum(derived.summary_joined_admissions_discharges."DischargeCount") AS "TotalDischarges", 
        sum(derived.summary_joined_admissions_discharges."DeathCount") AS "TotalDeaths",
        sum(derived.summary_joined_admissions_discharges."Death<24hrsCount") AS "TotalDeaths<24hrs",
        sum(derived.summary_joined_admissions_discharges."Death>24hrsCount") AS "TotalDeaths>24hrs", 
        sum(derived.summary_joined_admissions_discharges."TransferredOutCount") AS "TotalTransferredOut", 
        sum(derived.summary_joined_admissions_discharges."AbscondedCount") AS "TotalAbsconded", 
        sum(derived.summary_joined_admissions_discharges."DischargeOnRequestCount") AS "TotalDischargeOnRequest", 
        sum(derived.summary_joined_admissions_discharges."WithinFacilityCount") AS "TotalAdmittedFronWithinFacility", 
        sum(derived.summary_joined_admissions_discharges."OutsideFacilityCount") AS "TotalAdmittedFromOutsideFacility",
        sum(derived.summary_joined_admissions_discharges."PrematureCount") AS "TotalPremBabies", 
        sum(derived.summary_joined_admissions_discharges."HypothermiaCount") AS "TotalHypothemiaBabies", 
        sum(derived.summary_joined_admissions_discharges."Less28wks/1kgCount") AS "TotalBabiesWith<28wksAnd1kgs"
FROM derived.summary_joined_admissions_discharges
GROUP BY derived.summary_joined_admissions_discharges."AdmissionMonthYear",
derived.summary_joined_admissions_discharges."AdmissionMonthYearSort"
ORDER BY derived.summary_joined_admissions_discharges."AdmissionMonthYearSort" ASC, 
derived.summary_joined_admissions_discharges."AdmissionMonthYear" ASC;