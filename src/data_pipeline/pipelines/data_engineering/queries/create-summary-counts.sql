DROP TABLE IF EXISTS derived.summary_counts;
CREATE TABLE derived.summary_counts AS
SELECT  
derived.summary_joined_admissions_discharges."AdmissionMonthYear" AS "AdmissionMonthYear",
        derived.summary_joined_admissions_discharges."AdmissionMonthYearSort" AS "AdmissionMonthYearSort", 
        sum(derived.summary_joined_admissions_discharges."AdmissionCount") AS "sum", 
        sum(derived.summary_joined_admissions_discharges."DischargeCount") AS "sum_2", 
        sum(derived.summary_joined_admissions_discharges."DeathCount") AS "sum_3",
        sum(derived.summary_joined_admissions_discharges."Death<24hrsCount") AS "sum_4",
        sum(derived.summary_joined_admissions_discharges."Death>24hrsCount") AS "sum_5", 
        sum(derived.summary_joined_admissions_discharges."TransferredOutCount") AS "sum_6", 
        sum(derived.summary_joined_admissions_discharges."AbscondedCount") AS "sum_7", 
        sum(derived.summary_joined_admissions_discharges."DischargeOnRequestCount") AS "sum_8", 
        sum(derived.summary_joined_admissions_discharges."WithinFacilityCount") AS "sum_9", 
        sum(derived.summary_joined_admissions_discharges."OutsideFacilityCount") AS "sum_10",
        sum(derived.summary_joined_admissions_discharges."PrematureCount") AS "sum_11", 
        sum(derived.summary_joined_admissions_discharges."HypothermiaCount") AS "sum_12", 
        sum(derived.summary_joined_admissions_discharges."Less28wks/1kgCount") AS "sum_13"
FROM derived.summary_joined_admissions_discharges
GROUP BY derived.summary_joined_admissions_discharges."AdmissionMonthYear", 
derived.summary_joined_admissions_discharges."AdmissionMonthYearSort"
ORDER BY derived.summary_joined_admissions_discharges."AdmissionMonthYearSort" ASC, 
derived.summary_joined_admissions_discharges."AdmissionMonthYear" ASC;