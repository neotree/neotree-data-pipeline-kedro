from conf.base.catalog import params

#Query to create the summary_counts table
def summary_counts_query():
        #Customise Queries For different Use Cases
        outcome_month_year = ' '
        group_by_additional_columns = ' '
        
        if('country' in params and str(params['country']).lower()) =='zimbabwe':
           #Add Outcome Year Month For Zim Use Case
           outcome_month_year ='derived.summary_joined_admissions_discharges."OutcomeMonthYear" AS "OutcomeMonthYear",'   
           group_by_additional_columns = ',derived.summary_joined_admissions_discharges."OutcomeMonthYear" '
        return '''DROP TABLE IF EXISTS derived.summary_counts;
                CREATE TABLE derived.summary_counts AS
                SELECT  
                derived.summary_joined_admissions_discharges."AdmissionMonthYear" AS "AdmissionMonthYear",
                derived.summary_joined_admissions_discharges."facility" AS "facility",
                derived.summary_joined_admissions_discharges."AdmissionMonthYearSort" AS "AdmissionMonthYearSort", 
                sum(derived.summary_joined_admissions_discharges."AdmissionCount") AS "TotalAdmissions", 
                sum(derived.summary_joined_admissions_discharges."DischargeCount") AS "TotalDischarges", {0}
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
                summary_joined_admissions_discharges."facility",
                derived.summary_joined_admissions_discharges."AdmissionMonthYearSort" {1} 
                 ORDER BY derived.summary_joined_admissions_discharges."AdmissionMonthYearSort" ASC, 
                derived.summary_joined_admissions_discharges."AdmissionMonthYear" ASC;
                '''.format(outcome_month_year, group_by_additional_columns)