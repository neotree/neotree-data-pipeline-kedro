from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists

#Query to create summary day one vitals table
def summary_joined_vitals_query():
    prefix = f''' DROP TABLE IF EXISTS derived.summary_joined_vitals;;
                CREATE TABLE derived.summary_joined_vitals AS   '''
    where =''
    if table_exists("derived","summary_joined_vitals"):
        prefix= f'''INSERT INTO "derived"."summary_joined_vitals" (
    "NeoTreeID", "facility", "LengthOfStayInDays", "NeonateSepsisStudy", "Day1Date", "Day1DayOfWeek", "Day1PublicHolidayH", 
    "Day1NoOfNursesDayShift", "Day1NoOfNursesNightShift", "Day1NoOfNursesContinousShift", "Day1NoOfNeonates", 
    "Day1NoOfSstudents", "Day1Location", "D1FrequencyOfMonitoring", "Day1_1stTimeofVitalSigns", "Day1Time1", "Day1_2ndTimeofVitalSigns", 
    "Day1Time2", "Day1_3rdTimeofVitalSigns", "Day1Time3", "Day1_4thTimeofVitalSigns", "Day1Time4", "Day1_5thTimeofVitalSigns", "Day1Time5", "Day3_1stTimeofVitalSigns", 
    "Day3Time1", "Day3_2ndTimeofVitalSigns", "Day3Time2", "Day3_3rdTimeofVitalSigns", "Day3Time3", "Day3_4thTimeofVitalSigns", "Day3Time4", 
    "Day3_5thTimeofVitalSigns", "Day3Time5", "Day3Time6", "WasBabyHypothermic", "Temperature1", "TimeOfTemperature1", 
    "WasFollowUpTemperatureDone", "Temperature2", "TimeOfTemperature2"
)  '''
        where = f''' WHERE NOT EXISTS ( SELECT 1  FROM derived.summary_joined_vitals  WHERE "NeoTreeID" = "derived.summary_day1_vitals.NeoTreeID") '''
    
    return prefix+f'''
                    SELECT "derived"."summary_day1_vitals"."Facility Name" AS "Facility Name",
                    "derived"."summary_day1_vitals"."NeoTreeID" AS "NeoTreeID",
                    Date("derived"."summary_day1_vitals"."Date") AS "Date",
                    "derived"."summary_day1_vitals"."Length of stay" AS "Length of stay",
                    "derived"."summary_day1_vitals"."Neonatal Sepsis Study" AS "Neonatal Sepsis Study",
                    "derived"."summary_day1_vitals"."Day of the Week" AS "Day of the Week",
                    "derived"."summary_day1_vitals"."Public Holiday" AS "Public Holiday",
                    "derived"."summary_day1_vitals"."No of Day Shift Nurses" AS "No of Day Shift Nurses",
                    "derived"."summary_day1_vitals"."No of Night Shift Nurses" AS "No of Night Shift Nurses",
                    "derived"."summary_day1_vitals"."No of Countious Shift Nurses" AS "No of Countious Shift Nurses",
                    "derived"."summary_day1_vitals"."No of patients" AS "No of patients",
                    "derived"."summary_day1_vitals"."No of students" AS "No of students",
                    "derived"."summary_day1_vitals"."Location" AS "Location",
                    "derived"."summary_day1_vitals"."Frequency Monitoring" AS "Frequency Monitoring",
                    "derived"."summary_day1_vitals"."Time1" AS "Time1",
                    "derived"."summary_day1_vitals"."Time2" AS "Time2",
                    "derived"."summary_day1_vitals"."Time3" AS "Time3",
                    "derived"."summary_day1_vitals"."Time4" AS "Time4",
                    "derived"."summary_day1_vitals"."Time5" AS "Time5",
                    "derived"."summary_day1_vitals"."Time6" AS "Time6",
                    "derived"."summary_day1_vitals"."Hypothermia" AS "Hypothermia",
                    "derived"."summary_day1_vitals"."Temperature 1" AS "Temperature 1",
                    "derived"."summary_day1_vitals"."Temperature 1 Time" AS "Temperature 1 Time",
                    "derived"."summary_day1_vitals"."Temperature 2 done" AS "Temperature 2 done",
                    "derived"."summary_day1_vitals"."Temperature2" AS "Temperature2",
                    "derived"."summary_day1_vitals"."Temperature2 Time" AS "Temperature2 Time",
                    "derived"."summary_day1_vitals"."Day" AS "Day"
                    FROM "derived"."summary_day1_vitals {where}"
                    UNION ALL
                    SELECT "derived"."summary_day2_vitals"."Facility Name" AS "Facility Name",
                    "derived"."summary_day2_vitals"."NeoTreeID" AS "NeoTreeID",
                    Date("derived"."summary_day2_vitals"."Date") AS "Date",
                    "derived"."summary_day2_vitals"."Length of stay" AS "Length of stay",
                    "derived"."summary_day2_vitals"."Neonatal Sepsis Study" AS "Neonatal Sepsis Study",
                    "derived"."summary_day2_vitals"."Day of the Week" AS "Day of the Week",
                    "derived"."summary_day2_vitals"."Public Holiday" AS "Public Holiday",
                    "derived"."summary_day2_vitals"."No of Day Shift Nurses" AS "No of Day Shift Nurses",
                    "derived"."summary_day2_vitals"."No of Night Shift Nurses" AS "No of Night Shift Nurses",
                    "derived"."summary_day2_vitals"."No of Countious Shift Nurses" AS "No of Countious Shift Nurses",
                    "derived"."summary_day2_vitals"."No of patients" AS "No of patients",
                    "derived"."summary_day2_vitals"."No of students" AS "No of students",
                    "derived"."summary_day2_vitals"."Location" AS "Location",
                    "derived"."summary_day2_vitals"."Frequency Monitoring" AS "Frequency Monitoring",
                    "derived"."summary_day2_vitals"."Time1" AS "Time1",
                    "derived"."summary_day2_vitals"."Time2" AS "Time2",
                    "derived"."summary_day2_vitals"."Time3" AS "Time3",
                    "derived"."summary_day2_vitals"."Time4" AS "Time4",
                    "derived"."summary_day2_vitals"."Time5" AS "Time5",
                    "derived"."summary_day2_vitals"."Time6" AS "Time6",
                    "derived"."summary_day2_vitals"."Hypothermia" AS "Hypothermia",
                    "derived"."summary_day2_vitals"."Temperature 1" AS "Temperature 1",
                    "derived"."summary_day2_vitals"."Temperature 1 Time" AS "Temperature 1 Time",
                    "derived"."summary_day2_vitals"."Temperature 2 done" AS "Temperature 2 done",
                    "derived"."summary_day2_vitals"."Temperature2" AS "Temperature2",
                    "derived"."summary_day2_vitals"."Temperature2 Time" AS "Temperature2 Time",
                    "derived"."summary_day2_vitals"."Day" AS "Day"
                    FROM "derived"."summary_day2_vitals {where}"
                    UNION ALL
                    SELECT "derived"."summary_day3_vitals"."Facility Name" AS "Facility Name",
                    "derived"."summary_day3_vitals"."NeoTreeID" AS "NeoTreeID",
                    Date("derived"."summary_day3_vitals"."Date") AS "Date",
                    "derived"."summary_day3_vitals"."Length of stay" AS "Length of stay",
                    "derived"."summary_day3_vitals"."Neonatal Sepsis Study" AS "Neonatal Sepsis Study",
                    "derived"."summary_day3_vitals"."Day of the Week" AS "Day of the Week",
                    "derived"."summary_day3_vitals"."Public Holiday" AS "Public Holiday",
                    "derived"."summary_day3_vitals"."No of Day Shift Nurses" AS "No of Day Shift Nurses",
                    "derived"."summary_day3_vitals"."No of Night Shift Nurses" AS "No of Night Shift Nurses",
                    "derived"."summary_day3_vitals"."No of Countious Shift Nurses" AS "No of Countious Shift Nurses",
                    "derived"."summary_day3_vitals"."No of patients" AS "No of patients",
                    "derived"."summary_day3_vitals"."No of students" AS "No of students",
                    "derived"."summary_day3_vitals"."Location" AS "Location",
                    "derived"."summary_day3_vitals"."Frequency Monitoring" AS "Frequency Monitoring",
                    "derived"."summary_day3_vitals"."Time1" AS "Time1",
                    "derived"."summary_day3_vitals"."Time2" AS "Time2",
                    "derived"."summary_day3_vitals"."Time3" AS "Time3",
                    "derived"."summary_day3_vitals"."Time4" AS "Time4",
                    "derived"."summary_day3_vitals"."Time5" AS "Time5",
                    "derived"."summary_day3_vitals"."Time6" AS "Time6",
                    "derived"."summary_day3_vitals"."Hypothermia" AS "Hypothermia",
                    "derived"."summary_day3_vitals"."Temperature 1" AS "Temperature 1",
                    "derived"."summary_day3_vitals"."Temperature 1 Time" AS "Temperature 1 Time",
                    "derived"."summary_day3_vitals"."Temperature 2 done" AS "Temperature 2 done",
                    "derived"."summary_day3_vitals"."Temperature2" AS "Temperature2",
                    "derived"."summary_day3_vitals"."Temperature2 Time" AS "Temperature2 Time",
                    "derived"."summary_day3_vitals"."Day" AS "Day"
                FROM "derived"."summary_day3_vitals " {where};;
        '''