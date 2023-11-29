#Query to create summary day two vitals table
def summary_day_one_vitals_query():
    return '''DROP TABLE IF EXISTS derived.summary_day1_vitals;;
        CREATE TABLE derived.summary_day1_vitals AS 
        SELECT "derived"."vitalsigns"."facility" AS "Facility Name",
        "derived"."vitalsigns"."uid" AS "NeoTreeID",
        "derived"."vitalsigns"."D1Date.value" AS "Date",
        "derived"."vitalsigns"."LengthOfStay.value" AS "Length of stay",
        "derived"."vitalsigns"."NSS.label" AS "Neonatal Sepsis Study",
        CASE WHEN "derived"."vitalsigns"."D1DayOfWk.value" = 'M' Then 'Monday' 
        WHEN "derived"."vitalsigns"."D1DayOfWk.value" = 'T' Then 'Tuesday' 
        WHEN "derived"."vitalsigns"."D1DayOfWk.value" = 'W' Then 'Wednesday' 
        WHEN "derived"."vitalsigns"."D1DayOfWk.value" = 'Th' Then 'Thursday' 
        WHEN "derived"."vitalsigns"."D1DayOfWk.value" = 'F' Then 'Friday' 
        WHEN "derived"."vitalsigns"."D1DayOfWk.value" = 'Sat' Then 'Saturday' 
        WHEN "derived"."vitalsigns"."D1DayOfWk.value" = 'Sun' Then 'Sunday' 
        END AS "Day of the Week",
        "derived"."vitalsigns"."D1PH.label" AS "Public Holiday",
        "derived"."vitalsigns"."D1DayNurses.value" AS "No of Day Shift Nurses",
        "derived"."vitalsigns"."D1NightNurses.value" AS "No of Night Shift Nurses",
        "derived"."vitalsigns"."D1Nurses40.value" AS "No of Countious Shift Nurses",
        "derived"."vitalsigns"."D1Patients.value" AS "No of patients",
        "derived"."vitalsigns"."D1students.value" AS "No of students",
        "derived"."vitalsigns"."D1Location.label" AS "Location",
        "derived"."vitalsigns"."D1FreqMon.value" AS "Frequency Monitoring",
        substring("derived"."vitalsigns"."D1Time1.value" from (position('T' in "derived"."vitalsigns"."D1Time1.value") + 1) for 5) AS "Time1",
        substring("derived"."vitalsigns"."D1Time2.value" from (position('T' in "derived"."vitalsigns"."D1Time2.value") + 1) for 5) AS "Time2",
        substring("derived"."vitalsigns"."D1Time3.value" from (position('T' in "derived"."vitalsigns"."D1Time3.value") + 1) for 5) AS "Time3",
        substring("derived"."vitalsigns"."D1Time4.value" from (position('T' in "derived"."vitalsigns"."D1Time4.value") + 1) for 5) AS "Time4",
        substring("derived"."vitalsigns"."D1Time5.value" from (position('T' in "derived"."vitalsigns"."D1Time5.value") + 1) for 5) AS "Time5",
        substring("derived"."vitalsigns"."D1Time6.value" from (position('T' in "derived"."vitalsigns"."D1Time6.value") + 1) for 5) AS "Time6",
        "derived"."vitalsigns"."HypothPres.label" AS "Hypothermia",
        "derived"."vitalsigns"."Temp1.value" AS "Temperature 1",
        "derived"."vitalsigns"."TimeTemp1.value" AS "Temperature 1 Time",
        "derived"."vitalsigns"."Temp2done.value" AS "Temperature 2 done",
        "derived"."vitalsigns"."Temp2.value" AS "Temperature2",
        "derived"."vitalsigns"."TimeTemp2.value" AS "Temperature2 Time",
        CASE WHEN "derived"."vitalsigns"."D1Date.value" IS NOT NULL THEN 1 END AS "Day" FROM "derived"."vitalsigns";;
        '''