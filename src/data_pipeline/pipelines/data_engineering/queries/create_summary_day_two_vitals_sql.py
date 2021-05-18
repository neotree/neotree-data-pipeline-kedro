#Query to create summary day one vitals table
def summary_day_two_vitals_query():
    return '''DROP TABLE IF EXISTS derived.summary_day2_vitals;
        CREATE TABLE derived.summary_day2_vitals AS 
        SELECT "derived"."vitalsigns"."facility" AS "Facility Name",
        "derived"."vitalsigns"."uid" AS "NeoTreeID",
        "derived"."vitalsigns"."D2Date.value" AS "Date",
        "derived"."vitalsigns"."LengthOfStay.value" AS "Length of stay",
        "derived"."vitalsigns"."NSS.label" AS "Neonatal Sepsis Study",
        CASE WHEN "derived"."vitalsigns"."D2DayOfWk.value" = 'M' Then 'Monday'
        WHEN "derived"."vitalsigns"."D2DayOfWk.value" = 'T' Then 'Tuesday' 
        WHEN "derived"."vitalsigns"."D2DayOfWk.value" = 'W' Then 'Wednesday' 
        WHEN "derived"."vitalsigns"."D2DayOfWk.value" = 'Th' Then 'Thursday' 
        WHEN "derived"."vitalsigns"."D2DayOfWk.value" = 'F' Then 'Friday' 
        WHEN "derived"."vitalsigns"."D2DayOfWk.value" = 'Sat' Then 'Saturday' 
        WHEN "derived"."vitalsigns"."D2DayOfWk.value" = 'Sun' Then 'Sunday' END as "Day of the Week",
        "derived"."vitalsigns"."D2PH.label" AS "Public Holiday",
        "derived"."vitalsigns"."D2DayNurses.value" AS "No of Day Shift Nurses",
        "derived"."vitalsigns"."D2NightNurses.value" AS "No of Night Shift Nurses",
        "derived"."vitalsigns"."D2Nurses40.value" AS "No of Countious Shift Nurses",
        "derived"."vitalsigns"."D2Patients.value" AS "No of patients",
        "derived"."vitalsigns"."D2Students.value" AS "No of students",
        "derived"."vitalsigns"."D2Location.label" AS "Location",
        "derived"."vitalsigns"."D2FreqMon.value" AS "Frequency Monitoring",
        substring("derived"."vitalsigns"."D2Time1.value" from (position('T' in "derived"."vitalsigns"."D2Time1.value") + 1) for 5) AS "Time1",
        substring("derived"."vitalsigns"."D2Time2.value" from (position('T' in "derived"."vitalsigns"."D2Time2.value") + 1) for 5) AS "Time2",
        substring("derived"."vitalsigns"."D2Time3.value" from (position('T' in "derived"."vitalsigns"."D2Time3.value") + 1) for 5) AS "Time3",
        substring("derived"."vitalsigns"."D2Time4.value" from (position('T' in "derived"."vitalsigns"."D2Time4.value") + 1) for 5) AS "Time4",
        substring("derived"."vitalsigns"."D2Time5.value" from (position('T' in "derived"."vitalsigns"."D2Time5.value") + 1) for 5) AS "Time5",
        (select '' ) AS "Time6","derived"."vitalsigns"."HypothPres.label" AS "Hypothermia","derived"."vitalsigns"."Temp1.value" AS "Temperature 1",
        "derived"."vitalsigns"."TimeTemp1.value" AS "Temperature 1 Time",
        "derived"."vitalsigns"."Temp2done.value" AS "Temperature 2 done",
        "derived"."vitalsigns"."Temp2.value" AS "Temperature2",
        "derived"."vitalsigns"."TimeTemp2.value" AS "Temperature2 Time",
        CASE WHEN "derived"."vitalsigns"."D1Date.value" IS NOT NULL THEN 2 END As "Day" FROM "derived"."vitalsigns" 
        '''