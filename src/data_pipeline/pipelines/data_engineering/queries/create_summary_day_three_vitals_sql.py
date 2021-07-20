# Query to create summary day one vitals table
def summary_day_three_vitals_query():
    return '''DROP TABLE IF EXISTS derived.summary_day3_vitals;
        CREATE TABLE derived.summary_day3_vitals AS 
       SELECT "derived"."vitalsigns"."facility" AS "Facility Name",
"derived"."vitalsigns"."uid" AS "NeoTreeID",
Date("derived"."vitalsigns"."D3Date.value") AS "Date",
"derived"."vitalsigns"."LengthOfStay.value" AS "Length of stay",
"derived"."vitalsigns"."NSS.label" AS "Neonatal Sepsis Study",
case
when "derived"."vitalsigns"."D3DayOfWk.value" = 'M' Then 'Monday'
when "derived"."vitalsigns"."D3DayOfWk.value" = 'T' Then 'Tuesday'
when "derived"."vitalsigns"."D3DayOfWk.value" = 'W' Then 'Wednesday'
when "derived"."vitalsigns"."D3DayOfWk.value" = 'Th' Then 'Thursday'
when "derived"."vitalsigns"."D3DayOfWk.value" = 'F' Then 'Friday'
when "derived"."vitalsigns"."D3DayOfWk.value" = 'Sat' Then 'Saturday'
when "derived"."vitalsigns"."D3DayOfWk.value" = 'Sun' Then 'Sunday'
end as "Day of the Week",
"derived"."vitalsigns"."D3PH.label" AS "Public Holiday",
"derived"."vitalsigns"."D3DayNurses.value" AS "No of Day Shift Nurses",
"derived"."vitalsigns"."D3NightNurses.value" AS "No of Night Shift Nurses",
"derived"."vitalsigns"."D3Nurses40.value" AS "No of Countious Shift Nurses",
"derived"."vitalsigns"."D3Patients.value" AS "No of patients",
"derived"."vitalsigns"."D3Students.value" AS "No of students",
"derived"."vitalsigns"."D3Location.label" AS "Location",
"derived"."vitalsigns"."D3FreqMon.value" AS "Frequency Monitoring",
substring("derived"."vitalsigns"."D3Time1.value" from (position('T' in "derived"."vitalsigns"."D3Time1.value") + 1) for 5) AS "Time1",
substring("derived"."vitalsigns"."D3Time2.value" from (position('T' in "derived"."vitalsigns"."D3Time2.value") + 1) for 5) AS "Time2",
substring("derived"."vitalsigns"."D3Time3.value" from (position('T' in "derived"."vitalsigns"."D3Time3.value") + 1) for 5) AS "Time3",
substring("derived"."vitalsigns"."D3Time4.value" from (position('T' in "derived"."vitalsigns"."D3Time4.value") + 1) for 5) AS "Time4",
substring("derived"."vitalsigns"."D3Time5.value" from (position('T' in "derived"."vitalsigns"."D3Time5.value") + 1) for 5) AS "Time5",
substring("derived"."vitalsigns"."D3Time6.value" from (position('T' in "derived"."vitalsigns"."D3Time6.value") + 1) for 5) AS "Time6",
"derived"."vitalsigns"."HypothPres.label" AS "Hypothermia",
"derived"."vitalsigns"."Temp1.value" AS "Temperature 1",
"derived"."vitalsigns"."TimeTemp1.value" AS "Temperature 1 Time",
"derived"."vitalsigns"."Temp2done.value" AS "Temperature 2 done",
"derived"."vitalsigns"."Temp2.value" AS "Temperature2",
"derived"."vitalsigns"."TimeTemp2.value" AS "Temperature2 Time",
case
when "derived"."vitalsigns"."D3Date.value" IS NOT NULL THEN 3
end As "Day"
FROM "derived"."vitalsigns"; 
        '''
