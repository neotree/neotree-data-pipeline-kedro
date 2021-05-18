#Query to create summary day one vitals table
def summary_joined_vitals_query():
    return '''DROP TABLE IF EXISTS derived.summary_joined_vitals;
        CREATE TABLE derived.summary_joined_vitals AS 
        SELECT "source1"."Facility Name" AS "Facility Name",
        "source1"."NeoTreeID" AS "NeoTreeID", 
        "source1"."Date" AS "Date", "source1"."Length of stay" AS "Length of stay", 
        "source1"."Neonatal Sepsis Study" AS "Neonatal Sepsis Study",
        "source1"."Day of the week" AS "Day of the week", 
        "source1"."DayOftheWeekCount" AS "DayOftheWeekCount",
        "source1"."Public Holiday" AS "Public Holiday", 
        "source1"."No of Day Shift Nurses" AS "No of Day Shift Nurses",
        "source1"."No of Night Shift Nurses" AS "No of Night Shift Nurses",
        "source1"."No of Countious Shift Nurses" AS "No of Countious Shift Nurses", 
        "source1"."No of patients" AS "No of patients", 
        "source1"."No of students" AS "No of students", 
        "source1"."Location" AS "Location", 
        "source1"."Frequency Monitoring" AS "Frequency Monitoring", 
        "source1"."Time1" AS "Time1", 
        "source1"."Time2" AS "Time2",
        "source1"."Time3" AS "Time3", 
        "source1"."Time4" AS "Time4", 
        "source1"."Time5" AS "Time5", 
        "source1"."Time6" AS "Time6", 
        "source1"."Hypothermia" AS "Hypothermia",
        "source1"."Temperature 1" AS "Temperature 1",
        "source1"."Temperature 1 Time" AS "Temperature 1 Time",
        "source1"."Temperature 2 done" AS "Temperature 2 done", 
        "source1"."Temperature2" AS "Temperature2",
        "source1"."Temperature2 Time" AS "Temperature2 Time", "source1"."Day" AS "Day"
FROM (SELECT "derived"."vitalsigns"."facility" AS "Facility Name",
"derived"."vitalsigns"."uid" AS "NeoTreeID",
DATE("derived"."vitalsigns"."D2Date.value") AS "Date",
"derived"."vitalsigns"."LengthOfStay.value" AS "Length of stay",
"derived"."vitalsigns"."NSS.label" AS "Neonatal Sepsis Study",
case
when "derived"."vitalsigns"."D2DayOfWk.value" = 'M' Then 'Monday'
when "derived"."vitalsigns"."D2DayOfWk.value" = 'T' Then 'Tuesday'
when "derived"."vitalsigns"."D2DayOfWk.value" = 'W' Then 'Wednesday'
when "derived"."vitalsigns"."D2DayOfWk.value" = 'Th' Then 'Thursday'
when "derived"."vitalsigns"."D2DayOfWk.value" = 'F' Then 'Friday'
when "derived"."vitalsigns"."D2DayOfWk.value" = 'Sat' Then 'Saturday'
when "derived"."vitalsigns"."D2DayOfWk.value" = 'Sun' Then 'Sunday'
end as "Day of the week",
case
when "derived"."vitalsigns"."D2DayOfWk.value" = 'M' Then 1
when "derived"."vitalsigns"."D2DayOfWk.value" = 'T' Then 2
when "derived"."vitalsigns"."D2DayOfWk.value" = 'W' Then 3
when "derived"."vitalsigns"."D2DayOfWk.value" = 'Th' Then 4
when "derived"."vitalsigns"."D2DayOfWk.value" = 'F' Then 5
when "derived"."vitalsigns"."D2DayOfWk.value" = 'Sat' Then 6
when "derived"."vitalsigns"."D2DayOfWk.value" = 'Sun' Then 7
end as "DayOftheWeekCount",
"derived"."vitalsigns"."D2PH.label" AS "Public Holiday",
Case
when "derived"."vitalsigns"."D2DayNurses.value" = 1 Then '1-Nurse'
when "derived"."vitalsigns"."D2DayNurses.value" = 2 Then '2-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 3 Then '3-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 4 Then '4-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 5 Then '5-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 6 Then '6-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 7 Then '7-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 8 Then '8-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 9 Then '9-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" >= 10 Then '>10 Nurses'
end as "No of Day Shift Nurses",
Case
when "derived"."vitalsigns"."D2NightNurses.value" = 1 Then '1-Nurse'
when "derived"."vitalsigns"."D2NightNurses.value" = 2 Then '2-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 3 Then '3-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 4 Then '4-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 5 Then '5-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 6 Then '6-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 7 Then '7-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 8 Then '8-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 9 Then '9-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" >= 10 Then '>=10 Nurses'
end as "No of Night Shift Nurses",
Case
when "derived"."vitalsigns"."D2Nurses40.value" = 1 Then '1-Nurse'
when "derived"."vitalsigns"."D2Nurses40.value" = 2 Then '2-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 3 Then '3-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 4 Then '4-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 5 Then '5-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 6 Then '6-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 7 Then '7-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 8 Then '8-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 9 Then '9-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" >= 10 Then '>=10 Nurses'
end as "No of Countious Shift Nurses",
"derived"."vitalsigns"."D2Patients.value" AS "No of patients",
"derived"."vitalsigns"."D2Students.value" AS "No of students",
case
when "derived"."vitalsigns"."D2Location.label" = 'HIgh-Risk' Then 'High-Risk'
when "derived"."vitalsigns"."D2Location.label" = 'High-Risk' Then 'High-Risk'
when "derived"."vitalsigns"."D2Location.label" = 'Isolation' Then 'Isolation'
when "derived"."vitalsigns"."D2Location.label" = 'Location of baby (Day 3)' Then 'Unknown' 
when "derived"."vitalsigns"."D2Location.label" = 'Low-Risk' Then 'Low-Risk'
end as "Location",
"derived"."vitalsigns"."D2FreqMon.value" AS "Frequency Monitoring",
substring("derived"."vitalsigns"."D2Time1.value" from (position('T' in "derived"."vitalsigns"."D2Time1.value") + 1) for 5) AS "Time1",
substring("derived"."vitalsigns"."D2Time2.value" from (position('T' in "derived"."vitalsigns"."D2Time2.value") + 1) for 5) AS "Time2",
substring("derived"."vitalsigns"."D2Time3.value" from (position('T' in "derived"."vitalsigns"."D2Time3.value") + 1) for 5) AS "Time3",
substring("derived"."vitalsigns"."D2Time4.value" from (position('T' in "derived"."vitalsigns"."D2Time4.value") + 1) for 5) AS "Time4",
substring("derived"."vitalsigns"."D2Time5.value" from (position('T' in "derived"."vitalsigns"."D2Time5.value") + 1) for 5) AS "Time5",
(select 'N/A' ) AS "Time6",
"derived"."vitalsigns"."HypothPres.label" AS "Hypothermia",
"derived"."vitalsigns"."Temp1.value" AS "Temperature 1",
"derived"."vitalsigns"."TimeTemp1.value" AS "Temperature 1 Time",
"derived"."vitalsigns"."Temp2done.value" AS "Temperature 2 done",
"derived"."vitalsigns"."Temp2.value" AS "Temperature2",
"derived"."vitalsigns"."TimeTemp2.value" AS "Temperature2 Time",
case
when "derived"."vitalsigns"."D1Date.value" IS NOT NULL THEN 2
end As "Day"
FROM "derived"."vitalsigns") "source1"

UNION ALL

SELECT "source"."Facility Name" AS "Facility Name", "source"."NeoTreeID" AS "NeoTreeID", "source"."Date" AS "Date", "source"."Length of stay" AS "Length of stay", "source"."Neonatal Sepsis Study" AS "Neonatal Sepsis Study", "source"."Day of the week" AS "Day of the week", "source"."DayOftheWeekCount" AS "DayOftheWeekCount","source"."Public Holiday" AS "Public Holiday", "source"."No of Day Shift Nurses" AS "No of Day Shift Nurses", "source"."No of Night Shift Nurses" AS "No of Night Shift Nurses", "source"."No of Countious Shift Nurses" AS "No of Countious Shift Nurses", "source"."No of patients" AS "No of patients", "source"."No of students" AS "No of students", "source"."Location" AS "Location", "source"."Frequency Monitoring" AS "Frequency Monitoring", "source"."Time1" AS "Time1", "source"."Time2" AS "Time2", "source"."Time3" AS "Time3", "source"."Time4" AS "Time4", "source"."Time5" AS "Time5", "source"."Time6" AS "Time6", "source"."Hypothermia" AS "Hypothermia", "source"."Temperature 1" AS "Temperature 1", "source"."Temperature 1 Time" AS "Temperature 1 Time", "source"."Temperature 2 done" AS "Temperature 2 done", "source"."Temperature2" AS "Temperature2", "source"."Temperature2 Time" AS "Temperature2 Time", "source"."Day" AS "Day"
FROM (SELECT "derived"."vitalsigns"."facility" AS "Facility Name",
"derived"."vitalsigns"."uid" AS "NeoTreeID",
DATE("derived"."vitalsigns"."D1Date.value") AS "Date",
"derived"."vitalsigns"."LengthOfStay.value" AS "Length of stay",
"derived"."vitalsigns"."NSS.label" AS "Neonatal Sepsis Study",
case
when "derived"."vitalsigns"."D1DayOfWk.value" = 'M' Then 'Monday'
when "derived"."vitalsigns"."D1DayOfWk.value" = 'T' Then 'Tuesday'
when "derived"."vitalsigns"."D1DayOfWk.value" = 'W' Then 'Wednesday'
when "derived"."vitalsigns"."D1DayOfWk.value" = 'Th' Then 'Thursday'
when "derived"."vitalsigns"."D1DayOfWk.value" = 'F' Then 'Friday'
when "derived"."vitalsigns"."D1DayOfWk.value" = 'Sat' Then 'Saturday'
when "derived"."vitalsigns"."D1DayOfWk.value" = 'Sun' Then 'Sunday'
end as "Day of the week",
case
when "derived"."vitalsigns"."D1DayOfWk.value" = 'M' Then 1
when "derived"."vitalsigns"."D1DayOfWk.value" = 'T' Then 2
when "derived"."vitalsigns"."D1DayOfWk.value" = 'W' Then 3
when "derived"."vitalsigns"."D1DayOfWk.value" = 'Th' Then 4
when "derived"."vitalsigns"."D1DayOfWk.value" = 'F' Then 5
when "derived"."vitalsigns"."D1DayOfWk.value" = 'Sat' Then 6
when "derived"."vitalsigns"."D1DayOfWk.value" = 'Sun' Then 7
end as "DayOftheWeekCount",
"derived"."vitalsigns"."D1PH.label" AS "Public Holiday",
Case
when "derived"."vitalsigns"."D2DayNurses.value" = 1 Then '1-Nurse'
when "derived"."vitalsigns"."D2DayNurses.value" = 2 Then '2-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 3 Then '3-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 4 Then '4-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 5 Then '5-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 6 Then '6-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 7 Then '7-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 8 Then '8-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 9 Then '9-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" >= 10 Then '>10 Nurses'
end as "No of Day Shift Nurses",
Case
when "derived"."vitalsigns"."D2NightNurses.value" = 1 Then '1-Nurse'
when "derived"."vitalsigns"."D2NightNurses.value" = 2 Then '2-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 3 Then '3-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 4 Then '4-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 5 Then '5-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 6 Then '6-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 7 Then '7-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 8 Then '8-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 9 Then '9-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" >= 10 Then '>=10 Nurses'
end as "No of Night Shift Nurses",
Case
when "derived"."vitalsigns"."D2Nurses40.value" = 1 Then '1-Nurse'
when "derived"."vitalsigns"."D2Nurses40.value" = 2 Then '2-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 3 Then '3-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 4 Then '4-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 5 Then '5-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 6 Then '6-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 7 Then '7-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 8 Then '8-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 9 Then '9-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" >= 10 Then '>=10 Nurses'
end as "No of Countious Shift Nurses",
"derived"."vitalsigns"."D1Patients.value" AS "No of patients",
"derived"."vitalsigns"."D1students.value" AS "No of students",
case
when "derived"."vitalsigns"."D1Location.label" = 'HIgh-Risk' Then 'High-Risk'
when "derived"."vitalsigns"."D1Location.label" = 'High-Risk' Then 'High-Risk'
when "derived"."vitalsigns"."D1Location.label" = 'Isolation' Then 'Isolation'
when "derived"."vitalsigns"."D1Location.label" = 'Location of baby (Day 3)' Then 'Unknown' 
when "derived"."vitalsigns"."D1Location.label" = 'Low-Risk' Then 'Low-Risk'
end as "Location",
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
case
when "derived"."vitalsigns"."D1Date.value" IS NOT NULL THEN 1
end As "Day"
FROM "derived"."vitalsigns") "source"

UNION ALL

SELECT "source3"."Facility Name" AS "Facility Name", "source3"."NeoTreeID" AS "NeoTreeID", "source3"."Date" AS "Date", "source3"."Length of stay" AS "Length of stay", "source3"."Neonatal Sepsis Study" AS "Neonatal Sepsis Study", "source3"."Day of the week" AS "Day of the week", "source3"."DayOftheWeekCount" AS "DayOftheWeekCount", "source3"."Public Holiday" AS "Public Holiday", "source3"."No of Day Shift Nurses" AS "No of Day Shift Nurses", "source3"."No of Night Shift Nurses" AS "No of Night Shift Nurses", "source3"."No of Countious Shift Nurses" AS "No of Countious Shift Nurses", "source3"."No of patients" AS "No of patients", "source3"."No of students" AS "No of students", "source3"."Location" AS "Location", "source3"."Frequency Monitoring" AS "Frequency Monitoring", "source3"."Time1" AS "Time1", "source3"."Time2" AS "Time2", "source3"."Time3" AS "Time3", "source3"."Time4" AS "Time4", "source3"."Time5" AS "Time5", "source3"."Time6" AS "Time6", "source3"."Hypothermia" AS "Hypothermia", "source3"."Temperature 1" AS "Temperature 1", "source3"."Temperature 1 Time" AS "Temperature 1 Time", "source3"."Temperature 2 done" AS "Temperature 2 done", "source3"."Temperature2" AS "Temperature2", "source3"."Temperature2 Time" AS "Temperature2 Time", "source3"."Day" AS "Day"
FROM (SELECT "derived"."vitalsigns"."facility" AS "Facility Name",
"derived"."vitalsigns"."uid" AS "NeoTreeID",
DATE("derived"."vitalsigns"."D2Date.value") AS "Date",
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
end as "Day of the week",
case
when "derived"."vitalsigns"."D3DayOfWk.value" = 'M' Then 1
when "derived"."vitalsigns"."D3DayOfWk.value" = 'T' Then 2
when "derived"."vitalsigns"."D3DayOfWk.value" = 'W' Then 3
when "derived"."vitalsigns"."D3DayOfWk.value" = 'Th' Then 4
when "derived"."vitalsigns"."D3DayOfWk.value" = 'F' Then 5
when "derived"."vitalsigns"."D3DayOfWk.value" = 'Sat' Then 6
when "derived"."vitalsigns"."D3DayOfWk.value" = 'Sun' Then 7
end as "DayOftheWeekCount",
"derived"."vitalsigns"."D3PH.label" AS "Public Holiday",
Case
when "derived"."vitalsigns"."D2DayNurses.value" = 1 Then '1-Nurse'
when "derived"."vitalsigns"."D2DayNurses.value" = 2 Then '2-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 3 Then '3-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 4 Then '4-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 5 Then '5-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 6 Then '6-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 7 Then '7-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 8 Then '8-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" = 9 Then '9-Nurses'
when "derived"."vitalsigns"."D2DayNurses.value" >= 10 Then '>10 Nurses'
end as "No of Day Shift Nurses",
Case
when "derived"."vitalsigns"."D2NightNurses.value" = 1 Then '1-Nurse'
when "derived"."vitalsigns"."D2NightNurses.value" = 2 Then '2-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 3 Then '3-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 4 Then '4-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 5 Then '5-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 6 Then '6-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 7 Then '7-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 8 Then '8-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" = 9 Then '9-Nurses'
when "derived"."vitalsigns"."D2NightNurses.value" >= 10 Then '>=10 Nurses'
end as "No of Night Shift Nurses",
Case
when "derived"."vitalsigns"."D2Nurses40.value" = 1 Then '1-Nurse'
when "derived"."vitalsigns"."D2Nurses40.value" = 2 Then '2-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 3 Then '3-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 4 Then '4-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 5 Then '5-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 6 Then '6-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 7 Then '7-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 8 Then '8-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" = 9 Then '9-Nurses'
when "derived"."vitalsigns"."D2Nurses40.value" >= 10 Then '>=10 Nurses'
end as "No of Countious Shift Nurses",
"derived"."vitalsigns"."D3Patients.value" AS "No of patients",
"derived"."vitalsigns"."D3Students.value" AS "No of students",
case
when "derived"."vitalsigns"."D3Location.label" = 'HIgh-Risk' Then 'High-Risk'
when "derived"."vitalsigns"."D3Location.label" = 'High-Risk' Then 'High-Risk'
when "derived"."vitalsigns"."D3Location.label" = 'Isolation' Then 'Isolation'
when "derived"."vitalsigns"."D3Location.label" = 'Location of baby (Day 3)' Then 'Unknown' 
when "derived"."vitalsigns"."D3Location.label" = 'Low-Risk' Then 'Low-Risk'
end as "Location",
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
when "derived"."vitalsigns"."D1Date.value" IS NOT NULL THEN 3
end As "Day"
FROM "derived"."vitalsigns") "source3";
        '''