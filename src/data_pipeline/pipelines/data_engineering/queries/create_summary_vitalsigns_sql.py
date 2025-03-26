from data_pipeline.pipelines.data_engineering.queries.check_table_exists_sql import table_exists
#Query to create summary_vitalsigns table
def summary_vital_signs_query():
    prefix =f''' DROP TABLE IF EXISTS derived.summary_vitalsigns;;
            CREATE TABLE derived.summary_vitalsigns AS  '''
    where=''
    if (table_exists("derived","summary_vitalsigns")):
        prefix= f''' INSERT INTO derived.summary_vitalsigns (
    "NeoTreeID", "facility", "LengthOfStayInDays", "NeonateSepsisStudy", "Day1Date", "Day1DayOfWeek", "Day1PublicHolidayH", 
    "Day1NoOfNursesDayShift", "Day1NoOfNursesNightShift", "Day1NoOfNursesContinousShift", "Day1NoOfNeonates", "Day1NoOfSstudents", 
    "Day1Location", "D1FrequencyOfMonitoring", "Day1_1stTimeofVitalSigns", "Day1Time1", "Day1_2ndTimeofVitalSigns", "Day1Time2", 
    "Day1_3rdTimeofVitalSigns", "Day1Time3", "Day1_4thTimeofVitalSigns", "Day1Time4", "Day1_5thTimeofVitalSigns", "Day1Time5", 
    "Day3_1stTimeofVitalSigns", "Day3Time1", "Day3_2ndTimeofVitalSigns", "Day3Time2", "Day3_3rdTimeofVitalSigns", "Day3Time3","Day3_4thTimeofVitalSigns", "Day3Time4", 
    "Day3_5thTimeofVitalSigns", "Day3Time5", "Day3Time6", "WasBabyHypothermic", "Temperature1", "TimeOfTemperature1", "WasFollowUpTemperatureDone", 
    "Temperature2", "TimeOfTemperature2"
)  '''
        where = f''' WHERE NOT EXISTS ( SELECT 1  FROM derived.summary_vitalsigns  WHERE "NeoTreeID" = "derived.vitalsigns.uid") '''
       
    return prefix+ f'''
            SELECT "derived"."vitalsigns"."uid" AS "NeoTreeID",
            derived.vitalsigns."facility" AS "facility",
            derived.vitalsigns."LengthOfStay.value" AS "LengthOfStayInDays",
            derived.vitalsigns."NSS.label" AS "NeonateSepsisStudy",
            DATE(derived.vitalsigns."D1Date.value") AS "Day1Date",
            derived.vitalsigns."D1DayOfWk.label" AS "Day1DayOfWeek",
            derived.vitalsigns."D1PH.label" AS "Day1PublicHolidayH",
            derived.vitalsigns."D1DayNurses.value" AS "Day1NoOfNursesDayShift",
            derived.vitalsigns."D1NightNurses.value" AS "Day1NoOfNursesNightShift",
            derived.vitalsigns."D1Nurses40.value" AS "Day1NoOfNursesContinousShift",
            derived.vitalsigns."D1Patients.value" AS "Day1NoOfNeonates",
            derived.vitalsigns."D1students.value" AS "Day1NoOfSstudents",
            derived.vitalsigns."D1Location.label" AS "Day1Location",
            derived.vitalsigns."D1FreqMon.value" AS "D1FrequencyOfMonitoring",
            derived.vitalsigns."D1Time1.value" AS "Day1_1stTimeofVitalSigns",
            substring(derived.vitalsigns."D1Time1.value" from (position('T' in derived.vitalsigns."D1Time1.value") + 1) for 5) AS "Day1Time1",
            derived.vitalsigns."D1Time2.value" AS "Day1_2ndTimeofVitalSigns",
            substring(derived.vitalsigns."D1Time2.value" from (position('T' in "derived"."vitalsigns"."D1Time2.value") + 1) for 5) AS "Day1Time2",
            derived.vitalsigns."D1Time3.value" AS "Day1_3rdTimeofVitalSigns",
            substring(derived.vitalsigns."D1Time3.value" from (position('T' in "derived"."vitalsigns"."D1Time3.value") + 1) for 5) AS "Day1Time3",
            derived.vitalsigns."D1Time4.value" AS "Day1_4thTimeofVitalSigns",
            substring(derived.vitalsigns."D1Time4.value" from (position('T' in "derived"."vitalsigns"."D1Time4.value") + 1) for 5) AS "Day1Time4",
            derived.vitalsigns."D1Time5.value" AS "Day1_5thTimeofVitalSigns",
            substring(derived.vitalsigns."D1Time5.value" from (position('T' in "derived"."vitalsigns"."D1Time5.value") + 1) for 5) AS "Day1Time5",
            derived.vitalsigns."D3Time1.value" AS "Day3_1stTimeofVitalSigns",
            substring("derived"."vitalsigns"."D3Time1.value" from (position('T' in "derived"."vitalsigns"."D3Time1.value") + 1) for 5) AS "Day3Time1",
            derived.vitalsigns."D3Time2.value" AS "Day3_2ndTimeofVitalSigns",
            substring(derived."vitalsigns"."D3Time2.value" from (position('T' in "derived"."vitalsigns"."D3Time2.value") + 1) for 5) AS "Day3Time2",
            derived.vitalsigns."D3Time3.value" AS "Day3_3rdTimeofVitalSigns",
            substring(derived.vitalsigns."D3Time3.value" from (position('T' in "derived"."vitalsigns"."D3Time3.value") + 1) for 5) AS "Day3Time3",
            derived.vitalsigns."D3Time4.value" AS "Day3_4thTimeofVitalSigns",substring("derived"."vitalsigns"."D3Time4.value" from (position('T' in "derived"."vitalsigns"."D3Time4.value") + 1) for 5) AS "Day3Time4",
            "derived"."vitalsigns"."D2Time5.value" AS "Day3_5thTimeofVitalSigns",substring("derived"."vitalsigns"."D3Time5.value" from (position('T' in "derived"."vitalsigns"."D3Time5.value") + 1) for 5) AS "Day3Time5",
            substring(derived.vitalsigns."D3Time6.value" from (position('T' in "derived"."vitalsigns"."D3Time6.value") + 1) for 5) AS "Day3Time6",
            derived.vitalsigns."HypothPres.label" AS "WasBabyHypothermic",
            derived.vitalsigns."Temp1.value" AS "Temperature1",
            derived.vitalsigns."TimeTemp1.value" AS "TimeOfTemperature1",
            derived.vitalsigns."Temp2done.label" AS "WasFollowUpTemperatureDone",
            derived.vitalsigns."Temp2.value" AS "Temperature2",
            derived.vitalsigns."TimeTemp2.value" AS "TimeOfTemperature2"FROM derived.vitalsigns  {where};; '''