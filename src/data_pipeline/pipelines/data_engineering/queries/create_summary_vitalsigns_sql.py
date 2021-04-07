def summary_vital_signs_query():
    return ''' DROP TABLE IF EXISTS derived.summary_vitalsigns;
            CREATE TABLE derived.summary_vitalsigns AS 
            SELECT "derived"."vitalsigns"."uid" AS "NeoTreeID",
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
            derived.vitalsigns."TimeTemp2.value" AS "TimeOfTemperature2"FROM derived.vitalsigns; '''